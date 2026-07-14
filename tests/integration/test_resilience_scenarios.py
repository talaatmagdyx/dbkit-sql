"""Resilience / chaos suite (§12, §15, §32.3). Marked ``integration``.

Failures are induced the way rabbitkit's SRE suite does — real infrastructure faults, not
mocks: killing PostgreSQL backends (``pg_terminate_backend``), restarting the container
(``docker restart``), failing over to a genuinely different backend behind a proxy, racing a
kill against an in-flight commit, and driving many concurrent operations. Assertions center on:
recovery, correct error classification, no connection leaks, and bounded resources.

Scenarios that only need SQL (backend termination, pool bounds, cancellation, shutdown) run
against ``DBKIT_TEST_DSN`` too. Scenarios that must restart/replace the server require Docker;
they self-skip when it's unavailable.
"""

from __future__ import annotations

import asyncio
import contextlib
import random
import subprocess
import time
from collections.abc import AsyncIterator, Iterator

import pytest

from dbkit import AsyncDatabase, DatabaseTarget, Query, sql
from dbkit.errors import DatabaseError

from ..conftest import RecordingMetrics

pytestmark = pytest.mark.integration

TARGET = DatabaseTarget(database="app", role="write")

INSERT = Query(
    name="chaos.insert",
    statement=sql("INSERT INTO dbkit_chaos (id, v) VALUES (:id, :v) ON CONFLICT (id) DO NOTHING"),
    operation="write",
    idempotent=True,
)


def _skip_no_docker() -> None:
    try:
        import docker
    except ImportError:
        pytest.skip("docker SDK not installed")
    try:
        docker.from_env().ping()
    except Exception:
        pytest.skip("Docker daemon not reachable")


async def _make_db(dsn: str, **pool: object) -> AsyncDatabase:
    db = AsyncDatabase.from_config(
        {
            "databases": {"app": {"primary": {"url": dsn}}},
            "defaults": {
                "pool": {"pre_ping": True, "timeout_seconds": 2.0, **pool},
                "observability": {"metrics": False, "slow_query_ms": 1e9},
            },
        }
    )
    await db.start()
    return db


@pytest.fixture
async def chaos_db(base_config: dict) -> AsyncIterator[AsyncDatabase]:
    db = AsyncDatabase.from_config(base_config)
    await db.start()
    await db.execute(
        sql("CREATE TABLE IF NOT EXISTS dbkit_chaos (id bigint PRIMARY KEY, v int)"), target=TARGET
    )
    await db.execute(sql("TRUNCATE dbkit_chaos"), target=TARGET)
    try:
        yield db
    finally:
        await db.close()


# --- SQL-fault scenarios (run against DBKIT_TEST_DSN) ------------------------------- #


async def test_backend_termination_mid_transaction_is_classified(chaos_db: AsyncDatabase) -> None:
    """Killing the backend during a transaction surfaces a classified error, not a raw driver
    exception, and the connection is invalidated (§13, §12.3)."""
    admin = chaos_db  # a second logical connection from the same pool works fine for killing

    with pytest.raises(DatabaseError):
        async with chaos_db.transaction(target=TARGET) as tx:
            await tx.execute(INSERT, {"id": 1, "v": 1})
            # figure out this transaction's backend pid and kill it from another connection
            pid = await tx.fetch_value(sql("SELECT pg_backend_pid()"))
            await admin.execute(
                sql("SELECT pg_terminate_backend(:pid)"), {"pid": pid}, target=TARGET
            )
            # next statement on the dead connection must raise a classified DatabaseError
            await tx.execute(INSERT, {"id": 2, "v": 2})

    # the pool recovers: a fresh operation succeeds
    assert await chaos_db.fetch_value(sql("SELECT 1"), target=TARGET) == 1


async def test_connection_count_stays_bounded_under_concurrency(base_config: dict) -> None:
    """Many concurrent operations through a small pool must not open unbounded connections
    (leak guard, mirrors rabbitkit's bounded-channel test)."""
    db = await _make_db(base_config["databases"]["app"]["primary"]["url"], size=3, max_overflow=2)
    try:
        await db.execute(sql("SELECT 1"), target=TARGET)  # warm

        async def op(i: int) -> int:
            # CAST is required for asyncpg: a bare literal with no column context leaves the
            # parameter untyped, and asyncpg (unlike psycopg) refuses to bind an int against an
            # inferred text type — a SQLAlchemy+asyncpg limitation, not a dbkit one.
            return await db.fetch_value(sql("SELECT CAST(:n AS integer)"), {"n": i}, target=TARGET)

        results = await asyncio.gather(*[op(i) for i in range(200)])
        assert sorted(results) == list(range(200))

        snap = db.pool_status()[0]
        # created connections may never exceed pool capacity (size + overflow)
        assert snap.created <= snap.total_capacity
        assert snap.checked_out == 0  # everything returned
    finally:
        await db.close()


async def test_client_side_timeout_actually_cancels_the_server_side_statement(
    base_config: dict,
) -> None:
    """A client-side ``timeout=`` on an async call must not just abandon the connection while
    the backend keeps executing — the driver must send a real PostgreSQL cancel request, or a
    client timeout under saturation (a query stuck behind a lock) would let the abandoned query
    keep consuming a backend process/locks after the caller already reports failure, worsening
    exactly the saturation the timeout exists to bound (performance review §3/§13).

    Verified empirically via ``pg_stat_activity``, not assumed: both psycopg3
    (``AsyncConnection.wait()``) and asyncpg (``Connection._cancel_current_command``) send a
    real cancel request when an ``asyncio.CancelledError`` interrupts an in-flight query wait —
    which is exactly what dbkit's ``asyncio.timeout()``-based client deadline triggers. This test
    locks that behavior in as a permanent regression check using a raw psycopg admin connection
    to hold a row lock and inspect server state, independent of which driver dbkit itself is
    configured to use for the timed-out call.
    """
    from urllib.parse import urlsplit

    import psycopg

    dsn = base_config["databases"]["app"]["primary"]["url"]
    parsed = urlsplit(dsn)
    raw_dsn = f"{parsed.scheme.split('+', 1)[0]}://{parsed.netloc}{parsed.path}"

    holder = await psycopg.AsyncConnection.connect(raw_dsn, autocommit=False)
    admin = await psycopg.AsyncConnection.connect(raw_dsn, autocommit=True)
    try:
        async with holder.cursor() as cur:
            await cur.execute(
                "CREATE TABLE IF NOT EXISTS dbkit_timeout_probe (id int PRIMARY KEY, v int)"
            )
            await cur.execute(
                "INSERT INTO dbkit_timeout_probe (id, v) VALUES (1, 1) ON CONFLICT (id) DO NOTHING"
            )
            await holder.commit()
            await cur.execute("BEGIN")
            await cur.execute("SELECT v FROM dbkit_timeout_probe WHERE id = 1 FOR UPDATE")

        db = await _make_db(dsn)
        try:
            with pytest.raises(DatabaseError):
                await db.execute(
                    sql("UPDATE dbkit_timeout_probe SET v = 2 WHERE id = 1"),
                    target=TARGET,
                    timeout=0.5,
                )
        finally:
            await db.close()

        # The abandoned UPDATE's backend must be gone (cancelled), not still waiting on the
        # lock — a lingering blocked backend here would mean the driver only abandoned the
        # client-side wait without telling the server to stop.
        async with admin.cursor() as cur:
            await cur.execute(
                "SELECT count(*) FROM pg_stat_activity "
                "WHERE query ILIKE '%dbkit_timeout_probe%' AND state != 'idle in transaction' "
                "AND pid != pg_backend_pid()"
            )
            (still_running,) = await cur.fetchone()
        assert still_running == 0, (
            "the timed-out UPDATE's backend is still active/waiting after the client gave up — "
            "the driver abandoned the client-side wait without cancelling server-side"
        )
    finally:
        await holder.rollback()
        await holder.close()
        await admin.close()


async def test_cancellation_storm_leaves_no_checked_out_connections(
    chaos_db: AsyncDatabase,
) -> None:
    """Cancelling many in-flight ops must return every connection to the pool (§12.3)."""
    tasks = [
        asyncio.create_task(
            chaos_db.fetch_value(sql("SELECT pg_sleep(5)"), target=TARGET, timeout=10)
        )
        for _ in range(10)
    ]
    await asyncio.sleep(0.3)
    for t in tasks:
        t.cancel()
    for t in tasks:
        with contextlib.suppress(asyncio.CancelledError):
            await t
    await asyncio.sleep(0.3)  # allow the pool to reclaim
    assert chaos_db.pool_status()[0].checked_out == 0
    # pool is still usable afterwards
    assert await chaos_db.fetch_value(sql("SELECT 1"), target=TARGET) == 1


async def test_graceful_shutdown_while_operations_in_flight(base_config: dict) -> None:
    """close() during active load completes within its grace period and disposes engines."""
    db = await _make_db(base_config["databases"]["app"]["primary"]["url"], size=5, max_overflow=0)
    await db.execute(sql("SELECT 1"), target=TARGET)

    async def worker() -> None:
        with contextlib.suppress(DatabaseError, asyncio.CancelledError):
            for _ in range(50):
                await db.fetch_value(sql("SELECT 1"), target=TARGET)

    workers = [asyncio.create_task(worker()) for _ in range(10)]
    await asyncio.sleep(0.1)
    start = time.monotonic()
    await db.close(grace_period=5.0)
    assert time.monotonic() - start < 5.0
    for w in workers:
        w.cancel()
    await asyncio.gather(*workers, return_exceptions=True)


async def test_commit_unknown_when_backend_dies_during_commit(base_config: dict) -> None:
    """If the connection dies at COMMIT, dbkit reports a distinct commit-unknown outcome and
    never silently reports success (§15). Best-effort: skipped if the kill misses the window."""
    dsn = base_config["databases"]["app"]["primary"]["url"]
    db = await _make_db(dsn, size=2, max_overflow=2)
    admin = await _make_db(dsn, size=1, max_overflow=0)
    from dbkit.errors import DatabaseCommitUnknownError

    try:
        await db.execute(
            sql("CREATE TABLE IF NOT EXISTS dbkit_commit (id int PRIMARY KEY)"), target=TARGET
        )
        outcome = None
        kill_tasks: list[asyncio.Task[None]] = []  # keep strong refs so tasks aren't GC'd
        for attempt in range(20):
            base_id = 1000 + attempt * 10
            try:
                async with db.transaction(target=TARGET) as tx:
                    pid = await tx.fetch_value(sql("SELECT pg_backend_pid()"))
                    await tx.execute(
                        sql("INSERT INTO dbkit_commit (id) VALUES (:id) ON CONFLICT DO NOTHING"),
                        {"id": base_id},
                    )

                    async def _kill(pid: int = pid) -> None:
                        await admin.execute(
                            sql("SELECT pg_terminate_backend(:pid)"),
                            {"pid": pid},
                            target=TARGET,
                        )

                    # race a kill against the imminent commit
                    kill_tasks.append(asyncio.create_task(_kill()))
            except DatabaseCommitUnknownError as exc:
                assert exc.transaction_state_unknown is True
                outcome = "commit_unknown"
                break
            except DatabaseError:
                # kill landed before commit -> normal rollback/connection error; retry
                continue
        if outcome != "commit_unknown":
            pytest.skip("kill never landed inside the commit window in this environment")
    finally:
        for t in kill_tasks:
            t.cancel()
        await asyncio.gather(*kill_tasks, return_exceptions=True)
        await db.close()
        await admin.close()


# --- retry / circuit-breaker scenarios (Phase 2) ------------------------------------ #


async def test_serialization_failure_is_retried_and_succeeds(base_config: dict) -> None:
    """A transient serialization failure (SQLSTATE 40001) on an idempotent read is retried
    transparently and eventually succeeds (§14)."""
    cfg = {
        **base_config,
        "defaults": {
            **base_config["defaults"],
            "retry": {"attempts": 5, "initial_delay_ms": 5, "retry_reads": True},
        },
    }
    db = AsyncDatabase.from_config(cfg)
    await db.start()
    try:
        # A read that raises 40001 on its first two executions, then succeeds. A SEQUENCE is
        # used as the counter because ``nextval`` is NOT rolled back — so it survives the
        # rollback that each failed attempt performs (a table counter would reset every time).
        await db.execute(sql("DROP SEQUENCE IF EXISTS dbkit_flaky_seq"), target=TARGET)
        await db.execute(sql("CREATE SEQUENCE dbkit_flaky_seq"), target=TARGET)
        await db.execute(
            sql(
                """
                CREATE OR REPLACE FUNCTION dbkit_flaky_read() RETURNS int
                LANGUAGE plpgsql AS $$
                DECLARE cur int;
                BEGIN
                    cur := nextval('dbkit_flaky_seq');
                    IF cur < 3 THEN
                        RAISE EXCEPTION 'transient' USING ERRCODE = '40001';
                    END IF;
                    RETURN cur;
                END;
                $$;
                """
            ),
            target=TARGET,
        )
        flaky = Query(
            name="chaos.flaky_read",
            statement=sql("SELECT dbkit_flaky_read()"),
            operation="read",
            idempotent=True,
        )
        value = await db.fetch_value(flaky, target=TARGET)
        assert value == 3  # succeeded on the third attempt
    finally:
        await db.close()


async def test_circuit_opens_under_sustained_failure(base_config: dict) -> None:
    """After enough infrastructure failures the breaker opens and fails fast with
    DatabaseCircuitOpenError instead of hammering a downed backend (§16)."""
    from dbkit.errors import DatabaseCircuitOpenError, DatabaseConnectionError

    # point at a dead port so every connect fails with a connection error
    dead_dsn = "postgresql+psycopg://nobody@127.0.0.1:1/none"
    cfg = {
        "databases": {"app": {"primary": {"url": dead_dsn}}},
        "defaults": {
            "query_timeout_seconds": 2.0,
            "pool": {"connect_timeout_seconds": 1, "pre_ping": True},
            "retry": {"attempts": 1, "retry_reads": True},
            "circuit_breaker": {
                "enabled": True,
                "failure_threshold": 3,
                "window_seconds": 30,
                "open_seconds": 30,
            },
            "observability": {"metrics": False},
        },
    }
    db = AsyncDatabase.from_config(cfg)
    # do not require_ready (it would fail); just drive operations
    db._started = True  # skip engine warmup
    try:
        conn_failures = 0
        circuit_open = False
        for _ in range(8):
            try:
                await db.fetch_value(sql("SELECT 1"), target=TARGET)
            except DatabaseCircuitOpenError:
                circuit_open = True
                break
            except DatabaseConnectionError:
                conn_failures += 1
        assert conn_failures >= 3, "expected several connection failures before opening"
        assert circuit_open, "breaker should have opened and fast-failed"
    finally:
        await db.close()


# --- deadlock storm / retry storm scenarios (task #91, real concurrent load) ------- #


async def test_deadlock_storm_is_classified_and_recovers_via_manual_retry(
    base_config: dict, recording_metrics: RecordingMetrics
) -> None:
    """Many concurrent explicit transactions racing on two rows in opposite lock order reliably
    trigger real PostgreSQL deadlocks (SQLSTATE 40P01) — not simulated. dbkit does NOT auto-retry
    an explicit ``transaction()`` block (silently re-running an arbitrary user transaction body
    would be unsafe in general — see ``should_retry``'s write-idempotence gate), so callers catch
    and retry themselves; this test verifies that contract holds under a genuine storm: every
    deadlock surfaces as a classified, ``retryable`` ``DatabaseDeadlockError``, the losing side's
    connection returns to the pool clean (one rollback per loss, no leak), and a manual retry
    loop converges to the correct final state (§14, §15)."""
    from dbkit.errors import DatabaseDeadlockError

    cfg = {
        **base_config,
        "defaults": {
            **base_config["defaults"],
            "query_timeout_seconds": 15.0,
            "transaction_timeout_seconds": 15.0,
            "pool": {"size": 10, "max_overflow": 10},
        },
    }
    db = AsyncDatabase.from_config(cfg, metrics=recording_metrics)
    await db.start()
    try:
        await db.execute(
            sql("CREATE TABLE IF NOT EXISTS dbkit_deadlock (id int PRIMARY KEY, v int)"),
            target=TARGET,
        )
        await db.execute(sql("TRUNCATE dbkit_deadlock"), target=TARGET)
        await db.execute(
            sql("INSERT INTO dbkit_deadlock (id, v) VALUES (1, 0), (2, 0)"), target=TARGET
        )

        deadlocks_seen: list[int] = []

        async def _bump(first: int, second: int) -> None:
            for attempt in range(50):
                try:
                    async with db.transaction(target=TARGET) as tx:
                        await tx.execute(
                            sql("UPDATE dbkit_deadlock SET v = v + 1 WHERE id = :id"),
                            {"id": first},
                        )
                        await asyncio.sleep(0.05)  # widen the window for a real lock conflict
                        await tx.execute(
                            sql("UPDATE dbkit_deadlock SET v = v + 1 WHERE id = :id"),
                            {"id": second},
                        )
                    return
                except DatabaseDeadlockError as exc:
                    assert exc.retryable is True
                    deadlocks_seen.append(1)
                    # Jittered, growing backoff desynchronizes retries -- without it, the same
                    # losing transaction can retry in lockstep against a fresh wave of other
                    # losers and keep colliding, exhausting its budget on bad luck under system
                    # load rather than genuine starvation.
                    base = min(0.02 * (attempt + 1), 0.3)
                    await asyncio.sleep(random.uniform(base, base * 2))
                    continue
            pytest.fail("never recovered from repeated deadlocks within the retry budget")

        # Pool capacity comfortably exceeds task count -- no pool-checkout queueing, so any
        # blocking observed is genuine row-lock contention, not connection starvation.
        pairs = 5
        tasks = [asyncio.create_task(_bump(1, 2)) for _ in range(pairs)]
        tasks += [asyncio.create_task(_bump(2, 1)) for _ in range(pairs)]
        await asyncio.gather(*tasks)

        assert deadlocks_seen, "expected at least one genuine deadlock under this concurrent load"

        total = await db.fetch_value(sql("SELECT sum(v) FROM dbkit_deadlock"), target=TARGET)
        assert total == 4 * pairs  # every task bumps both rows by 1, 2*pairs tasks total

        assert recording_metrics.count("db_transaction_rollback_total") >= len(deadlocks_seen)
        assert db.pool_status()[0].checked_out == 0
    finally:
        await db.close()


async def test_retry_storm_against_intermittently_killed_backends_mostly_recovers(
    base_config: dict, recording_metrics: RecordingMetrics
) -> None:
    """Many concurrent idempotent writes, retried transparently, against a backend that is
    repeatedly killing whichever connection is actively running one of them — a genuine retry
    storm sustained over the whole load window, not one staged failure. Verifies the retry
    policy (§14) absorbs sustained real connection failures: the large majority of operations
    still succeed, ``OP_RETRIES`` actually fires (proving retries genuinely landed, not that
    kills never hit), and the pool is left clean once the chaos stops."""
    cfg = {
        **base_config,
        "defaults": {
            **base_config["defaults"],
            "retry": {
                "attempts": 8,
                "initial_delay_ms": 5,
                "max_delay_ms": 50,
                "retry_reads": True,
                "retry_writes": True,
            },
            "pool": {"size": 10, "max_overflow": 10},
        },
    }
    db = AsyncDatabase.from_config(cfg, metrics=recording_metrics)
    admin = await _make_db(base_config["databases"]["app"]["primary"]["url"])
    await db.start()
    try:
        await db.execute(
            sql("CREATE TABLE IF NOT EXISTS dbkit_retry_storm (id int PRIMARY KEY, v int)"),
            target=TARGET,
        )
        await db.execute(sql("TRUNCATE dbkit_retry_storm"), target=TARGET)

        upsert = Query(
            name="chaos.retry_storm.upsert",
            statement=sql(
                "INSERT INTO dbkit_retry_storm (id, v) VALUES (:id, 1) "
                "ON CONFLICT (id) DO UPDATE SET v = dbkit_retry_storm.v + 1"
            ),
            operation="write",
            idempotent=True,
        )

        stop = asyncio.Event()

        async def _chaos() -> None:
            while not stop.is_set():
                try:
                    pid = await admin.fetch_value(
                        sql(
                            "SELECT pid FROM pg_stat_activity "
                            "WHERE query ILIKE '%dbkit_retry_storm%' AND pid != pg_backend_pid() "
                            "LIMIT 1"
                        ),
                        target=TARGET,
                    )
                    if pid:
                        await admin.execute(
                            sql("SELECT pg_terminate_backend(:pid)"),
                            {"pid": pid},
                            target=TARGET,
                        )
                except DatabaseError:
                    pass
                await asyncio.sleep(0.02)

        chaos_task = asyncio.create_task(_chaos())

        async def _op(i: int) -> bool:
            try:
                await db.execute(upsert, {"id": i % 20}, target=TARGET, timeout=5.0)
                return True
            except DatabaseError:
                return False

        try:
            await asyncio.sleep(0.05)  # let chaos start landing before load begins
            results = await asyncio.gather(*[_op(i) for i in range(200)])
        finally:
            stop.set()
            await chaos_task

        succeeded = sum(results)
        assert succeeded / len(results) >= 0.9, (
            f"expected the large majority to survive via retry, got {succeeded}/{len(results)}"
        )
        assert recording_metrics.count("db_operation_retries_total") > 0, (
            "expected genuine retries to have fired during the storm"
        )
        await asyncio.sleep(0.2)  # allow the pool to reclaim any in-flight checkouts
        assert db.pool_status()[0].checked_out == 0
    finally:
        await db.close()
        await admin.close()


# --- container-restart scenario (requires Docker) ----------------------------------- #


async def test_recovers_after_full_database_restart() -> None:
    """A full server restart mid-life is recovered transparently on the next operation
    (§10.6). Uses a dedicated container so the restart is isolated."""
    _skip_no_docker()
    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:16", driver="psycopg") as pg:
        dsn = pg.get_connection_url()
        db = await _make_db(dsn, size=2, max_overflow=2, recycle_seconds=1)
        try:
            assert await db.fetch_value(sql("SELECT 1"), target=TARGET) == 1

            # restart the server (atomic stop+start), then poll for readiness via dbkit
            container = pg.get_wrapped_container()
            container.restart()

            deadline = time.monotonic() + 60
            recovered = False
            last_err: Exception | None = None
            while time.monotonic() < deadline:
                try:
                    if await db.fetch_value(sql("SELECT 1"), target=TARGET) == 1:
                        recovered = True
                        break
                except DatabaseError as exc:  # pre-ping/connect errors during boot are expected
                    last_err = exc
                    await asyncio.sleep(1.0)
            if not recovered:
                # Diagnose *why* on failure instead of leaving only a client-side connection
                # error: was the container itself never running again (a Docker/environment
                # issue distinct from dbkit), or did Postgres inside it fail to come back up
                # (visible in its own logs, e.g. a stale postmaster.pid or crash-recovery loop)?
                container.reload()
                logs_tail = container.logs(tail=60)
                logs_text = (logs_tail[1] if isinstance(logs_tail, tuple) else logs_tail).decode(
                    errors="replace"
                )
                pytest.fail(
                    f"did not recover after restart; last error: {last_err}\n"
                    f"container status: {container.status}\n"
                    f"--- last 60 lines of container logs ---\n{logs_text}"
                )
        finally:
            await db.close()


# --- primary-failover scenario (a genuinely different backend, not a restart) ------- #

# Uses the raw `docker` CLI directly rather than testcontainers: testcontainers' Ryuk reaper
# container bind-mounts the host's docker.sock, which fails under some Docker Desktop / VM
# configurations unrelated to dbkit (the same reason `test_recovers_after_full_database_
# restart` above is environment-sensitive). Managing two throwaway containers with plain
# `docker run`/`stop`/`rm` sidesteps that entirely and is exactly how the rest of this file's
# faults are induced — real infrastructure, not mocks.

_FAILOVER_A_PORT = 15511
_FAILOVER_B_PORT = 15512


def _docker_cli(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["docker", *args], capture_output=True, text=True, timeout=30)


def _skip_no_docker_cli() -> None:
    try:
        result = _docker_cli("ps")
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pytest.skip("docker CLI not available")
    if result.returncode != 0:
        pytest.skip(f"docker daemon not reachable: {result.stderr.strip()}")


@contextlib.contextmanager
def _throwaway_postgres(name: str, host_port: int) -> Iterator[None]:
    _docker_cli("rm", "-f", name)
    started = _docker_cli(
        "run",
        "-d",
        "--name",
        name,
        "-e",
        "POSTGRES_USER=dbkit",
        "-e",
        "POSTGRES_PASSWORD=dbkit",
        "-e",
        "POSTGRES_DB=dbkit",
        "-p",
        f"{host_port}:5432",
        "--tmpfs",
        "/var/lib/postgresql/data",
        "postgres:16",
    )
    if started.returncode != 0:
        pytest.skip(f"could not start container {name!r}: {started.stderr.strip()}")
    try:
        deadline = time.monotonic() + 30
        ready = False
        while time.monotonic() < deadline:
            # pg_isready alone races with PostgreSQL's own brief "the database system is
            # starting up" window right after it starts accepting TCP connections but before
            # it's actually ready to run queries — require a real query to succeed too.
            if (
                _docker_cli("exec", name, "pg_isready", "-U", "dbkit").returncode == 0
                and _docker_cli(
                    "exec", name, "psql", "-U", "dbkit", "-d", "dbkit", "-c", "SELECT 1"
                ).returncode
                == 0
            ):
                ready = True
                break
            time.sleep(0.5)
        if not ready:
            pytest.skip(f"container {name!r} never became ready")
        yield
    finally:
        _docker_cli("rm", "-f", name)


class _FailoverProxy:
    """A minimal local TCP proxy so dbkit's DSN stays constant across a simulated primary
    failover — a real PgBouncer/HAProxy/cloud load balancer plays exactly this role. Each
    accepted client connection is piped to whatever ``(host, port)`` was current *at accept
    time*; :meth:`repoint` only affects connections accepted afterward."""

    def __init__(self, host: str, port: int) -> None:
        self._upstream = (host, port)
        self._server: asyncio.Server | None = None
        self.port = 0

    def repoint(self, host: str, port: int) -> None:
        self._upstream = (host, port)

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        self.port = self._server.sockets[0].getsockname()[1]

    async def stop(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        host, port = self._upstream
        try:
            up_reader, up_writer = await asyncio.open_connection(host, port)
        except OSError:
            writer.close()
            return

        async def pump(src: asyncio.StreamReader, dst: asyncio.StreamWriter) -> None:
            try:
                while True:
                    data = await src.read(65536)
                    if not data:
                        break
                    dst.write(data)
                    await dst.drain()
            except (ConnectionResetError, BrokenPipeError, OSError):
                pass
            finally:
                with contextlib.suppress(Exception):
                    dst.close()

        await asyncio.gather(pump(reader, up_writer), pump(up_reader, writer))


async def test_recovers_after_primary_failover_to_a_different_backend() -> None:
    """A primary *failover* — traffic moves to a genuinely different backend, not just a
    restart of the same instance — is recovered transparently via dbkit's retry/pre-ping,
    exactly like the same-instance-restart scenario above. This is the scenario the review
    flagged as untested: `test_recovers_after_full_database_restart` only proves recovery
    after the *same* container comes back, not after the primary actually changes (§10.6, §16).
    """
    _skip_no_docker_cli()

    with (
        _throwaway_postgres("dbkit-failover-a", _FAILOVER_A_PORT),
        _throwaway_postgres("dbkit-failover-b", _FAILOVER_B_PORT),
    ):
        # Seed each backend with a distinguishing marker, connecting directly (bypassing the
        # proxy) — this lets the test prove traffic actually moved to B, not just that "some
        # connection succeeded" after the failover.
        for port, marker in ((_FAILOVER_A_PORT, "A"), (_FAILOVER_B_PORT, "B")):
            direct_dsn = f"postgresql+psycopg://dbkit:dbkit@127.0.0.1:{port}/dbkit"
            seed_db = await _make_db(direct_dsn, size=1, max_overflow=0)
            await seed_db.execute(
                sql("CREATE TABLE IF NOT EXISTS dbkit_failover_marker (v text)"), target=TARGET
            )
            await seed_db.execute(sql("TRUNCATE dbkit_failover_marker"), target=TARGET)
            await seed_db.execute(
                sql("INSERT INTO dbkit_failover_marker VALUES (:v)"), {"v": marker}, target=TARGET
            )
            await seed_db.close()

        proxy = _FailoverProxy("127.0.0.1", _FAILOVER_A_PORT)
        await proxy.start()
        db = await _make_db(
            f"postgresql+psycopg://dbkit:dbkit@127.0.0.1:{proxy.port}/dbkit",
            size=2,
            max_overflow=2,
        )
        try:
            marker = Query(
                name="chaos.failover_marker",
                statement=sql("SELECT v FROM dbkit_failover_marker"),
                operation="read",
                idempotent=True,
            )
            assert await db.fetch_value(marker, target=TARGET) == "A"

            # Failover: the old primary goes away and new connections must land on the new one.
            proxy.repoint("127.0.0.1", _FAILOVER_B_PORT)
            _docker_cli("stop", "-t", "1", "dbkit-failover-a")

            deadline = time.monotonic() + 60
            recovered_marker: str | None = None
            last_err: Exception | None = None
            while time.monotonic() < deadline:
                try:
                    recovered_marker = await db.fetch_value(marker, target=TARGET)
                    break
                except DatabaseError as exc:
                    last_err = exc
                    await asyncio.sleep(1.0)
            assert recovered_marker == "B", (
                f"did not recover onto the new backend after failover; last error: {last_err}"
            )
        finally:
            await db.close()
            await proxy.stop()
