"""The sync frontend mirrors the async one exactly — same features, no ``await`` (§8). Run:

DBKIT_DSN=postgresql+psycopg://localhost/postgres python examples/sync_feature_parity.py
"""

from __future__ import annotations

import os

from dbkit import Database, DatabaseTarget, Query, sql
from dbkit.errors import DatabaseUniqueViolationError

DSN = os.environ.get("DBKIT_DSN", "postgresql+psycopg://localhost/postgres")
TARGET = DatabaseTarget(database="app", role="write")

INSERT = Query(
    name="sync_demo.insert",
    statement=sql("INSERT INTO dbkit_sync_demo (id, v) VALUES (:id, :v)"),
    operation="write",
)


def main() -> None:
    db = Database.from_config({"databases": {"app": {"primary": {"url": DSN}}}})
    db.start()
    try:
        db.execute(
            sql("CREATE TABLE IF NOT EXISTS dbkit_sync_demo (id int PRIMARY KEY, v text)"),
            target=TARGET,
        )
        db.execute(sql("TRUNCATE dbkit_sync_demo"), target=TARGET)

        # Transaction + savepoint (sync).
        with db.transaction(target=TARGET) as tx:
            tx.execute(INSERT, {"id": 1, "v": "kept"})
            try:
                with tx.savepoint():
                    tx.execute(INSERT, {"id": 2, "v": "rolled back"})
                    raise RuntimeError("nested failure")
            except RuntimeError:
                pass
            tx.execute(INSERT, {"id": 3, "v": "kept"})
        ids = db.fetch_values(sql("SELECT id FROM dbkit_sync_demo ORDER BY id"), target=TARGET)
        print(f"sync transaction+savepoint: ids={ids} (expect [1, 3])")

        # Error classification (sync).
        try:
            db.execute(INSERT, {"id": 1, "v": "dup"}, target=TARGET)
        except DatabaseUniqueViolationError as e:
            print(f"sync unique violation classified: sqlstate={e.sqlstate}")

        # Streaming (sync) — a regular ``for`` loop, no ``async``.
        total = 0
        with db.stream(sql("SELECT i FROM generate_series(1, 5000) AS i"), target=TARGET) as rows:
            for _row in rows:
                total += 1
        print(f"sync streamed {total} rows, pool checked_out={db.pool_status()[0].checked_out}")

        # COPY (sync).
        db.execute(sql("CREATE TABLE IF NOT EXISTS dbkit_sync_copy (id int)"), target=TARGET)
        db.execute(sql("TRUNCATE dbkit_sync_copy"), target=TARGET)
        result = db.copy_records(
            "dbkit_sync_copy", ["id"], ((i,) for i in range(2000)), target=TARGET
        )
        print(f"sync COPY wrote {result.row_count} rows")

        # Health (sync).
        report = db.health()
        print(f"sync health: live={report.live} ready={report.ready}")
    finally:
        db.close()


if __name__ == "__main__":
    main()
