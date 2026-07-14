"""dbkit CLI — configuration validation, health, and pool diagnostics (§31).

Requires the ``cli`` extra: ``pip install dbkit[cli]``. All output redacts secrets; commands
that touch the network report classified errors cleanly instead of raw tracebacks.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from pathlib import Path
from typing import Any, TypeVar

import typer

from .._async.database import AsyncDatabase
from .._core.config import DbkitConfig
from .._core.errors import DatabaseConfigurationError, DatabaseError
from .._core.idempotency_lint import looks_unsafe_to_retry
from .._core.query import default_registry

app = typer.Typer(add_completion=False, help="dbkit — configuration, health, and pool diagnostics.")

T = TypeVar("T")


def _load_config(config_path: Path) -> DbkitConfig:
    if not config_path.exists():
        typer.echo(f"config file not found: {config_path}", err=True)
        raise typer.Exit(code=1)
    try:
        return DbkitConfig.from_yaml(str(config_path))
    except DatabaseConfigurationError as exc:
        typer.echo(f"configuration error: {exc}", err=True)
        raise typer.Exit(code=1) from exc


def _run(coro: Awaitable[T]) -> T:
    try:
        return asyncio.run(coro)  # type: ignore[arg-type]
    except DatabaseError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from exc


@app.command()
def check(
    config_path: Path = typer.Argument(..., help="Path to a dbkit YAML config file."),
) -> None:
    """Validate configuration, then run a readiness check against every required database."""
    config = _load_config(config_path)
    typer.echo(
        f"configuration OK: {len(config.databases)} database(s), environment={config.environment!r}"
    )

    async def _go() -> bool:
        db = AsyncDatabase.from_config(config)
        await db.start()
        try:
            report = await db.health()
            for t in report.targets:
                status = "OK" if t.healthy else f"FAILED: {t.error}"
                typer.echo(f"  {t.key}: {status}")
            return report.ready
        finally:
            await db.close()

    if not _run(_go()):
        raise typer.Exit(code=1)
    typer.echo("all required databases are ready")


@app.command()
def health(
    config_path: Path = typer.Argument(..., help="Path to a dbkit YAML config file."),
    database: str | None = typer.Option(None, help="Limit output to one named database."),
) -> None:
    """Run a readiness health check and print per-target status."""
    config = _load_config(config_path)

    async def _go() -> bool:
        db = AsyncDatabase.from_config(config)
        await db.start()
        try:
            report = await db.health()
            for t in report.targets:
                if database and not t.key.startswith(f"{database}."):
                    continue
                status = "OK" if t.healthy else f"FAILED: {t.error}"
                typer.echo(f"{t.key}: {status}")
            return report.ready
        finally:
            await db.close()

    if not _run(_go()):
        raise typer.Exit(code=1)


@app.command()
def pools(
    config_path: Path = typer.Argument(..., help="Path to a dbkit YAML config file."),
) -> None:
    """Print current pool status for every configured database (after warming a connection)."""
    config = _load_config(config_path)

    async def _go() -> list[dict[str, Any]]:
        db = AsyncDatabase.from_config(config)
        await db.start(warm=True)
        try:
            return [s.to_dict() for s in db.pool_status()]
        finally:
            await db.close()

    snaps = _run(_go())
    if not snaps:
        typer.echo("no engines created")
        return
    for s in snaps:
        typer.echo(
            f"{s['key']}: size={s['size']} checked_out={s['checked_out']} "
            f"overflow={s['overflow']} utilization={s['utilization']:.0%} "
            f"created={s['created']} closed={s['closed']} invalidations={s['invalidations']}"
        )


@app.command()
def engines(
    config_path: Path = typer.Argument(..., help="Path to a dbkit YAML config file."),
) -> None:
    """List the configured database targets without connecting to any of them."""
    config = _load_config(config_path)
    for name, db in config.databases.items():
        typer.echo(f"{name}.primary  driver={db.primary.driver}  required={db.primary.required}")
        for r in db.replicas:
            typer.echo(f"{name}.replica:{r.name}  driver={r.driver}  weight={r.weight}")


@app.command("config-validate")
def config_validate(
    config_path: Path = typer.Argument(..., help="Path to a dbkit YAML config file."),
) -> None:
    """Validate a config file and print a secret-redacted summary (no network access)."""
    config = _load_config(config_path)
    typer.echo(f"environment: {config.environment}")
    typer.echo(f"databases: {list(config.databases)}")
    redacted = config.redacted()
    for name, db in redacted.databases.items():
        typer.echo(f"  {name}.primary: {db.primary.url}")
        for r in db.replicas:
            typer.echo(f"  {name}.replica:{r.name}: {r.url}")
    typer.echo("configuration is valid")


@app.command("connection-budget")
def connection_budget(
    config_path: Path = typer.Argument(..., help="Path to a dbkit YAML config file."),
    replicas: int = typer.Option(1, help="Number of application replicas/pods."),
) -> None:
    """Print the projected cluster-wide connection budget (no network access)."""
    config = _load_config(config_path)
    report = config.connection_budget_report(replicas=replicas)
    typer.echo(f"per-process:    {report['per_process']}")
    typer.echo(f"app replicas:   {report['app_replicas']}")
    typer.echo(f"cluster total:  {report['cluster_total']}")
    limit = config.connection_budget.maximum_per_process
    if limit is not None:
        status = "OK" if report["per_process"] <= limit else "EXCEEDS budget"
        typer.echo(f"configured per-process budget: {limit} ({status})")


@app.command("query-list")
def query_list() -> None:
    """List queries registered on the process-global default registry.

    Only queries constructed in this process appear — import your application's query
    modules first if you need them listed. Writes marked ``idempotent=True`` with no visible
    ``ON CONFLICT``-style guard in their SQL text get a best-effort warning — dbkit's retry
    executor trusts ``idempotent=True`` as-is; it does not verify the SQL is actually safe to
    run twice (§14).
    """
    names = default_registry.names()
    if not names:
        typer.echo("no queries registered in the default registry")
        return
    for name in names:
        q = default_registry.get(name)
        assert q is not None
        line = f"{name}  operation={q.operation}  idempotent={q.idempotent}"
        if looks_unsafe_to_retry(q):
            line += (
                "  [WARNING: idempotent=True but no ON CONFLICT/guard detected — "
                "verify this write is actually safe to run twice]"
            )
        typer.echo(line)


def main() -> None:
    """Entry point for the ``dbkit`` console script."""
    app()


if __name__ == "__main__":
    main()
