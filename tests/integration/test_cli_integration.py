"""CLI integration tests — network commands against a real PostgreSQL."""

from __future__ import annotations

import pytest

typer_testing = pytest.importorskip("typer.testing")

from dbkit.cli.main import app  # noqa: E402

pytestmark = pytest.mark.integration

runner = typer_testing.CliRunner()


@pytest.fixture
def config_file(tmp_path, base_config: dict):
    dsn = base_config["databases"]["app"]["primary"]["url"]
    path = tmp_path / "config.yaml"
    path.write_text(f"databases:\n  app:\n    primary:\n      url: {dsn}\n")
    return path


def test_check_against_real_database(config_file) -> None:
    result = runner.invoke(app, ["check", str(config_file)])
    assert result.exit_code == 0
    assert "app.primary: OK" in result.output
    assert "all required databases are ready" in result.output


def test_health_against_real_database(config_file) -> None:
    result = runner.invoke(app, ["health", str(config_file)])
    assert result.exit_code == 0
    assert "app.primary: OK" in result.output


def test_pools_against_real_database(config_file) -> None:
    result = runner.invoke(app, ["pools", str(config_file)])
    assert result.exit_code == 0
    assert "size=" in result.output
    assert "checked_out=" in result.output


def test_health_against_unreachable_database(tmp_path) -> None:
    path = tmp_path / "config.yaml"
    path.write_text(
        "databases:\n"
        "  app:\n"
        "    primary:\n"
        "      url: postgresql+psycopg://nobody@127.0.0.1:1/none\n"
        "defaults:\n"
        "  query_timeout_seconds: 1\n"
        "  pool: {connect_timeout_seconds: 1, timeout_seconds: 1}\n"
    )
    result = runner.invoke(app, ["health", str(path)])
    assert result.exit_code == 1
    assert "FAILED" in result.output
