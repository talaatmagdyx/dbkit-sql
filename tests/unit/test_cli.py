from __future__ import annotations

import pytest

typer_testing = pytest.importorskip("typer.testing")

from dbkit.cli.main import app  # noqa: E402

runner = typer_testing.CliRunner()


@pytest.fixture
def config_file(tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text(
        """
        environment: test
        databases:
          app:
            primary:
              url: postgresql+psycopg://user:supersecret@localhost:5432/app
        defaults:
          pool:
            size: 5
            max_overflow: 2
        connection_budget:
          maximum_per_process: 100
        """
    )
    return path


def test_config_validate_redacts_secrets(config_file) -> None:
    result = runner.invoke(app, ["config-validate", str(config_file)])
    assert result.exit_code == 0
    assert "supersecret" not in result.output
    assert "***" in result.output
    assert "configuration is valid" in result.output


def test_connection_budget_report(config_file) -> None:
    result = runner.invoke(app, ["connection-budget", str(config_file), "--replicas", "4"])
    assert result.exit_code == 0
    assert "per-process:    7" in result.output
    assert "cluster total:  28" in result.output
    assert "budget: 100 (OK)" in result.output


def test_connection_budget_exceeded(tmp_path) -> None:
    path = tmp_path / "config.yaml"
    path.write_text(
        """
        databases:
          app:
            primary:
              url: postgresql+psycopg://localhost/app
        defaults:
          pool: {size: 50, max_overflow: 0}
        connection_budget:
          maximum_per_process: 10
        """
    )
    result = runner.invoke(app, ["connection-budget", str(path)])
    assert result.exit_code == 0
    assert "EXCEEDS budget" in result.output


def test_engines_lists_targets_without_connecting(config_file) -> None:
    result = runner.invoke(app, ["engines", str(config_file)])
    assert result.exit_code == 0
    assert "app.primary" in result.output
    assert "driver=psycopg" in result.output


def test_missing_config_file_exits_nonzero(tmp_path) -> None:
    result = runner.invoke(app, ["config-validate", str(tmp_path / "nope.yaml")])
    assert result.exit_code == 1
    assert "not found" in result.output


def test_invalid_config_exits_nonzero(tmp_path) -> None:
    path = tmp_path / "bad.yaml"
    path.write_text("databases: {}")
    result = runner.invoke(app, ["config-validate", str(path)])
    assert result.exit_code == 1
    assert "configuration error" in result.output


def test_query_list_empty_by_default() -> None:
    result = runner.invoke(app, ["query-list"])
    assert result.exit_code == 0
