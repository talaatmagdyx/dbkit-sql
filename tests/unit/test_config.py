from __future__ import annotations

import pytest

from dbkit import DbkitConfig
from dbkit.errors import DatabaseConfigurationError

BASE = {
    "environment": "test",
    "databases": {
        "app": {
            "primary": {"url": "postgresql+psycopg://u:secret@h:5432/app"},
            "replicas": [{"name": "r1", "url": "postgresql+psycopg://u:secret@r:5432/app"}],
        }
    },
}


def test_from_dict_builds_targets() -> None:
    cfg = DbkitConfig.from_dict(BASE)
    assert cfg.environment == "test"
    app = cfg.databases["app"]
    assert app.primary.driver == "psycopg"
    assert app.primary.dialect == "postgresql"
    assert len(app.replicas) == 1
    assert app.replicas[0].name == "r1"


def test_connection_budget_math() -> None:
    cfg = DbkitConfig.from_dict(
        {
            "defaults": {"pool": {"size": 10, "max_overflow": 5}},
            "databases": {"app": {"primary": {"url": "postgresql+psycopg://h/app"}}},
        }
    )
    # one target * (10 + 5)
    assert cfg.max_connections_per_process() == 15
    report = cfg.connection_budget_report(replicas=4)
    assert report == {"per_process": 15, "app_replicas": 4, "cluster_total": 60}


def test_connection_budget_enforced_at_startup() -> None:
    data = {
        "defaults": {"pool": {"size": 50, "max_overflow": 0}},
        "connection_budget": {"maximum_per_process": 20, "enforce_at_startup": True},
        "databases": {"app": {"primary": {"url": "postgresql+psycopg://h/app"}}},
    }
    with pytest.raises(DatabaseConfigurationError, match="connection budget exceeded"):
        DbkitConfig.from_dict(data)


def test_per_database_connection_budget_enforced() -> None:
    """A single database's own budget can fail startup independently of the global one (§10.3)."""
    data = {
        "defaults": {"pool": {"size": 20, "max_overflow": 0}},
        "databases": {
            "small": {
                "primary": {"url": "postgresql+psycopg://h/small"},
                "connection_budget": {"maximum_per_process": 10, "enforce_at_startup": True},
            },
        },
    }
    with pytest.raises(DatabaseConfigurationError, match="database 'small'"):
        DbkitConfig.from_dict(data)


def test_per_database_connection_budget_only_applies_to_its_own_database() -> None:
    data = {
        "defaults": {"pool": {"size": 5, "max_overflow": 0}},
        "databases": {
            "ok": {
                "primary": {"url": "postgresql+psycopg://h/ok"},
                "connection_budget": {"maximum_per_process": 10, "enforce_at_startup": True},
            },
            "unbounded": {"primary": {"url": "postgresql+psycopg://h/unbounded"}},
        },
    }
    cfg = DbkitConfig.from_dict(data)  # does not raise: "ok" is within its own 10-conn budget
    assert cfg.databases["ok"].max_connections(cfg.defaults) == 5


def test_max_engines_and_evict_lru_round_trip() -> None:
    cfg = DbkitConfig.from_dict(
        {
            "max_engines": 50,
            "evict_lru_engines": True,
            "databases": {"app": {"primary": {"url": "postgresql+psycopg://h/app"}}},
        }
    )
    assert cfg.max_engines == 50
    assert cfg.evict_lru_engines is True


def test_max_engines_defaults_to_unbounded() -> None:
    cfg = DbkitConfig.from_dict(BASE)
    assert cfg.max_engines is None
    assert cfg.evict_lru_engines is False


def test_long_transaction_warning_seconds_default() -> None:
    cfg = DbkitConfig.from_dict(BASE)
    assert cfg.defaults.long_transaction_warning_seconds == 5.0


def test_long_transaction_warning_seconds_override() -> None:
    cfg = DbkitConfig.from_dict(
        {
            "defaults": {"long_transaction_warning_seconds": 1.5},
            "databases": {"app": {"primary": {"url": "postgresql+psycopg://h/app"}}},
        }
    )
    assert cfg.defaults.long_transaction_warning_seconds == 1.5


def test_budget_not_enforced_when_disabled() -> None:
    data = {
        "defaults": {"pool": {"size": 50, "max_overflow": 0}},
        "connection_budget": {"maximum_per_process": 20, "enforce_at_startup": False},
        "databases": {"app": {"primary": {"url": "postgresql+psycopg://h/app"}}},
    }
    cfg = DbkitConfig.from_dict(data)  # does not raise
    assert cfg.max_connections_per_process() == 50


def test_env_expansion(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("APP_URL", "postgresql+psycopg://h/app")
    cfg = DbkitConfig.from_dict({"databases": {"app": {"primary": {"url": "${APP_URL}"}}}})
    assert cfg.databases["app"].primary.url == "postgresql+psycopg://h/app"


def test_env_expansion_default() -> None:
    cfg = DbkitConfig.from_dict(
        {"databases": {"app": {"primary": {"url": "${MISSING:-postgresql+psycopg://h/app}"}}}}
    )
    assert cfg.databases["app"].primary.url == "postgresql+psycopg://h/app"


def test_env_expansion_missing_raises() -> None:
    with pytest.raises(DatabaseConfigurationError, match="not set"):
        DbkitConfig.from_dict({"databases": {"app": {"primary": {"url": "${DEFINITELY_MISSING}"}}}})


def test_redacted_hides_passwords() -> None:
    cfg = DbkitConfig.from_dict(BASE)
    red = cfg.redacted()
    assert "secret" not in red.databases["app"].primary.url
    assert "***" in red.databases["app"].primary.url
    assert "secret" not in red.databases["app"].replicas[0].url
    # original is untouched
    assert "secret" in cfg.databases["app"].primary.url


def test_missing_databases_rejected() -> None:
    with pytest.raises(DatabaseConfigurationError):
        DbkitConfig.from_dict({"databases": {}})


def test_missing_primary_rejected() -> None:
    with pytest.raises(DatabaseConfigurationError, match="primary"):
        DbkitConfig.from_dict({"databases": {"app": {}}})


def test_from_yaml(tmp_path) -> None:
    import textwrap

    p = tmp_path / "cfg.yaml"
    p.write_text(
        textwrap.dedent(
            """
            dbkit:
              environment: prod
              databases:
                app:
                  primary:
                    url: postgresql+psycopg://h/app
            """
        )
    )
    cfg = DbkitConfig.from_yaml(str(p))
    assert cfg.environment == "prod"


def test_pgbouncer_compatible_default_off() -> None:
    cfg = DbkitConfig.from_dict(BASE)
    assert cfg.defaults.pool.pgbouncer_compatible is False


def test_pgbouncer_compatible_round_trip() -> None:
    cfg = DbkitConfig.from_dict(
        {
            "defaults": {"pool": {"pgbouncer_compatible": True}},
            "databases": {"app": {"primary": {"url": "postgresql+psycopg://h/app"}}},
        }
    )
    assert cfg.defaults.pool.pgbouncer_compatible is True


def test_insert_strategy_type_accepts_unnest() -> None:
    from dbkit._core.bulk import InsertStrategy

    strategy: InsertStrategy = "unnest"
    assert strategy == "unnest"
