from __future__ import annotations

from dbkit._core.idempotency_lint import looks_unsafe_to_retry, statement_text
from dbkit._core.query import Query, sql


def _write(statement: str, *, idempotent: bool) -> Query:
    return Query(name="q", statement=sql(statement), operation="write", idempotent=idempotent)


def test_statement_text_extracts_raw_sql() -> None:
    q = Query(name="q", statement=sql("SELECT 1"))
    assert statement_text(q) == "SELECT 1"


def test_plain_insert_marked_idempotent_is_flagged() -> None:
    q = _write("INSERT INTO orders (id) VALUES (:id)", idempotent=True)
    assert looks_unsafe_to_retry(q) is True


def test_plain_insert_not_marked_idempotent_is_not_flagged() -> None:
    """Only idempotent=True writes are linted — a non-idempotent write is already correctly
    excluded from retries regardless of its SQL shape."""
    q = _write("INSERT INTO orders (id) VALUES (:id)", idempotent=False)
    assert looks_unsafe_to_retry(q) is False


def test_insert_on_conflict_is_not_flagged() -> None:
    q = _write("INSERT INTO orders (id) VALUES (:id) ON CONFLICT (id) DO NOTHING", idempotent=True)
    assert looks_unsafe_to_retry(q) is False


def test_insert_where_not_exists_is_not_flagged() -> None:
    q = _write(
        "INSERT INTO orders (id) SELECT :id WHERE NOT EXISTS (SELECT 1 FROM orders WHERE id = :id)",
        idempotent=True,
    )
    assert looks_unsafe_to_retry(q) is False


def test_update_is_never_flagged_even_without_a_guard() -> None:
    """UPDATE targeting a specific row is naturally idempotent — no guard needed."""
    q = _write("UPDATE orders SET status = :status WHERE id = :id", idempotent=True)
    assert looks_unsafe_to_retry(q) is False


def test_delete_is_never_flagged() -> None:
    q = _write("DELETE FROM orders WHERE id = :id", idempotent=True)
    assert looks_unsafe_to_retry(q) is False


def test_read_query_is_never_flagged() -> None:
    q = Query(name="q", statement=sql("SELECT 1"), operation="read", idempotent=True)
    assert looks_unsafe_to_retry(q) is False
