from __future__ import annotations

from dbkit.postgres.unnest import columnar_params, unnest_insert_sql


def test_unnest_insert_sql_plain() -> None:
    stmt = unnest_insert_sql("users", ["id", "email"], {"id": "INTEGER", "email": "TEXT"})
    text = str(stmt)
    assert 'INSERT INTO "users" ("id", "email")' in text
    assert "SELECT * FROM unnest(" in text
    assert "CAST(:id AS INTEGER[])" in text
    assert "CAST(:email AS TEXT[])" in text
    assert "ON CONFLICT" not in text


def test_unnest_insert_sql_do_nothing() -> None:
    stmt = unnest_insert_sql(
        "users",
        ["id", "email"],
        {"id": "INTEGER", "email": "TEXT"},
        conflict_index_elements=["id"],
    )
    text = str(stmt)
    assert 'ON CONFLICT ("id") DO NOTHING' in text


def test_unnest_insert_sql_do_update() -> None:
    stmt = unnest_insert_sql(
        "users",
        ["id", "email"],
        {"id": "INTEGER", "email": "TEXT"},
        conflict_index_elements=["id"],
        update_columns=["email"],
    )
    text = str(stmt)
    assert 'ON CONFLICT ("id") DO UPDATE SET "email" = EXCLUDED."email"' in text


def test_unnest_insert_sql_bindparams_are_actually_recognized() -> None:
    """Guards against the SQLAlchemy text() gotcha: ':name::type' (the shorthand cast) is
    silently left unparsed as a bindparam, producing an empty params dict. CAST(:name AS
    type[]) must be used instead so ``:name`` is correctly recognized as a bindparam."""
    stmt = unnest_insert_sql("t", ["a", "b"], {"a": "INTEGER", "b": "TEXT"})
    assert set(stmt.compile().params) == {"a", "b"}
    assert "::" not in str(stmt)  # no shorthand cast syntax anywhere


def test_columnar_params_transposes_rows() -> None:
    rows = [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}, {"id": 3, "v": "c"}]
    params = columnar_params(rows, ["id", "v"])
    assert params == {"id": [1, 2, 3], "v": ["a", "b", "c"]}


def test_columnar_params_empty_rows() -> None:
    assert columnar_params([], ["id", "v"]) == {"id": [], "v": []}


def test_columnar_params_column_subset() -> None:
    rows = [{"id": 1, "v": "a", "extra": "x"}]
    params = columnar_params(rows, ["id"])
    assert params == {"id": [1]}
