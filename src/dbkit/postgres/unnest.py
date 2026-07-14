"""PostgreSQL ``unnest()`` bulk insert/upsert — a mid-tier strategy between ``execute_many``
and ``COPY`` (§19).

``execute_many`` binds one parameter *per column per row*, so a batch is capped by
PostgreSQL's 65535 bind-parameter ceiling divided by column count. ``unnest`` instead binds
one parameter *per column* — each holding the full array of that column's values for the
batch — so batch size is bounded only by memory/payload, not the parameter ceiling. It sends
exactly one statement per batch, like ``execute_many``, but without that per-row multiplication.

This is plain SQL executed through the normal connection — unlike COPY, it needs no raw-driver
escape hatch. psycopg (and most PostgreSQL drivers) adapt a Python list bound to a single
placeholder into a native PostgreSQL array automatically.

Each array parameter is bound with an explicit ``::type[]`` cast (derived from the target
table's column types) — without it, PostgreSQL cannot resolve which ``unnest`` overload
applies when several array-typed parameters are passed with an unknown element type
(``AmbiguousFunction``). This covers ordinary scalar types (int, text, bool, timestamps,
uuid); composite/enum/array-of-array columns may still need the caller to avoid this strategy.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from sqlalchemy import TextClause, text


def unnest_insert_sql(
    table: str,
    columns: Sequence[str],
    column_types: Mapping[str, str],
    *,
    conflict_index_elements: Sequence[str] | None = None,
    update_columns: Sequence[str] | None = None,
) -> TextClause:
    """Build ``INSERT ... SELECT * FROM unnest(:c1::t1[], :c2::t2[], ...)``, optionally with
    ``ON CONFLICT``.

    ``column_types`` maps each column name to its PostgreSQL type (e.g. ``"INTEGER"``,
    ``"TEXT"``) — every column needs an explicit array cast, or PostgreSQL cannot resolve
    which ``unnest`` overload applies when several unknown-typed array parameters are passed
    (``AmbiguousFunction``).

    ``conflict_index_elements=None`` builds a plain insert. Given index elements with
    ``update_columns=None`` builds ``ON CONFLICT (...) DO NOTHING``; with columns given,
    ``ON CONFLICT (...) DO UPDATE SET col = EXCLUDED.col`` for each.
    """
    col_list = ", ".join(f'"{c}"' for c in columns)
    # CAST(:name AS type[]), not the ":name::type[]" shorthand — SQLAlchemy's text() bind-param
    # regex does not recognize a name immediately followed by "::" and silently leaves it
    # unbound, which the driver then rejects as invalid syntax.
    unnest_args = ", ".join(f"CAST(:{c} AS {column_types[c]}[])" for c in columns)
    alias_cols = ", ".join(f'"{c}"' for c in columns)
    sql = (
        f'INSERT INTO "{table}" ({col_list}) '
        f"SELECT * FROM unnest({unnest_args}) AS _dbkit_unnest({alias_cols})"
    )
    if conflict_index_elements is not None:
        idx = ", ".join(f'"{c}"' for c in conflict_index_elements)
        if update_columns is None:
            sql += f" ON CONFLICT ({idx}) DO NOTHING"
        else:
            set_clause = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in update_columns)
            sql += f" ON CONFLICT ({idx}) DO UPDATE SET {set_clause}"
    return text(sql)


def columnar_params(
    rows: Sequence[Mapping[str, Any]], columns: Sequence[str]
) -> dict[str, list[Any]]:
    """Transpose row-dicts into ``{column: [value, value, ...]}`` — one array bind per column."""
    return {c: [row[c] for row in rows] for c in columns}
