"""Error classification, redaction, and cardinality enforcement (§13, §29). Run:

DBKIT_DSN=postgresql+psycopg://localhost/postgres python examples/error_handling.py
"""

from __future__ import annotations

import asyncio
import os

from dbkit import AsyncDatabase, DatabaseTarget, Query, sql
from dbkit.errors import (
    DatabaseCheckViolationError,
    DatabaseForeignKeyViolationError,
    DatabaseNotNullViolationError,
    DatabaseResultError,
    DatabaseSyntaxError,
    DatabaseUniqueViolationError,
)

DSN = os.environ.get("DBKIT_DSN", "postgresql+psycopg://localhost/postgres")
TARGET = DatabaseTarget(database="app", role="write")

CREATE_PARENT = sql("CREATE TABLE IF NOT EXISTS dbkit_err_parent (id int PRIMARY KEY)")
CREATE_DEMO = sql(
    """
    CREATE TABLE IF NOT EXISTS dbkit_err_demo (
        id int PRIMARY KEY,
        email text NOT NULL UNIQUE,
        age int CHECK (age >= 0),
        parent_id int REFERENCES dbkit_err_parent(id)
    )
    """
)
INSERT = Query(
    name="err.insert",
    statement=sql(
        "INSERT INTO dbkit_err_demo (id, email, age, parent_id) "
        "VALUES (:id, :email, :age, :parent_id)"
    ),
    operation="write",
)


async def main() -> None:
    db = AsyncDatabase.from_config({"databases": {"app": {"primary": {"url": DSN}}}})
    await db.start()
    try:
        await db.execute(CREATE_PARENT, target=TARGET)
        await db.execute(CREATE_DEMO, target=TARGET)
        await db.execute(sql("TRUNCATE dbkit_err_demo, dbkit_err_parent CASCADE"), target=TARGET)
        await db.execute(sql("INSERT INTO dbkit_err_parent VALUES (1)"), target=TARGET)
        await db.execute(
            INSERT, {"id": 1, "email": "a@x.com", "age": 30, "parent_id": 1}, target=TARGET
        )

        # 1. Unique violation -> SQLSTATE 23505
        try:
            await db.execute(
                INSERT, {"id": 2, "email": "a@x.com", "age": 20, "parent_id": 1}, target=TARGET
            )
        except DatabaseUniqueViolationError as e:
            print(f"unique violation: sqlstate={e.sqlstate} retryable={e.retryable}")

        # 2. Foreign key violation -> SQLSTATE 23503
        try:
            await db.execute(
                INSERT, {"id": 3, "email": "b@x.com", "age": 20, "parent_id": 999}, target=TARGET
            )
        except DatabaseForeignKeyViolationError as e:
            print(f"foreign key violation: sqlstate={e.sqlstate}")

        # 3. Not-null violation -> SQLSTATE 23502
        try:
            await db.execute(
                sql("INSERT INTO dbkit_err_demo (id, age, parent_id) VALUES (4, 20, 1)"),
                target=TARGET,
            )
        except DatabaseNotNullViolationError as e:
            print(f"not-null violation: sqlstate={e.sqlstate}")

        # 4. Check violation -> SQLSTATE 23514
        try:
            await db.execute(
                INSERT, {"id": 5, "email": "c@x.com", "age": -1, "parent_id": 1}, target=TARGET
            )
        except DatabaseCheckViolationError as e:
            print(f"check violation: sqlstate={e.sqlstate}")

        # 5. Syntax error -> classified, never a raw driver exception
        try:
            await db.execute(sql("SELEC 1"), target=TARGET)
        except DatabaseSyntaxError as e:
            print(f"syntax error classified as: {type(e).__name__}")

        # 6. Cardinality: fetch_one demands exactly one row
        try:
            await db.fetch_one(sql("SELECT * FROM dbkit_err_demo WHERE id = 999"), target=TARGET)
        except DatabaseResultError as e:
            print(f"cardinality violation: {e}")

        # 7. Secrets never leak into error messages.
        bad_db = AsyncDatabase.from_config(
            {
                "databases": {
                    "app": {
                        "primary": {"url": "postgresql+psycopg://user:supersecret@127.0.0.1:1/none"}
                    }
                }
            }
        )
        try:
            await bad_db.fetch_value(sql("SELECT 1"), target=TARGET, timeout=1.0)
        except Exception as e:
            msg = str(e)
            print(f"connection error message (secret redacted): {'supersecret' not in msg}")
        finally:
            await bad_db.close()
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
