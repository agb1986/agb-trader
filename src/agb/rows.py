"""Reading a row a statement is certain to return."""

from typing import Any

import psycopg


def one(cursor: psycopg.Cursor[Any]) -> tuple[Any, ...]:
    """The single row of a statement that always returns one (an `INSERT ... RETURNING`, an
    aggregate, a view over a one-row table). A missing row is a broken schema or a broken
    statement, not a case to handle, so it raises instead of returning None."""
    row = cursor.fetchone()
    if row is None:
        raise LookupError("the statement returned no row")
    return row
