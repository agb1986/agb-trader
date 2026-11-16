"""The account as execution reads it, for tests that build a basket without running execution.

Execution reads the broker's positions at every close (`agb.execution.snapshot`), and a basket
is only built on a book read at its signal session's close or later (`agb.basket.book`). A test
about something else stands that read in with `read_book`."""

from datetime import date, timedelta

from agb.nyse import NyseCalendar


def read_book(db, as_of: date) -> None:
    """The account read at `as_of`'s close, unless a read of that session or later is already on
    the record: the latest positions are carried forward, as an account nobody traded holds
    them, and an account never read holds nothing. The read is stamped just after that close, as
    execution's would be, so the fills it ingests from there on are the ones that follow it."""
    conn = db.connect("execution")
    [(last_id, last_date)] = conn.execute(
        "SELECT max(id), max(snapshot_date) FROM agb.position_snapshot"
    ).fetchall()
    if last_date is not None and last_date >= as_of:
        return
    held = conn.execute(
        "SELECT symbol, qty FROM agb.position WHERE snapshot_id = %s", (last_id,)
    ).fetchall()
    [(snapshot_id,)] = conn.execute(
        "INSERT INTO agb.position_snapshot (snapshot_date, positions, observed_at, actor)"
        " VALUES (%s, %s, %s, 'execution') RETURNING id",
        (
            as_of,
            len(held),
            NyseCalendar(as_of, as_of + timedelta(days=7)).close_at(as_of) + timedelta(minutes=5),
        ),
    ).fetchall()
    for symbol, qty in held:
        conn.execute(
            "INSERT INTO agb.position (snapshot_id, symbol, qty) VALUES (%s, %s, %s)",
            (snapshot_id, symbol, qty),
        )
