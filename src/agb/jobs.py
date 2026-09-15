"""Claiming scheduled work. Every poller (pipeline, execution) takes jobs through here.

A claim is a lease: `SELECT ... FOR UPDATE SKIP LOCKED` picks a job no one holds, and a
conditional update sets `claimed_by` and `lease_until`. Two workers can never hold the same
job, and a worker that finds a job locked skips it rather than waiting. The worker renews the
lease while it works and releases it by completing or failing the job.

What happens when a lease expires depends on the kind. A pipeline stage is deterministic and
writes only to the database, so its expired lease is simply reclaimed (`reclaim_expired`).
Execution submission has side effects at the broker, so it is never reclaimed: its expired
lease stays visible in `agb.job_state` until a human reconciles.

Every state change appends a `job_event` with an actor, in the same transaction as the lease
change it records.
"""

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta

import psycopg


class LeaseLost(Exception):
    """The worker no longer holds the job's lease: it expired, or someone reclaimed it."""


@dataclass(frozen=True)
class Claim:
    job_id: int
    kind: str
    period: str
    worker: str
    lease_until: datetime
    reclaimed: bool


_CANDIDATE = """
    SELECT j.id, j.kind, j.period, j.claimed_by, j.lease_until
    FROM agb.job j
    WHERE j.kind = ANY(%(kinds)s)
      AND j.not_before <= now()
      AND (j.claimed_by IS NULL OR (%(reclaim)s AND j.lease_until < now()))
      AND NOT (j.id = ANY(%(passed)s))
      AND NOT EXISTS (
          SELECT 1 FROM agb.job_event e
          WHERE e.job_id = j.id AND e.event IN ('completed', 'failed'))
    ORDER BY j.not_before, j.id
    LIMIT 1
    FOR UPDATE OF j SKIP LOCKED
"""

_ENDED = """
    SELECT EXISTS (
        SELECT 1 FROM agb.job_event
        WHERE job_id = %s AND event IN ('completed', 'failed'))
"""

_EVENT = """
    INSERT INTO agb.job_event (job_id, event, actor, lease_until, detail)
    VALUES (%(job_id)s, %(event)s, %(actor)s, %(lease_until)s, %(detail)s)
    RETURNING id
"""

# Whoever sees an expired lease first records it, once per lease.
_EXPIRY = """
    INSERT INTO agb.job_event (job_id, event, actor, lease_until)
    VALUES (%s, 'lease_expired', %s, %s)
    ON CONFLICT (job_id, lease_until) WHERE event = 'lease_expired' DO NOTHING
"""


def _event(
    conn: psycopg.Connection,
    job_id: int,
    event: str,
    actor: str,
    lease_until: datetime | None = None,
    detail: str | None = None,
) -> int:
    params = {
        "job_id": job_id,
        "event": event,
        "actor": actor,
        "lease_until": lease_until,
        "detail": detail,
    }
    return conn.execute(_EVENT, params).fetchone()[0]


def claim(
    conn: psycopg.Connection,
    kinds: Sequence[str],
    worker: str,
    lease: timedelta,
    *,
    reclaim_expired: bool = False,
) -> Claim | None:
    """Claim the earliest due job of one of `kinds`, or return None if there is none free.

    With `reclaim_expired`, a job whose lease has expired is free as well; the claim records
    the expiry and the reclaim. Only kinds that are safe to run twice may pass it.
    """
    passed: list[int] = []
    while True:
        with conn.transaction():
            row = conn.execute(
                _CANDIDATE, {"kinds": list(kinds), "reclaim": reclaim_expired, "passed": passed}
            ).fetchone()
            if row is None:
                return None
            job_id, kind, period, previous_holder, previous_lease = row
            # The candidate query's snapshot predates the lock, so a job that ended just
            # before we locked it can slip through. Check again now that we hold the row.
            if conn.execute(_ENDED, (job_id,)).fetchone()[0]:
                passed.append(job_id)
                continue
            (lease_until,) = conn.execute(
                "UPDATE agb.job SET claimed_by = %s, lease_until = now() + %s "
                "WHERE id = %s RETURNING lease_until",
                (worker, lease, job_id),
            ).fetchone()
            reclaimed = previous_holder is not None
            if reclaimed:
                conn.execute(_EXPIRY, (job_id, worker, previous_lease))
            _event(conn, job_id, "reclaimed" if reclaimed else "claimed", worker, lease_until)
            return Claim(job_id, kind, period, worker, lease_until, reclaimed)


def renew(conn: psycopg.Connection, claim: Claim, lease: timedelta) -> Claim:
    """Extend a lease the worker still holds. An expired lease cannot be renewed."""
    row = conn.execute(
        "UPDATE agb.job SET lease_until = now() + %s "
        "WHERE id = %s AND claimed_by = %s AND lease_until >= now() RETURNING lease_until",
        (lease, claim.job_id, claim.worker),
    ).fetchone()
    if row is None:
        raise LeaseLost(f"job {claim.job_id}: {claim.worker} no longer holds the lease")
    return Claim(claim.job_id, claim.kind, claim.period, claim.worker, row[0], claim.reclaimed)


def _release(conn: psycopg.Connection, claim: Claim) -> None:
    released = conn.execute(
        "UPDATE agb.job SET claimed_by = NULL, lease_until = NULL "
        "WHERE id = %s AND claimed_by = %s",
        (claim.job_id, claim.worker),
    )
    if released.rowcount != 1:
        raise LeaseLost(f"job {claim.job_id}: {claim.worker} no longer holds the lease")


def complete(conn: psycopg.Connection, claim: Claim) -> int:
    """Complete a claimed job and fulfil the expected events waiting on it.

    Returns the id of the `completed` job_event, which each fulfilled expected event
    references.
    """
    with conn.transaction():
        _release(conn, claim)
        event_id = _event(conn, claim.job_id, "completed", claim.worker)
        conn.execute(
            """
            INSERT INTO agb.expected_event_completion (expected_event_id, ref_table, ref_id, actor)
            SELECT id, 'job_event', %s, %s FROM agb.expected_event WHERE job_id = %s
            ON CONFLICT (expected_event_id) DO NOTHING
            """,
            (event_id, claim.worker, claim.job_id),
        )
        return event_id


def fail(conn: psycopg.Connection, claim: Claim, detail: str) -> int:
    """Fail a claimed job. It stays failed; running the work again is a new period."""
    with conn.transaction():
        _release(conn, claim)
        return _event(conn, claim.job_id, "failed", claim.worker, detail=detail)


def fulfil(
    conn: psycopg.Connection, kind: str, period: str, ref_table: str, ref_id: int, actor: str
) -> bool:
    """Record that the row `ref_table.ref_id` fulfilled an expected event.

    For expected events no job completes, such as a submission. Returns False if the event
    was already fulfilled, or was never expected.
    """
    done = conn.execute(
        """
        INSERT INTO agb.expected_event_completion (expected_event_id, ref_table, ref_id, actor)
        SELECT id, %s, %s, %s FROM agb.expected_event WHERE kind = %s AND period = %s
        ON CONFLICT (expected_event_id) DO NOTHING
        """,
        (ref_table, ref_id, actor, kind, period),
    )
    return done.rowcount == 1


def record_expiries(conn: psycopg.Connection, actor: str) -> int:
    """Append a `lease_expired` event for every expired lease not yet recorded.

    It only records: it neither reclaims a job nor clears its lease. Returns how many it
    recorded.
    """
    recorded = conn.execute(
        """
        INSERT INTO agb.job_event (job_id, event, actor, lease_until)
        SELECT id, 'lease_expired', %s, lease_until FROM agb.job WHERE lease_until < now()
        ON CONFLICT (job_id, lease_until) WHERE event = 'lease_expired' DO NOTHING
        """,
        (actor,),
    )
    return recorded.rowcount
