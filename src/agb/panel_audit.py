"""The panel store's integrity check: read a sample of the bodies back and say what is wrong.

The database refers to every panel body by the SHA-256 of its uncompressed bytes, and the
store is named by that hash, so reading a body back through `PanelStore.get` checks it: the
hash is recomputed and compared. This module draws a random sample of the hashes the database
refers to, reads each one, and sorts them into sound, missing (no file) and corrupt (a file
whose content no longer matches its hash). The pipeline runs it nightly and records the
result; the bot turns a missing or corrupt body into an alert.

Sampling is uniform over distinct referenced hashes, not over rows: a cached model call cites
the same request body as the call it was answered from, and a body cited twice should not be
twice as likely to be checked. Each night draws its own sample, so over a month the check
covers the store many times over without ever spending more than minutes on one night.
"""

from dataclasses import dataclass

import psycopg

from agb.panel_store import PanelStore, StoreError

# How many bodies one night reads back. Set from the measured run time on the deploy machine
# (docs/build/measured-limits.md): the check shares one slow disk with the household stack, so
# the sample is sized to finish well inside the nightly job's grace period.
SAMPLE = 200

# Every hash the database refers to, each once. A body cited by several rows is one body here.
_REFERENCED = """
    WITH referenced AS (
        SELECT document_sha256 AS sha256 FROM agb.panel_filing
        UNION
        SELECT text_sha256 FROM agb.panel_filing
        UNION
        SELECT request_sha256 FROM agb.panel_model_call
        UNION
        SELECT response_sha256 FROM agb.panel_model_call
        UNION
        SELECT extract_sha256 FROM agb.panel_extract WHERE extract_sha256 IS NOT NULL
    )
"""

_COUNT = f"{_REFERENCED} SELECT count(*) FROM referenced"

_SAMPLE = f"{_REFERENCED} SELECT sha256 FROM referenced ORDER BY random() LIMIT %s"

_RECORD = """
    INSERT INTO agb.panel_audit (period, job_id, bodies, checked, sound, missing, corrupt, actor)
    VALUES (%(period)s, %(job_id)s, %(bodies)s, %(checked)s, %(sound)s,
            %(missing)s, %(corrupt)s, %(actor)s)
    ON CONFLICT (period) DO NOTHING
    RETURNING id
"""


@dataclass(frozen=True)
class Audit:
    """What one night's check found."""

    bodies: int
    checked: int
    sound: int
    missing: tuple[str, ...]
    corrupt: tuple[str, ...]

    @property
    def unsound(self) -> tuple[str, ...]:
        """The hashes to go and look for, missing ones first."""
        return self.missing + self.corrupt


def check(conn: psycopg.Connection, store: PanelStore, sample: int = SAMPLE) -> Audit:
    """Read a sample of the referenced bodies back through `store` and sort them.

    A hash with no file in the store is missing. A hash whose file is there but does not read
    back as its own content is corrupt, whether the bytes were rewritten, truncated, or
    cannot be decompressed at all.
    """
    bodies = conn.execute(_COUNT).fetchone()[0]
    drawn = [row[0] for row in conn.execute(_SAMPLE, (sample,))]
    sound = 0
    missing: list[str] = []
    corrupt: list[str] = []
    for digest in drawn:
        if not store.has(digest):
            missing.append(digest)
            continue
        try:
            store.get(digest)
        except (StoreError, OSError):
            corrupt.append(digest)
        else:
            sound += 1
    return Audit(bodies, len(drawn), sound, tuple(missing), tuple(corrupt))


def record(
    conn: psycopg.Connection, period: str, job_id: int, audit: Audit, actor: str
) -> int | None:
    """Record one night's result. Returns None if that period was already recorded."""
    row = conn.execute(
        _RECORD,
        {
            "period": period,
            "job_id": job_id,
            "bodies": audit.bodies,
            "checked": audit.checked,
            "sound": audit.sound,
            "missing": list(audit.missing),
            "corrupt": list(audit.corrupt),
            "actor": actor,
        },
    ).fetchone()
    return None if row is None else row[0]
