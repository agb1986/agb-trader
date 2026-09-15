"""Liveness. Every long-running service upserts its own `heartbeat` row each cycle.

The row is keyed by the database role the service connects as, so a service can only
ever report itself.
"""

import logging
import os
import signal
import threading
from collections.abc import Callable
from datetime import UTC, datetime

import psycopg

log = logging.getLogger(__name__)

DEFAULT_INTERVAL_SECONDS = 30.0

_UPSERT = """
    INSERT INTO agb.heartbeat (service, started_at, beat_at, version)
    VALUES (current_user, %(started_at)s, now(), %(version)s)
    ON CONFLICT (service) DO UPDATE
    SET started_at = excluded.started_at, beat_at = excluded.beat_at, version = excluded.version
"""


def beat(conn: psycopg.Connection, started_at: datetime, version: str | None) -> None:
    conn.execute(_UPSERT, {"started_at": started_at, "version": version})


def interval_from_env() -> float:
    return float(os.environ.get("AGB_HEARTBEAT_SECONDS", DEFAULT_INTERVAL_SECONDS))


def stop_on_signals() -> threading.Event:
    """Return an event that SIGTERM or SIGINT sets, so `docker stop` ends the loop cleanly."""
    stop = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.set())
    return stop


def serve(
    service: str,
    interval: float,
    stop: threading.Event,
    cycle: Callable[[psycopg.Connection], None] | None = None,
) -> None:
    """Beat, run one cycle of the service's work, wait, and repeat until `stop` is set.

    Connection details come from the standard PG* environment variables, which the
    service's Compose block sets. A database error propagates and ends the process, and
    Compose restarts it.
    """
    started_at = datetime.now(UTC)
    version = os.environ.get("AGB_VERSION")
    with psycopg.connect(application_name=service, autocommit=True) as conn:
        role = conn.execute("SELECT current_user").fetchone()[0]
        if role != service:
            raise RuntimeError(f"{service} must connect as the {service!r} role, not {role!r}")
        log.info("%s started as role %s, heartbeat every %ss", service, role, interval)
        while not stop.is_set():
            beat(conn, started_at, version)
            if cycle is not None:
                cycle(conn)
            stop.wait(interval)
    log.info("%s stopped", service)
