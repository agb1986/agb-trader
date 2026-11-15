"""Liveness. Every long-running service upserts its own `heartbeat` row each cycle.

The row is keyed by the database role the service connects as, so a service can only
ever report itself.

`python -m agb.heartbeat` is each service's Docker healthcheck: it reads the age of the row
of the role it connects as and fails once that is stale, so `docker compose up --wait` only
passes once every service is beating. Given a URL, as the UI's check is, it also requires a
HEAD of that URL to answer 200, addressed to a name the UI answers. It imports nothing but
psycopg and the standard library's HTTP client, since it runs every interval inside containers
with tight memory limits.
"""

import logging
import os
import signal
import sys
import threading
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from http.client import HTTPConnection
from urllib.parse import urlsplit

import psycopg

log = logging.getLogger(__name__)

DEFAULT_INTERVAL_SECONDS = 30.0

# A service whose heartbeat is older than this is stale: ten missed beats at the default
# 30-second interval.
STALE_AFTER = timedelta(minutes=5)

_UPSERT = """
    INSERT INTO agb.heartbeat (service, started_at, beat_at, version)
    VALUES (current_user, %(started_at)s, now(), %(version)s)
    ON CONFLICT (service) DO UPDATE
    SET started_at = excluded.started_at, beat_at = excluded.beat_at, version = excluded.version
"""


_TOUCH = "UPDATE agb.heartbeat SET beat_at = now() WHERE service = current_user"


def beat(conn: psycopg.Connection, started_at: datetime, version: str | None) -> None:
    conn.execute(_UPSERT, {"started_at": started_at, "version": version})


def progress(conn: psycopg.Connection) -> None:
    """A beat from inside a cycle, for work that runs longer than the staleness window. Only
    `beat_at` moves: `started_at` and `version` are `serve`'s to set, and a cycle that has not
    started cannot report progress. The service is still only as alive as the work it gets
    through between beats, which is what the staleness check is there to see.
    """
    conn.execute(_TOUCH)


def interval_from_env() -> float:
    return float(os.environ.get("AGB_HEARTBEAT_SECONDS", DEFAULT_INTERVAL_SECONDS))


def stale_after_from_env() -> timedelta:
    seconds = os.environ.get("AGB_STALE_AFTER_SECONDS")
    return timedelta(seconds=float(seconds)) if seconds else STALE_AFTER


_OWN_AGE = "SELECT now() - beat_at FROM agb.heartbeat WHERE service = current_user"


def own_age(conn: psycopg.Connection) -> timedelta | None:
    """How long ago the role this connection runs as last beat, or None if it never has."""
    row = conn.execute(_OWN_AGE).fetchone()
    return row[0] if row else None


_ALLOWED_HOSTS = "AGB_UI_ALLOWED_HOSTS"


def probe_host_from_env() -> str | None:
    """The `Host` the UI's check sends: the first name in the UI's own AGB_UI_ALLOWED_HOSTS,
    since that list replaces the UI's default names when it is set, or None for the URL's own,
    `127.0.0.1:8080`, which the default names include. The parsing is agb.services.ui's
    (comma-separated, stripped, lower-cased), repeated here because that module imports this
    one and the check must stay small."""
    names = [name.strip().lower() for name in os.environ.get(_ALLOWED_HOSTS, "").split(",")]
    return next((name for name in names if name), None)


def answers(url: str, host: str | None = None) -> str | None:
    """None if a HEAD of `url`, sent with `host` as its Host header, answers 200, otherwise why
    not. The UI's server runs in a thread beside its heartbeat loop, so a beating UI may still
    be one that serves nothing; this asks the server itself. The UI refuses a Host it does not
    know with a 421, so the header must be one of its names; None sends the URL's own."""
    parts = urlsplit(url)
    client = HTTPConnection(parts.hostname, parts.port, timeout=5)
    try:
        client.request("HEAD", parts.path or "/", headers={"Host": host} if host else {})
        status = client.getresponse().status
    except OSError as exc:
        return f"{url}: {exc}"
    finally:
        client.close()
    return None if status == 200 else f"{url} answered {status}"


def healthcheck(stale_after: timedelta, url: str | None = None, host: str | None = None) -> int:
    """0 if the service this container runs beat within `stale_after` and, given a `url`, a
    HEAD of it with `host` as its Host header answers 200; 1 otherwise, with the reason on
    stderr, where `docker inspect` shows it. Connection details come from the service's own PG*
    environment, so the row read is the service's own.
    """
    try:
        with psycopg.connect(application_name="healthcheck", connect_timeout=5) as conn:
            age = own_age(conn)
    except psycopg.Error as exc:
        print(f"healthcheck: {exc}".strip(), file=sys.stderr)
        return 1
    if age is None:
        print("healthcheck: no heartbeat yet", file=sys.stderr)
        return 1
    if age > stale_after:
        print(f"healthcheck: stale, last beat {age} ago", file=sys.stderr)
        return 1
    if url and (failure := answers(url, host)):
        print(f"healthcheck: {failure}", file=sys.stderr)
        return 1
    print(f"last beat {age} ago")
    return 0


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


if __name__ == "__main__":
    sys.exit(healthcheck(stale_after_from_env(), *sys.argv[1:2], host=probe_host_from_env()))
