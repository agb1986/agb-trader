"""The backup service. Each night it takes the scheduler's `backup` job, dumps the database with
pg_dump into a dated, gzipped file, and prunes old dumps.

The backup is an ordinary scheduled job with an expected event. A dump that fails, or never
runs, leaves the expected event unfulfilled, so it shows in the overdue view like any other
missed work. A failed dump never prunes anything.

Dumps are plain SQL, `agb-YYYY-MM-DD.sql.gz`, written as `.partial` and renamed once pg_dump
has exited cleanly and written its trailer. `agb-stack restore` loads one into a scratch
database.
"""

import contextlib
import gzip
import logging
import os
import re
import subprocess
import tempfile
from collections.abc import Sequence
from datetime import timedelta
from pathlib import Path

import psycopg

from agb import jobs, schedule
from agb.heartbeat import interval_from_env, serve, stop_on_signals
from agb.roles import BACKUP

log = logging.getLogger(__name__)

# Longer than any dump should take; an expired lease is reclaimed, since a dump is safe to redo.
LEASE = timedelta(hours=1)
DEFAULT_DIR = Path("/backups")
DEFAULT_KEEP = 14
# The backup role cannot bypass row-level security, so pg_dump has to read with it on. Every
# policy lets `backup` read every row (migrations/README.md), and tests/test_backup.py checks it.
PG_DUMP_ARGS = ("--no-password", "--enable-row-security")
# pg_dump writes this at the end of a dump it finished. Since 17.6 an \unrestrict line follows.
TRAILER = b"-- PostgreSQL database dump complete"
DUMP_NAME = re.compile(r"^agb-\d{4}-\d{2}-\d{2}\.sql\.gz$")
CHUNK = 1 << 16


class DumpFailed(Exception):
    pass


def dump(command: Sequence[str], target: Path, name: str) -> None:
    """Run `command` and gzip its standard output into `target`, synced to disk.

    `name` goes in the gzip header. Raises DumpFailed unless the command exits 0 and its
    output ends with pg_dump's trailer.
    """
    with tempfile.TemporaryFile() as err, open(target, "wb") as raw:
        with gzip.GzipFile(filename=name, mode="wb", fileobj=raw) as out:
            try:
                proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=err)
            except OSError as exc:
                raise DumpFailed(f"cannot run {command[0]}: {exc}") from exc
            tail = b""
            with proc.stdout:
                while chunk := proc.stdout.read(CHUNK):
                    out.write(chunk)
                    tail = (tail + chunk)[-CHUNK:]
            code = proc.wait()
        raw.flush()
        os.fsync(raw.fileno())
        err.seek(0)
        stderr = err.read().decode(errors="replace").strip()
    if code != 0:
        raise DumpFailed(f"{command[0]} exited {code}: {stderr[-2000:]}")
    if TRAILER not in tail:
        raise DumpFailed(f"{command[0]} exited 0 but the dump has no trailer: {stderr[-2000:]}")


def prune(directory: Path, keep: int) -> list[Path]:
    """Delete all but the newest `keep` dumps, and any partial dump left behind. Returns them."""
    names = sorted(p.name for p in directory.iterdir())
    dumps = [n for n in names if DUMP_NAME.match(n)]
    partials = [n for n in names if n.endswith(".partial") and DUMP_NAME.match(n[:-8])]
    removed = [directory / n for n in dumps[: max(len(dumps) - keep, 0)] + partials]
    for path in removed:
        path.unlink(missing_ok=True)
    return removed


def _fsync_dir(directory: Path) -> None:
    fd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class Backup:
    """One backup cycle, run by `serve`. The pg_dump command is injectable for tests; the
    connection details reach it through the PG* environment, as they reach the service."""

    def __init__(
        self,
        directory: Path = DEFAULT_DIR,
        keep: int = DEFAULT_KEEP,
        pg_dump: Sequence[str] = ("pg_dump",),
        *,
        lease: timedelta = LEASE,
    ) -> None:
        if keep < 1:
            raise ValueError("keep at least one dump")
        self.directory = directory
        self.keep = keep
        self.command = (*pg_dump, *PG_DUMP_ARGS)
        self.lease = lease

    @classmethod
    def from_env(cls) -> "Backup":
        return cls(
            Path(os.environ.get("AGB_BACKUP_DIR", DEFAULT_DIR)),
            int(os.environ.get("AGB_BACKUP_KEEP", DEFAULT_KEEP)),
        )

    def __call__(self, conn: psycopg.Connection) -> None:
        claim = jobs.claim(conn, [schedule.BACKUP], BACKUP, self.lease, reclaim_expired=True)
        if claim is None:
            return
        name = f"agb-{claim.period}.sql"
        final = self.directory / f"{name}.gz"
        partial = self.directory / f"{name}.gz.partial"
        try:
            dump(self.command, partial, name)
            partial.replace(final)
            _fsync_dir(self.directory)
        except (DumpFailed, OSError) as exc:
            with contextlib.suppress(OSError):
                partial.unlink(missing_ok=True)
            jobs.fail(conn, claim, str(exc))
            log.error("backup %s failed: %s", claim.period, exc)
            return
        jobs.complete(conn, claim)
        log.info("backup %s written to %s (%d bytes)", claim.period, final, final.stat().st_size)
        for path in prune(self.directory, self.keep):
            log.info("pruned %s", path.name)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    serve(BACKUP, interval_from_env(), stop_on_signals(), Backup.from_env())


if __name__ == "__main__":
    main()
