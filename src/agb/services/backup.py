"""The backup service. Each night it takes the scheduler's `backup` job, dumps the database with
pg_dump into a dated, gzipped file, copies the panel store's new bodies into a mirror beside
the dumps, and prunes old dumps.

The backup is an ordinary scheduled job with an expected event. A dump or a panel copy that
fails, or never runs, leaves the expected event unfulfilled, so it shows in the overdue view
like any other missed work. A failed night never prunes anything.

Dumps are plain SQL, `agb-YYYY-MM-DD.sql.gz`, written as `.partial` and renamed once pg_dump
has exited cleanly and written its trailer. Each is created with mode 0600 whatever the umask,
and each night sets every dump to 0600 and the directory to 0700. `agb-stack restore` loads one
into a scratch database.

The panel store (agb.panel_store) holds the paid model answers, which nothing can pull again.
Its bodies are content-addressed and never change, so the mirror under `<dir>/panel/` is
incremental: a body the mirror lacks is read from the live store, checked against its hash,
written, read back and checked again. A body already in the mirror is left alone, so a body
that later goes bad in the live store never overwrites its good copy, and a body the live
store loses stays in the mirror. A bad live body is skipped and logged; the nightly panel
audit alerts on it. `python -m agb.services.backup restore-panel` copies bodies back.
"""

import argparse
import contextlib
import gzip
import logging
import os
import re
import subprocess
import tempfile
from collections.abc import Iterator, Sequence
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path

import psycopg

from agb import jobs, schedule
from agb.heartbeat import interval_from_env, serve, stop_on_signals
from agb.panel_store import DIR_ENV as PANEL_DIR_ENV
from agb.panel_store import PanelStore, StoreError, decompress
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
# The dumps hold every table, the operator's orders, fills and audit log among them, and the
# mirror holds the panel's prompts and answers, so only their owner may read either.
FILE_MODE = 0o600
DIR_MODE = 0o700
# The panel mirror's place in the backup directory. prune() never looks inside it.
PANEL = "panel"
_BODY = re.compile(r"^([0-9a-f]{2})[0-9a-f]{62}\.zst$")


class DumpFailed(Exception):
    pass


def dump(command: Sequence[str], target: Path, name: str) -> None:
    """Run `command` and gzip its standard output into `target`, synced to disk.

    `name` goes in the gzip header. Raises DumpFailed unless the command exits 0 and its
    output ends with pg_dump's trailer.
    """
    with tempfile.TemporaryFile() as err, _create(target) as raw:
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


def _create(path: Path):
    """Open `path` for writing, truncated, with mode 0600 whatever the umask or its old mode."""
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, FILE_MODE)
    try:
        os.fchmod(fd, FILE_MODE)
    except BaseException:
        os.close(fd)
        raise
    return os.fdopen(fd, "wb")


def tighten(directory: Path) -> None:
    """Set the backup directory to 0700 and every dump in it to 0600: dumps written before the
    service set their mode were left readable by everyone. Best effort, since a file someone
    else owns cannot be changed and is no reason to fail the night."""
    for path in (directory, *(p for p in directory.iterdir() if DUMP_NAME.match(p.name))):
        mode = DIR_MODE if path == directory else FILE_MODE
        try:
            if path.stat().st_mode & 0o777 != mode:
                path.chmod(mode)
        except OSError as exc:
            log.warning("cannot set %s to %o: %s", path, mode, exc)


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


@dataclass
class Copied:
    """What one pass of copy_bodies did, by digest."""

    copied: list[str] = field(default_factory=list)
    bad: list[str] = field(default_factory=list)  # the source's copy does not match its hash


def bodies(root: Path) -> Iterator[str]:
    """The digest of every body file under a panel store's root, in order. Anything else there,
    such as a writer's temporary file, is ignored."""
    for sub in sorted(root.iterdir()):
        if len(sub.name) != 2 or not sub.is_dir():
            continue
        for path in sorted(sub.iterdir()):
            match = _BODY.match(path.name)
            if match and match.group(1) == sub.name:
                yield path.name.removesuffix(".zst")


def _sound(store: PanelStore, digest: str) -> bool:
    try:
        store.get(digest)
    except StoreError:
        return False
    return True


def _copy_body(source: PanelStore, target: PanelStore, digest: str) -> bool:
    """Copy one body if the source's copy is sound, and check the written file before it takes
    the body's name. Returns False, having written nothing, if the source's copy is bad."""
    compressed = source.path(digest).read_bytes()
    try:
        decompress(compressed, digest, source.root)
    except StoreError:
        return False
    path = target.path(digest)
    if not path.parent.is_dir():
        path.parent.mkdir(mode=DIR_MODE)
        _fsync_dir(target.root)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{digest}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as out:
            out.write(compressed)
            out.flush()
            os.fsync(out.fileno())
        try:
            decompress(Path(tmp).read_bytes(), digest, target.root)
        except StoreError as exc:
            raise OSError(f"the copy did not read back: {exc}") from exc
        os.replace(tmp, path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise
    _fsync_dir(path.parent)
    return True


def copy_bodies(source: Path, target: Path, *, replace_bad: bool = False) -> Copied:
    """Copy into `target` every sound body of `source` that `target` lacks.

    A body already in `target` is left as it is, unless `replace_bad` is set and it does not
    match its hash, in which case a sound copy from `source` replaces it. Nothing in `target`
    is ever deleted, and a bad body in `source` is never copied: it is listed in the result.
    An I/O error, such as a body that cannot be read, raises.
    """
    src, dst = PanelStore(source), PanelStore(target)
    target.mkdir(mode=DIR_MODE, exist_ok=True)
    result = Copied()
    for digest in bodies(source):
        if dst.path(digest).exists() and (not replace_bad or _sound(dst, digest)):
            continue
        if _copy_body(src, dst, digest):
            result.copied.append(digest)
        else:
            result.bad.append(digest)
    return result


class Backup:
    """One backup cycle, run by `serve`. The pg_dump command is injectable for tests; the
    connection details reach it through the PG* environment, as they reach the service."""

    def __init__(
        self,
        directory: Path = DEFAULT_DIR,
        keep: int = DEFAULT_KEEP,
        pg_dump: Sequence[str] = ("pg_dump",),
        *,
        panel: Path | None = None,
        lease: timedelta = LEASE,
    ) -> None:
        if keep < 1:
            raise ValueError("keep at least one dump")
        self.directory = directory
        self.keep = keep
        self.command = (*pg_dump, *PG_DUMP_ARGS)
        # The live panel store, read only. None backs up the database alone.
        self.panel = panel
        self.lease = lease

    @classmethod
    def from_env(cls) -> "Backup":
        panel = os.environ.get(PANEL_DIR_ENV)
        return cls(
            Path(os.environ.get("AGB_BACKUP_DIR", DEFAULT_DIR)),
            int(os.environ.get("AGB_BACKUP_KEEP", DEFAULT_KEEP)),
            panel=Path(panel) if panel else None,
        )

    def __call__(self, conn: psycopg.Connection) -> None:
        claim = jobs.claim(conn, [schedule.BACKUP], BACKUP, self.lease, reclaim_expired=True)
        if claim is None:
            return
        # Each part runs whatever became of the other, so a failed dump still copies the
        # night's new panel bodies, and either failure fails the night.
        errors = [e for e in (self._dump(claim.period), self._copy_panel()) if e]
        if errors:
            jobs.fail(conn, claim, "; ".join(errors))
            log.error("backup %s failed: %s", claim.period, "; ".join(errors))
            return
        jobs.complete(conn, claim)
        tighten(self.directory)
        for path in prune(self.directory, self.keep):
            log.info("pruned %s", path.name)

    def _dump(self, period: str) -> str | None:
        """Write the night's dump. Returns what went wrong, or None."""
        name = f"agb-{period}.sql"
        final = self.directory / f"{name}.gz"
        partial = self.directory / f"{name}.gz.partial"
        try:
            dump(self.command, partial, name)
            partial.replace(final)
            _fsync_dir(self.directory)
        except (DumpFailed, OSError) as exc:
            with contextlib.suppress(OSError):
                partial.unlink(missing_ok=True)
            return str(exc)
        log.info("backup %s written to %s (%d bytes)", period, final, final.stat().st_size)
        return None

    def _copy_panel(self) -> str | None:
        """Copy the panel store's new bodies into the mirror. Returns what went wrong, or None."""
        if self.panel is None:
            return None
        try:
            done = copy_bodies(self.panel, self.directory / PANEL)
        except OSError as exc:
            return f"panel store: {exc}"
        for digest in done.bad:
            log.error("panel body %s does not match its hash; not backed up", digest)
        log.info("panel store: %d new bodies backed up", len(done.copied))
        return None


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    parser = argparse.ArgumentParser(
        prog="python -m agb.services.backup",
        description="Run the backup service, or restore panel bodies from the mirror and exit.",
    )
    commands = parser.add_subparsers(dest="command")
    restore = commands.add_parser(
        "restore-panel",
        help="copy into the store every sound body of the mirror that the store lacks or "
        "holds bad, then exit",
    )
    restore.add_argument("mirror", type=Path, help="the backup's panel mirror")
    restore.add_argument("store", type=Path, help="the live panel store")
    args = parser.parse_args(argv)
    if args.command is None:
        serve(BACKUP, interval_from_env(), stop_on_signals(), Backup.from_env())
        return
    if not args.mirror.is_dir():
        raise SystemExit(f"no panel mirror at {args.mirror}")
    done = copy_bodies(args.mirror, args.store, replace_bad=True)
    for digest in done.bad:
        log.error("mirror body %s does not match its hash; not restored", digest)
    log.info("restored %d panel bodies into %s", len(done.copied), args.store)
    if done.bad:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
