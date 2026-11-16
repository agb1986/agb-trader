"""Pulling the lake from the server, read-only.

The workstation never pulls from the vendor: its lake is a copy of the server's, fetched with
rsync over the operator's own SSH configuration before a research run. The transfer only ever
reads from the server. The source must be remote and the destination local, so the command
can never be turned around into a push, and nothing is ever deleted on either side.

`make sync-lake` runs this on the host rather than in the research container, which has no
network and no SSH keys."""

import shlex
import subprocess
from pathlib import Path

SOURCE_VAR = "AGB_LAKE_SOURCE"
# The spill directory `lake.connect` may leave beside the datasets: a scratch area, not data.
EXCLUDED = ("/.query/",)


class SyncError(Exception):
    """A sync that must not run: a missing or malformed source, or a destination that is not a
    local directory."""


def read_env(path: Path) -> dict[str, str]:
    """The KEY=VALUE pairs of an env file, as Compose reads them: one per line, `#` comments and
    blank lines skipped, values taken literally."""
    values: dict[str, str] = {}
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        values[key.strip()] = value.strip()
    return values


def source_from(env_file: Path) -> str:
    """The lake's rsync source named in `env_file`."""
    if not env_file.is_file():
        raise SyncError(f"{env_file} does not exist; `make env` creates it from the example")
    source = read_env(env_file).get(SOURCE_VAR, "")
    if not source:
        raise SyncError(f"{SOURCE_VAR} is not set in {env_file}")
    return source


def is_remote(spec: str) -> bool:
    """Whether rsync would treat `spec` as host:path. rsync's own rule: a colon before any
    slash names a host; `rsync://` URLs and daemon `host::module` syntax count too."""
    if spec.startswith("rsync://"):
        return True
    head = spec.split("/", 1)[0]
    return ":" in head


def command(source: str, lake: Path, *, dry_run: bool = False) -> list[str]:
    """The rsync argv that pulls `source` into `lake`: a remote source, a local destination,
    contents copied as the calling user, nothing deleted."""
    if not is_remote(source):
        raise SyncError(f"the lake source must be remote (host:path), not {source!r}")
    if is_remote(str(lake)):
        raise SyncError(f"the lake destination must be a local directory, not {lake!r}")
    argv = [
        "rsync",
        "--archive",
        "--no-owner",
        "--no-group",
        "--compress",
        "--partial",
        "--human-readable",
        "--info=progress2",
        *(f"--exclude={pattern}" for pattern in EXCLUDED),
    ]
    if dry_run:
        argv.append("--dry-run")
    # A trailing slash on both sides copies the lake's contents into the directory, not the
    # directory into it. `--` ends the options, so an operand is never read as one.
    argv += ["--", source.rstrip("/") + "/", str(lake).rstrip("/") + "/"]
    return argv


def run(env_file: Path, lake: Path, *, dry_run: bool = False, runner=subprocess.run) -> list[str]:
    """Pull the lake named in `env_file` into `lake`, and return the argv that ran."""
    argv = command(source_from(env_file), lake, dry_run=dry_run)
    lake.mkdir(parents=True, exist_ok=True)
    print("+", shlex.join(argv), flush=True)
    runner(argv, check=True)
    return argv
