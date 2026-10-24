"""Peak memory per container during a run, so the limits in `compose.yaml` are measured.

The server is a Celeron N3450 with about 3.6 GB free, shared with a household stack, so a
limit that is a guess is either too small, and the kernel kills the service mid-rebalance, or
too large, and the stack has no headroom left. This samples `docker stats` while a real run is
going on and reports, per service, the highest memory it was seen holding.

Sampling is the honest part and the weak part: a spike shorter than the interval can fall
between two samples, so the peak here is a lower bound on the true peak. The suggested limit
adds headroom on top of it (`HEADROOM`, with a floor of `FLOOR` and rounded up to `STEP`)
rather than pretending the measurement is exact.

It depends on nothing outside the standard library, so it also runs on the server, which has
Docker and a system Python but no project environment:

    ssh casaos.local 'python3 - --out peaks.md' < src/agb/measure.py

It stops on the first interrupt and writes its table then, so the operator starts it before
the run and interrupts it once the run is over. With `--out` it rewrites that file after every
reading as well, so an hour-long run can be read while it is going on and survives being
killed rather than interrupted.

`scripts/deploy/sizing.sh` drives it against the server, and docs/build/measured-limits.md is
the procedure it belongs to.
"""

import argparse
import re
import signal
import subprocess
import sys
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path

PROJECT = "agb-trader"
INTERVAL = 5.0

MIB = 1024 * 1024
# A limit is the measured peak plus room for the run that happens to be heavier than the one
# measured: half again as much, never less than 64 MiB more, rounded up to a round number.
HEADROOM = 1.5
FLOOR = 64 * MIB
STEP = 32 * MIB

# What a service is allowed to use is per service, not per container, so the table is keyed by
# the compose service label and the container name is kept only to say what was watched.
_PS_FORMAT = '{{.Names}}\t{{.Label "com.docker.compose.service"}}'
_STATS_FORMAT = "{{.Name}}\t{{.MemUsage}}"

_UNITS = {
    "b": 1,
    "kb": 1000,
    "mb": 1000**2,
    "gb": 1000**3,
    "tb": 1000**4,
    "kib": 1024,
    "mib": 1024**2,
    "gib": 1024**3,
    "tib": 1024**4,
}

_SIZE = re.compile(r"^\s*([0-9]*\.?[0-9]+)\s*([a-zA-Z]*)\s*$")

Run = Callable[[Sequence[str]], str]


class MeasureError(Exception):
    """Docker said something this cannot read, or could not be asked at all."""


def run_command(argv: Sequence[str]) -> str:
    """Run `argv` and return its standard output. The injection point for the tests."""
    try:
        done = subprocess.run(list(argv), capture_output=True, text=True, check=True)
    except FileNotFoundError as exc:
        raise MeasureError(f"{argv[0]} is not on the path") from exc
    except subprocess.CalledProcessError as exc:
        raise MeasureError(f"{' '.join(argv)} failed: {exc.stderr.strip()}") from exc
    return done.stdout


def parse_size(text: str) -> int:
    """Bytes from one of docker's sizes: `412.1MiB`, `2GiB`, `0B`.

    Docker prints binary units for memory and decimal units elsewhere, and both appear in the
    same line, so both are read as what they say rather than as each other.
    """
    match = _SIZE.match(text)
    if match is None:
        raise MeasureError(f"cannot read the size {text!r}")
    amount, unit = match.groups()
    factor = _UNITS.get(unit.lower() or "b")
    if factor is None:
        raise MeasureError(f"cannot read the unit of {text!r}")
    return int(float(amount) * factor)


def services(run: Run = run_command, project: str = PROJECT) -> dict[str, str]:
    """Each running container of `project`, mapped to the compose service it belongs to."""
    found: dict[str, str] = {}
    argv = [
        "docker",
        "ps",
        "--filter",
        f"label=com.docker.compose.project={project}",
        "--format",
        _PS_FORMAT,
    ]
    for line in run(argv).splitlines():
        if not line.strip():
            continue
        name, _, service = line.partition("\t")
        found[name.strip()] = service.strip() or name.strip()
    return found


def usage(names: Sequence[str], run: Run = run_command) -> dict[str, tuple[int, int]]:
    """One reading of memory used and memory allowed, in bytes, for each named container."""
    if not names:
        return {}
    argv = ["docker", "stats", "--no-stream", "--format", _STATS_FORMAT, *names]
    readings: dict[str, tuple[int, int]] = {}
    for line in run(argv).splitlines():
        if not line.strip():
            continue
        name, _, mem = line.partition("\t")
        used, _, allowed = mem.partition("/")
        readings[name.strip()] = (parse_size(used), parse_size(allowed))
    return readings


@dataclass
class Peak:
    service: str
    container: str
    peak: int
    limit: int
    samples: int


def suggested(peak: int) -> int:
    """The limit a measured peak argues for: headroom, a floor under it, rounded up."""
    want = max(int(peak * HEADROOM), peak + FLOOR)
    return -(-want // STEP) * STEP


def format_size(size: int) -> str:
    """Bytes as the MiB or GiB a compose limit is written in."""
    if size >= 1024 * MIB and size % (1024 * MIB) == 0:
        return f"{size // (1024 * MIB)}g"
    return f"{-(-size // MIB)}m"


class Peaks:
    """The highest reading seen for each container, and how many readings that is from."""

    def __init__(self) -> None:
        self.seen: dict[str, Peak] = {}

    def update(self, found: dict[str, str], readings: dict[str, tuple[int, int]]) -> None:
        for container, (used, limit) in readings.items():
            service = found.get(container, container)
            previous = self.seen.get(container)
            if previous is None:
                self.seen[container] = Peak(service, container, used, limit, 1)
                continue
            previous.peak = max(previous.peak, used)
            previous.limit = limit
            previous.samples += 1

    def rows(self) -> list[Peak]:
        return sorted(self.seen.values(), key=lambda p: p.service)

    def table(self) -> str:
        """The measurement as the Markdown table the build note records."""
        header = (
            "| service | container | samples | peak | current limit | limit it argues for |\n"
            "| --- | --- | --: | --: | --: | --: |"
        )
        lines = [header]
        for row in self.rows():
            lines.append(
                f"| {row.service} | {row.container} | {row.samples} | "
                f"{format_size(row.peak)} | {format_size(row.limit)} | "
                f"{format_size(suggested(row.peak))} |"
            )
        return "\n".join(lines) + "\n"


class Stop:
    """Interrupt once and the sampling stops after the reading it is taking."""

    def __init__(self) -> None:
        self.asked = False
        for caught in (signal.SIGINT, signal.SIGTERM):
            signal.signal(caught, self._ask)

    def _ask(self, *_: object) -> None:
        self.asked = True


def sample(
    peaks: Peaks, run: Run = run_command, project: str = PROJECT
) -> dict[str, tuple[int, int]]:
    """Take one reading of every running container of `project` into `peaks`."""
    found = services(run, project)
    readings = usage(sorted(found), run)
    peaks.update(found, readings)
    return readings


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="measure",
        description="Sample docker stats until interrupted, then report the peak per service.",
    )
    parser.add_argument("--project", default=PROJECT, help="the compose project to watch")
    parser.add_argument("--interval", type=float, default=INTERVAL, help="seconds between readings")
    parser.add_argument("--out", help="write the table here as well as to standard output")
    parser.add_argument(
        "--samples", type=int, help="stop after this many readings instead of on an interrupt"
    )
    args = parser.parse_args(argv)

    peaks = Peaks()
    stop = Stop()
    taken = 0
    failed = False
    try:
        while not stop.asked and (args.samples is None or taken < args.samples):
            readings = sample(peaks, run_command, args.project)
            taken += 1
            if not readings:
                print(f"measure: nothing running in {args.project}", file=sys.stderr)
            # Written after every reading rather than at the end alone: a sizing run lasts as
            # long as a rebalance, and the operator reads this file while it is going on. It
            # also means a run that is killed rather than interrupted still leaves its peaks.
            if args.out and peaks.seen:
                Path(args.out).write_text(peaks.table())
            if stop.asked or (args.samples is not None and taken >= args.samples):
                break
            time.sleep(args.interval)
    except MeasureError as exc:
        print(f"measure: {exc}", file=sys.stderr)
        failed = True
    if not peaks.seen:
        print("measure: no readings, nothing to report", file=sys.stderr)
        return 1
    table = peaks.table()
    print(table, end="")
    if args.out:
        Path(args.out).write_text(table)
    # Docker stopping answering part way through leaves a measurement, but not the one asked
    # for, so the readings are reported and the exit code still says the run did not finish.
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
