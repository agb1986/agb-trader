"""Land the sample's SEP export as the backfill does, and die at one point of the third
partition's landing, the way a killed process dies: no `finally`, no cleanup, no unwinding.

    python tests/lake_crash.py POINT LAKE LEDGER [revised]

POINT is where it dies: `write` half way through the Parquet bytes, `record` once they are
synced but before the ledger has them, `rename` once the ledger has them but before they are
renamed into place, `sync` once they are renamed but before the directory is synced. LEDGER is
a file the record callback appends "<partition> <sha256>" lines to, standing in for
agb.lake_snapshot. With `revised`, the vendor has since corrected AAPL's volume in every
session, so each partition the lake already holds is rewritten.

It exits CRASHED when it died where it was told to, and 0 if it never got there.
"""

import io
import os
import sys
from datetime import date
from pathlib import Path

from sharadar_fake import sample

from agb import lake

CRASHED = 86
AT = 3  # the partition it dies on


def vendor(revised: bool):
    vendor = sample()
    if revised:
        for row in vendor.rows["stocks"]:
            if row["ticker"] == "AAPL":
                row["volume"] = "1250001"
    return vendor


class Ledger:
    def __init__(self, path: Path, die_before: bool = False, die_after: bool = False) -> None:
        self.path = path
        self.calls = 0
        self.die_before = die_before
        self.die_after = die_after

    def __call__(self, partition: lake.Partition) -> None:
        self.calls += 1
        if self.die_before and self.calls == AT:
            os._exit(CRASHED)
        with open(self.path, "a") as f:
            f.write(f"{partition.path} {partition.sha256}\n")
        if self.die_after and self.calls == AT:
            os._exit(CRASHED)


def main(point: str, root: Path, ledger: Path, revised: bool) -> None:
    record = Ledger(ledger, die_before=point == "record", die_after=point == "rename")
    if point == "write":
        real = lake.pq.write_table
        writes = 0

        def half_written(table, out, **kwargs):
            nonlocal writes
            writes += 1
            if writes < AT:
                return real(table, out, **kwargs)
            whole = io.BytesIO()
            real(table, whole, **kwargs)
            out.write(whole.getvalue()[: len(whole.getvalue()) // 2])
            out.flush()
            os._exit(CRASHED)

        lake.pq.write_table = half_written
    if point == "sync":
        real_sync = lake._fsync_dir
        syncs = 0

        def renamed_then_dead(directory):
            nonlocal syncs
            syncs += 1
            if syncs == AT:
                os._exit(CRASHED)
            real_sync(directory)

        lake._fsync_dir = renamed_then_dead
    work = root.parent / "work"
    work.mkdir(exist_ok=True)
    client = lake.Sharadar("k3y-not-a-real-one", vendor(revised), sleep=lambda _: None)
    archive = client.bulk(lake.SEP, work / "sep.zip")
    lake.land_bulk(root, lake.SEP, archive, date(1998, 1, 1), work, record)


if __name__ == "__main__":
    point, root, ledger, *rest = sys.argv[1:]
    main(point, Path(root), Path(ledger), rest == ["revised"])
    sys.exit(0)
