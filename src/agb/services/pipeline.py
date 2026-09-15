"""The pipeline service. For now it lands the vendor's data: for each session it takes the
scheduler's `data_pull` job, pulls Sharadar's SEP, SF1 and TICKERS into the Parquet lake
(agb.lake), records each partition's date and hash in `agb.lake_snapshot`, and completes the
job, which fulfils the pull's expected event. A pull that fails, or never runs, leaves that
event unfulfilled, so it shows in the overdue view like any other missed work.

Pulling is safe to redo: a partition that lands with the same bytes is left untouched and
recorded nothing new. So an expired lease is reclaimed. A session the vendor has not yet
published is not a failure: the service keeps the job and shortens its lease to RETRY, so the
pull is tried again once the lease runs out, until it lands.

    python -m agb.services.pipeline             run the service
    python -m agb.services.pipeline backfill    land the vendor's history from 1998, then exit
"""

import argparse
import logging
import os
import shutil
from collections.abc import Iterable
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import psycopg
import pyarrow as pa

from agb import jobs, lake, nyse, schedule
from agb.heartbeat import interval_from_env, serve, stop_on_signals
from agb.lake import LakeError, NotYet, Partition, Sharadar
from agb.roles import PIPELINE

log = logging.getLogger(__name__)

# Longer than a daily pull should take.
LEASE = timedelta(hours=1)
# How long to wait before asking the vendor again for a session it has not published.
RETRY = timedelta(minutes=15)

_RECORD = """
    INSERT INTO agb.lake_snapshot
        (snapshot_date, dataset, partition, sha256, row_count, job_id, actor)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (dataset, partition, sha256) DO NOTHING
"""


def record(
    conn: psycopg.Connection, snapshot: date, partitions: Iterable[Partition], job_id: int | None
) -> int:
    """Record each partition's hash under `snapshot`. A hash already recorded for the same
    partition is skipped, so landing the same bytes again records nothing. Returns how many
    rows it added."""
    rows = [(snapshot, p.dataset, p.path, p.sha256, p.rows, job_id, PIPELINE) for p in partitions]
    if not rows:
        return 0
    with conn.transaction(), conn.cursor() as cur:
        cur.executemany(_RECORD, rows)
        return cur.rowcount


class Pipeline:
    """One pipeline cycle, run by `serve`. The URL opener is injectable for tests."""

    def __init__(
        self,
        lake_dir: Path = lake.DEFAULT_DIR,
        api_key: str = "",
        *,
        opener: lake.Opener = lake.urlopen,
        lease: timedelta = LEASE,
        retry: timedelta = RETRY,
    ) -> None:
        self.lake_dir = lake_dir
        self.api_key = api_key
        self.opener = opener
        self.lease = lease
        self.retry = retry

    @classmethod
    def from_env(cls) -> "Pipeline":
        return cls(
            Path(os.environ.get("AGB_LAKE_DIR", lake.DEFAULT_DIR)),
            os.environ.get("SHARADAR_API_KEY", ""),
        )

    def client(self) -> Sharadar:
        return Sharadar(self.api_key, self.opener)

    def __call__(self, conn: psycopg.Connection) -> None:
        claim = jobs.claim(conn, [schedule.DATA_PULL], PIPELINE, self.lease, reclaim_expired=True)
        if claim is None:
            return
        session = date.fromisoformat(claim.period)
        try:
            partitions = lake.pull(self.lake_dir, self.client(), session)
        except NotYet as exc:
            jobs.renew(conn, claim, self.retry)
            log.info("data pull %s: %s; trying again in %s", claim.period, exc, self.retry)
            return
        except (LakeError, OSError, pa.ArrowException) as exc:
            jobs.fail(conn, claim, str(exc))
            log.error("data pull %s failed: %s", claim.period, exc)
            return
        added = record(conn, session, partitions, claim.job_id)
        jobs.complete(conn, claim)
        log.info(
            "data pull %s landed %d partitions, %d of them new or changed",
            claim.period,
            len(partitions),
            added,
        )

    def backfill(
        self, conn: psycopg.Connection, since: date = lake.SINCE, snapshot: date | None = None
    ) -> list[Partition]:
        """Land the vendor's full SEP and SF1 from `since` on, and its tickers table as it
        stands, recording each partition under `snapshot` (today's trading date by default).
        Safe to run again: unchanged partitions are left as they are."""
        snapshot = snapshot or nyse.trading_date(datetime.now(UTC))
        client = self.client()
        work = self.lake_dir / ".backfill"
        landed: list[Partition] = []
        try:
            for dataset in (lake.SEP, lake.SF1):
                shutil.rmtree(work, ignore_errors=True)
                work.mkdir(parents=True)
                log.info("downloading the vendor's %s export", dataset.name)
                archive = client.bulk(dataset, work / f"{dataset.name}.zip")
                partitions = lake.land_bulk(self.lake_dir, dataset, archive, since, work)
                added = record(conn, snapshot, partitions, None)
                log.info(
                    "%s: %d partitions, %d new or changed", dataset.name, len(partitions), added
                )
                landed += partitions
            partitions = lake.land(self.lake_dir, lake.TICKERS, client.tickers(), snapshot)
            record(conn, snapshot, partitions, None)
            landed += partitions
        finally:
            shutil.rmtree(work, ignore_errors=True)
        return landed


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    parser = argparse.ArgumentParser(
        prog="python -m agb.services.pipeline",
        description="Run the pipeline service, or backfill the Parquet lake and exit.",
    )
    commands = parser.add_subparsers(dest="command")
    backfill = commands.add_parser(
        "backfill", help="land the vendor's full SEP and SF1 history and its tickers, then exit"
    )
    backfill.add_argument(
        "--since",
        type=date.fromisoformat,
        default=lake.SINCE,
        help="the first date to keep (default %(default)s)",
    )
    args = parser.parse_args(argv)
    pipeline = Pipeline.from_env()
    if args.command is None:
        serve(PIPELINE, interval_from_env(), stop_on_signals(), pipeline)
        return
    try:
        with psycopg.connect(application_name=PIPELINE, autocommit=True) as conn:
            partitions = pipeline.backfill(conn, args.since)
    except LakeError as exc:
        raise SystemExit(f"backfill failed: {exc}") from None
    log.info("backfill landed %d partitions in %s", len(partitions), pipeline.lake_dir)


if __name__ == "__main__":
    main()
