"""The pipeline service. It takes the scheduler's jobs in turn, one per cycle:

  * `data_pull`: pulls Sharadar's SEP, SF1 and TICKERS for the session into the Parquet lake
    (agb.lake), records each partition's date and hash in `agb.lake_snapshot`, and completes
    the job, which fulfils the pull's expected event. A pull that fails, or never runs, leaves
    that event unfulfilled, so it shows in the overdue view like any other missed work.
  * `rebalance`: proposes the month's basket (agb.basket) from the universe as of the signal
    session, ranked by the promoted certification's model (agb.registry, loaded from
    AGB_MODELS_DIR and checked against its certified digest on every rebalance), blended with
    the LLM panel's safety scores when a panel configuration exists (agb.panel, agb.blend:
    the newest `panel_config`, the EDGAR client from EDGAR_USER_AGENT, the Anthropic model
    from ANTHROPIC_API_KEY and the store under AGB_PANEL_DIR), runs the
    pre-trade gate on it (agb.gate), and completes the job, which fulfils the proposal's
    expected event. The basket is sized on the account's equity as execution last read it
    (`agb.broker_state_now`); with no reading fresh enough the job is kept and tried again,
    and AGB_CAPITAL_USD overrides the account for a run that must be sized by hand. The gate
    halts a basket that fails any check and opens the halt latch; in paper mode it approves one
    that passes, and in live mode (or in paper mode set to ask) leaves it for the human. With
    no model promoted, or the promoted one voided or altered, the job fails and no basket is
    proposed.
  * `panel_audit`: reads a sample of the panel store's bodies back through the store, which
    checks each against the hash it is named by, and records what it found in
    `agb.panel_audit` (agb.panel_audit). The pipeline runs it because it is the only service
    that mounts the store. A missing or corrupt body is not this job's failure: the job
    completes, having done its work, and the bot alerts on what the row says.

Before taking a job it builds any rerun the operator asked for (`agb rerun`, agb.approval):
a copy of the missed basket under its new reference, for the next session, gated like any
other. A rerun has no job; the command is done once its basket has a gate run.

Both are safe to redo, so an expired lease is reclaimed: a partition that lands with the same
bytes is left untouched and recorded nothing new, a reference already proposed is kept, and a
basket already gated keeps its verdict. The same holds for a rerun interrupted between
building and gating.
Data the vendor has not yet published is not a failure: the service keeps the job and shortens
its lease to RETRY, so the work is tried again once the lease runs out, until it lands.

A job that fails on its own data -- a corrupt panel body, a database error, an unreadable
Parquet file, or any of the errors each job names -- fails with the cause named in its
`failed` event (`StoreError: body ... does not match its hash`), and the service raises a
`pipeline_job_failed` alert for it and moves on to the next cycle. A failed job is not taken
again, so the alert is raised once. Only a database connection that has gone away is left to
stop the service, so it restarts with a new one. A job whose lease was lost -- it ran out and
another worker reclaimed it -- is left to that worker: the cycle logs it and ends without
failing the job. A rerun that fails is logged and raises a `pipeline_rerun_failed` alert, once
per rerun; it has no job to fail, so it is tried again each cycle. That includes a gate that
refuses to approve a basket past its cutoff (the `transition_timely` trigger's
`CheckViolation`). A rerun whose basket was built but never gated, and has since been retired
as missed by the approval sweep, is skipped: the sweep has already alerted on it.

    python -m agb.services.pipeline             run the service
    python -m agb.services.pipeline backfill    land the vendor's history from 1998, then exit
"""

import argparse
import logging
import os
import shutil
from collections.abc import Callable, Iterable
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path

import duckdb
import psycopg
import pyarrow as pa

from agb import (
    analyst,
    basket,
    edgar,
    gate,
    heartbeat,
    jobs,
    lake,
    nyse,
    panel,
    panel_audit,
    panel_store,
    registry,
    schedule,
)
from agb.basket import BasketError, NotReady
from agb.gate import GateError
from agb.heartbeat import interval_from_env, serve, stop_on_signals
from agb.lake import LakeError, NotYet, Partition, Sharadar
from agb.panel import PanelError
from agb.panel_store import PanelStore, StoreError
from agb.ranker import Ranker
from agb.registry import RegistryError
from agb.roles import PIPELINE
from agb.universe import UniverseError

log = logging.getLogger(__name__)

# Longer than a daily pull should take.
LEASE = timedelta(hours=1)
# How long to wait before asking the vendor again for a session it has not published.
RETRY = timedelta(minutes=15)

_RERUNS = """
    SELECT command_id, actor, basket_id, reference, session_date FROM agb.rerun_pending
"""

# Raised when a job fails, once per job: a failed job is not taken again.
JOB_FAILED = "pipeline_job_failed"
# Raised when a rerun fails, once per rerun: a rerun has no job to fail, so it is tried again
# each cycle, and the alert says so the first time.
RERUN_FAILED = "pipeline_rerun_failed"

# The state of the basket already built under a rerun's reference, if there is one.
_RERUN_STATE = """
    SELECT s.state
    FROM agb.basket b JOIN agb.basket_state s ON s.id = b.id
    WHERE b.reference = %s
"""

_RAISE = """
    INSERT INTO agb.alert (kind, subject, period, body, raised_by)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (kind, subject, period) DO NOTHING
"""

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


def cause(exc: BaseException) -> str:
    """A failure as the job's `failed` event records it: the error's type, then its message."""
    return f"{type(exc).__name__}: {exc}"


def fail(conn: psycopg.Connection, claim: jobs.Claim, what: str, exc: Exception) -> None:
    """Fail `claim` with `exc` named as its cause and raise an alert for it.

    A database error on a connection that has gone away is re-raised instead: nothing more
    can be written on it, and the service restarts with a new one. A job whose lease was lost
    raises `jobs.LeaseLost`, which the cycle handles. The job is failed before the alert is
    raised, so an alert always names a job that has stopped.
    """
    if isinstance(exc, psycopg.Error) and (conn.closed or conn.broken):
        raise exc
    detail = cause(exc)
    jobs.fail(conn, claim, detail)
    log.error("%s %s failed: %s", what, claim.period, detail)
    body = f"The {what} for {claim.period} failed and will not be tried again: {detail}"
    alert(conn, JOB_FAILED, claim.kind, claim.period, body)


def alert(conn: psycopg.Connection, kind: str, subject: str, period: str, body: str) -> None:
    """Raise an alert, once: one already raised for the same kind, subject and period is kept.
    An alert that cannot be written is logged, unless the connection has gone away, which is
    re-raised so the service restarts."""
    try:
        conn.execute(_RAISE, (kind, subject, period, body, PIPELINE))
    except psycopg.Error as exc:
        if conn.closed or conn.broken:
            raise
        log.error("%s %s %s: could not raise its alert: %s", kind, subject, period, exc)


def sample_from_env() -> int:
    """How many panel bodies the nightly check reads back, from AGB_PANEL_AUDIT_SAMPLE."""
    raw = os.environ.get(panel_audit.SAMPLE_ENV, "")
    if not raw:
        return panel_audit.SAMPLE
    try:
        sample = int(raw)
    except ValueError:
        raise SystemExit(f"{panel_audit.SAMPLE_ENV} is not a number: {raw!r}") from None
    if sample < 1:
        raise SystemExit(f"{panel_audit.SAMPLE_ENV} is not a number of bodies: {raw!r}")
    return sample


def capital_from_env() -> Decimal | None:
    """The capital a basket is sized against, in dollars, from AGB_CAPITAL_USD. None when the
    variable is unset, which is the normal case: the rebalance then sizes on the account's own
    equity, and the variable is only an override for a run that must be sized by hand."""
    raw = os.environ.get(basket.CAPITAL_ENV, "")
    if not raw:
        return None
    try:
        return Decimal(raw)
    except InvalidOperation:
        raise SystemExit(f"{basket.CAPITAL_ENV} is not a number: {raw!r}") from None


class Pipeline:
    """One pipeline cycle, run by `serve`. The URL opener, the capital, the panel audit's
    sample size, the gate's limits and the clock the gate reads are injectable for tests. A
    ranker given here is used instead of the promoted model, for tests only; otherwise each
    rebalance loads the promoted model from `models_dir`. Capital left as None, which is how
    `from_env` leaves it unless AGB_CAPITAL_USD is set, sizes each basket on the account's
    equity as execution last read it."""

    def __init__(
        self,
        lake_dir: Path = lake.DEFAULT_DIR,
        api_key: str = "",
        *,
        opener: lake.Opener = lake.urlopen,
        ranker: Ranker | None = None,
        models_dir: Path = registry.DEFAULT_MODELS_DIR,
        panel_dir: Path = panel_store.DEFAULT_DIR,
        runner: basket.Stage2 | None = None,
        capital: Decimal | None = None,
        lease: timedelta = LEASE,
        retry: timedelta = RETRY,
        sample: int = panel_audit.SAMPLE,
        limits: gate.Limits = gate.LIMITS,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self.lake_dir = lake_dir
        self.api_key = api_key
        self.opener = opener
        self.ranker = ranker
        self.models_dir = models_dir
        self.panel_dir = panel_dir
        self.runner = runner
        self.capital = capital
        self.lease = lease
        self.retry = retry
        self.sample = sample
        self.limits = limits
        self.clock = clock

    @classmethod
    def from_env(cls) -> "Pipeline":
        return cls(
            Path(os.environ.get("AGB_LAKE_DIR", lake.DEFAULT_DIR)),
            os.environ.get("SHARADAR_API_KEY", ""),
            models_dir=Path(os.environ.get(registry.MODELS_ENV, registry.DEFAULT_MODELS_DIR)),
            panel_dir=Path(os.environ.get(panel_store.DIR_ENV, panel_store.DEFAULT_DIR)),
            capital=capital_from_env(),
            sample=sample_from_env(),
        )

    def client(self) -> Sharadar:
        return Sharadar(self.api_key, self.opener)

    def stage2(self, conn: psycopg.Connection) -> basket.Stage2 | None:
        """The panel the month's basket is blended with: the newest `panel_config`, driven by
        the real EDGAR client, model and store. None when no configuration has been made,
        which leaves the rebalance stage-1 only. A configuration made for other prompts, rubric
        or scrubber raises, so the month fails loudly rather than blending with a stale panel."""
        if self.runner is not None:
            return self.runner
        config = panel.newest_config(conn)
        if config is None:
            return None
        panel.check(config)
        return panel.Runner(
            config,
            edgar.Edgar.from_env(),
            analyst.AnthropicModel(),
            PanelStore(self.panel_dir),
            beat=lambda: heartbeat.progress(conn),
        )

    def __call__(self, conn: psycopg.Connection) -> None:
        for row in conn.execute(_RERUNS).fetchall():
            self.rerun(conn, *row)
        claim = jobs.claim(
            conn,
            [schedule.DATA_PULL, schedule.REBALANCE, schedule.PANEL_AUDIT],
            PIPELINE,
            self.lease,
            reclaim_expired=True,
        )
        if claim is None:
            return
        try:
            if claim.kind == schedule.REBALANCE:
                self.rebalance(conn, claim)
            elif claim.kind == schedule.PANEL_AUDIT:
                self.audit(conn, claim)
            else:
                self.pull(conn, claim)
        except jobs.LeaseLost as exc:
            # Another worker reclaimed the job after the lease ran out; the job is its to
            # finish, and nothing here is broken, so the cycle ends quietly.
            log.error(
                "%s %s: lease lost, leaving the job to its holder: %s",
                claim.kind,
                claim.period,
                exc,
            )

    def rebalance(self, conn: psycopg.Connection, claim: jobs.Claim) -> None:
        try:
            ranker = self.ranker or registry.load_promoted(conn, self.models_dir)
            if isinstance(ranker, registry.CertifiedRanker) and ranker.waiver is not None:
                log.warning("rebalance %s ranks under a waiver: %s", claim.period, ranker.waiver)
            proposal = basket.propose(
                conn,
                claim,
                self.lake_dir,
                ranker,
                capital=(
                    self.capital
                    if self.capital is not None
                    else basket.account_capital(conn, self.clock())
                ),
                panel=self.stage2(conn),
            )
            verdict = gate.run(
                conn,
                proposal.basket_id,
                members=proposal.members,
                marks=proposal.marks,
                now=self.clock(),
                limits=self.limits,
            )
        except NotReady as exc:
            jobs.renew(conn, claim, self.retry)
            log.info("rebalance %s: %s; trying again in %s", claim.period, exc, self.retry)
            return
        except (
            BasketError,
            GateError,
            PanelError,
            RegistryError,
            UniverseError,
            LakeError,
            StoreError,
            OSError,
            ValueError,
            duckdb.Error,
            psycopg.Error,
            pa.ArrowException,
        ) as exc:
            fail(conn, claim, "rebalance", exc)
            return
        jobs.complete(conn, claim)
        log.info(
            "rebalance %s proposed basket %d%s: %d names, %d orders%s",
            claim.period,
            proposal.basket_id,
            " (already proposed by an earlier attempt)" if proposal.existing else "",
            proposal.names,
            proposal.orders,
            f", shadow basket {proposal.shadow_id}" if proposal.shadow_id else "",
        )
        if verdict.failed:
            log.error(
                "rebalance %s halted: basket %d failed %d of %d pre-trade checks: %s",
                claim.period,
                verdict.basket_id,
                len(verdict.failed),
                len(verdict.checks),
                "; ".join(f"{c.name} {c.symbol or ''}".strip() for c in verdict.failed),
            )
        else:
            log.info(
                "rebalance %s: basket %d passed %d pre-trade checks in %s mode and is %s",
                claim.period,
                verdict.basket_id,
                len(verdict.checks),
                verdict.mode,
                verdict.state,
            )

    def rerun(
        self,
        conn: psycopg.Connection,
        command_id: int,
        actor: str,
        original: int,
        reference: str,
        session_date: date,
    ) -> None:
        state = conn.execute(_RERUN_STATE, (reference,)).fetchone()
        if state is not None and state[0] != "proposed":
            # Built, never gated, and retired since -- the approval sweep marks such a basket
            # missed at its cutoff and alerts on it. The gate would refuse it every cycle.
            log.debug("rerun %s is already %s; not gating it", reference, state[0])
            return
        try:
            proposal = basket.rerun(
                conn, original, reference, session_date, command_id, self.lake_dir
            )
            verdict = gate.run(
                conn,
                proposal.basket_id,
                members=proposal.members,
                marks=proposal.marks,
                now=self.clock(),
                limits=self.limits,
            )
        except (
            BasketError,
            GateError,
            UniverseError,
            LakeError,
            StoreError,
            OSError,
            ValueError,
            duckdb.Error,
            psycopg.Error,
            pa.ArrowException,
        ) as exc:
            if isinstance(exc, psycopg.Error) and (conn.closed or conn.broken):
                raise
            detail = cause(exc)
            log.error("rerun %s, asked for by %s, failed: %s", reference, actor, detail)
            body = (
                f"The rerun {reference} of basket {original}, asked for by {actor} for"
                f" {session_date}, failed and is tried again each cycle: {detail}"
            )
            alert(conn, RERUN_FAILED, reference, session_date.isoformat(), body)
            return
        log.info(
            "rerun %s of basket %d, asked for by %s, is basket %d for %s: %s, %s",
            reference,
            original,
            actor,
            proposal.basket_id,
            session_date,
            f"passed {len(verdict.checks)} pre-trade checks" if verdict.passed else "halted",
            verdict.state,
        )

    def audit(self, conn: psycopg.Connection, claim: jobs.Claim) -> None:
        """Check a sample of the panel store's bodies and record the result.

        A body that is missing or no longer matches its hash is a finding, not a failure: the
        row is written and the job completes, and the bot raises the alert from the row. The
        job fails only when the check itself could not be run.
        """
        store = PanelStore(self.panel_dir)
        try:
            audit = panel_audit.check(conn, store, self.sample)
            panel_audit.record(conn, claim.period, claim.job_id, audit, PIPELINE)
        except (StoreError, OSError, psycopg.Error) as exc:
            fail(conn, claim, "panel audit", exc)
            return
        jobs.complete(conn, claim)
        if audit.unsound:
            log.error(
                "panel audit %s: %d of %d bodies read back are unsound "
                "(%d missing, %d corrupt), first %s",
                claim.period,
                len(audit.unsound),
                audit.checked,
                len(audit.missing),
                len(audit.corrupt),
                audit.unsound[0],
            )
        else:
            log.info(
                "panel audit %s: %d of %d bodies read back sound",
                claim.period,
                audit.sound,
                audit.bodies,
            )

    def pull(self, conn: psycopg.Connection, claim: jobs.Claim) -> None:
        session = date.fromisoformat(claim.period)
        try:
            partitions = lake.pull(self.lake_dir, self.client(), session)
        except NotYet as exc:
            jobs.renew(conn, claim, self.retry)
            log.info("data pull %s: %s; trying again in %s", claim.period, exc, self.retry)
            return
        except (LakeError, OSError, pa.ArrowException, psycopg.Error) as exc:
            fail(conn, claim, "data pull", exc)
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
