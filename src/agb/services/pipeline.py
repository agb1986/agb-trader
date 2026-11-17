"""The pipeline service. It takes the scheduler's jobs in turn, one per cycle:

  * `data_pull`: pulls Sharadar's SEP, SF1 and TICKERS for the session into the Parquet lake
    (agb.lake), records each partition's date and hash in `agb.lake_snapshot` before its file
    is renamed into place, and completes the job, which fulfils the pull's expected event. A
    pull that fails, or never runs, leaves that event unfulfilled, so it shows in the overdue
    view like any other missed work.
  * `rebalance`: proposes the month's basket (agb.basket) from the universe as of the signal
    session, ranked by the promoted certification's model (agb.registry, loaded from
    AGB_MODELS_DIR and checked against its certified digest on every rebalance), blended with
    the LLM panel's safety scores when a panel configuration exists (agb.panel, agb.blend:
    the newest `panel_config`, the EDGAR client from EDGAR_USER_AGENT, the Anthropic model
    from ANTHROPIC_API_KEY and the store under AGB_PANEL_DIR), runs the
    pre-trade gate on it (agb.gate), and completes the job, which fulfils the proposal's
    expected event. The basket is sized on the account's equity as execution last read it
    (`agb.broker_state_now`) in the trading mode in force, and starts from the positions it
    read at the signal session's close; with no reading fresh enough, none in this mode, or
    no position snapshot that recent, the job is kept and tried again. AGB_CAPITAL_USD
    overrides the account for a run that must be sized by hand. The gate
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

The service claims under a name of its own, its role plus its process id and a random suffix
(`jobs.worker_name`), so two workers never share one and the lease is conditional on it: only
the holder can renew, complete, fail or release the job. A rebalance renews its lease between
its steps, and the panel after every retrieval and model call, so a run longer than the lease
keeps it; a renewal that finds the lease gone raises `jobs.LeaseLost` and the work stops.

Both are safe to redo, so an expired lease is reclaimed: a partition that lands with the same
bytes is left untouched and recorded nothing new, a pull or backfill stopped part way leaves
every file it renamed into place recorded (agb.lake lands a partition only after its
record), a reference already proposed is kept, and a basket already gated keeps its verdict.
The same holds for a rerun interrupted between building and gating.

Data the vendor has not yet published is not a failure: the service keeps the job and shortens
its lease to RETRY, so the work is tried again once the lease runs out, until it lands. That
holds for a session the vendor is still publishing, one short of the row floor (agb.lake): it
is deferred quietly, with no alert of its own. The earlier sessions a deferred pull did land
were recorded as they landed. A rebalance waiting for execution to read the account (`NotReady`)
is kept the same way. Neither waits for ever: each job has a deadline past which its work is
of no use -- for a pull, the time the next session's pull is due, which lands its session too;
for a rebalance, its trade session's MOC cutoff -- and a job still waiting then is failed
once, as `expired`, with the alert below. A request the vendor fails is made again by the
lake client, with backoff.

A job that stops on an error is handled by the error's class (`classify`), which is the first
word of its `failed` event:

  * transient -- an operating-system error such as a timeout or a reset socket, a database
    operational error, or a DuckDB I/O error. The job is released (a `released` event naming
    the error) and taken again next cycle, up to TRANSIENT_RETRIES times; the next such
    error fails it.
  * configuration -- a setting the service needs is missing (agb.errors.NotConfigured), such
    as SHARADAR_API_KEY or EDGAR_USER_AGENT. The job is held, with one `pipeline_job_held`
    alert, and tried again every RETRY until its deadline, when it fails as expired.
  * permanent -- everything else: a corrupt panel body, an unreadable Parquet file, a missing
    path, a refused check, or any of the errors each job names. The job fails at once.

A failed job's event names the cause (`permanent: StoreError: body ... does not match its
hash`), and the service raises a `pipeline_job_failed` alert for it and moves on to the next
cycle. A failed job is not taken again, so the alert is raised once. It shows in `agb.overdue`
until someone acknowledges it (`agb acknowledge`) or it is older than
`agb.failed_overdue_age()`. Only a database connection that has gone away is left to stop the
service, so it restarts with a new one. A job whose lease was lost -- it ran out and another
worker reclaimed it -- is left to that worker: the cycle logs it and ends without completing,
failing or releasing the job. A rerun that fails is logged and raises a
`pipeline_rerun_failed` alert, once per rerun; it has no job to fail, so it is tried again each
cycle. That includes a gate that refuses to approve a basket past its cutoff (the
`transition_timely` trigger's `CheckViolation`). A rerun that is `NotReady`, because execution
has not yet read the account at its signal session's close, is not a failure: it is logged and
tried again next cycle, with no alert. A rerun whose basket was built but never gated, and has
since been retired as missed by the approval sweep, is skipped: the sweep has already alerted
on it.

    python -m agb.services.pipeline             run the service
    python -m agb.services.pipeline backfill    land the vendor's history from 1998, then exit
"""

import argparse
import logging
import os
import shutil
import time
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
from agb.errors import NotConfigured
from agb.gate import GateError
from agb.heartbeat import interval_from_env, serve, stop_on_signals
from agb.lake import LakeError, NotYet, Partition, Sharadar
from agb.panel import PanelError
from agb.panel_store import PanelStore, StoreError
from agb.ranker import Ranker
from agb.registry import RegistryError
from agb.roles import PIPELINE
from agb.rows import one
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


class Ledger:
    """Records each partition in `agb.lake_snapshot` as agb.lake hands it over, before its file
    is renamed into place, and counts the rows it added."""

    def __init__(self, conn: psycopg.Connection, snapshot: date, job_id: int | None) -> None:
        self.conn = conn
        self.snapshot = snapshot
        self.job_id = job_id
        self.added = 0

    def __call__(self, partition: Partition) -> None:
        self.added += record(self.conn, self.snapshot, [partition], self.job_id)


def cause(exc: BaseException) -> str:
    """A failure as the job's `failed` event records it: the error's type, then its message."""
    return f"{type(exc).__name__}: {exc}"


# How a job that stopped on an error is handled, decided by `classify` from the error's type.
# Each is the first word of the job's `failed` event, so the event names its cause.
#   transient: the machine, not the work -- a disk, a socket, a database lock. The job is
#       released and taken again next cycle, up to TRANSIENT_RETRIES times, then failed.
#   configuration: a setting the service needs is missing (agb.errors.NotConfigured). The
#       job is held, with one `pipeline_job_held` alert, until the setting is supplied or
#       its date passes, then failed.
#   permanent: anything else, such as bad data or a refused check. The job is failed.
#   expired: not an error but a deadline -- data the vendor has not published, or an
#       account not yet read, by the time the job's work could still be used. The job is
#       failed.
TRANSIENT = "transient"
CONFIGURATION = "configuration"
PERMANENT = "permanent"
EXPIRED = "expired"

# How many times a job released on a transient error is taken again before it fails.
TRANSIENT_RETRIES = 3

# Raised once per held job, when it is first held for missing configuration.
JOB_HELD = "pipeline_job_held"

# Errors from the operating system that trying again does not mend: a path that is not there,
# or not what it should be, or that the service may not read.
_LASTING_OS_ERRORS = (FileNotFoundError, NotADirectoryError, IsADirectoryError, PermissionError)

_RELEASES = "SELECT count(*) FROM agb.job_event WHERE job_id = %s AND event = 'released'"
_SESSION = "SELECT session_date FROM agb.job WHERE id = %s"


def classify(exc: BaseException) -> str:
    """Which of TRANSIENT, CONFIGURATION or PERMANENT `exc` is. The one place a job's error is
    sorted; a database error on a connection that has gone away never reaches it."""
    if isinstance(exc, NotConfigured):
        return CONFIGURATION
    if isinstance(exc, OSError) and not isinstance(exc, _LASTING_OS_ERRORS):
        return TRANSIENT
    if isinstance(exc, (psycopg.OperationalError, duckdb.IOException)):
        return TRANSIENT
    return PERMANENT


def fail(conn: psycopg.Connection, claim: jobs.Claim, what: str, detail: str) -> None:
    """Fail `claim` with `detail` as its cause and raise an alert for it, once.

    A job whose lease was lost raises `jobs.LeaseLost`, which the cycle handles. The job is
    failed before the alert is raised, so an alert always names a job that has stopped.
    """
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
    """One pipeline cycle, run by `serve`. The URL opener, the wait between vendor retries,
    the capital, the panel audit's sample size, the gate's limits and the clock the gate reads
    are injectable for tests. A ranker given here is used instead of the promoted model, for
    tests only; otherwise each rebalance loads the promoted model from `models_dir`. Capital
    left as None, which is how `from_env` leaves it unless AGB_CAPITAL_USD is set, sizes each
    basket on the account's equity as execution last read it."""

    def __init__(
        self,
        lake_dir: Path = lake.DEFAULT_DIR,
        api_key: str = "",
        *,
        opener: lake.Opener = lake.urlopen,
        sleep: Callable[[float], None] = time.sleep,
        ranker: Ranker | None = None,
        models_dir: Path = registry.DEFAULT_MODELS_DIR,
        panel_dir: Path = panel_store.DEFAULT_DIR,
        runner: basket.Stage2 | None = None,
        capital: Decimal | None = None,
        lease: timedelta = LEASE,
        retry: timedelta = RETRY,
        worker: str | None = None,
        sample: int = panel_audit.SAMPLE,
        limits: gate.Limits = gate.LIMITS,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self.lake_dir = lake_dir
        self.api_key = api_key
        self.opener = opener
        self.sleep = sleep
        self.ranker = ranker
        self.models_dir = models_dir
        self.panel_dir = panel_dir
        self.runner = runner
        self.capital = capital
        self.lease = lease
        self.retry = retry
        self.worker = worker or jobs.worker_name(PIPELINE)
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
        return Sharadar(self.api_key, self.opener, sleep=self.sleep)

    def beat(self, conn: psycopg.Connection, claim: jobs.Claim) -> None:
        """Show the service is alive and extend the job's lease by a full term, from the long
        steps of a job. Raises `jobs.LeaseLost` if the lease is gone, which stops the work."""
        heartbeat.progress(conn)
        jobs.renew(conn, claim, self.lease)

    def stage2(self, conn: psycopg.Connection, claim: jobs.Claim) -> basket.Stage2 | None:
        """The panel the month's basket is blended with: the newest `panel_config`, driven by
        the real EDGAR client, model and store, and renewing `claim`'s lease after each
        retrieval and model call. None when no configuration has been made, which leaves the
        rebalance stage-1 only. A configuration made for other prompts, rubric
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
            beat=lambda: self.beat(conn, claim),
        )

    def deadline(self, conn: psycopg.Connection, claim: jobs.Claim) -> datetime | None:
        """The last moment `claim`'s work is still worth doing, or None for a job that is
        never held. A data pull is done for by the time the next session's pull is due, which
        lands its session too (agb.lake); a rebalance, by its trade session's MOC cutoff,
        after which its basket cannot be submitted."""
        if claim.kind not in (schedule.DATA_PULL, schedule.REBALANCE):
            return None
        session: date = one(conn.execute(_SESSION, (claim.job_id,)))[0]
        calendar = nyse.NyseCalendar(session - timedelta(days=7), session + timedelta(days=14))
        if claim.kind == schedule.DATA_PULL:
            return calendar.close_at(calendar.next_session(session)) + schedule.PULL_DELAY
        return calendar.moc_cutoff(session)

    def wait(self, conn: psycopg.Connection, claim: jobs.Claim, what: str, reason: str) -> bool:
        """Keep `claim` for another try in `self.retry`, while its deadline has not passed.
        Past it, fail the job as expired instead, with an alert, and return False."""
        deadline = self.deadline(conn, claim)
        if deadline is not None and self.clock() >= deadline:
            fail(
                conn,
                claim,
                what,
                f"{EXPIRED}: its work could be used only until {deadline:%Y-%m-%d %H:%M} UTC;"
                f" {reason}",
            )
            return False
        jobs.renew(conn, claim, self.retry)
        return True

    def stop(self, conn: psycopg.Connection, claim: jobs.Claim, what: str, exc: Exception) -> None:
        """Handle a job that stopped on `exc`, by the class `classify` gives it: release it to
        be tried again, hold it, or fail it with the class and the error named as its cause.

        A database error on a connection that has gone away is re-raised instead: nothing more
        can be written on it, and the service restarts with a new one.
        """
        if isinstance(exc, psycopg.Error) and (conn.closed or conn.broken):
            raise exc
        failure = classify(exc)
        detail = cause(exc)
        if failure == TRANSIENT:
            tries = one(conn.execute(_RELEASES, (claim.job_id,)))[0]
            if tries < TRANSIENT_RETRIES:
                jobs.release(conn, claim, f"{TRANSIENT}: {detail}")
                log.warning(
                    "%s %s: %s; released to be tried again (%d of %d)",
                    what,
                    claim.period,
                    detail,
                    tries + 1,
                    TRANSIENT_RETRIES,
                )
                return
            fail(conn, claim, what, f"{TRANSIENT}, still after {tries} retries: {detail}")
            return
        if failure == CONFIGURATION:
            if self.wait(conn, claim, what, f"held for missing configuration: {detail}"):
                log.error("%s %s held: %s", what, claim.period, detail)
                body = (
                    f"The {what} for {claim.period} is held: {detail}. Set it in the"
                    f" pipeline's env file and restart the service; the job is tried again"
                    f" every {self.retry} until its date passes, then fails."
                )
                alert(conn, JOB_HELD, claim.kind, claim.period, body)
            return
        fail(conn, claim, what, f"{failure}: {detail}")

    def __call__(self, conn: psycopg.Connection) -> None:
        for row in conn.execute(_RERUNS).fetchall():
            self.rerun(conn, *row)
        claim = jobs.claim(
            conn,
            [schedule.DATA_PULL, schedule.REBALANCE, schedule.PANEL_AUDIT],
            self.worker,
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
            jobs.renew(conn, claim, self.lease)
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
                panel=self.stage2(conn, claim),
            )
            jobs.renew(conn, claim, self.lease)
            verdict = gate.run(
                conn,
                proposal.basket_id,
                members=proposal.members,
                marks=proposal.marks,
                now=self.clock(),
                limits=self.limits,
            )
        except NotReady as exc:
            if self.wait(conn, claim, "rebalance", f"not ready: {exc}"):
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
            self.stop(conn, claim, "rebalance", exc)
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
        if proposal.sizing_note:
            log.warning("rebalance %s: %s", claim.period, proposal.sizing_note)
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
        except NotReady as exc:
            # Execution has not read the account since the signal session: the rerun waits for
            # the read, as a rebalance does, and its cutoff bounds the wait.
            log.info("rerun %s: %s; trying again next cycle", reference, exc)
            return
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
            self.stop(conn, claim, "panel audit", exc)
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
        ledger = Ledger(conn, session, claim.job_id)
        try:
            partitions = lake.pull(self.lake_dir, self.client(), session, ledger)
        except NotYet as exc:
            # The sessions before this one that the pull did land were recorded as they landed,
            # so the lake and its ledger agree while this session waits.
            if not self.wait(conn, claim, "data pull", f"not yet published: {exc}"):
                return
            log.info(
                "data pull %s: %s; landed %d earlier partitions, %d of them new or changed;"
                " trying again in %s",
                claim.period,
                exc,
                len(exc.landed),
                ledger.added,
                self.retry,
            )
            return
        except (LakeError, OSError, pa.ArrowException, psycopg.Error) as exc:
            self.stop(conn, claim, "data pull", exc)
            return
        jobs.complete(conn, claim)
        log.info(
            "data pull %s landed %d partitions, %d of them new or changed",
            claim.period,
            len(partitions),
            ledger.added,
        )

    def backfill(
        self, conn: psycopg.Connection, since: date = lake.SINCE, snapshot: date | None = None
    ) -> list[Partition]:
        """Land the vendor's full SEP and SF1 from `since` on, and its tickers table as it
        stands, recording each partition under `snapshot` (today's trading date by default)
        before its file is renamed into place. Safe to run again, including after a crash part
        way: unchanged partitions are left as they are, and every file in place is recorded."""
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
                ledger = Ledger(conn, snapshot, None)
                partitions = lake.land_bulk(self.lake_dir, dataset, archive, since, work, ledger)
                log.info(
                    "%s: %d partitions, %d new or changed",
                    dataset.name,
                    len(partitions),
                    ledger.added,
                )
                landed += partitions
            ledger = Ledger(conn, snapshot, None)
            landed += lake.land(self.lake_dir, lake.TICKERS, client.tickers(), snapshot, ledger)
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
