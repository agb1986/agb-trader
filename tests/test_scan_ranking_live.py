"""Whether the vectorbt scan *orders* candidate configurations the way the engine does.
Skipped unless AGB_LIVE_LAKE=1, with AGB_LAKE_DIR pointing at a lake the operator has already
synced or copied down and AGB_RESEARCH_DIR at a directory holding a saved panel:

    AGB_LIVE_LAKE=1 AGB_LAKE_DIR=lake AGB_RESEARCH_DIR=research \
        uv run pytest -m live -s tests/test_scan_ranking_live.py

Nothing here talks to the vendor, the broker or the deployment; the lake is only read, and the
trial ledger is a throwaway under the test's own temporary directory, so a measurement run
appends nothing to the research ledger and cannot move a declared trial count.

`tests/test_backtest_live.py` bounds how far the scan's *level* may sit from the engine's: the
annualised return gap measured 1.7 points over 67 months and 1.9 points over 343. That is not
the quantity the scan is used for. The scan is a filter: it decides which configurations are
worth an engine run, so what matters is whether the gap is ordered. If the scan ranks a set of
candidates the way the engine does, a level error of a couple of points is harmless, because
every candidate carries it; if the ranking crosses over, the filter sends the wrong
configurations forward and every hour it saves is wasted.

The two numbers this module reports are the rank correlation between the two orderings and the
top-k agreement -- of the k configurations the scan would send forward, how many are in the
engine's own top k. The second is the one that decides whether the filter is usable, since a
filter is used by taking its head and not by reading its tail.

The two sides do not run quite the same window, and the difference is reported rather than
hidden: the engine starts after the early months the lake cannot price with the liquidity
gate's bars, where the scan runs the whole panel. Over the saved panel that is two months out
of 344. An operator screening configurations runs the scan as it stands, so the asymmetry
belongs in the measurement; it is one of the reasons the scan's level sits above the engine's.

The candidate set varies the signal across the implemented features and the sizing around the
live twenty-of-thirty, and includes one configuration deliberately sized to provoke trims: the
same live configuration run at the $100,000 certification capital, where each of twenty names
is a $5,000 position and whole-share sizing on a $300 name leaves real cash unspent. Trims and
whole shares are two of the three frictions the scan omits, so if the omissions reorder
anything they should reorder that one."""

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pytest

from agb import lake
from agb.research import backtest, scan, train
from agb.research.panel import Panel
from agb.research.trials import FILE, Ledger

pytestmark = [
    pytest.mark.live,
    pytest.mark.skipif(os.environ.get("AGB_LIVE_LAKE") != "1", reason="set AGB_LIVE_LAKE=1 to run"),
]

# How many configurations a filter would send forward out of the candidate set. The set is ten,
# and an operator screening signals before an engine run takes a handful, not most of them; a k
# that is most of the set makes agreement arithmetically easy and measures nothing.
TOP_K = 3
# How many candidates must have run both sides for the agreement statistic to mean anything.
# The scan refuses some configurations the engine runs, so the rankable set is smaller than the
# candidate set; a top-TOP_K agreement over a set barely larger than TOP_K is arithmetic rather
# than a measurement.
RANKABLE = 6
# The floor this module asserts, derived from the measurement recorded in
# docs/architecture/01-data-broker-backtesting.md: over the seven rankable candidates the two
# sides picked the same three configurations, and the engine's third sat 0.054 Sharpe clear of
# its fourth, so the agreement is not a boundary accident. Membership is what is checked, not
# the order within the head, which a filter does not use. One swap is allowed anyway, because
# the pair either side of the boundary can close that gap as the lake grows;
# losing two of the three means the scan's head is not the engine's head and the filter is
# sending the wrong configurations forward.
TOP_K_FLOOR = 2
# The rank correlation floor, over the same measurement, which reported +0.964 on net Sharpe
# and +0.857 on annual return over seven candidates. The floor sits well below both: this
# statistic is the weaker of the two and a single transposition in the tail costs a seven-name
# set about 0.07, so a tight bound here would fail on a reordering a filter never notices.
# Below 0.70 the disagreement is no longer local, and the top-k check above is what catches a
# head that has gone wrong while the tail still correlates.
SPEARMAN_FLOOR = 0.70
# How many of the panel's months the engine may run fewer than the scan. The engine starts
# after the months the lake cannot price with the liquidity gate's bars, because no name in
# them can hold the bars the gate counts; the scan applies no such rule and runs the whole
# panel. Over the saved panel that is a two-month difference out of 344. It is a real
# asymmetry -- the scan compounds two early months the engine never trades -- and it is left in
# rather than papered over, because an operator screening configurations runs the scan as it
# is. Beyond a couple of quarters the two would no longer be describing the same window.
WARMUP = 6


@dataclass(frozen=True)
class Candidate:
    """One configuration both sides run. The two Options classes share these fields, which is
    what makes the comparison a comparison of the engines and not of the configurations."""

    label: str
    signal: str = "mom_12_1"
    size: int = 20
    hold_through: int = 30
    init_cash: float = 1_000_000.0


# The three non-momentum signals are kept although the scan refuses all three over the saved
# panel, within seconds, at the panel's first rebalances: rev_1m on DKB.B at 1998-03-02,
# earnings_yield on NSTLQ and book_to_price on RADCQ at 1998-02-02. Each ranks a name the lake
# has no price or liquidity for at the session the scan needs one, where the engine runs the
# same configuration end to end without comment. That is the hardest form of this module's
# finding and it costs three seconds to record, so the candidates stay in the set.
CANDIDATES = (
    Candidate("live"),
    Candidate("mom-6-1", signal="mom_6_1"),
    Candidate("mom-volscaled", signal="mom_12_1_volscaled"),
    Candidate("reversal", signal="rev_1m"),
    Candidate("earnings-yield", signal="earnings_yield"),
    Candidate("book-to-price", signal="book_to_price"),
    Candidate("concentrated", size=10),
    Candidate("broad", size=40, hold_through=60),
    Candidate("no-hold-band", hold_through=20),
    Candidate("thin-capital", init_cash=100_000.0),
)


@dataclass(frozen=True)
class Measured:
    """One candidate's two reports, or the reason the scan would not run it.

    A refusal is not a missing measurement, it is the strongest possible version of the
    finding: a configuration the scan cannot rank at all is one it cannot filter."""

    candidate: Candidate
    engine: dict[str, Any] | None = None
    scan: dict[str, Any] | None = None
    refusal: str | None = None

    @property
    def ranked(self) -> bool:
        return self.engine is not None and self.scan is not None


@pytest.fixture(scope="module")
def lake_dir() -> Path:
    root = Path(os.environ.get("AGB_LAKE_DIR", lake.DEFAULT_DIR))
    if not (root / "sep").is_dir():
        pytest.skip(f"no lake at {root}; set AGB_LAKE_DIR")
    return root


@pytest.fixture(scope="module")
def real_panel() -> Panel:
    """The saved full-history panel, and only that one. Building the five-year window as well
    would double the engine runs, which are what this measurement costs."""
    saved = Path(os.environ.get("AGB_RESEARCH_DIR", "/research")) / "panel.parquet"
    if not saved.exists():
        pytest.skip(f"no saved panel at {saved}; set AGB_RESEARCH_DIR")
    return Panel.load(saved)


@pytest.fixture(scope="module")
def measured(real_panel, lake_dir, tmp_path_factory) -> list[Measured]:
    """Every candidate through both sides, once, with the scan going first.

    The order matters for cost. The scan is the side that can refuse a configuration outright:
    it raises when a name it wants to hold has no price or liquidity at a rebalance session,
    where the engine simply does not trade that name. The scan is also about a sixth of an
    engine run over the saved panel, so running it first means a refusal costs minutes instead
    of being discovered after the engine has already spent half an hour on the same candidate.

    The engine's start-date sensitivity check is pinned to the panel's penultimate month, which
    makes it one two-month run instead of three runs over most of the panel. Left at its
    default each candidate would cost about two and a half full runs, and the sensitivity
    reports are not read here: this compares the whole-panel ordering, which the sensitivity
    runs do not enter."""
    ledger = Ledger(tmp_path_factory.mktemp("ranking") / FILE)
    short = (real_panel.months[-2].as_of.isoformat(),)
    out: list[Measured] = []
    with lake.connect(lake_dir) as conn:
        for candidate in CANDIDATES:
            options = scan.Options(
                signal=candidate.signal,
                size=candidate.size,
                hold_through=candidate.hold_through,
                init_cash=candidate.init_cash,
            )
            # Each candidate costs the best part of half an hour over the saved panel, so say
            # how each one went as it lands: a run that dies at the seventh candidate should
            # still leave the first six on the operator's terminal.
            try:
                orders = scan.build_orders(real_panel.frame, real_panel, options, conn)
                fast = scan.simulate(orders, options).results
            except scan.ScanError as error:
                out.append(Measured(candidate, refusal=str(error)))
                print(
                    f"\n[{len(out)}/{len(CANDIDATES)}] {candidate.label}: scan refused: {error}",
                    flush=True,
                )
                continue
            engine = backtest.run(
                real_panel,
                backtest.Options(
                    signal=candidate.signal,
                    size=candidate.size,
                    hold_through=candidate.hold_through,
                    init_cash=candidate.init_cash,
                    starts=short,
                ),
                ledger,
                lake_dir=lake_dir,
                db=conn,
            ).results
            out.append(Measured(candidate, engine=engine, scan=fast))
            print(
                f"\n[{len(out)}/{len(CANDIDATES)}] {candidate.label}:"
                f" engine return/volatility {engine['return_over_volatility']:.3f}"
                f" annual {engine['annual_return']:+.4f},"
                f" scan return/volatility {fast['return_over_volatility']:.3f}"
                f" annual {fast['annual_return']:+.4f}",
                flush=True,
            )
    return out


@pytest.fixture(scope="module")
def ranked(measured) -> list[Measured]:
    """The candidates both sides reported on, which are the only ones an ordering exists over.
    A refused candidate is a finding, not a gap, and the test above reports it; here it is
    simply not rankable."""
    survivors = [report for report in measured if report.ranked]
    if len(survivors) < RANKABLE:
        refused = [report.candidate.label for report in measured if report.refusal]
        pytest.fail(
            f"only {len(survivors)} of {len(CANDIDATES)} candidates ran both sides"
            f" (scan refused {refused}); a top-{TOP_K} agreement over that few measures nothing,"
            " so the candidate set needs configurations the scan can run"
        )
    return survivors


def _ordering(reports: list[Measured], key: str) -> np.ndarray:
    return np.asarray([report.scan[key] for report in reports], dtype="float64")


def _engine_ordering(reports: list[Measured], key: str) -> np.ndarray:
    return np.asarray([report.engine[key] for report in reports], dtype="float64")


def _top(values: np.ndarray, k: int) -> set[int]:
    """The indices of the k best. Ties are broken by index, which is a candidate the filter
    would have to pick between anyway."""
    return set(np.argsort(-values, kind="stable")[:k].tolist())


def test_every_candidate_either_ran_both_sides_or_the_scan_refused_it(measured):
    """The table the operator reads, and a sanity check that both sides ran the same panel.

    A refusal is printed, not asserted away. The scan raising on a configuration the engine
    runs without comment is the hardest version of this module's finding: the filter cannot
    rank what it will not simulate."""
    ran = [report for report in measured if report.ranked]
    engine_months = {report.engine["months"] for report in ran}
    scan_months = {report.scan["months"] for report in ran}
    print(
        f"\n{len(ran)} of {len(measured)} candidates ranked, over {engine_months} engine months"
        f" and {scan_months} scan months"
    )
    print(
        f"{'candidate':<16}{'engine sharpe':>14}{'scan sharpe':>13}"
        f"{'engine annual':>15}{'scan annual':>13}{'trims':>7}{'exits':>7}"
    )
    for report in ran:
        print(
            f"{report.candidate.label:<16}{report.engine['return_over_volatility']:>14.3f}"
            f"{report.scan['return_over_volatility']:>13.3f}{report.engine['annual_return']:>+15.4f}"
            f"{report.scan['annual_return']:>+13.4f}{report.engine['trimmed']:>7}"
            f"{report.engine['delisting_exits']:>7}"
        )
    for report in measured:
        if report.refusal:
            print(f"{report.candidate.label:<16} scan refused: {report.refusal}")
    assert len(engine_months) == 1, "the engine ran the same months for every candidate"
    assert len(scan_months) == 1, "the scan ran the same months for every candidate"
    skipped = scan_months.pop() - engine_months.pop()
    assert 0 <= skipped <= WARMUP, f"the engine skipped {skipped} months the scan ran"


def test_the_scan_orders_the_candidates_the_way_the_engine_does(ranked):
    """Rank correlation over the rankable set, reported for both headline statistics and
    asserted on net Sharpe. This is the weaker of the two findings -- a correlation is moved by
    the tail of the set, which a filter never looks at -- but it is the one that says whether
    the disagreement is local or wholesale."""
    correlations = {}
    for key in ("return_over_volatility", "annual_return"):
        correlations[key] = train.spearman(_engine_ordering(ranked, key), _ordering(ranked, key))
    print("\nrank correlation: " + ", ".join(f"{k} {v}" for k, v in correlations.items()))
    assert correlations["return_over_volatility"] is not None, (
        "the engine or the scan ranked every candidate flat"
    )
    assert correlations["return_over_volatility"] >= SPEARMAN_FLOOR


def test_the_scan_sends_forward_the_configurations_the_engine_would_have_chosen(ranked):
    """Top-k agreement, which is what decides whether the scan is usable as a filter: of the
    TOP_K configurations the scan would send forward, how many the engine also puts in its own
    top TOP_K. A filter that loses the engine's best configuration has cost more than it saved,
    however well it correlates over the rest of the set."""
    engine = _top(_engine_ordering(ranked, "return_over_volatility"), TOP_K)
    fast = _top(_ordering(ranked, "return_over_volatility"), TOP_K)
    labels = [report.candidate.label for report in ranked]
    print(
        f"\ntop-{TOP_K} engine {sorted(labels[i] for i in engine)}"
        f" scan {sorted(labels[i] for i in fast)}"
        f" agreement {len(engine & fast)}/{TOP_K}"
    )
    assert len(engine & fast) >= TOP_K_FLOOR
