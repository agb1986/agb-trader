"""The research profile's model side: the panel and its quintile labels, purged and embargoed
CPCV, the walk-forward proof, the trial ledger, reproduction and the vectorbt scan.

`research_panel` in conftest is built once per session over the fake lake in research_fake:
two dozen names from 2022 to August 2026, so thirty month-ends from January 2024 carry a
settled label."""

import json
import subprocess
import sys
import threading
from datetime import date, timedelta

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from research_fake import END, PANEL_START, Market

from agb import cost, features
from agb.research import cpcv, labels, reproduce, scan, train, trials, walkforward
from agb.research.panel import Month, Panel
from agb.research.trials import Ledger, LedgerError, Trial

# Small enough to refit a hundred times in a test, large enough to learn the fake's drifts.
CONFIG = train.Config(num_boost_round=20, min_data_in_leaf=5)
SMALL = cpcv.Options(groups=4, test_groups=2, embargo_months=1)


@pytest.fixture
def ledger(tmp_path) -> Ledger:
    return Ledger(tmp_path / "trials.parquet")


def synthetic_months(count: int, start: date = date(1998, 1, 31)) -> list[Month]:
    """`count` months whose windows abut exactly as the real ones do: each entry is the day
    after the month-end and each exit is the day after the next month-end."""
    ends = []
    day = start
    for _ in range(count + 1):
        ends.append(day)
        day = cpcv.add_months(day.replace(day=1), 2) - timedelta(days=1)
    return [
        Month(
            as_of=end,
            snapshot_date=end,
            entry=end + timedelta(days=1),
            exit=nxt + timedelta(days=1),
        )
        for end, nxt in zip(ends, ends[1:], strict=False)
    ]


# The panel and its labels ---------------------------------------------------------------------


def test_the_panel_has_one_row_per_name_per_settled_month(research_panel):
    months = research_panel.months
    assert months[0].as_of == date(2024, 1, 31)
    # August 2026 is the lake's last month, so July's window has not closed.
    assert months[-1].as_of == date(2026, 6, 30)
    assert len(months) == 30
    for month in months:
        assert month.snapshot_date <= month.as_of < month.entry < month.exit
    assert list(research_panel.frame.columns) == list(
        ("as_of", "snapshot_date", "entry", "exit", "ticker", *features.FEATURES,
         "forward_return", "grade")
    )  # fmt: skip
    assert research_panel.frame.groupby("as_of")["ticker"].count().between(20, 24).all()


def test_grades_are_quintiles_of_the_forward_return_within_each_month(research_panel):
    for _, month in research_panel.frame.groupby("as_of"):
        counts = month["grade"].value_counts()
        assert set(counts.index) == set(range(labels.GRADES))
        assert counts.max() - counts.min() <= 1, "the fifths are as even as the count allows"
        ordered = month.sort_values("forward_return")["grade"].to_numpy()
        assert (np.diff(ordered) >= 0).all(), "a higher return never gets a lower grade"


def test_grades_follow_the_shared_percentile_rank():
    assert labels.grades([0.1, -0.2, 0.05, None, 0.3, 0.0]) == [3, 0, 2, None, 4, 1]
    assert labels.grades([1.0] * 4) == [2, 2, 2, 2]
    assert labels.grades([]) == []


def test_the_panel_carries_the_lake_digest_through_its_last_exit(research_panel, tmp_path):
    saved = tmp_path / "panel.parquet"
    research_panel.save(saved)
    loaded = Panel.load(saved)

    assert loaded.partition_hash == research_panel.partition_hash
    assert loaded.cost_model_version == cost.CURRENT.version
    assert loaded.months == research_panel.months
    assert list(loaded.frame["sector"].cat.categories) == list(features.SECTORS)
    pd.testing.assert_frame_equal(loaded.frame, research_panel.frame)


# Purging and the embargo ----------------------------------------------------------------------


def assert_disjoint(split: cpcv.Split, embargo_months: int) -> None:
    for train_month in split.train:
        for test_month in split.test:
            assert train_month.exit <= test_month.entry or train_month.entry >= test_month.exit, (
                f"{train_month} shares returns with {test_month}"
            )
            fence = cpcv.add_months(test_month.exit, embargo_months)
            assert not test_month.as_of < train_month.as_of < fence, (
                f"{train_month} lies within the embargo after {test_month}"
            )


def test_no_training_window_overlaps_a_test_window_after_purge_and_embargo(research_panel):
    options = cpcv.Options(groups=6, test_groups=2, embargo_months=1)
    split_list = cpcv.splits(research_panel.months, options)

    assert len(split_list) == 15
    for split in split_list:
        assert_disjoint(split, options.embargo_months)
        assert not set(split.train) & set(split.test)
        dropped = set(research_panel.months) - set(split.train) - set(split.test)
        if split.test_groups == (4, 5):
            assert not dropped, "nothing follows the last groups, so nothing is embargoed"
        else:
            assert dropped, "the embargo drops the months after each test group"
            for month in dropped:
                assert any(cpcv.embargoed(month, test, 1) for test in split.test)


def test_the_purge_drops_the_months_around_a_test_group_and_no_others():
    months = synthetic_months(24)
    options = cpcv.Options(groups=4, test_groups=1, embargo_months=1)

    [split] = [s for s in cpcv.splits(months, options) if s.test_groups == (1,)]

    assert split.test == tuple(months[6:12])
    # The test windows run from August to the first of February. June's window ends where
    # July's begins, so nothing before the test is purged. January's month-end sits inside
    # the last test window and February's inside the embargo month after it; March is the
    # first clean month.
    assert split.train == tuple(months[:6]) + tuple(months[14:])
    assert_disjoint(split, options.embargo_months)


def test_the_default_options_make_120_splits_and_fifteen_paths():
    months = synthetic_months(16 * 12)
    options = cpcv.Options()

    split_list = cpcv.splits(months, options)
    path_list = cpcv.paths(split_list, options)

    assert len(split_list) == 120
    assert len(path_list) == 15
    for path in path_list:
        assert [group for group, _ in path] == list(range(16))
        for group, index in path:
            assert group in split_list[index].test_groups
    assert len({index for path in path_list for _, index in path}) == 120
    for split in split_list:
        assert_disjoint(split, options.embargo_months)


def test_options_are_validated():
    with pytest.raises(ValueError, match="test_groups"):
        cpcv.Options(groups=4, test_groups=4)
    with pytest.raises(ValueError, match="embargo"):
        cpcv.Options(embargo_months=-1)
    with pytest.raises(ValueError, match="cannot make"):
        cpcv.group_months(synthetic_months(3), 4)


# CPCV end to end ------------------------------------------------------------------------------


def test_cpcv_runs_end_to_end_with_a_result_per_path(research_panel, ledger):
    run = cpcv.run(research_panel, CONFIG, SMALL, ledger)

    results = run.results
    assert sorted(results["groups"]) == [7, 7, 8, 8]
    assert [split["index"] for split in results["splits"]] == list(range(6))
    assert [path["path"] for path in results["paths"]] == [0, 1, 2]
    for path in results["paths"]:
        held = [
            group
            for group, _ in cpcv.paths(cpcv.splits(research_panel.months, SMALL), SMALL)[
                path["path"]
            ]
        ]
        assert held == [0, 1, 2, 3]
        assert path["metrics"]["months"] == 30, "a path scores every month out of sample"
        assert -1 <= path["metrics"]["ic_mean"] <= 1
        assert 0 <= path["metrics"]["ndcg_mean"] <= 1
    assert results["summary"]["paths"] == 3
    assert results["summary"]["ic_mean_min"] <= results["summary"]["ic_mean"]
    assert results["summary"]["ic_mean"] <= results["summary"]["ic_mean_max"]
    # The fake's drifts persist, so a model that learned them ranks the future better than chance.
    assert results["summary"]["ic_mean"] > 0

    [row] = ledger.rows()
    assert row["trial_id"] == run.trial_id
    assert (row["kind"], row["status"], row["refits"]) == ("cpcv", "succeeded", 6)
    assert json.loads(row["config"])["cpcv"] == {"groups": 4, "test_groups": 2, "embargo_months": 1}
    assert json.loads(row["features"]) == list(features.FEATURES)
    assert (row["train_start"], row["train_end"]) == (date(2024, 1, 31), date(2026, 6, 30))
    assert row["partition_hash"] == research_panel.partition_hash
    assert row["cost_model_version"] == cost.CURRENT.version
    assert json.loads(row["results"]) == results


# The walk-forward proof -----------------------------------------------------------------------


def test_the_model_for_each_year_is_trained_only_on_data_through_the_year_before(
    research_panel, ledger, monkeypatch
):
    seen: list[tuple[date, date]] = []
    real_fit = train.fit

    def spy(frame, config, *, trial):
        seen.append((frame["as_of"].max(), frame["exit"].max()))
        return real_fit(frame, config, trial=trial)

    monkeypatch.setattr(train, "fit", spy)

    run = walkforward.run(research_panel, CONFIG, walkforward.Options(), ledger)

    years = run.results["years"]
    assert [year["year"] for year in years] == [2025, 2026]
    assert [year["train_through"] for year in years] == ["2024-12-31", "2025-12-31"]
    for year, (last_as_of, last_exit) in zip(years, seen, strict=True):
        cutoff = date(year["year"] - 1, 12, 31)
        assert last_exit <= cutoff, "every training label had settled by the cutoff"
        assert last_as_of <= cutoff
    fold_list = walkforward.folds(research_panel.months)
    for fold, year in zip(fold_list, years, strict=True):
        assert {month.entry.year for month in fold.test} == {fold.year}
        assert all(month.exit <= fold.cutoff for month in fold.train)
        assert (year["train_months"], year["test_months"]) == (len(fold.train), len(fold.test))
    assert years[1]["train_months"] > years[0]["train_months"], "the window expands"
    assert set(fold_list[0].train) < set(fold_list[1].train)
    assert run.results["overall"]["months"] == 19
    [row] = ledger.rows()
    assert (row["kind"], row["status"], row["refits"]) == ("walkforward", "succeeded", 2)


def test_november_and_december_belong_to_the_next_years_test_set():
    months = synthetic_months(35, date(2020, 1, 31))

    fold_list = walkforward.folds(months)

    assert [fold.year for fold in fold_list] == [2021, 2022]
    first = fold_list[0]
    assert first.train == tuple(months[:10]), "October's window closes in November"
    assert first.test[0].as_of == date(2020, 12, 31), "December's basket is bought in January"
    assert len(first.test) == 12
    assert walkforward.folds(months, walkforward.Options(first_test_year=2022))[0].year == 2022
    assert walkforward.folds(months[:6]) == []


# The trial ledger -----------------------------------------------------------------------------


def open_trial(ledger: Ledger, kind: str = "test", **overrides):
    values = {
        "features": ("mom_12_1",),
        "train_start": date(2024, 1, 31),
        "train_end": date(2025, 12, 31),
        "partition_hash": "abc",
        "cost_model_version": cost.CURRENT.version,
    }
    return ledger.trial(kind, {"seed": 1}, **{**values, **overrides})


def test_a_trial_is_recorded_before_it_runs_and_finished_after(ledger):
    assert ledger.count() == 0
    with open_trial(ledger) as trial:
        [row] = ledger.rows()
        assert row["trial_id"] == trial.trial_id
        assert row["status"] == "running"
        assert row["finished_at"] is None
        trial.refit()
        trial.refit()
        trial.results = {"ic": 0.1, "when": date(2026, 1, 2)}
    [row] = ledger.rows()
    assert (row["status"], row["refits"], row["error"]) == ("succeeded", 2, None)
    assert json.loads(row["results"]) == {"ic": 0.1, "when": "2026-01-02"}
    assert row["finished_at"] >= row["started_at"]
    assert json.loads(row["config"]) == {"seed": 1}
    assert ledger.get(trial.trial_id) == row
    with pytest.raises(LedgerError):
        ledger.get("nope")


def test_a_failed_run_keeps_its_row_and_its_error(ledger):
    with pytest.raises(RuntimeError, match="boom"):
        with open_trial(ledger) as trial:
            trial.refit()
            raise RuntimeError("boom")
    [row] = ledger.rows()
    assert (row["status"], row["refits"], row["error"]) == ("failed", 1, "RuntimeError: boom")
    assert row["results"] is None


def test_an_interrupted_run_is_abandoned_not_lost(ledger):
    with pytest.raises(KeyboardInterrupt):
        with open_trial(ledger):
            raise KeyboardInterrupt
    [row] = ledger.rows()
    assert (row["status"], row["error"]) == ("abandoned", "interrupted")


def test_every_trial_appends_and_the_digest_names_them_all(ledger):
    with open_trial(ledger):
        pass
    first = ledger.digest()
    with open_trial(ledger, kind="other"):
        pass
    assert ledger.count() == 2
    assert len({row["trial_id"] for row in ledger.rows()}) == 2
    assert ledger.digest() != first
    assert [row["status"] for row in ledger.rows()] == ["succeeded", "succeeded"]


def test_the_declared_count_is_configurations_searched_not_rows(ledger):
    """Eight rows, four configurations searched.

    The scan, the backtest and the walkforward are three. The backtest that crashed, the rerun
    of it and the reproduction of that are the same configuration however often it is run. The
    certification is the fourth; the cross-validation and the backtest it ran inside itself are
    how it was evaluated, not two more configurations tried.
    """
    with open_trial(ledger, kind="scan"):
        pass
    with pytest.raises(RuntimeError, match="boom"):
        with open_trial(ledger, kind="backtest", features=("mom_6_1",)):
            raise RuntimeError("boom")
    for _ in range(2):
        with open_trial(ledger, kind="backtest", features=("mom_6_1",)):
            pass
    with open_trial(ledger, kind="certification"):
        with open_trial(ledger, kind="cpcv"):
            pass
        with open_trial(ledger, kind="backtest"):
            pass
    with open_trial(ledger, kind="walkforward"):
        pass
    assert ledger.count() == 8
    assert ledger.declared() == 4
    assert ledger.declared(rule="configurations") == 4
    # What every certification up to lgbm-2026-10 declared, and what their manifests are still
    # read under.
    assert ledger.declared(rule="rows") == 8


def test_a_run_killed_without_writing_its_end_does_not_swallow_the_trials_after_it(ledger):
    """A row left `running` by a kill has no end, so nothing says when it stopped holding other
    runs. The run asking the question is open until it finishes and counts what it nests; any
    other open row closes at its own start, or one zombie would hide every trial since."""
    with open_trial(ledger, kind="certification"):
        [killed] = ledger.rows()
    assert killed["status"] == "running" and killed["finished_at"] is None
    with open_trial(ledger, kind="scan"):
        pass
    with open_trial(ledger, kind="walkforward"):
        pass
    rows = [killed, *ledger.rows()[1:]]
    assert trials.declared(rows, rule="configurations") == 2
    assert trials.declared(rows, rule="configurations", current=killed["trial_id"]) == 1
    # The scan and the walkforward name no parent, so they are their own runs whichever row is
    # open, and the killed certification was started, so it counts too.
    assert trials.declared(rows) == 3
    assert trials.declared(rows, current=killed["trial_id"]) == 3


def test_a_failed_trial_counts_as_one_more_trial(ledger):
    """A configuration tried once that fell over is a trial the search spent, so a failure, or
    an interrupt, raises the declared count by one where the configuration rule ignored it."""
    with open_trial(ledger, kind="scan"):
        pass
    before = ledger.declared()
    with pytest.raises(RuntimeError, match="boom"):
        with open_trial(ledger, kind="backtest"):
            raise RuntimeError("boom")
    assert ledger.declared() == before + 1
    with pytest.raises(KeyboardInterrupt):
        with open_trial(ledger, kind="walkforward"):
            raise KeyboardInterrupt
    assert ledger.declared() == before + 2
    assert ledger.declared(rule="configurations") == before


def test_a_trial_inside_another_names_it_as_its_parent(ledger, tmp_path):
    with open_trial(ledger, kind="certification") as outer:
        with open_trial(ledger, kind="cpcv") as inner:
            with open_trial(Ledger(tmp_path / "other.parquet"), kind="scan"):
                pass
        with open_trial(ledger, kind="backtest"):
            pass
    with open_trial(ledger, kind="scan"):
        pass
    parents = {row["kind"]: row["parent"] for row in ledger.rows()}
    assert parents == {
        "certification": None,
        "cpcv": outer.trial_id,
        "backtest": outer.trial_id,
        "scan": None,
    }
    assert ledger.get(inner.trial_id)["parent"] == outer.trial_id
    # A trial on another ledger is nobody's child here, nor is the trial open there its parent.
    [other] = Ledger(tmp_path / "other.parquet").rows()
    assert other["parent"] is None


def test_a_run_that_overlaps_another_without_being_its_child_still_counts(ledger):
    """A scan started from another terminal while a certification was open sits inside its
    interval, which the clock rule read as one of its children. It was not opened inside the
    certification, so it names no parent and counts. A thread starts with a fresh context, as
    another process would."""

    def other_terminal():
        with open_trial(ledger, kind="scan", features=("mom_6_1",)):
            pass

    with open_trial(ledger, kind="certification") as outer:
        with open_trial(ledger, kind="cpcv"):
            pass
        thread = threading.Thread(target=other_terminal)
        thread.start()
        thread.join()
    rows = ledger.rows()
    assert {r["kind"]: r["parent"] for r in rows} == {
        "certification": None,
        "cpcv": outer.trial_id,
        "scan": None,
    }
    assert trials.declared(rows, rule="configurations") == 1
    assert trials.declared(rows) == 2


def test_a_row_is_rooted_at_the_run_it_names_as_its_parent_not_the_run_around_it(ledger):
    """A certification started from another terminal while one is open runs its backtest inside
    the first one's interval, and its root is still the certification that opened it. Rows from
    before the chain are rooted by their interval."""
    legacy = [{**row, "parent": None, "previous": None} for row in _legacy_rows(ledger)]

    def other_terminal(opened):
        with open_trial(ledger, kind="certification", features=("mom_6_1",)) as theirs:
            with open_trial(ledger, kind="backtest") as backtest:
                opened.extend([theirs.trial_id, backtest.trial_id])

    opened: list[str] = []
    with open_trial(ledger, kind="certification") as outer:
        with open_trial(ledger, kind="backtest") as mine:
            pass
        thread = threading.Thread(target=other_terminal, args=(opened,))
        thread.start()
        thread.join()
    theirs, their_backtest = opened
    rows = ledger.rows()
    assert their_backtest in trials.nested(rows, current=None)
    roots = trials.roots(rows)
    assert roots[mine.trial_id]["trial_id"] == outer.trial_id
    assert roots[their_backtest]["trial_id"] == theirs
    assert roots[theirs]["trial_id"] == theirs
    by_kind = {row["kind"]: row for row in legacy}
    old = trials.roots(legacy)
    assert old[by_kind["cpcv"]["trial_id"]] == by_kind["certification"]
    assert all(old[r["trial_id"]] is r for r in legacy if r["kind"] != "cpcv")


def test_the_started_count_never_falls_below_the_configuration_count(ledger):
    """Over a ledger holding every kind of row the change could affect, the new rule counts at
    least as many trials as the rule it replaces: rows from before the chain, a run killed before
    it wrote its end and the runs inside it, failures, reruns and nested runs."""
    legacy = [{**row, "parent": None, "previous": None} for row in _legacy_rows(ledger)]
    with open_trial(ledger, kind="scan"):
        pass
    with pytest.raises(RuntimeError):
        with open_trial(ledger, kind="backtest", features=("mom_6_1",)):
            raise RuntimeError("boom")
    with open_trial(ledger, kind="backtest", features=("mom_6_1",)):
        pass
    with open_trial(ledger, kind="certification") as outer:
        with open_trial(ledger, kind="cpcv"):
            pass
        with open_trial(ledger, kind="backtest"):
            pass
        killed = ledger.get(outer.trial_id)
    chained = ledger.rows()
    rows = [*legacy, *[killed if r["trial_id"] == killed["trial_id"] else r for r in chained]]
    for current in (None, killed["trial_id"]):
        for upto in range(len(rows) + 1):
            prefix = rows[:upto]
            assert trials.declared(prefix, current=current) >= trials.declared(
                prefix, rule="configurations", current=current
            )


def _legacy_rows(ledger: Ledger) -> list[dict]:
    """Rows as a ledger from before the chain holds them: a certification and what it nested,
    a scan that failed, and a scan that succeeded, with no parent and no link."""
    scratch = Ledger(ledger.path.with_name("legacy.parquet"))
    with open_trial(scratch, kind="certification", partition_hash="old"):
        with open_trial(scratch, kind="cpcv", partition_hash="old"):
            pass
    with pytest.raises(RuntimeError):
        with open_trial(scratch, kind="scan", partition_hash="old"):
            raise RuntimeError("boom")
    with open_trial(scratch, kind="scan", partition_hash="old", features=("mom_6_1",)):
        pass
    return scratch.rows()


def test_every_row_links_to_the_one_before(ledger):
    with open_trial(ledger, kind="certification"):
        with open_trial(ledger, kind="cpcv"):
            pass
    with pytest.raises(RuntimeError):
        with open_trial(ledger, kind="scan"):
            raise RuntimeError("boom")
    rows = ledger.rows()
    assert rows[0]["previous"] == trials.GENESIS
    assert [r["previous"] for r in rows[1:]] == [trials.link(r) for r in rows[:-1]]
    ledger.verify()


@pytest.mark.parametrize(
    ("tamper", "problem"),
    [
        (lambda rows: [rows[0], *rows[2:]], "row 1, trial .* does not link to the row before it"),
        (lambda rows: rows[1:], "row 0, trial .* does not link to the row before it"),
        (lambda rows: [rows[0], rows[2], rows[1], rows[3]], "does not link"),
        (lambda rows: [*rows[:2], {**rows[2], "kind": "scan"}, rows[3]], "row 3, .* does not link"),
        (lambda rows: [*rows, rows[1]], "appears twice"),
        (
            lambda rows: [*rows[:3], {**rows[3], "previous": None}],
            "has no link but follows rows that do",
        ),
    ],
    ids=["middle row removed", "first row removed", "rows swapped", "row rewritten",
         "row repeated", "link stripped"],
)  # fmt: skip
def test_a_ledger_whose_chain_is_broken_is_refused(ledger, tamper, problem):
    for kind in ("scan", "backtest", "walkforward", "cpcv"):
        with open_trial(ledger, kind=kind):
            pass
    rows = tamper(ledger.rows())
    ledger._write(rows)
    with pytest.raises(LedgerError, match=f"chain through .* is broken: .*{problem}"):
        ledger.verify()


def test_rows_from_before_the_chain_are_read_and_the_chain_starts_after_them(ledger):
    """A ledger written before the `parent` and `previous` columns existed reads with both
    empty, and the first trial appended to it links to its last row."""
    legacy = _legacy_rows(ledger)
    old = trials.SCHEMA.remove(trials.SCHEMA.get_field_index("previous"))
    old = old.remove(old.get_field_index("parent"))
    pq.write_table(pa.Table.from_pylist(legacy, schema=old), ledger.path)
    assert [r["previous"] for r in ledger.rows()] == [None] * len(legacy)
    ledger.verify()
    before = ledger.declared(rule="configurations")
    with open_trial(ledger, kind="walkforward"):
        pass
    rows = ledger.rows()
    assert rows[-1]["previous"] == trials.link(rows[-2])
    ledger.verify()
    assert ledger.declared() >= before + 1


_WRITER = """
import sys
from datetime import date
from pathlib import Path

from agb.research.trials import Ledger

ledger = Ledger(Path(sys.argv[1]))
for i in range(int(sys.argv[3])):
    with ledger.trial(
        sys.argv[2], {"i": i}, features=("mom_12_1",), train_start=date(2024, 1, 31),
        train_end=date(2025, 12, 31), partition_hash="abc", cost_model_version="v1",
    ):
        pass
"""


def test_two_writers_at_once_both_keep_their_rows(ledger):
    """Two processes appending to one ledger at the same time: every row of each lands, the
    rows still chain, and no temporary file is left behind."""
    each = 25
    writers = [
        subprocess.Popen([sys.executable, "-c", _WRITER, str(ledger.path), kind, str(each)])
        for kind in ("scan", "backtest")
    ]
    assert [writer.wait(timeout=300) for writer in writers] == [0, 0]
    rows = ledger.rows()
    assert len(rows) == 2 * each
    assert {(r["kind"], r["status"]) for r in rows} == {
        ("scan", "succeeded"),
        ("backtest", "succeeded"),
    }
    assert sorted(json.loads(r["config"])["i"] for r in rows if r["kind"] == "scan") == list(
        range(each)
    )
    ledger.verify()
    assert not list(ledger.path.parent.glob("*.tmp"))


def test_an_unknown_trial_count_rule_is_refused(ledger):
    with open_trial(ledger, kind="scan"):
        pass
    with pytest.raises(LedgerError, match="unknown trial count rule 'guesses'"):
        ledger.declared(rule="guesses")


def test_nothing_trains_without_a_trial(research_panel):
    rows = research_panel.select(research_panel.months[:3])
    with pytest.raises(TypeError, match="open trial"):
        train.fit(rows, CONFIG, trial=None)
    with pytest.raises(TypeError, match="open trial"):
        train.fit(rows, CONFIG, trial=object())
    trial = Trial("t")
    assert train.fit(rows, CONFIG, trial=trial) is not None
    assert trial.refits == 1


def test_a_run_that_fails_mid_way_still_has_its_trial_row(research_panel, ledger, monkeypatch):
    calls = []
    real_fit = train.fit

    def failing(frame, config, *, trial):
        calls.append(trial)
        if len(calls) == 3:
            raise RuntimeError("out of memory")
        return real_fit(frame, config, trial=trial)

    monkeypatch.setattr(train, "fit", failing)

    with pytest.raises(RuntimeError, match="out of memory"):
        cpcv.run(research_panel, CONFIG, SMALL, ledger)

    [row] = ledger.rows()
    assert (row["kind"], row["status"]) == ("cpcv", "failed")
    assert row["refits"] == 2, "the refits that completed are counted"
    assert row["error"] == "RuntimeError: out of memory"


def test_training_rows_must_be_grouped_by_month(research_panel):
    rows = research_panel.select(research_panel.months[:2])
    assert train.groups(rows) == [len(m) for _, m in rows.groupby("as_of")]
    with pytest.raises(ValueError, match="sorted by month"):
        train.groups(rows.iloc[::-1])
    with pytest.raises(ValueError, match="not features"):
        CONFIG.with_features(("mom_12_1", "alpha"))
    assert train.Config.from_dict(CONFIG.to_dict()) == CONFIG


# Reproduction ---------------------------------------------------------------------------------


def test_a_run_is_reproduced_from_its_trial_row_and_partition_hash(research_panel, ledger):
    options = walkforward.Options(first_test_year=2026)
    original = walkforward.run(
        research_panel, CONFIG.with_features(features.NUMERIC[:6]), options, ledger
    )

    result = reproduce.reproduce(ledger, original.trial_id, research_panel)

    assert result.matches
    assert result.rerun.trial_id != original.trial_id
    assert result.rerun.results == original.results
    assert ledger.count() == 2, "the rerun is a trial too"
    rerun_row, original_row = ledger.get(result.rerun.trial_id), ledger.get(original.trial_id)
    for key in ("kind", "config", "features", "partition_hash", "cost_model_version", "refits"):
        assert rerun_row[key] == original_row[key], key


def test_a_panel_from_a_moved_lake_is_refused(research_panel, ledger):
    original = walkforward.run(research_panel, CONFIG, walkforward.Options(), ledger)
    moved = Panel(frame=research_panel.frame, partition_hash="0" * 64)

    with pytest.raises(reproduce.ReproduceError, match="sync the lake the trial saw"):
        reproduce.reproduce(ledger, original.trial_id, moved)
    with pytest.raises(reproduce.ReproduceError, match="cost model"):
        reproduce.reproduce(
            ledger,
            original.trial_id,
            Panel(research_panel.frame, research_panel.partition_hash, "0"),
        )
    assert ledger.count() == 1, "a refused reproduction trains nothing"


def test_only_a_succeeded_trial_can_be_reproduced(research_panel, ledger):
    with pytest.raises(RuntimeError):
        with open_trial(ledger, kind="cpcv", partition_hash=research_panel.partition_hash) as trial:
            raise RuntimeError("boom")
    with pytest.raises(reproduce.ReproduceError, match="failed"):
        reproduce.reproduce(ledger, trial.trial_id, research_panel)


def test_results_are_compared_to_nine_figures():
    assert reproduce.same({"a": [1.0, None, "x"]}, {"a": [1.0 + 1e-12, None, "x"]})
    assert not reproduce.same({"a": 1.0}, {"a": 1.001})
    assert not reproduce.same({"a": 1}, {"b": 1})
    assert not reproduce.same([1], [1, 2])
    assert not reproduce.same(True, 1)


# The vectorbt scan ----------------------------------------------------------------------------


def test_the_scan_costs_every_order_with_the_live_cost_model(
    research_panel, research_lake, ledger, monkeypatch
):
    charged: list[tuple[str, float, float, float]] = []
    liquidity_calls = []
    real_cost, real_liquidity = cost.trade_cost, cost.liquidity

    def spy_cost(side, order_usd, shares, liq, model=cost.CURRENT):
        result = real_cost(side, order_usd, shares, liq, model)
        charged.append((side, order_usd, shares, result.total_bps))
        return result

    def spy_liquidity(high, low, close, volume, model=cost.CURRENT):
        liquidity_calls.append(len(close))
        return real_liquidity(high, low, close, volume, model)

    monkeypatch.setattr(cost, "trade_cost", spy_cost)
    monkeypatch.setattr(cost, "liquidity", spy_liquidity)
    options = scan.Options(signal="mom_12_1", size=5, hold_through=8)

    run = scan.run(research_panel, options, ledger, lake_dir=research_lake)

    results = run.results
    assert results["months"] == 30
    assert results["orders"] == len(charged) > 30, "every order, and only an order, is costed"
    assert {usd for _, usd, _, _ in charged} == {options.init_cash / 5}, "sized to weight"
    assert results["fees_paid"] > 0
    assert set(liquidity_calls) == {scan.LIQUIDITY_BARS}, "the cost model's own window"
    assert results["final_value"] == pytest.approx(
        options.init_cash * (1 + results["total_return"])
    )
    assert results["max_drawdown"] <= 0
    [row] = ledger.rows()
    assert (row["kind"], row["status"], row["refits"]) == ("scan", "succeeded", 0)
    assert json.loads(row["features"]) == ["mom_12_1"]
    assert json.loads(row["config"]) == {
        "scan": {"signal": "mom_12_1", "size": 5, "hold_through": 8, "init_cash": 1e6}
    }


def test_vectorbt_charges_each_fill_the_fee_the_cost_model_priced(research_panel, research_lake):
    from agb import lake as lake_module

    options = scan.Options(signal="mom_12_1", size=5, hold_through=8)
    conn = lake_module.connect(research_lake)
    try:
        orders = scan.build_orders(research_panel.frame, research_panel, options, conn)
    finally:
        conn.close()

    simulation = scan.simulate(orders, options)

    trades = simulation.trades
    assert len(trades) == simulation.results["orders"] == int((orders.fees > 0).sum().sum())
    for trade in trades.itertuples(index=False):
        fraction = orders.fees.at[trade.Timestamp, trade.Column]
        assert fraction > 0, "the fill was priced before it was placed"
        assert trade.Fees == pytest.approx(fraction * trade.Size * trade.Price)
        assert trade.Side == ("Buy" if orders.weights.at[trade.Timestamp, trade.Column] else "Sell")
    assert simulation.results["fees_paid"] == pytest.approx(trades["Fees"].sum())


def test_the_scan_rebalances_by_the_live_rules(research_panel):
    from agb import rebalance

    month = research_panel.months[0]
    rows = research_panel.select([month])
    options = scan.Options(signal="mom_6_1", size=5, hold_through=8)

    ranked = scan.ranking(rows, options)

    assert ranked == rows.sort_values("mom_6_1", ascending=False)["ticker"].tolist()
    first = rebalance.select(ranked, (), size=5, hold_through=8)
    assert first.buy == tuple(ranked[:5])
    # An incumbent that slips to eighth is kept; one that slips to ninth is sold.
    later = rebalance.select(ranked, (ranked[7], ranked[8]), size=5, hold_through=8)
    assert later.keep == (ranked[7],)
    assert later.sell == (ranked[8],)
    assert later.buy == tuple(ranked[:4])


def test_a_scan_signal_is_a_feature_or_a_supplied_score(research_panel, research_lake, ledger):
    with pytest.raises(scan.ScanError, match="not a feature"):
        scan.run(research_panel, scan.Options(signal="alpha"), ledger, lake_dir=research_lake)
    assert ledger.count() == 0
    with pytest.raises(ValueError, match="hold-through"):
        scan.Options(size=30, hold_through=20)
    with pytest.raises(ValueError, match="size"):
        scan.Options(size=0)

    scores = research_panel.frame["mom_12_1"].to_numpy() * -1
    run = scan.run(
        research_panel,
        scan.Options(signal="alpha", size=5, hold_through=8),
        ledger,
        lake_dir=research_lake,
        scores=scores,
    )
    assert run.results["months"] == 30
    assert json.loads(ledger.get(run.trial_id)["features"]) == ["alpha"]


def test_a_lake_without_the_cost_columns_is_refused(tmp_path, ledger):
    from market_fake import Market as ServiceMarket

    root = ServiceMarket(tmp_path / "lake").names(3).land()
    frame = pd.DataFrame(columns=["as_of", "ticker", "mom_12_1"])
    panel = Panel(frame=frame, partition_hash="x")
    from agb import lake as lake_module

    conn = lake_module.connect(root)
    try:
        with pytest.raises(scan.ScanError, match=r"lacks \['closeadj'\]"):
            scan.build_orders(frame, panel, scan.Options(), conn)
    finally:
        conn.close()


def test_the_fake_lake_is_deterministic(tmp_path):
    first = Market(tmp_path / "a", END - timedelta(days=60), END, seed=3).names(2).land()
    second = Market(tmp_path / "b", END - timedelta(days=60), END, seed=3).names(2).land()
    from agb import lake as lake_module

    assert lake_module.partition_digest(first) == lake_module.partition_digest(second)
    assert PANEL_START < END
