"""The certification cache on the fake panel: `agb.research.cache` and the stages that use it.

Acceptance criteria of T33 (issue #104): a second certification over the same panel,
configuration and cost model reuses the cross-validation scores and every path's backtest and
does the work of neither (a); a changed model configuration misses both, and a changed cost
multiple misses the backtest while the cross-validation still hits (b); a run interrupted part
way through the paths resumes at the first path it has no report for (c); an entry written
under another `AGB_VERSION` is not reused (d); and a certification served from the cache
produces the same manifest, digests included, as the run that filled it (e).

The proof that a hit did no work is that the work is counted: `backtest.simulate` is replaced
with a function that fails the test if it is called at all, and `train.fit` with one that
counts, since a certification served entirely from the cache still fits the model it ships.
Counting is stronger than timing a run that takes seconds either way on a fake market."""

import shutil
from dataclasses import asdict, replace

import pandas as pd
import pytest

from agb.research import backtest, cache, certify, cpcv, train, trials
from agb.research.panel import Panel
from agb.research.trials import Ledger

# `certify` refuses to run without a build version.
pytestmark = pytest.mark.usefixtures("certifiable")

MODEL_ID = "ranker-cache.1"
MODEL = train.Config(num_boost_round=20, min_data_in_leaf=5)
OTHER = train.Config(num_boost_round=21, min_data_in_leaf=5)
VALIDATION = cpcv.Options(groups=6, test_groups=2, embargo_months=1)
# The fake market's passing configuration: a six-name book held to the eighth session, ranked
# against three plain-signal priors. Since issue #187 the verdict also fails a Sharpe above 2.0
# on any basis, and the ten-session hold this used before cleared the deflated Sharpe only with a
# gross Sharpe above 2.0.
SIZE, HOLD_THROUGH = 6, 8
# Plain-signal backtests the first certification is ranked against: with no other candidate
# the overfitting gate has nothing to measure and fails (issue #166).
PRIORS = ("mom_12_1", "mom_6_1", "rev_1m")
# The manifest fields a rerun is entitled to differ on: when it ran, where it ran, its own
# trial row, and everything downstream of the ledger that row was appended to. Every other
# field -- every gate value and diagnostic, the paths, the nested trial ids, the declared
# count, the partition digest and the digests of the model and the paths -- is the same work
# and must come out identical.
RERUN_DIFFERS = {
    "certified_at",
    "certified_path",
    "trial_id",
    "ledger_rows",
    "ledger_sha256",
    "artifact_sha256",
    "files",
}


def options(model_id: str) -> certify.Options:
    return certify.Options(model_id=model_id, size=SIZE, hold_through=HOLD_THROUGH)


def trading(cost_multiple: float = 1.0) -> backtest.Options:
    return backtest.Options(
        signal="score",
        size=SIZE,
        hold_through=HOLD_THROUGH,
        init_cash=certify.CAPITAL,
        cost_multiple=cost_multiple,
    )


def certify_run(panel, ledger, lake_dir, model_id, *, config=MODEL, checkpoint=True):
    return certify.run(
        panel, config, VALIDATION, options(model_id), ledger,
        research_dir=ledger.path.parent, lake_dir=lake_dir, checkpoint=checkpoint,
    )  # fmt: skip


def kinds(ledger: Ledger) -> list[str]:
    return [str(row["kind"]) for row in ledger.rows()]


def refuse(what: str):
    def called(*args, **kwargs):
        raise AssertionError(f"{what} was called; the cache should have served this")

    return called


def counted(func, calls: list[int]):
    def call(*args, **kwargs):
        calls.append(1)
        return func(*args, **kwargs)

    return call


def served(monkeypatch) -> list[int]:
    """Refuse every simulation and count every fit.

    A certification served from the cache still fits the configuration over the whole panel,
    which is the model it ships and no part of what the cache holds; that fit is the one call
    `fits` is allowed to have. Nothing else may train and nothing at all may simulate."""
    fits: list[int] = []
    monkeypatch.setattr(train, "fit", counted(train.fit, fits))
    monkeypatch.setattr(backtest, "simulate", refuse("backtest.simulate"))
    return fits


@pytest.fixture(scope="module")
def panel(research_panel) -> Panel:
    return research_panel


@pytest.fixture(scope="module")
def first(panel, research_lake, tmp_path_factory):
    """A research directory with one certification in it, the backtests it was ranked
    against, and its cache."""
    ledger = Ledger(tmp_path_factory.mktemp("cache-research") / trials.FILE)
    for signal in PRIORS:
        options = backtest.Options(signal=signal, size=SIZE, hold_through=HOLD_THROUGH)
        backtest.run(panel, options, ledger, lake_dir=research_lake)
    return ledger, certify_run(panel, ledger, research_lake, MODEL_ID)


@pytest.fixture(scope="module")
def pair(panel, research_lake, first, tmp_path_factory):
    """One certification and the same certification run again, served from its cache.

    An artifact is never overwritten, so the second run is given a copy of the first's
    research directory -- ledger, cache and all -- with the certifications removed. The same
    model id over the same ledger is what makes the two manifests comparable at all: run again
    under another model id it would be another configuration, and the declared trial count and
    the gate that reads it would move for that reason rather than for the cache's.

    `monkeypatch` is per test and this is per module, so the refusal is installed by hand: it
    has to be in force for the run itself, which happens once and is then asserted against
    several times.
    """
    ledger, filled = first
    root = tmp_path_factory.mktemp("cache-rerun")
    shutil.copytree(ledger.path.parent, root, dirs_exist_ok=True)
    shutil.rmtree(root / certify.DIRECTORY)
    again = Ledger(root / trials.FILE)
    before = again.count()
    with pytest.MonkeyPatch.context() as patch:
        fits = served(patch)
        return filled, certify_run(panel, again, research_lake, MODEL_ID), fits, again, before


@pytest.fixture
def scored(panel, first) -> dict[str, pd.Series]:
    """The first certification's path scores, read back out of its cache."""
    ledger, _ = first
    entry = cache.cpcv_key(
        cache.Cache.under(ledger.path.parent),
        panel,
        model=MODEL.to_dict(),
        options={"groups": 6, "test_groups": 2, "embargo_months": 1},
    )
    table = entry.table()
    assert table is not None, "the first certification wrote no cpcv scores"
    paths = cache.scores_from(table, panel)
    assert paths is not None
    return {str(p): paths[p] for p in sorted(paths)}


# (a) and (e): the second certification ------------------------------------------------------


def test_a_second_certification_does_the_work_of_neither_stage(pair):
    _, again, fits, ledger, before = pair
    assert again.passed
    assert len(fits) == 1, "the only fit is the model the certification ships"
    # Its own row and nothing else: the cross-validation and the backtest wrote none.
    assert ledger.count() == before + 1
    assert kinds(ledger)[before:] == ["certification"]


def test_a_cached_certification_names_the_runs_it_reused(pair):
    original, again, _, _, _ = pair
    assert again.manifest["cpcv_trial_id"] == original.manifest["cpcv_trial_id"]
    assert again.manifest["backtest_trial_id"] == original.manifest["backtest_trial_id"]


def test_a_cached_certification_reaches_the_same_verdict_and_digests(pair):
    original, again, _, _, _ = pair
    for field, value in original.manifest.items():
        if field in RERUN_DIFFERS:
            continue
        assert again.manifest[field] == value, field
    # The two fields of `files` that are the computed artifact rather than the ledger.
    for name in ("model.txt", "paths.json"):
        assert again.manifest["files"][name] == original.manifest["files"][name], name


def test_turning_the_cache_off_recomputes_and_writes_the_rows(panel, research_lake, first):
    ledger, original = first
    before = ledger.count()
    again = certify_run(panel, ledger, research_lake, "ranker-cache.2", checkpoint=False)
    assert kinds(ledger)[before:] == ["certification", "cpcv", "backtest"]
    assert again.manifest["cpcv_trial_id"] != original.manifest["cpcv_trial_id"]
    # The same numbers out of the same work. The gates themselves are not compared: this run
    # is a second configuration over the first's ledger, so the overfitting gate has a
    # candidate the first did not and the deflated Sharpe is deflated by one more trial.
    assert again.manifest["sharpe_net"] == original.manifest["sharpe_net"]
    assert again.manifest["path_quantiles"] == original.manifest["path_quantiles"]
    assert again.manifest["participation_max"] == original.manifest["participation_max"]


def test_a_certification_after_a_plain_cpcv_does_no_cross_validation_refit(
    panel, research_lake, tmp_path, monkeypatch
):
    """A cross-validation the operator runs by hand, with no scores asked for, fills the entry
    the certification of the same configuration reads, so the certification refits only the
    model it ships. Issue #188."""
    ledger = Ledger(tmp_path / trials.FILE)
    plain = cpcv.run(panel, MODEL, VALIDATION, ledger, cache=cache.Cache.under(tmp_path))
    fits: list[int] = []
    monkeypatch.setattr(train, "fit", counted(train.fit, fits))
    certified = certify_run(panel, ledger, research_lake, MODEL_ID)
    assert len(fits) == 1, "the only fit is the model the certification ships"
    assert certified.manifest["cpcv_trial_id"] == plain.trial_id
    assert kinds(ledger) == ["cpcv", "certification", "backtest"]


def test_a_plain_cpcv_run_again_is_served_without_a_refit(panel, tmp_path, monkeypatch):
    ledger = Ledger(tmp_path / trials.FILE)
    kept = cache.Cache.under(tmp_path)
    first = cpcv.run(panel, MODEL, VALIDATION, ledger, cache=kept)
    monkeypatch.setattr(train, "fit", refuse("train.fit"))
    again = cpcv.run(panel, MODEL, VALIDATION, ledger, cache=kept)
    assert (again.trial_id, again.results) == (first.trial_id, first.results)
    assert kinds(ledger) == ["cpcv"]


@pytest.mark.parametrize("version", ["test-build", ""])
def test_the_cpcv_command_fills_the_cache_only_for_a_versioned_build(
    panel, version, tmp_path, monkeypatch
):
    """The command a certification follows writes the entry it reads, under the same key, and
    a `dev` build, which `certify` refuses, writes none."""
    from agb.research.__main__ import main

    monkeypatch.setenv("AGB_VERSION", version)
    monkeypatch.setenv("AGB_RESEARCH_DIR", str(tmp_path))
    saved = tmp_path / "panel.parquet"
    panel.save(saved)
    main([
        "cpcv", "--panel", str(saved), "--rounds", "20", "--min-leaf", "5",
        "--groups", "6", "--test-groups", "2", "--embargo-months", "1",
    ])  # fmt: skip
    kept = cache.Cache.under(tmp_path)
    if not version:
        assert not kept.root.exists()
        return
    entry = cache.cpcv_key(
        kept, Panel.load(saved), model=MODEL.to_dict(), options=asdict(VALIDATION)
    )
    payload = entry.read()
    assert payload is not None and entry.table() is not None
    [row] = Ledger(tmp_path / trials.FILE).rows()
    assert payload["trial_id"] == row["trial_id"]


# (b): what a change misses ------------------------------------------------------------------


def test_another_model_configuration_misses_both_stages(panel, research_lake, first):
    ledger, _ = first
    before = ledger.count()
    certify_run(panel, ledger, research_lake, "ranker-cache.3", config=OTHER)
    assert kinds(ledger)[before:] == ["certification", "cpcv", "backtest"]


def test_another_cost_multiple_misses_the_backtest(panel, research_lake, first, scored, tmp_path):
    ledger = Ledger(tmp_path / trials.FILE)
    kept = cache.Cache.under(tmp_path)
    once = backtest.run(panel, trading(), ledger, lake_dir=research_lake, scores=scored, cache=kept)
    before = ledger.count()
    twice = backtest.run(
        panel, trading(2.0), ledger, lake_dir=research_lake, scores=scored, cache=kept
    )
    assert ledger.count() == before + 1, "a different cost multiple is a different backtest"
    assert twice.trial_id != once.trial_id
    assert twice.results["summary"] != once.results["summary"]


def test_a_changed_panel_misses_even_at_the_same_partition_digest(
    panel, research_lake, scored, tmp_path
):
    """A panel cut short is a different panel, though its partition digest is the panel's."""
    ledger = Ledger(tmp_path / trials.FILE)
    kept = cache.Cache.under(tmp_path)
    backtest.run(panel, trading(), ledger, lake_dir=research_lake, scores=scored, cache=kept)
    shorter = Panel(
        frame=panel.frame.iloc[:-1].reset_index(drop=True),
        partition_hash=panel.partition_hash,
        cost_model_version=panel.cost_model_version,
    )
    assert cache.fingerprint(shorter) != cache.fingerprint(panel)
    cut = {label: series.iloc[:-1] for label, series in scored.items()}
    before = ledger.count()
    backtest.run(shorter, trading(), ledger, lake_dir=research_lake, scores=cut, cache=kept)
    assert ledger.count() == before + 1


@pytest.mark.parametrize("column", ["mom_12_1", "forward_return"])
def test_one_changed_value_misses_at_the_same_rows_and_partition_digest(panel, column, tmp_path):
    """A panel rebuilt after a feature or label formula changed has the rows and the partition
    digest of the one before, so only its values tell the two apart. Issue #169."""
    ledger = Ledger(tmp_path / trials.FILE)
    kept = cache.Cache.under(tmp_path)
    cpcv.run(panel, MODEL, VALIDATION, ledger, cache=kept)
    frame = panel.frame.copy()
    row = frame[column].first_valid_index()
    frame.loc[row, column] += 1.0
    changed = Panel(
        frame=frame,
        partition_hash=panel.partition_hash,
        cost_model_version=panel.cost_model_version,
    )
    assert cache.fingerprint(changed) != cache.fingerprint(panel)
    before = ledger.count()
    cpcv.run(changed, MODEL, VALIDATION, ledger, cache=kept)
    assert ledger.count() == before + 1, f"a panel with another {column} value was reused"


# (c): resuming ------------------------------------------------------------------------------


def test_an_interrupted_run_resumes_at_the_first_path_it_has_no_report_for(
    panel, research_lake, scored, tmp_path, monkeypatch
):
    ledger = Ledger(tmp_path / trials.FILE)
    kept = cache.Cache.under(tmp_path)
    whole = backtest.run(
        panel, trading(), ledger, lake_dir=research_lake, scores=scored, cache=kept
    )
    stop_after = 2
    simulate = backtest.simulate
    simulated: list[int] = []
    fall_over = True

    def failing(feed, months, ranked, options, forced=()):
        if fall_over and len(simulated) >= stop_after:
            raise RuntimeError("the engine fell over")
        simulated.append(1)
        return simulate(feed, months, ranked, options, forced)

    broken = Ledger(tmp_path / "interrupted" / trials.FILE)
    cut = cache.Cache.under(tmp_path / "interrupted")
    monkeypatch.setattr(backtest, "simulate", failing)
    with pytest.raises(RuntimeError, match="fell over"):
        backtest.run(panel, trading(), broken, lake_dir=research_lake, scores=scored, cache=cut)
    assert len(simulated) == stop_after
    assert [row["status"] for row in broken.rows()] == ["failed"]

    fall_over = False
    simulated.clear()
    resumed = backtest.run(
        panel, trading(), broken, lake_dir=research_lake, scores=scored, cache=cut
    )
    assert len(simulated) == len(scored) - stop_after, "a finished path was simulated again"
    assert resumed.results["paths"] == whole.results["paths"]


def test_a_half_written_entry_is_a_miss_not_an_error(tmp_path):
    kept = cache.Cache.under(tmp_path)
    entry = kept.key("path", {"path": "0"})
    entry.write({"path": "0", "net": 0.1})
    (entry.directory / cache.ENTRY).write_text("{not json")
    assert entry.read() is None


# (d): the code version ----------------------------------------------------------------------


def test_an_entry_from_another_code_version_is_not_reused(
    panel, research_lake, scored, tmp_path, monkeypatch
):
    ledger = Ledger(tmp_path / trials.FILE)
    kept = cache.Cache.under(tmp_path)
    monkeypatch.setenv("AGB_VERSION", "aaaaaaaaaaaa")
    backtest.run(panel, trading(), ledger, lake_dir=research_lake, scores=scored, cache=kept)
    before = ledger.count()
    monkeypatch.setenv("AGB_VERSION", "bbbbbbbbbbbb")
    backtest.run(panel, trading(), ledger, lake_dir=research_lake, scores=scored, cache=kept)
    assert ledger.count() == before + 1, "an entry of another code version was reused"


def test_a_key_carries_the_record_it_was_hashed_from(tmp_path):
    kept = cache.Cache.under(tmp_path)
    entry = kept.key("cpcv", {"model": {"seed": 1}})
    assert entry.keyed_by["code_version"] == trials.code_version()
    assert entry.keyed_by["format"] == cache.FORMAT
    assert entry.keyed_by["kind"] == "cpcv"


def test_an_entry_whose_record_disagrees_is_a_miss(tmp_path):
    """The stored record is compared, so a digest collision misses rather than lies."""
    kept = cache.Cache.under(tmp_path)
    entry = kept.key("cpcv", {"model": {"seed": 1}})
    entry.write({"trial_id": "abc"})
    assert entry.read() is not None
    other = replace(entry, keyed_by={**entry.keyed_by, "model": {"seed": 2}})
    assert other.read() is None


def test_an_unknown_kind_is_refused(tmp_path):
    with pytest.raises(cache.CacheError, match="unknown cache kind"):
        cache.Cache.under(tmp_path).key("walkforward", {})


def test_an_unknown_backtest_stage_is_refused(panel, tmp_path):
    with pytest.raises(cache.CacheError, match="unknown backtest stage"):
        cache.backtest_key(
            cache.Cache.under(tmp_path), panel, stage="dry-run", options={}, scores={}
        )


# What the cache is keyed by -----------------------------------------------------------------


def test_a_run_and_its_replay_share_the_paths_but_not_the_run(panel, scored, tmp_path):
    kept = cache.Cache.under(tmp_path)
    once = cache.backtest_key(kept, panel, stage="run", options={}, scores=scored)
    again = cache.backtest_key(kept, panel, stage="replay", options={}, scores=scored)
    assert once.digest != again.digest
    paths = cache.path_keys(kept, panel, options={}, scores=scored)
    assert set(paths) == set(scored)
    assert len({key.digest for key in paths.values()}) == len(scored)


def test_a_path_is_keyed_by_every_paths_scores(panel, scored, tmp_path):
    """The feed is loaded for the union of the names any path ranks, so a path's report
    depends on the set it ran with, not on its own scores alone."""
    kept = cache.Cache.under(tmp_path)
    label = sorted(scored)[0]
    alone = cache.path_key(kept, panel, label=label, options={}, scores={label: scored[label]})
    together = cache.path_keys(kept, panel, options={}, scores=scored)[label]
    assert alone.digest != together.digest


def test_a_trial_the_ledger_does_not_hold_is_not_standing(panel, research_lake, scored, tmp_path):
    ledger = Ledger(tmp_path / trials.FILE)
    kept = cache.Cache.under(tmp_path)
    run = backtest.run(panel, trading(), ledger, lake_dir=research_lake, scores=scored, cache=kept)
    assert cache.standing(ledger, run.trial_id)
    assert not cache.standing(Ledger(tmp_path / "elsewhere" / trials.FILE), run.trial_id)
    assert not cache.standing(ledger, None)


def test_a_cache_whose_ledger_is_gone_recomputes(panel, research_lake, scored, tmp_path):
    """A cache carried to a research directory whose ledger does not hold the trial it names
    cannot be used: a manifest may only name rows the ledger it ships holds."""
    ledger = Ledger(tmp_path / trials.FILE)
    kept = cache.Cache.under(tmp_path)
    backtest.run(panel, trading(), ledger, lake_dir=research_lake, scores=scored, cache=kept)
    elsewhere = Ledger(tmp_path / "fresh" / trials.FILE)
    again = backtest.run(
        panel, trading(), elsewhere, lake_dir=research_lake, scores=scored, cache=kept
    )
    assert elsewhere.count() == 1
    assert again.results["summary"]
