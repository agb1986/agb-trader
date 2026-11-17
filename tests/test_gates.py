"""The validation gates on synthetic paths: each computed from a paths-by-months matrix with
its threshold, each with a fixture that passes and one that fails (acceptance criterion (a)
of T20), the deflated Sharpe falling as trials are declared (b), and the verdict passing
only when every gate does, the gross-Sharpe and start-date gates included (issue #187). The
certification itself is tests/test_certify.py."""

from itertools import pairwise
from math import comb, sqrt

import numpy as np
import pytest

from agb.research import gates

PATHS, MONTHS = 6, 60
LABELS = [f"{2020 + i // 12}-{1 + i % 12:02d}-28" for i in range(MONTHS)]


def paths(mean: float, spread: float, *, seed: int = 0, months: int = MONTHS) -> np.ndarray:
    """Paths that share most of their months, as the paths of one model do: one draw of the
    months, with a small wobble of its own on each path."""
    rng = np.random.default_rng(seed)
    return rng.normal(mean, spread, size=months) + rng.normal(0.0, 0.005, size=(PATHS, months))


# A model with a real edge: 1.3 percent a month against three percent of noise, an annualised
# Sharpe near 1.7, under the suspicious level of 2. A weak one earns a fraction of that.
STRONG = paths(0.013, 0.03, seed=2)
WEAK = paths(0.002, 0.03)


def lucky(months: int = MONTHS, count: int = 8) -> np.ndarray:
    """Configurations that each shine in their own block of months and lose elsewhere: the
    best one in sample is never the best out of it."""
    block = months // count
    matrix = np.full((months, count), -0.01)
    for i in range(count):
        matrix[i * block : (i + 1) * block, i] = 0.2
    return matrix


def robust(seed: int = 1) -> np.ndarray:
    """One configuration with an edge among nine of pure noise."""
    noise = np.random.default_rng(seed).normal(0.0, 0.03, size=(MONTHS, 9))
    return np.column_stack([STRONG.mean(axis=0), noise])


def fading() -> np.ndarray:
    """The strong model's paths, with the edge gone from the last quarter of the months."""
    late = STRONG.copy()
    late[:, 45:] -= 0.03
    return late


# The deflated Sharpe ratio --------------------------------------------------------------------


def test_a_strong_model_with_few_declared_trials_passes():
    result = gates.deflated_sharpe(STRONG, 3)
    assert result["dsr"] >= gates.Thresholds().dsr
    assert result["trials"] == 3 and result["paths"] == PATHS and result["months"] == MONTHS


def test_a_weak_model_fails():
    assert gates.deflated_sharpe(WEAK, 3)["dsr"] < gates.Thresholds().dsr


def test_declaring_more_trials_lowers_the_deflated_sharpe_until_it_fails():
    counts = (1, 3, 30, 300, 3000)
    values = [gates.deflated_sharpe(STRONG, n)["dsr"] for n in counts]
    assert all(later < earlier for earlier, later in pairwise(values))
    assert values[1] >= gates.Thresholds().dsr > values[3]


def test_one_trial_deflates_nothing():
    result = gates.deflated_sharpe(STRONG, 1)
    assert result["expected_max_sharpe_monthly"] == 0.0
    assert result["dsr"] > gates.deflated_sharpe(STRONG, 2)["dsr"]


def test_the_expected_maximum_grows_with_the_spread_across_paths():
    # The spread across paths is a floor under the variance across trials.
    tight = gates.deflated_sharpe(STRONG, 10)["expected_max_sharpe_monthly"]
    spread = np.vstack([STRONG[:3] - 0.015, STRONG[3:] + 0.015])
    assert gates.deflated_sharpe(spread, 10)["expected_max_sharpe_monthly"] > tight


def test_the_deflated_sharpe_refuses_what_it_cannot_read():
    with pytest.raises(gates.GateError, match="trial count"):
        gates.deflated_sharpe(STRONG, 0)
    with pytest.raises(gates.GateError, match="two months"):
        gates.deflated_sharpe(STRONG[:, :1], 3)
    with pytest.raises(gates.GateError, match="finite"):
        gates.deflated_sharpe(np.where(STRONG > 0.05, np.nan, STRONG), 3)


def test_the_variance_across_trials_raises_the_expected_maximum_and_lowers_the_dsr():
    """Issue #187: the paths of one model share their months, so their Sharpes barely differ;
    the configurations the ledger tried for the window spread much wider, and that spread is
    the one the expected best of `trials` tries is scaled by."""
    alone = gates.deflated_sharpe(STRONG, 10)
    tried = gates.deflated_sharpe(STRONG, 10, robust())
    columns = robust()
    spread = np.var(columns.mean(axis=0) / columns.std(axis=0, ddof=1), ddof=1)
    assert tried["sharpe_variance_trials"] == pytest.approx(spread)
    assert tried["trial_sharpes"] == 10 and alone["trial_sharpes"] == 0
    assert alone["sharpe_variance_trials"] is None
    assert tried["sharpe_variance"] == pytest.approx(max(spread, alone["sharpe_variance"]))
    assert tried["sharpe_variance"] > alone["sharpe_variance"]
    assert tried["expected_max_sharpe_monthly"] > alone["expected_max_sharpe_monthly"]
    assert tried["dsr"] < alone["dsr"]


def test_the_paths_and_the_sampling_variance_floor_the_variance_across_trials():
    series = STRONG.mean(axis=0)
    close = np.column_stack([series, series * 1.0001])
    alone = gates.deflated_sharpe(STRONG, 10)
    tried = gates.deflated_sharpe(STRONG, 10, close)
    assert tried["sharpe_variance_trials"] < alone["sharpe_variance"]
    assert tried["sharpe_variance"] == alone["sharpe_variance"]
    assert alone["sharpe_variance"] >= 1.0 / MONTHS
    assert tried["dsr"] == alone["dsr"]


def test_fewer_than_two_distinct_trials_leave_the_variance_to_the_paths():
    series = STRONG.mean(axis=0)
    alone = gates.deflated_sharpe(STRONG, 10)
    for candidates in (
        np.column_stack([series, series]),
        series[:, np.newaxis],
        np.empty((MONTHS, 0)),
    ):
        result = gates.deflated_sharpe(STRONG, 10, candidates)
        assert result["sharpe_variance_trials"] is None
        assert result["dsr"] == alone["dsr"]
    assert (
        gates.deflated_sharpe(STRONG, 10, np.column_stack([series, series]))["trial_sharpes"] == 1
    )
    with pytest.raises(gates.GateError, match="matrix"):
        gates.deflated_sharpe(STRONG, 10, series)
    with pytest.raises(gates.GateError, match="finite"):
        gates.deflated_sharpe(STRONG, 10, np.where(robust() > 0.05, np.nan, robust()))


def test_the_moments_are_those_of_the_deduplicated_series_and_the_lower_statistic_wins():
    result = gates.deflated_sharpe(STRONG, 3)
    deduplicated, pooled = STRONG.mean(axis=0), STRONG.ravel()
    assert result["skewness"] == gates._skewness(deduplicated)
    assert result["kurtosis"] == gates._kurtosis(deduplicated)
    assert result["skewness_pooled"] == gates._skewness(pooled)
    assert result["kurtosis_pooled"] == gates._kurtosis(pooled)
    args = (result["sharpe_monthly"], result["expected_max_sharpe_monthly"], MONTHS)
    statistics = {
        "deduplicated": gates._deflated_statistic(*args, result["skewness"], result["kurtosis"]),
        "pooled": gates._deflated_statistic(
            *args, result["skewness_pooled"], result["kurtosis_pooled"]
        ),
    }
    lower = min(statistics.values())
    assert result["dsr"] == pytest.approx(gates._NORMAL.cdf(lower))
    assert statistics[result["moments"]] == lower
    # Never above what the pooled moments alone gave, which is what the gate used to read.
    assert result["dsr"] <= gates._NORMAL.cdf(statistics["pooled"])


def test_a_variance_term_that_is_not_positive_raises(monkeypatch):
    monkeypatch.setattr(gates, "_skewness", lambda values: 100.0)
    with pytest.raises(gates.GateError, match="not positive"):
        gates.deflated_sharpe(STRONG, 3)


def test_on_the_excess_basis_the_trials_are_measured_net_of_the_benchmark():
    benchmark = paths(0.001, 0.005, seed=3)
    verdict = verdict_for(benchmark=benchmark)
    excess = robust() - benchmark.mean(axis=0)[:, np.newaxis]
    expected = gates.deflated_sharpe(STRONG - benchmark, 3, excess)
    assert verdict["deflated_sharpe"]["sharpe_variance"] == expected["sharpe_variance"]
    assert verdict["dsr"] == expected["dsr"]
    assert verdict["deflated_sharpe"]["trial_sharpes"] == 10
    with pytest.raises(gates.GateError, match="one row per month"):
        verdict_for(benchmark=benchmark, candidates=robust()[:12])


# The probability of backtest overfitting ------------------------------------------------------


def test_a_configuration_that_holds_up_out_of_sample_passes():
    result = gates.overfitting_probability(robust())
    assert result["pbo"] <= gates.Thresholds().pbo
    assert result["candidates"] == 10 and result["blocks"] == 16
    assert result["splits"] == comb(16, 8)


def test_configurations_that_only_shine_in_sample_fail():
    result = gates.overfitting_probability(lucky(), blocks=8)
    assert result["pbo"] == 1.0 > gates.Thresholds().pbo
    assert result["splits"] == comb(8, 4)


def test_a_single_candidate_leaves_the_overfitting_probability_undefined():
    # Operator decision on issue #166: with nothing to select among, the PBO measures nothing,
    # so it is reported as undefined and fails, where it used to read as a clean zero.
    assert gates.overfitting_probability(STRONG.mean(axis=0)[:, np.newaxis]) == {
        "pbo": 1.0,
        "defined": False,
        "cause": "PBO undefined: 1 distinct candidate, at least two needed",
        "candidates": 1,
        "columns": 1,
        "blocks": 0,
        "splits": 0,
    }
    assert gates.overfitting_probability(np.empty((MONTHS, 0)))["cause"] == (
        "PBO undefined: 0 distinct candidates, at least two needed"
    )


def test_identical_columns_are_one_candidate():
    series = STRONG.mean(axis=0)
    twins = gates.overfitting_probability(np.column_stack([series, series, series]))
    assert twins["candidates"] == 1 and twins["columns"] == 3 and not twins["defined"]
    padded = gates.overfitting_probability(np.column_stack([robust(), robust()[:, :3]]))
    assert padded["candidates"] == 10 and padded["columns"] == 13
    assert padded["pbo"] == gates.overfitting_probability(robust())["pbo"]


def test_a_single_candidate_fails_the_verdict_and_names_the_cause():
    verdict = verdict_for(candidates=STRONG.mean(axis=0)[:, np.newaxis])
    gate = verdict["gates"]["pbo"]
    assert not gate["passed"] and not verdict["passed"]
    assert gate["cause"] == "PBO undefined: 1 distinct candidate, at least two needed"
    # The threshold is the one in force, not loosened or tightened to make the case fail.
    assert gate["threshold"] == gates.Thresholds().pbo
    others = ("dsr", "sharpe_net", "dsr_2x", "gross_sharpe", "start_date")
    assert all(verdict["gates"][name]["passed"] for name in others)


def test_a_declared_configuration_without_a_column_fails_the_overfitting_gate():
    verdict = verdict_for(missing=["t1: a month that is not finite"])
    gate = verdict["gates"]["pbo"]
    assert gate["value"] <= gate["threshold"] and not gate["passed"]
    assert gate["cause"] == "candidate missing: t1: a month that is not finite"
    assert verdict["overfitting"]["missing"] == ["t1: a month that is not finite"]
    assert verdict_for()["gates"]["pbo"]["cause"] is None


def test_an_in_sample_best_that_lands_on_the_median_counts_as_overfit():
    """Issue #187: a tie at the median is no better than chance, so it counts against the
    model. Each half's best is the other half's middle candidate, a logit of exactly zero."""
    tied = np.array(
        [[0.10, 0.02, 0.00], [0.12, 0.03, 0.01], [0.02, 0.10, 0.00], [0.03, 0.12, 0.01]]
    )
    result = gates.overfitting_probability(tied, blocks=2)
    assert result["splits"] == 2 and result["pbo"] == 1.0


def test_the_blocks_never_outnumber_the_months():
    result = gates.overfitting_probability(lucky(10, 5))
    assert result["blocks"] == 10 and result["splits"] == comb(10, 5)


def test_the_overfitting_probability_refuses_a_vector():
    with pytest.raises(gates.GateError, match="matrix"):
        gates.overfitting_probability(STRONG.mean(axis=0))


# The cost-inclusive Sharpe --------------------------------------------------------------------


def test_the_sharpe_is_annualised_from_monthly_returns():
    assert gates.sharpe([0.02, 0.0, 0.04]) == pytest.approx(sqrt(12))
    assert gates.sharpe([0.02, 0.0, 0.04], annualise=False) == pytest.approx(1.0)
    assert gates.sharpe([0.01, 0.01, 0.01]) == 0.0
    assert gates.sharpe([0.01]) == 0.0


def test_the_path_sharpe_is_the_mean_over_paths():
    assert gates.path_sharpe(STRONG) == pytest.approx(np.mean([gates.sharpe(p) for p in STRONG]))


def test_a_model_that_loses_after_cost_fails_the_sharpe_gate():
    verdict = verdict_for(net=STRONG - 0.03)
    assert not verdict["gates"]["sharpe_net"]["passed"] and not verdict["passed"]
    assert verdict["gates"]["sharpe_net"]["rule"] == "above"


# The diagnostics ------------------------------------------------------------------------------


def test_the_break_even_multiple_is_the_gross_mean_over_the_mean_cost():
    gross = STRONG + 0.01
    assert gates.break_even_multiple(STRONG, gross) == pytest.approx(gross.mean() / 0.01)
    assert gates.break_even_multiple(WEAK - 0.03, WEAK - 0.01) == 0.0
    assert gates.break_even_multiple(STRONG, STRONG) is None


def test_a_gross_sharpe_above_two_fails_the_verdict():
    """Issue #187: the suspicion check is a gate, not a flag beside the verdict."""
    calm = paths(0.005, 0.02)
    quiet = verdict_for(net=calm, gross=calm + 0.001)
    assert not quiet["gross_sharpe_flag"] and quiet["gates"]["gross_sharpe"]["passed"]
    loud = verdict_for(net=calm, gross=calm + 0.03)
    gate = loud["gates"]["gross_sharpe"]
    assert loud["gross_sharpe_flag"] and not gate["passed"] and not loud["passed"]
    assert gate["value"] == loud["sharpe_gross"] and gate["rule"] == "at most"
    assert gate["threshold"] == gates.Thresholds().suspicious_sharpe


def test_a_net_sharpe_above_two_trips_the_same_gate():
    hot = STRONG + 0.01
    verdict = verdict_for(net=hot, gross=hot + 0.0001, net_2x=hot - 0.0001)
    assert verdict["sharpe_total"] > gates.Thresholds().suspicious_sharpe
    assert verdict["gross_sharpe_flag"] and not verdict["gates"]["gross_sharpe"]["passed"]
    assert not verdict["passed"]


def test_participation_is_the_worst_path_and_the_mean_path():
    verdict = verdict_for(participation=[(0.05, 0.01), (None, None), (0.02, 0.02)])
    assert verdict["participation_max"] == 0.05
    assert verdict["participation_mean"] == pytest.approx(0.015)


def test_start_date_sensitivity_reruns_from_a_quarter_a_half_and_three_quarters_in():
    starts = gates.start_date_sensitivity(STRONG, LABELS)
    assert [s["start"] for s in starts] == [LABELS[15], LABELS[30], LABELS[45]]
    assert [s["months"] for s in starts] == [45, 30, 15]
    assert starts[1]["sharpe_net"] == pytest.approx(gates.path_sharpe(STRONG[:, 30:]))
    assert gates.start_date_sensitivity(STRONG[:, :6], LABELS[:6]) == []


def test_the_path_quantiles_cover_drawdown_losing_run_and_dispersion():
    rows = np.array([[0.1, -0.1, -0.1, 0.1], [0.0, 0.0, 0.0, 0.0]])
    result = gates.path_quantiles(rows)
    assert result["paths"] == 2 and result["quantiles"] == [0.05, 0.25, 0.5, 0.75, 0.95]
    assert result["max_drawdown"]["min"] == pytest.approx(0.891 / 1.1 - 1.0)
    assert result["max_drawdown"]["max"] == 0.0
    assert result["longest_losing_run"] == {
        "min": 0.0, "max": 2.0, "q05": 0.1, "q25": 0.5, "q50": 1.0, "q75": 1.5, "q95": 1.9,
    }  # fmt: skip
    assert result["tracking_dispersion"]["min"] == result["tracking_dispersion"]["max"]
    assert gates.max_drawdown([]) == 0.0 and gates.longest_losing_run([-0.1, -0.1, 0.1]) == 2


# The verdict ----------------------------------------------------------------------------------


def verdict_for(**overrides):
    inputs = {
        "net": STRONG,
        "gross": STRONG + 0.002,
        "net_2x": STRONG - 0.002,
        "candidates": robust(),
        "trials": 3,
        "months": LABELS,
        "participation": [(0.05, 0.01)] * PATHS,
    }
    return gates.evaluate(**{**inputs, **overrides})


def test_the_verdict_passes_only_when_every_gate_does():
    verdict = verdict_for()
    assert verdict["passed"]
    assert set(verdict["gates"]) == {
        "dsr", "pbo", "sharpe_net", "dsr_2x", "gross_sharpe", "start_date"
    }  # fmt: skip
    assert all(gate["passed"] for gate in verdict["gates"].values())
    assert verdict["certifies"] == "stage 1 (classical ranker) only"
    assert verdict["declared_trials"] == 3 and verdict["months"] == MONTHS
    assert verdict["paths"] == PATHS


@pytest.mark.parametrize(
    ("gate", "overrides"),
    [
        ("dsr", {"trials": 300}),
        ("pbo", {"candidates": lucky(), "thresholds": gates.Thresholds(blocks=8)}),
        ("sharpe_net", {"net": WEAK - 0.01, "gross": WEAK, "net_2x": WEAK - 0.02}),
        ("dsr_2x", {"net_2x": WEAK}),
        ("gross_sharpe", {"gross": STRONG + 0.03}),
        ("start_date", {"net": fading()}),
    ],
)
def test_one_failing_gate_fails_the_verdict(gate, overrides):
    verdict = verdict_for(**overrides)
    assert not verdict["passed"]
    assert not verdict["gates"][gate]["passed"]
    failed = {name for name, g in verdict["gates"].items() if not g["passed"]}
    assert gate in failed
    # The rest of the report is still there for the reader of a failed certification.
    assert verdict["path_quantiles"]["paths"] == PATHS and verdict["start_dates"]


def test_an_edge_that_fades_by_a_later_start_date_fails_the_verdict():
    """Issue #187: the start-date check is a gate. The whole window still clears the Sharpe
    bar; the last quarter of it does not."""
    verdict = verdict_for(net=fading())
    gate = verdict["gates"]["start_date"]
    assert verdict["gates"]["sharpe_net"]["passed"]
    assert not verdict["start_date_stable"] and not gate["passed"] and not verdict["passed"]
    assert gate["value"] == min(start["sharpe_net"] for start in verdict["start_dates"])
    assert gate["value"] <= gate["threshold"] == gates.Thresholds().sharpe
    assert gate["starts"] == 3 and gate["cause"] is None
    assert verdict_for()["gates"]["start_date"]["passed"]


def test_the_second_pass_at_twice_the_cost_is_its_own_gate():
    verdict = verdict_for(net_2x=WEAK)
    assert verdict["gates"]["dsr"]["passed"] and not verdict["gates"]["dsr_2x"]["passed"]
    assert verdict["dsr_2x"] == gates.deflated_sharpe(WEAK, 3, robust())["dsr"]


def test_the_thresholds_are_reported_with_the_verdict():
    verdict = verdict_for(thresholds=gates.Thresholds(dsr=0.95, pbo=0.4))
    assert verdict["thresholds"]["dsr"] == 0.95 and verdict["gates"]["pbo"]["threshold"] == 0.4


def test_mismatched_paths_are_refused():
    with pytest.raises(gates.GateError, match="same shape"):
        verdict_for(gross=STRONG[:, :12])
    with pytest.raises(gates.GateError, match="one month label"):
        verdict_for(months=LABELS[:12])


@pytest.mark.parametrize("bad", [{"dsr": 0.0}, {"pbo": 1.0}, {"blocks": 3}, {"blocks": 0}])
def test_bad_thresholds_are_refused(bad):
    with pytest.raises(ValueError):
        gates.Thresholds(**bad)


# The excess basis (issue #167) ----------------------------------------------------------------


def test_a_book_that_is_the_benchmark_has_no_excess_sharpe_and_fails():
    """Acceptance: a long-only book whose every month is the benchmark's month has a strong
    total-return Sharpe from beta alone, and nothing over the benchmark, so the excess Sharpe
    is 0 and the gate fails where the total-return gate passed it."""
    verdict = verdict_for(benchmark=STRONG)

    assert verdict["sharpe_basis"] == gates.EXCESS
    assert verdict["sharpe_net"] == 0.0
    assert verdict["sharpe_total"] == pytest.approx(gates.path_sharpe(STRONG))
    assert verdict["sharpe_total"] > 0.5
    assert not verdict["gates"]["sharpe_net"]["passed"] and not verdict["passed"]
    assert all(verdict["gates"][key]["basis"] == gates.EXCESS for key in ("dsr", "sharpe_net"))
    assert verdict["gates"]["pbo"]["basis"] == gates.TOTAL, "candidates are ranked on total"
    assert verdict["excess"]["benchmark"] == "SPY" and verdict["excess"]["defined"]
    assert verdict_for()["gates"]["sharpe_net"]["passed"], "the same book on total returns"


def test_a_book_that_beats_the_benchmark_passes_on_excess_returns():
    verdict = verdict_for(benchmark=paths(0.001, 0.005, seed=3))

    assert verdict["passed"] and verdict["sharpe_basis"] == gates.EXCESS
    assert verdict["sharpe_net"] < verdict["sharpe_total"]
    assert all(start["sharpe_total"] is not None for start in verdict["start_dates"])


def test_without_a_benchmark_argument_the_gates_read_total_returns():
    verdict = verdict_for()

    assert verdict["sharpe_basis"] == gates.TOTAL
    assert verdict["sharpe_net"] == verdict["sharpe_total"]
    assert verdict["excess"] == {
        "benchmark": None, "definition": None, "defined": True, "cause": None
    }  # fmt: skip


def test_a_missing_benchmark_fails_the_four_sharpe_gates_and_names_the_cause():
    """The coordinator's rule: no benchmark is a failed gate, not an error, so the artifact is
    written and the paper-only waiver has something to waive. The stored values are finite."""
    verdict = verdict_for(benchmark_missing="the lake holds no SPY return for 3 of 48")

    assert not verdict["passed"] and verdict["sharpe_basis"] == gates.EXCESS
    for key in ("dsr", "sharpe_net", "dsr_2x", "start_date"):
        gate = verdict["gates"][key]
        assert not gate["passed"] and gate["defined"] is False
        assert gate["cause"] == "excess Sharpe undefined: the lake holds no SPY return for 3 of 48"
    assert verdict["gates"]["pbo"]["passed"], "the overfitting gate does not read the benchmark"
    assert verdict["gates"]["gross_sharpe"]["passed"], "gross and total do not need it either"
    assert verdict["gates"]["start_date"]["value"] is None
    assert (verdict["dsr"], verdict["sharpe_net"], verdict["dsr_2x"]) == (0.0, 0.0, 0.0)
    assert verdict["excess"]["defined"] is False and "3 of 48" in verdict["excess"]["cause"]
    assert verdict["sharpe_total"] > 0.5, "the total-return Sharpe is still reported"
    assert not verdict["start_date_stable"]
    assert all(start["sharpe_net"] is None for start in verdict["start_dates"])


def test_a_benchmark_and_a_missing_benchmark_together_are_refused():
    with pytest.raises(gates.GateError):
        verdict_for(benchmark=STRONG, benchmark_missing="both")
    with pytest.raises(gates.GateError, match="the paths' shape"):
        verdict_for(benchmark=STRONG[:, :12])
