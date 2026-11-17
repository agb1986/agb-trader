"""The validation gates: what a model's combinatorial paths have to show before it can be
certified, and the diagnostics reported beside them.

Every gate reads returns net of one times the modelled cost, and the whole set certifies
stage 1 of the ranker only, the classical LightGBM ranker. The LLM panel and the blend ride
on top unbacktested, and nothing here says anything about them.

Given the benchmark's monthly returns, the two deflated Sharpe gates and the cost-inclusive
Sharpe gate read excess returns (issue #167): each path's monthly net return minus the
benchmark's total return over the same month, measured between the same two sessions. A
long-only book holds the market's beta, and on total returns that beta alone clears a
positive-Sharpe bar, so the gates would certify the market rather than the ranker. The basis
is `excess_spy` there and `total` when no benchmark is handed in, as for every certification
written before the change; the total-return Sharpe is reported beside the gated one either
way. When the caller says the benchmark is missing, which certification does if the lake
lacks SPY's return for any traded month, the excess Sharpe is undefined: the three gates fail
with `defined` false and the cause named, and record a finite number that fails them, 0.0 for
the Sharpe (what `sharpe` gives a series with no evidence, and not above the bar) and 0.0 for
both DSRs (no probability of a real edge, and below the bar), because the certification row
holds a number for each whatever the verdict. The overfitting gate keeps total returns: its
candidates are the ledger's backtests, whose rows written before the change carry no
benchmark month, and a matrix with some columns on one basis and some on the other would rank
the basis rather than the configuration.

The gates, from docs/architecture/01:

    deflated Sharpe        Bailey and López de Prado's DSR: the probability that the observed
                           Sharpe beats the best Sharpe that `trials` independent tries of
                           nothing would have produced, with the trial count taken from the
                           ledger and never from the run: the distinct configurations it says
                           were searched, which `agb.research.certify` defines and counts.
                           Fails below the threshold (~0.9).
    overfitting            the probability of backtest overfitting, by combinatorially
                           symmetric cross-validation over the candidate configurations the
                           ledger holds for this window: how often the configuration that
                           looks best in-sample ranks in the worse half out of sample. Fails
                           above the threshold (~0.5). Identical series are one candidate,
                           and below two distinct candidates there is no selection to measure,
                           so the PBO is undefined and the gate fails with that cause named.
                           It also fails when a configuration the ledger declares should have
                           had a series and has none.
    cost-inclusive Sharpe  the annualised net Sharpe across paths, of excess returns when
                           the benchmark is given; the documents give it no number, so its
                           threshold is the least it can be, positive.
    deflated Sharpe at 2x  the DSR again on the paths rerun at twice the modelled cost, from
                           the same trial count: the specification wants DSR to pass there
                           too, and the 2x pass adds no trial.
    gross Sharpe           the suspicion check of docs/architecture/02: the highest of the
                           gross, total-return net and gated net annualised Sharpes must not
                           exceed the suspicious level (~2). A Sharpe that high on monthly
                           rebalancing is far likelier a leak than an edge, so it fails the
                           verdict rather than being flagged beside it (issue #187).
                           `gross_sharpe_flag` still reports the same check as a boolean.
    start date             the gated net Sharpe from each later start date (a quarter, a half
                           and three quarters of the way in, given eight months) must clear
                           the Sharpe bar: an edge that lives only in the first months fails
                           (issue #187). `start_date_stable` still reports it as a boolean, and
                           it fails with the cause named when the excess Sharpe is undefined.

Reported beside them: the total-return net Sharpe, the break-even cost multiple, the
participation the paths needed, the gated net Sharpe from each later start date, and the path
distribution of maximum drawdown, longest losing run and tracking dispersion, whose quantiles
become the live performance alerts' thresholds (docs/architecture/04). The break-even multiple
and the path distribution stay on total returns: a live month is compared against them as it
is, not net of the benchmark.

The arithmetic is on monthly returns: a path is one row of monthly net returns, and a Sharpe
here is the monthly mean over the monthly standard deviation, annualised by the square root
of twelve where it is reported and left monthly inside the DSR, whose `T` is the number of
months. The variance of the Sharpe across trials, which sets the expected best Sharpe of
`trials` tries, is the variance of the monthly Sharpe across the distinct candidate
configurations the ledger holds for this window, on the gated basis; the variance across this
model's paths and the sampling variance one over `T` are floors under it, so it is never
smaller than the paths alone gave. The skewness and kurtosis are those of the de-duplicated
monthly series, the paths' mean month, since the paths share most of their months and pooling
them repeats each month once per path; the pooled moments are still computed, and the DSR is
the lower of the two statistics. A variance term in the DSR's denominator that is not positive
raises rather than being floored. See `deflated_sharpe`."""

import math
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from itertools import combinations
from statistics import NormalDist
from typing import Any

import numpy as np

CERTIFIES = "stage 1 (classical ranker) only"
MONTHS_PER_YEAR = 12
EULER_GAMMA = 0.5772156649015329
QUANTILES = (0.05, 0.25, 0.5, 0.75, 0.95)
_NORMAL = NormalDist()
# The Sharpe basis a verdict records, and the benchmark the excess basis is measured over:
# `total` is the net return itself, `excess_spy` the net return minus SPY's over the same month.
TOTAL, EXCESS = "total", "excess_spy"
BASES = (TOTAL, EXCESS)
BENCHMARK = "SPY"
EXCESS_DEFINITION = (
    "excess return: each month's return net of modelled cost minus SPY's total return"
    " (split- and dividend-adjusted close to close) between the same two sessions"
)
# What an undefined excess Sharpe records, so the row holds a number that fails its gate.
UNDEFINED_SHARPE = 0.0
UNDEFINED_DSR = 0.0


class GateError(Exception):
    pass


@dataclass(frozen=True)
class Thresholds:
    """Where each gate fails, and where the gross-Sharpe suspicion starts."""

    dsr: float = 0.9  # DSR fails below this, at 1x and at 2x cost
    pbo: float = 0.5  # PBO fails above this
    sharpe: float = 0.0  # the annualised net Sharpe fails at or below this
    suspicious_sharpe: float = 2.0  # a gross or net annualised Sharpe above this fails
    blocks: int = 16  # the CSCV block count, even; reduced to the months when they are fewer

    def __post_init__(self) -> None:
        if not 0.0 < self.dsr <= 1.0:
            raise ValueError("the DSR threshold must be in (0, 1]")
        if not 0.0 <= self.pbo < 1.0:
            raise ValueError("the PBO threshold must be in [0, 1)")
        if self.blocks < 2 or self.blocks % 2:
            raise ValueError("the CSCV block count must be even and at least two")


# The gates ------------------------------------------------------------------------------------


def sharpe(returns: Sequence[float] | np.ndarray, *, annualise: bool = True) -> float:
    """The Sharpe ratio of a monthly return series, zero when it has no spread."""
    values = np.asarray(returns, dtype="float64")
    if len(values) < 2:
        return 0.0
    spread = float(np.std(values, ddof=1))
    if spread == 0.0:
        return 0.0
    ratio = float(np.mean(values)) / spread
    return ratio * math.sqrt(MONTHS_PER_YEAR) if annualise else ratio


def path_sharpe(paths: np.ndarray) -> float:
    """The annualised Sharpe of a set of paths: the mean of each path's own."""
    return float(np.mean([sharpe(path) for path in _paths(paths)]))


def deflated_sharpe(
    paths: np.ndarray, trials: int, candidates: np.ndarray | None = None
) -> dict[str, Any]:
    """The deflated Sharpe ratio of the paths given `trials` tries recorded against them.

    The expected best Sharpe of `trials` tries of pure noise is
        SR* = sqrt(V[SR]) * ((1 - g) Z(1 - 1/N) + g Z(1 - 1/(N e)))
    with `g` the Euler-Mascheroni constant and Z the standard normal quantile, zero for one
    trial. The DSR is the probability, given the observed series' length, skewness and
    kurtosis, that the true Sharpe exceeds it:
        DSR = Phi[(SR - SR*) sqrt(T - 1) / sqrt(1 - g3 SR + (g4 - 1) / 4 SR^2)].

    `V[SR]` is the variance of the monthly Sharpe across the trials: across the distinct
    columns of `candidates`, a months-by-configurations matrix of the configurations the ledger
    holds for this window on the paths' own basis, when it has at least two. The paths of one
    model share most of their months, so their spread understates it (issue #187); it is still
    taken, with the sampling variance one over `T`, as a floor under the trials' variance, so
    that the variance never comes out smaller than it did before the trials were read.

    The skewness and kurtosis are those of the de-duplicated monthly series, the paths' mean
    month, rather than of every path's months pooled, which repeat each month once per path
    that holds it. The pooled moments are still computed, and the DSR is the lower of the two,
    so that reading the de-duplicated series never raises it. A variance term
    `1 - g3 SR + (g4 - 1) / 4 SR^2` that is not positive raises `GateError` rather than being
    floored into an extreme statistic."""
    if trials < 1:
        raise GateError("the trial count must be at least one")
    rows = _paths(paths)
    months = rows.shape[1]
    if months < 2:
        raise GateError("the deflated Sharpe needs at least two months")
    sharpes = np.array([sharpe(row, annualise=False) for row in rows])
    observed = float(np.mean(sharpes))
    across_paths = float(np.var(sharpes, ddof=1)) if len(sharpes) > 1 else 0.0
    across_trials, trial_count = _trial_variance(candidates)
    variance = max(across_paths, 1.0 / months, across_trials or 0.0)
    if trials == 1:
        expected = 0.0
    else:
        expected = math.sqrt(variance) * (
            (1.0 - EULER_GAMMA) * _NORMAL.inv_cdf(1.0 - 1.0 / trials)
            + EULER_GAMMA * _NORMAL.inv_cdf(1.0 - 1.0 / (trials * math.e))
        )
    deduplicated, pooled = rows.mean(axis=0), rows.ravel()
    moments = {
        "deduplicated": (_skewness(deduplicated), _kurtosis(deduplicated)),
        "pooled": (_skewness(pooled), _kurtosis(pooled)),
    }
    statistics = {
        name: _deflated_statistic(observed, expected, months, skewness, kurtosis)
        for name, (skewness, kurtosis) in moments.items()
    }
    # The lower statistic, the de-duplicated moments on a tie.
    chosen = min(statistics, key=lambda name: statistics[name])
    return {
        "dsr": float(_NORMAL.cdf(statistics[chosen])),
        "sharpe_monthly": observed,
        "expected_max_sharpe_monthly": expected,
        "sharpe_variance": variance,
        "sharpe_variance_trials": across_trials,
        "sharpe_variance_paths": across_paths,
        "trial_sharpes": trial_count,
        "skewness": moments["deduplicated"][0],
        "kurtosis": moments["deduplicated"][1],
        "skewness_pooled": moments["pooled"][0],
        "kurtosis_pooled": moments["pooled"][1],
        "moments": chosen,
        "months": months,
        "trials": trials,
        "paths": len(rows),
    }


def overfitting_probability(candidates: np.ndarray, blocks: int = 16) -> dict[str, Any]:
    """The probability of backtest overfitting over `candidates`, a months-by-configurations
    matrix of monthly net returns, by combinatorially symmetric cross-validation: the months
    are cut into `blocks` even blocks, and for every way of taking half of them as the
    in-sample set, the configuration with the best in-sample Sharpe is ranked out of sample
    among all of them. The PBO is the share of those splits where it lands in the worse half.

    Identical columns are one configuration read twice, so they are reduced to one before
    anything is ranked: a duplicate would tie with its twin and pull the rank to the middle.
    Below two distinct candidates there is no selection to be fooled by, which leaves the PBO
    undefined rather than zero: an undefined PBO is reported as 1.0 with `defined` false and
    the `cause` named, so the gate fails and says why. 1.0 rather than none because the
    certification row records a number in [0, 1] whatever the verdict."""
    matrix = np.asarray(candidates, dtype="float64")
    if matrix.ndim != 2:
        raise GateError("the candidates must be a months-by-configurations matrix")
    columns = matrix.shape[1]
    if columns:
        _, first = np.unique(matrix, axis=1, return_index=True)
        matrix = matrix[:, np.sort(first)]
    months, count = matrix.shape
    if count < 2:
        return {
            "pbo": 1.0,
            "defined": False,
            "cause": (
                f"PBO undefined: {count} distinct candidate"
                f"{'' if count == 1 else 's'}, at least two needed"
            ),
            "candidates": count,
            "columns": columns,
            "blocks": 0,
            "splits": 0,
        }
    if months < 2:
        raise GateError("the probability of backtest overfitting needs at least two months")
    used = min(blocks, months - months % 2)
    pieces = np.array_split(np.arange(months), used)
    logits = []
    for chosen in combinations(range(used), used // 2):
        inside = np.concatenate([pieces[i] for i in chosen])
        outside = np.concatenate([pieces[i] for i in range(used) if i not in chosen])
        best = int(np.argmax(_column_sharpes(matrix[inside])))
        out = _column_sharpes(matrix[outside])
        rank = float(np.sum(out < out[best]) + 0.5 * (np.sum(out == out[best]) - 1) + 1)
        relative = rank / (count + 1)
        logits.append(math.log(relative / (1.0 - relative)))
    return {
        # A logit of exactly zero is the in-sample best landing on the median out of sample:
        # no better than chance, so it counts as overfit (issue #187).
        "pbo": float(np.mean(np.array(logits) <= 0.0)),
        "defined": True,
        "cause": None,
        "candidates": count,
        "columns": columns,
        "blocks": used,
        "splits": len(logits),
    }


def break_even_multiple(net: np.ndarray, gross: np.ndarray) -> float | None:
    """The multiple of the modelled cost at which the net mean return reaches zero: the gross
    mean over the mean cost, pooled over every path's months. Zero when the gross mean is
    already negative and none when nothing was charged."""
    net_rows, gross_rows = _paths(net), _paths(gross)
    cost = float(np.mean(gross_rows - net_rows))
    if cost <= 0.0:
        return None
    return max(float(np.mean(gross_rows)) / cost, 0.0)


def start_date_sensitivity(
    net: np.ndarray, months: Sequence[str], starts: Sequence[int] | None = None
) -> list[dict[str, Any]]:
    """The net Sharpe of the paths from later start dates: by default a quarter, a half and
    three quarters of the way through, when there are at least eight months."""
    rows = _paths(net)
    count = rows.shape[1]
    if starts is None:
        starts = [count // 4, count // 2, 3 * count // 4] if count >= 8 else []
    chosen = sorted({i for i in starts if 0 < i < count - 1})
    return [
        {"start": months[i], "months": count - i, "sharpe_net": path_sharpe(rows[:, i:])}
        for i in chosen
    ]


def path_quantiles(net: np.ndarray) -> dict[str, Any]:
    """The distribution across paths of the maximum drawdown, the longest losing run in
    months, the tracking dispersion (the standard deviation of a path's monthly return around
    the paths' mean month) and the mean month itself, at the standard quantiles plus the
    extremes. The live monitoring alerts read these as their thresholds, which is why the
    mean month is here: it is the expectation a live month is compared against."""
    rows = _paths(net)
    drawdowns = [max_drawdown(row) for row in rows]
    runs = [longest_losing_run(row) for row in rows]
    centre = rows.mean(axis=0)
    dispersion = [float(np.std(row - centre, ddof=1)) if rows.shape[1] > 1 else 0.0 for row in rows]
    monthly = [float(row.mean()) if rows.shape[1] else 0.0 for row in rows]
    return {
        "paths": len(rows),
        "quantiles": list(QUANTILES),
        "max_drawdown": _distribution(drawdowns),
        "longest_losing_run": _distribution(runs),
        "tracking_dispersion": _distribution(dispersion),
        "monthly_return": _distribution(monthly),
    }


def max_drawdown(returns: Sequence[float] | np.ndarray) -> float:
    """The deepest fall of the compounded series from its running peak, as a negative
    fraction."""
    marks = np.cumprod(1.0 + np.asarray(returns, dtype="float64"))
    peaks = np.maximum.accumulate(np.concatenate([[1.0], marks]))[1:]
    return float((marks / peaks - 1.0).min()) if len(marks) else 0.0


def longest_losing_run(returns: Sequence[float] | np.ndarray) -> int:
    longest = current = 0
    for value in returns:
        current = current + 1 if value < 0.0 else 0
        longest = max(longest, current)
    return longest


# The verdict ----------------------------------------------------------------------------------


def evaluate(
    *,
    net: np.ndarray,
    gross: np.ndarray,
    net_2x: np.ndarray,
    candidates: np.ndarray,
    trials: int,
    months: Sequence[str],
    participation: Sequence[tuple[float | None, float | None]],
    thresholds: Thresholds | None = None,
    missing: Sequence[str] = (),
    benchmark: np.ndarray | None = None,
    benchmark_missing: str | None = None,
) -> dict[str, Any]:
    """Every gate and diagnostic on one model's paths: `net`, `gross` and `net_2x` are
    paths-by-months matrices of monthly returns at one times cost, before cost, and at twice
    cost; `candidates` the months-by-configurations matrix the overfitting gate reads, which
    includes this model; `trials` the ledger's count; `participation` each path's maximum and
    mean participation; `missing` the reasons any configuration the ledger declares has no
    column in `candidates` though it should, each of which fails the overfitting gate, since
    a PBO over a matrix that leaves out a configuration that was tried is not the PBO of the
    search. The verdict passes only when every gate does.

    `benchmark`, a paths-by-months matrix of the benchmark's return over each path's months,
    puts the DSR, 2x DSR and Sharpe gates on excess returns; `benchmark_missing`, the reason
    there is no such matrix, puts them on the excess basis undefined, failing with that cause.
    Neither leaves them on total returns. `sharpe_net` is the gated Sharpe, on the basis the
    verdict's `sharpe_basis` names, `sharpe_total` the total-return net Sharpe whatever the
    basis, and `sharpe_gross` the total-return Sharpe before cost."""
    thresholds = Thresholds() if thresholds is None else thresholds
    net_rows, gross_rows, twice_rows = _paths(net), _paths(gross), _paths(net_2x)
    if not net_rows.shape == gross_rows.shape == twice_rows.shape:
        raise GateError("the net, gross and 2x paths must have the same shape")
    if net_rows.shape[1] != len(months):
        raise GateError("one month label per column")
    if benchmark is not None and benchmark_missing is not None:
        raise GateError("a benchmark is either given or missing, not both")
    basis = TOTAL if benchmark is None and benchmark_missing is None else EXCESS
    cause = None
    if benchmark_missing is not None:
        cause = f"excess Sharpe undefined: {benchmark_missing}"
    if benchmark is not None:
        bench_rows = _paths(benchmark)
        if bench_rows.shape != net_rows.shape:
            raise GateError("the benchmark must have the paths' shape")
        gated, gated_2x = net_rows - bench_rows, twice_rows - bench_rows
    else:
        gated, gated_2x = net_rows, twice_rows
    # The trials the DSR deflates by, on the gated basis: the candidates are total returns, so
    # on the excess basis each month's benchmark return, the same on every path holding it,
    # comes off every configuration's month.
    trial_matrix = np.asarray(candidates, dtype="float64")
    if benchmark is not None and trial_matrix.ndim == 2 and trial_matrix.shape[1]:
        if trial_matrix.shape[0] != net_rows.shape[1]:
            raise GateError("the candidates must have one row per month of the paths")
        trial_matrix = trial_matrix - bench_rows.mean(axis=0)[:, np.newaxis]
    sharpe_total = path_sharpe(net_rows)
    sharpe_gross = path_sharpe(gross_rows)
    if cause is None:
        deflated = {
            **deflated_sharpe(gated, trials, trial_matrix),
            "defined": True,
            "cause": None,
        }
        deflated_2x = {
            **deflated_sharpe(gated_2x, trials, trial_matrix),
            "defined": True,
            "cause": None,
        }
        sharpe_net = path_sharpe(gated)
        starts = [
            {**start, "sharpe_total": total["sharpe_net"]}
            for start, total in zip(
                start_date_sensitivity(gated, months),
                start_date_sensitivity(net_rows, months),
                strict=True,
            )
        ]
        stable = all(start["sharpe_net"] > thresholds.sharpe for start in starts)
    else:
        deflated = deflated_2x = {
            "dsr": UNDEFINED_DSR,
            "defined": False,
            "cause": cause,
            "months": net_rows.shape[1],
            "trials": trials,
            "paths": len(net_rows),
        }
        sharpe_net = UNDEFINED_SHARPE
        starts = [
            {**start, "sharpe_net": None, "sharpe_total": start["sharpe_net"]}
            for start in start_date_sensitivity(net_rows, months)
        ]
        stable = False
    overfitting = overfitting_probability(candidates, thresholds.blocks)
    sharpes = [sharpe_gross, sharpe_total] + ([sharpe_net] if cause is None else [])
    gates = {
        "dsr": _sharpe_gate(deflated["dsr"], thresholds.dsr, "at least", basis, cause),
        "pbo": {**_overfitting_gate(overfitting, thresholds.pbo, missing), "basis": TOTAL},
        "sharpe_net": _sharpe_gate(sharpe_net, thresholds.sharpe, "above", basis, cause),
        "dsr_2x": _sharpe_gate(deflated_2x["dsr"], thresholds.dsr, "at least", basis, cause),
        "gross_sharpe": _gate(max(sharpes), thresholds.suspicious_sharpe, "at most"),
        "start_date": _start_date_gate(starts, thresholds.sharpe, stable, basis, cause),
    }
    maxima = [p for p, _ in participation if p is not None]
    means = [m for _, m in participation if m is not None]
    return {
        "certifies": CERTIFIES,
        "passed": all(gate["passed"] for gate in gates.values()),
        "gates": gates,
        "thresholds": asdict(thresholds),
        "sharpe_basis": basis,
        "excess": {
            "benchmark": BENCHMARK if basis == EXCESS else None,
            "definition": EXCESS_DEFINITION if basis == EXCESS else None,
            "defined": cause is None,
            "cause": cause,
        },
        "dsr": deflated["dsr"],
        "pbo": overfitting["pbo"],
        "sharpe_net": sharpe_net,
        "dsr_2x": deflated_2x["dsr"],
        "sharpe_total": sharpe_total,
        "sharpe_gross": sharpe_gross,
        "gross_sharpe_flag": max(sharpes) > thresholds.suspicious_sharpe,
        "break_even_multiple": break_even_multiple(net_rows, gross_rows),
        "participation_max": max(maxima) if maxima else 0.0,
        "participation_mean": float(np.mean(means)) if means else 0.0,
        "start_dates": starts,
        "start_date_stable": stable,
        "path_quantiles": path_quantiles(net_rows),
        "deflated_sharpe": deflated,
        "deflated_sharpe_2x": deflated_2x,
        "overfitting": {**overfitting, "missing": list(missing)},
        "declared_trials": trials,
        "months": len(months),
        "paths": len(net_rows),
    }


def _gate(value: float, threshold: float, rule: str) -> dict[str, Any]:
    passed = {
        "at least": value >= threshold,
        "at most": value <= threshold,
        "above": value > threshold,
    }[rule]
    return {"value": value, "threshold": threshold, "rule": rule, "passed": bool(passed)}


def _sharpe_gate(
    value: float, threshold: float, rule: str, basis: str, cause: str | None
) -> dict[str, Any]:
    """A gate on the Sharpe of the paths, which names the basis it was measured on and, when
    the excess Sharpe is undefined, fails with the cause named whatever value it records."""
    gate = {**_gate(value, threshold, rule), "basis": basis, "defined": cause is None}
    if cause is not None:
        gate["passed"] = False
    gate["cause"] = cause
    return gate


def _start_date_gate(
    starts: Sequence[dict[str, Any]],
    threshold: float,
    stable: bool,
    basis: str,
    cause: str | None,
) -> dict[str, Any]:
    """The start-date gate: the gated net Sharpe from every later start date must clear the
    Sharpe bar, as docs/architecture/02 requires. Its value is the lowest of them, none when
    there is no later start date to read or the excess Sharpe is undefined, in which case the
    gate fails with the cause named."""
    sharpes = [start["sharpe_net"] for start in starts if start["sharpe_net"] is not None]
    lowest = min(sharpes) if sharpes and cause is None else None
    return {
        "value": lowest,
        "threshold": threshold,
        "rule": "above",
        "passed": bool(stable) and cause is None,
        "basis": basis,
        "defined": cause is None,
        "cause": cause,
        "starts": len(starts),
    }


def _overfitting_gate(
    overfitting: dict[str, Any], threshold: float, missing: Sequence[str]
) -> dict[str, Any]:
    """The PBO gate, which also fails, with the cause named, when the PBO is undefined or a
    declared configuration is missing from the matrix it was computed over."""
    gate = _gate(overfitting["pbo"], threshold, "at most")
    causes = [] if overfitting["defined"] else [overfitting["cause"]]
    causes += [f"candidate missing: {reason}" for reason in missing]
    if causes:
        gate["passed"] = False
    gate["cause"] = "; ".join(causes) or None
    return gate


# Arithmetic -----------------------------------------------------------------------------------


def _paths(paths: np.ndarray) -> np.ndarray:
    rows = np.asarray(paths, dtype="float64")
    if rows.ndim == 1:
        rows = rows[np.newaxis, :]
    if rows.ndim != 2 or rows.shape[0] < 1 or rows.shape[1] < 1:
        raise GateError("the paths must be a paths-by-months matrix")
    if not np.isfinite(rows).all():
        raise GateError("every monthly return must be finite")
    return rows


def _trial_variance(candidates: np.ndarray | None) -> tuple[float | None, int]:
    """The variance of the monthly Sharpe across the distinct columns of `candidates`, and how
    many there were: none when no matrix is given or it has fewer than two distinct columns or
    two months, since one Sharpe has no spread to measure."""
    if candidates is None:
        return None, 0
    matrix = np.asarray(candidates, dtype="float64")
    if matrix.ndim != 2:
        raise GateError("the candidates must be a months-by-configurations matrix")
    if matrix.shape[1] and not np.isfinite(matrix).all():
        raise GateError("every candidate's monthly return must be finite")
    if matrix.shape[1]:
        _, first = np.unique(matrix, axis=1, return_index=True)
        matrix = matrix[:, np.sort(first)]
    count = matrix.shape[1]
    if count < 2 or matrix.shape[0] < 2:
        return None, count
    return float(np.var(_column_sharpes(matrix), ddof=1)), count


def _deflated_statistic(
    observed: float, expected: float, months: int, skewness: float, kurtosis: float
) -> float:
    """The DSR's standardised statistic, which refuses a variance term that is not positive:
    flooring it would turn a series whose moments make no sense into an extreme statistic,
    a near-certain pass when the Sharpe beats the expected maximum."""
    scale = 1.0 - skewness * observed + (kurtosis - 1.0) / 4.0 * observed**2
    if not scale > 0.0:
        raise GateError(
            f"the deflated Sharpe's variance term is {scale:.6g}, not positive: skewness "
            f"{skewness:.6g} and kurtosis {kurtosis:.6g} at a monthly Sharpe of {observed:.6g}"
        )
    return (observed - expected) * math.sqrt(months - 1) / math.sqrt(scale)


def _column_sharpes(matrix: np.ndarray) -> np.ndarray:
    means = matrix.mean(axis=0)
    spreads = matrix.std(axis=0, ddof=1) if matrix.shape[0] > 1 else np.zeros(matrix.shape[1])
    with np.errstate(divide="ignore", invalid="ignore"):
        ratios = np.where(spreads > 0.0, means / np.where(spreads > 0.0, spreads, 1.0), 0.0)
    return ratios


def _skewness(values: np.ndarray) -> float:
    centred = values - values.mean()
    m2 = float(np.mean(centred**2))
    return float(np.mean(centred**3) / m2**1.5) if m2 > 0.0 else 0.0


def _kurtosis(values: np.ndarray) -> float:
    """Raw kurtosis: three for a normal series."""
    centred = values - values.mean()
    m2 = float(np.mean(centred**2))
    return float(np.mean(centred**4) / m2**2) if m2 > 0.0 else 3.0


def _distribution(values: Sequence[float]) -> dict[str, Any]:
    array = np.asarray(values, dtype="float64")
    return {
        "min": float(array.min()),
        "max": float(array.max()),
        **{f"q{int(q * 100):02d}": float(np.quantile(array, q)) for q in QUANTILES},
    }
