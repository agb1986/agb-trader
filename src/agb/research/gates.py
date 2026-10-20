"""The validation gates: what a model's combinatorial paths have to show before it can be
certified, and the diagnostics reported beside them.

Every gate reads returns net of one times the modelled cost, and the whole set certifies
stage 1 of the ranker only, the classical LightGBM ranker. The LLM panel and the blend ride
on top unbacktested, and nothing here says anything about them. The gates, from
docs/architecture/01:

    deflated Sharpe        Bailey and López de Prado's DSR: the probability that the observed
                           Sharpe beats the best Sharpe that `trials` independent tries of
                           nothing would have produced, with the trial count taken from the
                           ledger and never from the run. Fails below the threshold (~0.9).
    overfitting            the probability of backtest overfitting, by combinatorially
                           symmetric cross-validation over the candidate configurations the
                           ledger holds for this window: how often the configuration that
                           looks best in-sample ranks in the worse half out of sample. Fails
                           above the threshold (~0.5).
    cost-inclusive Sharpe  the annualised net Sharpe across paths; the documents give it no
                           number, so its threshold is the least it can be, positive.
    deflated Sharpe at 2x  the DSR again on the paths rerun at twice the modelled cost, from
                           the same trial count: the specification wants DSR to pass there
                           too, and the 2x pass adds no trial.

Reported beside them: the break-even cost multiple, the gross-Sharpe suspicion flag, the
participation the paths needed, the net Sharpe from later start dates, and the path
distribution of maximum drawdown, longest losing run and tracking dispersion, whose quantiles
become the live performance alerts' thresholds (docs/architecture/04).

The arithmetic is on monthly returns: a path is one row of monthly net returns, and a Sharpe
here is the monthly mean over the monthly standard deviation, annualised by the square root
of twelve where it is reported and left monthly inside the DSR, whose `T` is the number of
months. Skewness and kurtosis are pooled over every path's months, and the variance of the
Sharpe across trials, which the DSR needs and the ledger cannot give for every trial, is the
variance across the paths, floored at the sampling variance one over `T`."""

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


class GateError(Exception):
    pass


@dataclass(frozen=True)
class Thresholds:
    """Where each gate fails, and where the gross-Sharpe suspicion starts."""

    dsr: float = 0.9  # DSR fails below this, at 1x and at 2x cost
    pbo: float = 0.5  # PBO fails above this
    sharpe: float = 0.0  # the annualised net Sharpe fails at or below this
    suspicious_sharpe: float = 2.0  # a gross or net annualised Sharpe above this is flagged
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


def deflated_sharpe(paths: np.ndarray, trials: int) -> dict[str, Any]:
    """The deflated Sharpe ratio of the paths given `trials` tries recorded against them.

    The expected best Sharpe of `trials` tries of pure noise is
        SR* = sqrt(V[SR]) * ((1 - g) Z(1 - 1/N) + g Z(1 - 1/(N e)))
    with `g` the Euler-Mascheroni constant and Z the standard normal quantile, zero for one
    trial. The DSR is the probability, given the observed series' length, skewness and
    kurtosis, that the true Sharpe exceeds it:
        DSR = Phi[(SR - SR*) sqrt(T - 1) / sqrt(1 - g3 SR + (g4 - 1) / 4 SR^2)]."""
    if trials < 1:
        raise GateError("the trial count must be at least one")
    rows = _paths(paths)
    months = rows.shape[1]
    if months < 2:
        raise GateError("the deflated Sharpe needs at least two months")
    sharpes = np.array([sharpe(row, annualise=False) for row in rows])
    observed = float(np.mean(sharpes))
    across = float(np.var(sharpes, ddof=1)) if len(sharpes) > 1 else 0.0
    variance = max(across, 1.0 / months)
    pooled = rows.ravel()
    skewness, kurtosis = _skewness(pooled), _kurtosis(pooled)
    if trials == 1:
        expected = 0.0
    else:
        expected = math.sqrt(variance) * (
            (1.0 - EULER_GAMMA) * _NORMAL.inv_cdf(1.0 - 1.0 / trials)
            + EULER_GAMMA * _NORMAL.inv_cdf(1.0 - 1.0 / (trials * math.e))
        )
    scale = 1.0 - skewness * observed + (kurtosis - 1.0) / 4.0 * observed**2
    statistic = (observed - expected) * math.sqrt(months - 1) / math.sqrt(max(scale, 1e-12))
    return {
        "dsr": float(_NORMAL.cdf(statistic)),
        "sharpe_monthly": observed,
        "expected_max_sharpe_monthly": expected,
        "sharpe_variance": variance,
        "skewness": skewness,
        "kurtosis": kurtosis,
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
    A single candidate has no selection to be fooled by, so its PBO is zero."""
    matrix = np.asarray(candidates, dtype="float64")
    if matrix.ndim != 2:
        raise GateError("the candidates must be a months-by-configurations matrix")
    months, count = matrix.shape
    if count < 2:
        return {"pbo": 0.0, "candidates": count, "blocks": 0, "splits": 0}
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
        "pbo": float(np.mean(np.array(logits) < 0.0)),
        "candidates": count,
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
) -> dict[str, Any]:
    """Every gate and diagnostic on one model's paths: `net`, `gross` and `net_2x` are
    paths-by-months matrices of monthly returns at one times cost, before cost, and at twice
    cost; `candidates` the months-by-configurations matrix the overfitting gate reads, which
    includes this model; `trials` the ledger's count; `participation` each path's maximum and
    mean participation. The verdict passes only when every gate does."""
    thresholds = Thresholds() if thresholds is None else thresholds
    net_rows, gross_rows, twice_rows = _paths(net), _paths(gross), _paths(net_2x)
    if not net_rows.shape == gross_rows.shape == twice_rows.shape:
        raise GateError("the net, gross and 2x paths must have the same shape")
    if net_rows.shape[1] != len(months):
        raise GateError("one month label per column")
    deflated = deflated_sharpe(net_rows, trials)
    deflated_2x = deflated_sharpe(twice_rows, trials)
    overfitting = overfitting_probability(candidates, thresholds.blocks)
    sharpe_net = path_sharpe(net_rows)
    sharpe_gross = path_sharpe(gross_rows)
    gates = {
        "dsr": _gate(deflated["dsr"], thresholds.dsr, "at least"),
        "pbo": _gate(overfitting["pbo"], thresholds.pbo, "at most"),
        "sharpe_net": _gate(sharpe_net, thresholds.sharpe, "above"),
        "dsr_2x": _gate(deflated_2x["dsr"], thresholds.dsr, "at least"),
    }
    maxima = [p for p, _ in participation if p is not None]
    means = [m for _, m in participation if m is not None]
    starts = start_date_sensitivity(net_rows, months)
    return {
        "certifies": CERTIFIES,
        "passed": all(gate["passed"] for gate in gates.values()),
        "gates": gates,
        "thresholds": asdict(thresholds),
        "dsr": deflated["dsr"],
        "pbo": overfitting["pbo"],
        "sharpe_net": sharpe_net,
        "dsr_2x": deflated_2x["dsr"],
        "sharpe_gross": sharpe_gross,
        "gross_sharpe_flag": max(sharpe_gross, sharpe_net) > thresholds.suspicious_sharpe,
        "break_even_multiple": break_even_multiple(net_rows, gross_rows),
        "participation_max": max(maxima) if maxima else 0.0,
        "participation_mean": float(np.mean(means)) if means else 0.0,
        "start_dates": starts,
        "start_date_stable": all(start["sharpe_net"] > thresholds.sharpe for start in starts),
        "path_quantiles": path_quantiles(net_rows),
        "deflated_sharpe": deflated,
        "deflated_sharpe_2x": deflated_2x,
        "overfitting": overfitting,
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
