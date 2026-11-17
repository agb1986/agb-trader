"""The comparison the server makes between a certification's manifest and the verdict the gates
give when run again (issue #211): what counts as a difference, the tolerance numbers are held
to, and the thresholds a manifest may say it was gated at. The recompute over a real artifact
is in tests/test_certify.py and tests/test_registry.py."""

import dataclasses
import math

import pytest

from agb.research import gates, recompute


def test_equal_values_have_no_difference():
    value = {"dsr": 0.93, "passed": True, "gates": {"start_dates": [{"start": "2005-04-29"}]}}
    assert recompute.differences(value, value) == []


def test_numbers_are_held_to_a_relative_1e_9():
    assert recompute.REL_TOL == 1e-9
    assert recompute.differences({"dsr": 0.9}, {"dsr": 0.9 * (1 + 1e-12)}) == []
    assert recompute.differences({"dsr": 0.9}, {"dsr": 0.9 * (1 + 1e-6)}) == [
        f"dsr: manifest 0.9, recomputed {0.9 * (1 + 1e-6)!r}"
    ]


def test_numbers_near_zero_are_held_to_an_absolute_1e_12():
    assert recompute.differences({"pbo": 0.0}, {"pbo": 1e-13}) == []
    assert recompute.differences({"pbo": 0.0}, {"pbo": 1e-9}) == [
        "pbo: manifest 0.0, recomputed 1e-09"
    ]


def test_nan_equals_only_nan():
    assert recompute.differences(math.nan, math.nan) == []
    assert recompute.differences({"x": math.nan}, {"x": 0.0}) == ["x: manifest nan, recomputed 0.0"]


def test_a_boolean_is_never_a_number():
    assert recompute.differences({"passed": True}, {"passed": 1}) == [
        "passed: manifest True, recomputed 1"
    ]


def test_an_integer_and_its_float_are_the_same_number():
    assert recompute.differences({"months": 342}, {"months": 342.0}) == []


def test_a_field_added_or_dropped_is_a_difference():
    assert recompute.differences({"dsr": 0.9, "extra": 1}, {"dsr": 0.9, "basis": "x"}) == [
        "basis: manifest absent, recomputed has it",
        "extra: manifest has it, recomputed absent",
    ]


def test_a_list_of_another_length_is_a_difference():
    recorded, found = {"gates": {"start_dates": [1, 2]}}, {"gates": {"start_dates": [1]}}
    assert recompute.differences(recorded, found) == [
        "gates.start_dates: manifest 2 entries, recomputed 1"
    ]


def test_each_difference_is_named_by_its_path():
    recorded = {"gates": {"pbo": {"value": 0.0}, "start_dates": [{"sharpe_net": 0.5}]}}
    found = {"gates": {"pbo": {"value": 0.4}, "start_dates": [{"sharpe_net": 0.6}]}}
    assert recompute.differences(recorded, found) == [
        "gates.pbo.value: manifest 0.0, recomputed 0.4",
        "gates.start_dates[0].sharpe_net: manifest 0.5, recomputed 0.6",
    ]


def _thresholds(**changes) -> dict:
    return {**dataclasses.asdict(gates.Thresholds()), **changes}


def test_the_codes_thresholds_are_accepted():
    assert recompute.thresholds(_thresholds()) == gates.Thresholds()


def test_stricter_thresholds_are_recomputed_at():
    floor = gates.Thresholds()
    found = recompute.thresholds(_thresholds(dsr=floor.dsr + 0.05, pbo=floor.pbo - 0.1))
    assert (found.dsr, found.pbo) == (floor.dsr + 0.05, floor.pbo - 0.1)


@pytest.mark.parametrize(
    "name, worse",
    [
        ("dsr", lambda floor: floor.dsr - 0.1),
        ("pbo", lambda floor: floor.pbo + 0.1),
        ("sharpe", lambda floor: floor.sharpe - 0.1),
        ("suspicious_sharpe", lambda floor: floor.suspicious_sharpe + 1.0),
        ("blocks", lambda floor: floor.blocks + 2),
    ],
)
def test_looser_thresholds_are_refused(name, worse):
    with pytest.raises(recompute.RecomputeError, match=f"looser than this code's: {name} "):
        recompute.thresholds(_thresholds(**{name: worse(gates.Thresholds())}))


def test_thresholds_missing_or_extra_are_refused():
    missing = _thresholds()
    del missing["dsr"]
    with pytest.raises(recompute.RecomputeError, match="thresholds are not"):
        recompute.thresholds(missing)
    with pytest.raises(recompute.RecomputeError, match="thresholds are not"):
        recompute.thresholds(_thresholds(extra=1))
