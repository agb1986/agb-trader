"""The backtest's and the scan's outputs on the fake lake, pinned to the last digit.

Issue #188 restructured both: one setup shared by `backtest.run` and `backtest.replay`, one
metrics function shared by the backtest and the scan, and the bars built once per run rather
than once per path. None of that is meant to move a number, so the outputs of every entry
point it touched were captured before the change and are held here as golden values: a
single-signal backtest with its start-date runs, a three-path backtest, the same paths
replayed at twice the cost, and two scans, one of a feature and one of a model's scores.

The values are compared exactly, floats included. JSON keeps a float's shortest repr, which
reads back as the same double, so equality here is bit-for-bit.

To regenerate after a change that is meant to move them, run this module's `capture` with
`.venv/bin/python -c "import sys; sys.path.insert(0, 'tests'); import test_research_golden
as g; g.capture()"`, and say in the change why the numbers moved."""

import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest

from agb.research import backtest, scan
from agb.research.panel import Panel
from agb.research.trials import Ledger

GOLDEN = Path(__file__).parent / "golden" / "research.json"
SINGLE = backtest.Options(signal="mom_12_1", size=5, hold_through=8)
PATHS = backtest.Options(signal="score", size=4, hold_through=7)
REPLAY = backtest.Options(signal="score", size=4, hold_through=7, cost_multiple=2.0)
SCAN = scan.Options(signal="mom_12_1", size=5, hold_through=8)
SCAN_SCORES = scan.Options(signal="score", size=4, hold_through=7)


def path_scores(panel: Panel) -> dict[str, pd.Series]:
    """Three paths' scores over every row of the panel, from seeded draws: stable across
    numpy versions, since the generator's stream is."""
    index = panel.frame.index
    return {
        str(path): pd.Series(np.random.default_rng(path).standard_normal(len(index)), index=index)
        for path in range(3)
    }


def outputs(panel: Panel, lake_dir: Path, research_dir: Path) -> dict[str, Any]:
    """Every pinned output, as the JSON the golden file holds."""
    scores = path_scores(panel)

    def ledger(name: str) -> Ledger:
        return Ledger(research_dir / name / "trials.parquet")

    result = {
        "backtest_single": backtest.run(panel, SINGLE, ledger("single"), lake_dir=lake_dir).results,
        "backtest_paths": backtest.run(
            panel, PATHS, ledger("paths"), lake_dir=lake_dir, scores=scores
        ).results,
        "replay": backtest.replay(panel, REPLAY, scores, lake_dir=lake_dir),
        "scan": scan.run(panel, SCAN, ledger("scan"), lake_dir=lake_dir).results,
        "scan_scores": scan.run(
            panel, SCAN_SCORES, ledger("scan-scores"), lake_dir=lake_dir, scores=scores["0"]
        ).results,
    }
    return json.loads(json.dumps(result, sort_keys=True, default=str))


def capture() -> None:  # pragma: no cover - run by hand, as the module docstring says
    import tempfile

    from research_fake import END, PANEL_START, Market

    from agb.research import panel as panel_module

    with tempfile.TemporaryDirectory() as root:
        lake_dir = Market(Path(root) / "lake").names().benchmark().land()
        built = panel_module.build(lake_dir, PANEL_START, END)
        values = outputs(built, lake_dir, Path(root) / "research")
    GOLDEN.parent.mkdir(exist_ok=True)
    GOLDEN.write_text(json.dumps(values, sort_keys=True, indent=1) + "\n")


@pytest.fixture(scope="module")
def captured(research_panel, research_lake, tmp_path_factory) -> dict[str, Any]:
    return outputs(research_panel, research_lake, tmp_path_factory.mktemp("golden"))


@pytest.fixture(scope="module")
def golden() -> dict[str, Any]:
    return json.loads(GOLDEN.read_text())


@pytest.mark.parametrize(
    "name", ["backtest_single", "backtest_paths", "replay", "scan", "scan_scores"]
)
def test_the_output_is_the_golden_one_to_the_last_digit(captured, golden, name):
    assert captured[name] == golden[name]


def test_the_golden_outputs_are_not_trivial(golden):
    """A run that bought nothing would match a golden file of runs that bought nothing."""
    assert golden["backtest_single"]["orders"] > 0
    assert len(golden["backtest_single"]["start_dates"]) == 3
    assert [path["path"] for path in golden["backtest_paths"]["paths"]] == ["0", "1", "2"]
    assert all(path["orders"] > 0 for path in golden["replay"])
    assert golden["scan"]["orders"] > 0 and golden["scan_scores"]["orders"] > 0
    replayed = golden["replay"][0]["fees_paid"]
    assert replayed > golden["backtest_paths"]["paths"][0]["fees_paid"]
