"""No default run collects a live test, and `-m live` collects every live module.

A live test talks to a real service; one closes every position in the paper account. Each is
gated by its own AGB_LIVE_* variable, but a variable left exported in the shell must not turn
an ordinary run into a live one, so the marker deselects them as well. These tests only
collect, in a subprocess with every AGB_LIVE_* variable removed: nothing live runs here.
"""

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LIVE_MODULES = sorted(p.relative_to(ROOT).as_posix() for p in (ROOT / "tests").glob("*_live.py"))


def collected_files(*args: str) -> set[str]:
    env = {k: v for k, v in os.environ.items() if not k.startswith("AGB_LIVE_")}
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "--collect-only", "-q", "-p", "no:cacheprovider", *args],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    # 0: collected something; 5: everything was deselected.
    assert result.returncode in (0, 5), result.stdout + result.stderr
    return {line.split("::")[0] for line in result.stdout.splitlines() if "::" in line}


def recipe(target: str) -> str:
    """The one command a Makefile target runs."""
    lines = (ROOT / "Makefile").read_text().splitlines()
    [at] = [i for i, line in enumerate(lines) if line.startswith(f"{target}:")]
    return lines[at + 1].strip()


def test_there_are_live_modules_to_guard():
    assert "tests/test_liquidate_live.py" in LIVE_MODULES
    assert "tests/test_telegram_live.py" in LIVE_MODULES


def test_a_default_run_collects_no_live_module():
    """`make test`, and a plain `uv run pytest`: the default options deselect the live tests."""
    assert recipe("test") == 'uv run pytest -m "not live"'
    assert collected_files("-m", "not live", *LIVE_MODULES) == set()
    assert collected_files(*LIVE_MODULES) == set()


def test_the_fast_run_collects_no_live_module():
    """What `make test-fast` runs: its own -m replaces the default one, so it keeps "not live"."""
    assert recipe("test-fast") == 'uv run pytest -m "not stack and not live"'
    assert collected_files("-m", "not stack and not live", *LIVE_MODULES) == set()


def test_asking_for_live_collects_every_live_module():
    """What `make test-live` runs. Every *_live.py module carries the marker."""
    assert recipe("test-live") == "uv run pytest -m live"
    assert collected_files("-m", "live", *LIVE_MODULES) == set(LIVE_MODULES)
