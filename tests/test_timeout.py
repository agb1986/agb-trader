"""A test that runs past its timeout fails instead of hanging the run (#192)."""

import subprocess
import sys
import textwrap
import tomllib

from conftest import ROOT


def test_pyproject_sets_a_default_per_test_timeout():
    config = tomllib.loads((ROOT / "pyproject.toml").read_text())
    assert config["tool"]["pytest"]["ini_options"]["timeout"] > 0


def test_a_test_that_sleeps_past_the_timeout_fails_instead_of_hanging(tmp_path):
    (tmp_path / "test_sleeps.py").write_text(
        textwrap.dedent(
            """
            import time


            def test_sleeps():
                time.sleep(30)
            """
        )
    )
    # A rootdir of its own, so none of this repository's options apply but the plugin's.
    run = subprocess.run(
        [sys.executable, "-m", "pytest", "-p", "no:cacheprovider", "--timeout=1", "-q"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert run.returncode == 1, run.stdout
    assert "Timeout (>1.0s)" in run.stdout
    assert "1 failed" in run.stdout
