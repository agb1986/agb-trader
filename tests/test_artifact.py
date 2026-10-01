"""The dependency boundary the certification import rests on (issue #81).

Recording a certification runs on the server, inside a container on the stack's own network,
from the app image. That image is `uv sync --group app`; it carries every line of research
source but nothing of the `research` group, so `agb.research.certify`, which imports the
backtest engine and the trainer, cannot be imported there at all. That is why reading an
artifact back and inserting its row is `agb.research.artifact` instead.

Each import runs in a subprocess whose meta path refuses every module the app image would not
have, which is derived from uv.lock rather than listed here: the two groups are resolved the
way the Dockerfile resolves them and the difference is what is forbidden, so a dependency added
to the research group extends this test. A module that reaches for one fails here and names
it."""

import re
import subprocess
import sys
from importlib.metadata import packages_distributions

import pytest
from conftest import ROOT

# The image targets, as the Dockerfile builds them.
APP, RESEARCH = "app", "research"
# What runs on the server and so must import without the research group. `agb.cli` is the whole
# command line, because `agb import-certification` is the entry point the import service runs.
SERVER_MODULES = ("agb.research.artifact", "agb.cli")

SCRIPT = """
import sys
from importlib.abc import MetaPathFinder

forbidden = set(sys.argv[2].split(","))


class Refuse(MetaPathFinder):
    def find_spec(self, name, path=None, target=None):
        if name.split(".")[0] in forbidden:
            raise ImportError(f"the app image installs no {name}")
        return None


sys.meta_path.insert(0, Refuse())
__import__(sys.argv[1])
"""


def canonical(distribution: str) -> str:
    return re.sub(r"[-_.]+", "-", distribution).lower()


def locked(group: str) -> set[str]:
    """Every distribution `uv sync --frozen --no-default-groups --group <group>` installs, which
    is the line each image target runs."""
    exported = subprocess.run(
        ["uv", "export", "--frozen", "--no-default-groups", "--group", group,
         "--no-hashes", "--no-emit-project"],
        check=True, capture_output=True, text=True, cwd=ROOT,
    ).stdout  # fmt: skip
    return {
        canonical(re.split(r"[<>=!~\[ ]", line, maxsplit=1)[0])
        for line in exported.splitlines()
        if line and not line.startswith(("#", "-", " "))
    }


def forbidden_modules() -> list[str]:
    """The top-level modules the research image has and the app image does not. Resolved
    through the installed distributions, so a module whose import name is not its distribution
    name, scikit-learn's `sklearn` among them, is covered."""
    research_only = locked(RESEARCH) - locked(APP)
    return sorted(
        module
        for module, distributions in packages_distributions().items()
        if {canonical(name) for name in distributions} <= research_only
    )


def imports(module: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-c", SCRIPT, module, ",".join(forbidden_modules())],
        capture_output=True, text=True, cwd=ROOT,
    )  # fmt: skip


def test_the_boundary_is_the_image_the_dockerfile_builds():
    """The groups this module resolves are the ones the Dockerfile installs, and the headline
    research dependencies are among what the app image lacks. pandas deliberately is not:
    exchange-calendars requires it, so the app image has it whatever the research group says."""
    dockerfile = (ROOT / "Dockerfile").read_text()
    for target, group in ((APP, APP), (RESEARCH, RESEARCH)):
        assert f"FROM runtime AS {target}" in dockerfile
        assert f"uv sync --frozen --no-default-groups --group {group}\n" in dockerfile
    modules = forbidden_modules()
    assert {"nautilus_trader", "vectorbt", "plotly", "sklearn"} <= set(modules)
    assert "pandas" not in modules and "lightgbm" not in modules


@pytest.mark.parametrize("module", SERVER_MODULES)
def test_the_import_path_runs_without_the_research_dependencies(module):
    done = imports(module)
    assert done.returncode == 0, done.stderr


def test_certify_is_what_cannot_be_imported_there():
    """The reason the import is a module of its own. If this ever passes, the split has stopped
    being load-bearing, not that the boundary has moved."""
    done = imports("agb.research.certify")
    assert done.returncode != 0
    assert "the app image installs no" in done.stderr
