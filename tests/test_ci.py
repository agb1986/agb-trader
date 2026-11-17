"""Static checks on the CI workflow and the Dependabot configuration (#172).

CI runs on every pull request and on pushes to main, with read-only repository access, and
never reaches a live service: no job sets an AGB_LIVE_* variable or reads a secret, and the
test jobs deselect the live and stack tests as `make test-fast` does.
"""

import re

import pytest
import yaml
from conftest import ROOT
from test_measure import SERVER_PYTHON

WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"
DEPENDABOT = ROOT / ".github" / "dependabot.yml"
# A branch-protection rule requires the checks by these names.
REQUIRED_JOBS = {"lint", "fast-tests", "image-build"}
# Checks added beside them, which no rule requires yet.
JOBS = REQUIRED_JOBS | {"types", "coverage"}


@pytest.fixture(scope="module")
def workflow() -> dict:
    return yaml.safe_load(WORKFLOW.read_text())


def triggers(workflow: dict) -> dict:
    # YAML 1.1 reads a bare `on` key as the boolean true.
    return workflow.get("on", workflow.get(True))


def commands(job: dict) -> list[str]:
    return [step["run"] for step in job["steps"] if "run" in step]


def recipe(target: str) -> list[str]:
    """The commands a Makefile target runs."""
    lines = (ROOT / "Makefile").read_text().splitlines()
    [at] = [i for i, line in enumerate(lines) if line.startswith(f"{target}:")]
    body = []
    for line in lines[at + 1 :]:
        if not line.startswith("\t"):
            break
        body.append(line.strip())
    return body


def test_runs_on_every_pull_request_and_on_pushes_to_main(workflow):
    on = triggers(workflow)
    # No branch filter: pull requests into the wave integration branches get the checks too.
    assert on["pull_request"] is None
    assert on["push"] == {"branches": ["main"]}
    assert set(on) == {"pull_request", "push"}


def test_the_token_can_only_read_the_repository(workflow):
    assert workflow["permissions"] == {"contents": "read"}
    for name, job in workflow["jobs"].items():
        assert "permissions" not in job, name


def test_the_jobs_and_the_three_required_names(workflow):
    assert set(workflow["jobs"]) == JOBS
    assert REQUIRED_JOBS <= set(workflow["jobs"])


def test_lint_runs_what_make_lint_runs(workflow):
    assert commands(workflow["jobs"]["lint"]) == ["uv sync --frozen", *recipe("lint")]


def test_the_fast_tests_run_what_make_test_fast_runs(workflow):
    # The server's Python as well, so the sampler test runs rather than skips and CI passes as
    # many tests as a local run.
    assert commands(workflow["jobs"]["fast-tests"]) == [
        "uv sync --frozen",
        f"uv python install {SERVER_PYTHON}",
        *recipe("test-fast"),
    ]
    assert recipe("test-fast") == ['uv run --frozen pytest -m "not stack and not live"']


def test_the_type_check_runs_what_make_types_runs(workflow):
    assert commands(workflow["jobs"]["types"]) == ["uv sync --frozen", *recipe("types")]


def test_the_coverage_job_runs_what_make_cover_runs(workflow):
    assert commands(workflow["jobs"]["coverage"]) == [
        "uv sync --frozen",
        f"uv python install {SERVER_PYTHON}",
        *recipe("cover"),
    ]
    # The same tests as test-fast, measured.
    assert recipe("cover") == ['uv run --frozen pytest -m "not stack and not live" --cov']


def test_the_image_build_builds_the_app_target(workflow):
    assert commands(workflow["jobs"]["image-build"]) == ["docker build --target app ."]


def test_no_job_reaches_a_live_service(workflow):
    # No environment anywhere: no AGB_LIVE_* gate is set and no credential is passed in.
    assert "env" not in workflow
    for name, job in workflow["jobs"].items():
        assert "env" not in job, name
        for step in job["steps"]:
            assert "env" not in step, name
    text = WORKFLOW.read_text()
    assert "AGB_LIVE_" not in re.sub(r"#.*", "", text)
    assert "secrets." not in text
    for word in ("ALPACA", "TELEGRAM", "SHARADAR", "EDGAR", "ANTHROPIC", "WIREGUARD"):
        assert word not in text.upper(), word


def test_every_action_is_pinned_by_commit_sha(workflow):
    uses = [
        step["uses"] for job in workflow["jobs"].values() for step in job["steps"] if "uses" in step
    ]
    assert uses
    for ref in uses:
        assert re.fullmatch(r"[\w.-]+/[\w.-]+@[0-9a-f]{40}", ref), ref


def test_checkout_keeps_no_credentials(workflow):
    for name, job in workflow["jobs"].items():
        for step in job["steps"]:
            if step.get("uses", "").startswith("actions/checkout@"):
                assert step["with"]["persist-credentials"] is False, name


def test_dependabot_updates_uv_and_actions_weekly_in_groups():
    config = yaml.safe_load(DEPENDABOT.read_text())
    updates = {update["package-ecosystem"]: update for update in config["updates"]}
    assert set(updates) == {"uv", "github-actions"}
    for ecosystem, update in updates.items():
        assert update["directory"] == "/", ecosystem
        assert update["schedule"]["interval"] == "weekly", ecosystem
        assert update["groups"], ecosystem
