"""The whole stack, end to end: acceptance criteria (a), (b) and (c) of T01.

Builds the app image and brings the stack up under a Compose project name unique to this
worktree, with the committed example env files, then tears it down with its volume.
"""

import os
import shutil
import subprocess
import time

import pytest
from conftest import ROOT, worktree_id

pytestmark = pytest.mark.stack


@pytest.fixture(scope="module")
def image_tag():
    return f"agb-t-{worktree_id()}"


@pytest.fixture(scope="module")
def compose(tmp_path_factory, image_tag):
    project = image_tag
    env_dir = tmp_path_factory.mktemp("env")
    for example in (ROOT / "env").glob("*.env.example"):
        shutil.copy(example, env_dir / example.name.removesuffix(".example"))
    env = {
        **os.environ,
        "AGB_ENV_DIR": str(env_dir),
        "AGB_IMAGE_TAG": project,
        "AGB_HEARTBEAT_SECONDS": "1",
    }
    base = [
        "docker", "compose",
        "--project-name", project,
        "--project-directory", str(ROOT),
        "--file", str(ROOT / "compose.yaml"),
        "--env-file", str(env_dir / "db.env"),
    ]  # fmt: skip

    def run(*args, check=True):
        return subprocess.run([*base, *args], env=env, check=check, capture_output=True, text=True)

    run("down", "--volumes", "--remove-orphans")
    try:
        up = run("up", "--detach", "--build", "--wait", check=False)
        assert up.returncode == 0, up.stderr[-4000:]
        yield run
    finally:
        run("down", "--volumes", "--remove-orphans", check=False)


def scheduler_line(run) -> list[str]:
    out = run("run", "--rm", "cli", "status").stdout
    [line] = [line for line in out.splitlines() if line.startswith("scheduler")]
    return line.split()


def test_one_command_brings_up_the_database_and_a_heartbeating_service(compose):
    ps = compose("ps", "--all", "--format", "{{.Service}} {{.State}} {{.ExitCode}}").stdout
    states = dict(line.split(" ", 1) for line in ps.splitlines())
    assert states["db"].startswith("running")
    assert states["scheduler"].startswith("running")
    assert states["migrate"] == "exited 0"


def test_cli_lists_the_heartbeat_and_it_advances(compose):
    first = scheduler_line(compose)
    assert first[1] != "never", first
    deadline = time.monotonic() + 30
    while scheduler_line(compose)[1:3] == first[1:3]:
        assert time.monotonic() < deadline, "the scheduler heartbeat did not advance"
        time.sleep(0.5)


def test_heartbeat_reports_the_image_version(compose, image_tag):
    # The image is built with AGB_VERSION set to its tag, and the scheduler reports it.
    assert scheduler_line(compose)[-1] == image_tag
