"""scripts/deploy/agb-stack `activate`, run against a fake `docker` on PATH and a scratch deploy
directory. Nothing here touches a server or a real Docker daemon."""

import os
import shutil
import subprocess
from pathlib import Path

import pytest
from conftest import ROOT

# A docker that records every call as "<verb> <AGB_IMAGE_TAG>" in $FAKE_DOCKER_LOG and fails the
# ones $FAKE_FAIL_UP (release tags, space separated) and $FAKE_FAIL_VOID name: the releases
# whose `up` or `void-stale` fails.
FAKE_DOCKER = """#!/usr/bin/env bash
tag=${AGB_IMAGE_TAG:-}
case $1 in
compose)
    case " $* " in
    *" up "*)
        echo "up $tag" >> "$FAKE_DOCKER_LOG"
        [[ " ${FAKE_FAIL_UP:-} " != *" $tag "* ]] ;;
    *" run "*)
        echo "void-stale $tag" >> "$FAKE_DOCKER_LOG"
        [ "$tag" != "${FAKE_FAIL_VOID:-}" ] ;;
    *" down "*) echo "down $tag" >> "$FAKE_DOCKER_LOG" ;;
    esac ;;
image)
    case $2 in
    inspect) exit 0 ;;
    ls) for t in $FAKE_IMAGES; do echo "$t"; done ;;
    rm) echo "rm ${3#*:}" >> "$FAKE_DOCKER_LOG" ;;
    esac ;;
esac
"""


class Deploy:
    """A deploy directory whose releases each carry a copy of the script under test."""

    def __init__(self, root: Path):
        self.dir = root / "deploy"
        self.log = root / "docker.log"
        self.log.write_text("")
        bin_dir = root / "bin"
        bin_dir.mkdir()
        docker = bin_dir / "docker"
        docker.write_text(FAKE_DOCKER)
        docker.chmod(0o755)
        self.bin = bin_dir
        (self.dir / "env").mkdir(parents=True)
        (self.dir / "env/db.env").write_text("")
        self.images: list[str] = []

    def release(self, tag: str) -> None:
        rel = self.dir / "releases" / tag
        rel.mkdir(parents=True)
        (rel / "compose.yaml").write_text("services: {}\n")
        shutil.copy(ROOT / "scripts/deploy/agb-stack", rel / "agb-stack")
        self.images.append(tag)

    def link(self, name: str, tag: str) -> None:
        (self.dir / name).symlink_to(f"releases/{tag}")

    def stack(
        self, *args: str, fail_up: str = "", fail_void: str = ""
    ) -> subprocess.CompletedProcess:
        newest = args[-1] if args[0] == "activate" else ""
        script = self.dir / "releases" / newest / "agb-stack" if newest else self.dir / "agb-stack"
        return subprocess.run(
            [str(script), *args],
            env={
                **os.environ,
                "PATH": f"{self.bin}:{os.environ['PATH']}",
                "FAKE_DOCKER_LOG": str(self.log),
                "FAKE_FAIL_UP": fail_up,
                "FAKE_FAIL_VOID": fail_void,
                "FAKE_IMAGES": " ".join(self.images),
            },
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=60,
        )

    def tag(self, name: str) -> str | None:
        path = self.dir / name
        return path.resolve().name if path.is_symlink() else None

    def stack_link(self) -> str:
        """The release the deploy directory's `agb-stack` command belongs to."""
        return (self.dir / "agb-stack").resolve().parent.name

    def releases(self) -> set[str]:
        return {p.name for p in (self.dir / "releases").iterdir()}

    def calls(self) -> list[str]:
        return self.log.read_text().splitlines()


@pytest.fixture
def deploy(tmp_path) -> Deploy:
    return Deploy(tmp_path)


def running(deploy: Deploy, current: str, previous: str | None = None) -> None:
    """The directory as a good deploy of `current` over `previous` left it."""
    for tag in (previous, current):
        if tag:
            deploy.release(tag)
    deploy.link("current", current)
    deploy.link("healthy", current)
    (deploy.dir / "agb-stack").symlink_to("current/agb-stack")
    if previous:
        deploy.link("previous", previous)


def test_a_healthy_deploy_moves_current_and_records_the_release_as_healthy(deploy):
    running(deploy, "aaa")
    deploy.release("bbb")

    done = deploy.stack("activate", "bbb")

    assert done.returncode == 0, done.stderr
    assert (deploy.tag("current"), deploy.tag("previous"), deploy.tag("healthy")) == (
        "bbb",
        "aaa",
        "bbb",
    )
    assert deploy.stack_link() == "bbb"
    assert deploy.calls() == ["up bbb", "void-stale bbb"]


def test_a_deploy_that_does_not_come_up_leaves_current_on_the_release_that_was_running(deploy):
    running(deploy, "bbb", "aaa")
    deploy.release("ccc")

    done = deploy.stack("activate", "ccc", fail_up="ccc")

    assert done.returncode != 0
    assert "release ccc" in done.stderr and "back on release bbb" in done.stderr
    assert (deploy.tag("current"), deploy.tag("previous"), deploy.tag("healthy")) == (
        "bbb",
        "aaa",
        "bbb",
    )
    assert deploy.stack_link() == "bbb"
    # The previous release is brought back up, and nothing is pruned.
    assert deploy.calls() == ["up ccc", "up bbb"]
    assert {"aaa", "bbb"} <= deploy.releases()


def test_a_failed_void_stale_fails_the_deploy_and_returns_to_the_release_that_was_running(deploy):
    running(deploy, "bbb", "aaa")
    deploy.release("ccc")

    done = deploy.stack("activate", "ccc", fail_void="ccc")

    assert done.returncode != 0
    assert "could not void stale certifications" in done.stderr
    assert deploy.tag("current") == "bbb"
    assert deploy.tag("healthy") == "bbb"
    assert deploy.calls() == ["up ccc", "void-stale ccc", "up bbb"]


def test_a_fix_forward_deploy_after_a_failed_one_keeps_the_last_healthy_release(deploy):
    """The failed release is not current, so the next deploy does not mistake it for the
    release to keep: the one that was healthy becomes `previous` and survives the prune."""
    running(deploy, "bbb", "aaa")
    deploy.release("ccc")
    assert deploy.stack("activate", "ccc", fail_up="ccc").returncode != 0
    deploy.release("ddd")

    done = deploy.stack("activate", "ddd")

    assert done.returncode == 0, done.stderr
    assert (deploy.tag("current"), deploy.tag("previous"), deploy.tag("healthy")) == (
        "ddd",
        "bbb",
        "ddd",
    )
    assert deploy.releases() == {"bbb", "ddd"}
    assert "rm aaa" in deploy.calls() and "rm ccc" in deploy.calls()
    assert "rm bbb" not in deploy.calls()


def test_the_first_deploy_that_fails_has_nothing_to_return_to_and_stops_the_stack(deploy):
    deploy.release("aaa")

    done = deploy.stack("activate", "aaa", fail_up="aaa")

    assert done.returncode != 0
    assert "no earlier release" in done.stderr
    assert deploy.tag("current") is None and deploy.tag("healthy") is None
    assert deploy.calls() == ["up aaa", "down aaa"]


def test_a_failed_return_says_so_and_still_leaves_the_links_alone(deploy):
    running(deploy, "bbb", "aaa")
    deploy.release("ccc")

    done = deploy.stack("activate", "ccc", fail_up="ccc bbb")

    assert done.returncode != 0
    assert "release bbb did not come back up" in done.stderr
    assert (deploy.tag("current"), deploy.tag("previous"), deploy.tag("healthy")) == (
        "bbb",
        "aaa",
        "bbb",
    )


def test_rollback_returns_to_the_previous_release_and_records_it_healthy(deploy):
    running(deploy, "bbb", "aaa")

    done = deploy.stack("rollback")

    assert done.returncode == 0, done.stderr
    assert (deploy.tag("current"), deploy.tag("previous"), deploy.tag("healthy")) == (
        "aaa",
        "bbb",
        "aaa",
    )


def test_a_rollback_that_does_not_come_up_leaves_current_where_it_was(deploy):
    running(deploy, "bbb", "aaa")

    done = deploy.stack("rollback", fail_up="aaa")

    assert done.returncode != 0
    assert (deploy.tag("current"), deploy.tag("previous")) == ("bbb", "aaa")
    assert deploy.calls() == ["up aaa", "up bbb"]
