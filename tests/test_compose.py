"""Static checks on the deployment files: acceptance criterion (g) of T01 and the Compose
conventions every later ticket inherits."""

import json
import os
import shutil
import subprocess

import pytest
import yaml
from conftest import ROOT

from agb.roles import PASSWORD_VARS

ONE_SHOT = {"migrate", "cli"}
ENV_NAMES = {"db", "pipeline", "execution", "bot", "vpn", "research"}


@pytest.fixture(scope="module")
def resolved(tmp_path_factory):
    """compose.yaml as Compose itself resolves it, with every profile enabled and only the
    committed example env files."""
    env_dir = tmp_path_factory.mktemp("env")
    for example in (ROOT / "env").glob("*.env.example"):
        shutil.copy(example, env_dir / example.name.removesuffix(".example"))
    out = subprocess.run(
        [
            "docker", "compose",
            "--file", str(ROOT / "compose.yaml"),
            "--env-file", str(env_dir / "db.env"),
            "--profile", "*",
            "config", "--format", "json",
        ],
        check=True, capture_output=True, text=True,
        env={**os.environ, "AGB_ENV_DIR": str(env_dir)},
    ).stdout  # fmt: skip
    return json.loads(out)


def test_no_service_mounts_the_docker_socket(resolved):
    assert "docker.sock" not in (ROOT / "compose.yaml").read_text()
    for name, service in resolved["services"].items():
        for volume in service.get("volumes", []):
            assert "docker.sock" not in str(volume.get("source", "")), name


def test_long_running_services_restart_unless_stopped(resolved):
    for name, service in resolved["services"].items():
        expected = "no" if name in ONE_SHOT else "unless-stopped"
        assert service.get("restart") == expected, name


def test_every_service_has_a_memory_limit(resolved):
    for name, service in resolved["services"].items():
        assert service.get("mem_limit"), name


def test_the_database_is_not_published(resolved):
    assert not resolved["services"]["db"].get("ports")


def test_networks(resolved):
    # Resolved config drops networks no service joins yet, so read the declarations raw.
    declared = yaml.safe_load((ROOT / "compose.yaml").read_text())["networks"]
    assert {"core", "egress"} <= set(declared)
    assert set(resolved["services"]["db"]["networks"]) == {"core"}


def test_app_services_share_one_image(resolved):
    services = resolved["services"]
    built = [name for name, service in services.items() if "build" in service]
    assert built == ["migrate"], "only migrate builds; the others reuse its image"
    images = {service["image"] for name, service in services.items() if name != "db"}
    assert images == {services["migrate"]["image"]}


def test_dockerfile_has_every_target():
    stages = {
        line.split()[-1]
        for line in (ROOT / "Dockerfile").read_text().splitlines()
        if line.upper().startswith("FROM ") and " AS " in line.upper()
    }
    assert {"base", "app", "research"} <= stages


def test_every_env_file_has_an_example():
    names = {p.name.removesuffix(".env.example") for p in (ROOT / "env").glob("*.env.example")}
    assert names == ENV_NAMES


def test_db_example_sets_every_password():
    text = (ROOT / "env" / "db.env.example").read_text()
    keys = {line.split("=", 1)[0] for line in text.splitlines() if "=" in line and line[0] != "#"}
    assert {"POSTGRES_PASSWORD", *PASSWORD_VARS.values()} <= keys


def test_real_env_files_are_ignored():
    for name in ENV_NAMES:
        ignored = subprocess.run(["git", "check-ignore", "--quiet", f"env/{name}.env"], cwd=ROOT)
        assert ignored.returncode == 0, f"env/{name}.env is not git-ignored"


def test_migrate_and_cli_are_the_only_one_shot_services():
    services = yaml.safe_load((ROOT / "compose.yaml").read_text())["services"]
    assert {name for name, s in services.items() if s.get("restart") == "no"} == ONE_SHOT
