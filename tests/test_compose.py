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

ONE_SHOT = {"migrate", "cli", "research"}
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


def test_backup_writes_to_a_host_directory_as_its_owner(resolved):
    backup = resolved["services"]["backup"]
    assert backup.get("user"), "the dumps must belong to the host user that owns the directory"
    [volume] = backup["volumes"]
    assert (volume["type"], volume["target"]) == ("bind", "/backups")
    assert backup["environment"]["AGB_BACKUP_DIR"] == "/backups"
    assert backup["environment"]["PGUSER"] == "backup"
    assert set(backup["networks"]) == {"core"}


def test_pipeline_writes_the_lake_and_panel_store_to_host_directories_as_their_owner(resolved):
    pipeline = resolved["services"]["pipeline"]
    assert pipeline.get("user"), "the lake must belong to the host user that owns the directory"
    volumes = {volume["target"]: volume["type"] for volume in pipeline["volumes"]}
    assert volumes == {"/lake": "bind", "/panel": "bind", "/models": "bind"}
    assert pipeline["environment"]["AGB_LAKE_DIR"] == "/lake"
    assert pipeline["environment"]["AGB_PANEL_DIR"] == "/panel"
    assert pipeline["environment"]["PGUSER"] == "pipeline"
    assert set(pipeline["networks"]) == {"core"}


def test_the_pipeline_and_the_cli_only_read_the_certified_models(resolved):
    """T21: the pipeline ranks with the promoted artifact and `agb promote` verifies it, and
    neither can change it."""
    for name in ("pipeline", "cli"):
        service = resolved["services"][name]
        [models] = [v for v in service["volumes"] if v["target"] == "/models"]
        assert (models["type"], models.get("read_only")) == ("bind", True), name
        assert service["environment"]["AGB_MODELS_DIR"] == "/models", name
    for name, service in resolved["services"].items():
        if name not in ("pipeline", "cli"):
            assert "/models" not in {v["target"] for v in service.get("volumes", [])}, name


def test_only_the_pipeline_has_the_vendor_key(resolved):
    # Compose inlines each env file into the service's environment.
    for name, service in resolved["services"].items():
        has_key = "SHARADAR_API_KEY" in service.get("environment", {})
        assert has_key == (name == "pipeline"), name
    holders = [p.name for p in (ROOT / "env").glob("*.env.example") if "SHARADAR" in p.read_text()]
    assert holders == ["pipeline.env.example"]
    assert "SHARADAR" not in (ROOT / "compose.yaml").read_text()
    assert "pipeline" in server_env()


def test_app_services_share_one_image(resolved):
    services = resolved["services"]
    built = [name for name, service in services.items() if "build" in service]
    assert built == ["migrate", "research"], "migrate builds the app image; research its own"
    assert services["research"]["build"]["target"] == "research"
    shared = {n: s for n, s in services.items() if n not in {"db", "vpn", "research"}}
    assert {service["image"] for service in shared.values()} == {services["migrate"]["image"]}
    assert services["research"]["image"] != services["migrate"]["image"]


def test_only_the_bot_is_on_both_networks(resolved):
    services = resolved["services"]
    on = {
        network: {n for n, s in services.items() if network in s.get("networks", ())}
        for network in ("core", "egress")
    }
    assert on["core"] & on["egress"] == {"bot"}
    assert on["egress"] == {"vpn", "bot"}


def test_the_vpn_is_a_gluetun_tunnel_that_publishes_nothing(resolved):
    vpn = resolved["services"]["vpn"]
    assert vpn["image"].startswith("qmcgaw/gluetun:")
    assert not vpn.get("ports")
    assert vpn["cap_add"] == ["NET_ADMIN"]
    assert vpn["environment"]["HTTPPROXY"] == "on"
    assert vpn["environment"]["VPN_PORT_FORWARDING"] == "off"


def test_the_bot_reaches_telegram_through_the_vpn_and_the_database_directly(resolved):
    bot = resolved["services"]["bot"]
    env = bot["environment"]
    assert env["HTTPS_PROXY"] == "http://vpn:8888"
    assert "db" in env["NO_PROXY"].split(",")
    assert (env["PGHOST"], env["PGUSER"]) == ("db", "bot")
    assert bot["depends_on"]["vpn"]["condition"] == "service_healthy"


def test_only_the_bot_holds_the_telegram_settings(resolved):
    for name, service in resolved["services"].items():
        env = service.get("environment") or {}
        telegram = {key for key in env if key.startswith("TELEGRAM_") or key.endswith("_PROXY")}
        expected = {"TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID", "HTTPS_PROXY", "NO_PROXY"}
        assert telegram == (expected if name == "bot" else set()), name
    services = yaml.safe_load((ROOT / "compose.yaml").read_text())["services"]
    readers = {name for name, s in services.items() if "bot.env" in str(s.get("env_file", ""))}
    assert readers == {"bot"}
    holders = {
        path.name
        for path in (ROOT / "env").glob("*.env.example")
        if "TELEGRAM_BOT_TOKEN" in path.read_text()
    }
    assert holders == {"bot.env.example"}


def server_env() -> set[str]:
    """The env files deploy.sh requires on the server."""
    text = (ROOT / "scripts/deploy/deploy.sh").read_text()
    [line] = [line for line in text.splitlines() if line.startswith("server_env=(")]
    return set(line.removeprefix("server_env=(").removesuffix(")").split())


def test_vpn_and_bot_run_only_where_the_telegram_profile_is_enabled():
    services = yaml.safe_load((ROOT / "compose.yaml").read_text())["services"]
    profiled = {name for name, s in services.items() if "telegram" in s.get("profiles", [])}
    assert profiled == {"vpn", "bot"}
    assert "COMPOSE_PROFILES=telegram" in (ROOT / "scripts/deploy/agb-stack").read_text()
    assert {"bot", "vpn"} <= server_env()


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


def test_migrate_cli_and_research_are_the_only_one_shot_services():
    services = yaml.safe_load((ROOT / "compose.yaml").read_text())["services"]
    assert {name for name, s in services.items() if s.get("restart") == "no"} == ONE_SHOT


def test_research_runs_only_on_the_workstation_with_a_read_only_lake(resolved):
    """The research profile starts only on the workstation, off the network, and its lake is a
    read-only mount of what the host-side rsync pulled."""
    assert "research" not in server_env(), "deploy.sh never ships research.env"
    assert "research" not in (ROOT / "scripts/deploy/agb-stack").read_text()
    deploy = (ROOT / "scripts/deploy/deploy.sh").read_text()
    assert "--target app" in deploy and "--target research" not in deploy
    services = yaml.safe_load((ROOT / "compose.yaml").read_text())["services"]
    assert services["research"]["profiles"] == ["research"]
    assert services["research"]["network_mode"] == "none"
    research = resolved["services"]["research"]
    assert research["network_mode"] == "none"
    assert "networks" not in research
    assert research.get("user"), "the ledger must belong to the host user that owns the directory"
    env = research["environment"]
    assert env["AGB_LAKE_DIR"] == "/lake" and env["AGB_RESEARCH_DIR"] == "/research"
    assert ":" in env["AGB_LAKE_SOURCE"], "the example names the server's lake"
    assert not {key for key in env if key.startswith(("SHARADAR", "PG", "POSTGRES", "TELEGRAM"))}
    mounts = {volume["target"]: volume for volume in research["volumes"]}
    assert set(mounts) == {"/lake", "/research"}
    assert mounts["/lake"].get("read_only") is True, "the lake is never written by research"
    assert not mounts["/research"].get("read_only")
    assert all(volume["type"] == "bind" for volume in mounts.values())
