"""Static checks on the deployment files: acceptance criterion (g) of T01 and the Compose
conventions every later ticket inherits."""

import json
import os
import re
import shlex
import shutil
import subprocess

import pytest
import yaml
from conftest import ROOT

from agb.roles import PASSWORD_VARS

ONE_SHOT = {"migrate", "cli", "certification-import", "reconcile", "research"}
ENV_NAMES = {"db", "pipeline", "execution", "bot", "vpn", "research"}
# Every service that mounts the models directory, each of them read-only.
MODEL_READERS = {"pipeline", "cli", "certification-import"}
# Issue #170: the services with a route to the internet, each for a reason. The pipeline calls
# its vendors, execution and reconcile the broker, and vpn carries the bot's Telegram traffic.
# The bot is on vpn's network to reach its proxy. Every other service reads the database and
# nothing else, and has only the internal network.
REACHES_INTERNET = {"pipeline", "execution", "reconcile", "vpn", "bot"}
NO_MASQUERADE = "com.docker.network.bridge.enable_ip_masquerade"


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


def test_every_service_has_a_healthcheck(resolved):
    """`up --wait` passes only once each long-running service reports healthy, so each of them
    must check something; the one-shots disable it, because their exit status is the answer."""
    app_image = resolved["services"]["scheduler"]["image"]
    for name, service in resolved["services"].items():
        check = service.get("healthcheck")
        assert check, f"{name} declares no healthcheck"
        if name in ONE_SHOT:
            assert check == {"disable": True}, name
            continue
        assert not check.get("disable"), name
        assert check.get("test"), name
        assert {"interval", "timeout", "retries"} <= set(check), name
        # Docker Engine 20.10 on the server predates start_interval.
        assert "start_interval" not in check, name
        if service.get("image") == app_image:
            assert check["test"][:4] == ["CMD", "python", "-m", "agb.heartbeat"], name


def test_the_ui_check_asks_its_server_at_the_operator_s_address(resolved):
    """Its server runs in a thread beside the heartbeat loop, and it answers only the Host the
    operator's browser sends. The check runs in the container's environment, so it sees the
    same AGB_UI_ALLOWED_HOSTS the server does and addresses its request to one of those."""
    check = resolved["services"]["ui"]["healthcheck"]
    assert check["test"] == ["CMD", "python", "-m", "agb.heartbeat", "http://127.0.0.1:8080/"]
    assert "AGB_UI_ALLOWED_HOSTS" in resolved["services"]["ui"]["environment"]
    others = {
        name: s["healthcheck"]["test"]
        for name, s in resolved["services"].items()
        if name not in ONE_SHOT | {"ui"} and s.get("image") == resolved["services"]["ui"]["image"]
    }
    assert all(test == ["CMD", "python", "-m", "agb.heartbeat"] for test in others.values())


def test_the_heartbeat_check_judges_the_service_s_own_role(resolved):
    """The check reads the row of the role it connects as, so each app service must connect as
    its own role."""
    app_image = resolved["services"]["scheduler"]["image"]
    for name, service in resolved["services"].items():
        if name not in ONE_SHOT and service.get("image") == app_image:
            assert service["environment"]["PGUSER"] == name


def test_the_database_is_not_published(resolved):
    assert not resolved["services"]["db"].get("ports")


def test_networks(resolved):
    # Resolved config drops networks no service joins yet, so read the declarations raw.
    declared = yaml.safe_load((ROOT / "compose.yaml").read_text())["networks"]
    assert set(declared) == {"core", "outbound", "ui-port", "egress"}
    assert set(resolved["services"]["db"]["networks"]) == {"core"}


def reaches_internet(network: dict) -> bool:
    """Whether a container on this network has a translated route off the host. An internal
    network has no route at all; a bridge that does no masquerading has one, but what leaves by
    it carries the container's private address and finds no way back."""
    return (
        not network.get("internal") and network.get("driver_opts", {}).get(NO_MASQUERADE) != "false"
    )


def test_the_core_network_is_internal(resolved):
    """Issue #170: the database's network has no route off the host, so a service on it alone
    has no egress. Removing `internal` gives every one of them the internet."""
    assert resolved["networks"]["core"].get("internal") is True
    on_core = {n for n, s in resolved["services"].items() if "core" in s.get("networks", ())}
    assert on_core == set(resolved["services"]) - {"vpn", "research"}


def test_only_the_services_that_call_out_can_reach_the_internet(resolved):
    """A service on two networks reaches everything on both, so a service can reach the
    internet when any of its networks can. Putting a database-only service on a network with a
    route out, or giving the core network one, fails here."""
    networks = resolved["networks"]
    reach = {
        name
        for name, service in resolved["services"].items()
        if any(reaches_internet(networks[network]) for network in service.get("networks", ()))
    }
    assert reach == REACHES_INTERNET
    database_only = {"db", "migrate", "scheduler", "backup", "cli", "certification-import"}
    for name in database_only:
        assert set(resolved["services"][name]["networks"]) == {"core"}, name


def test_the_ui_port_network_carries_the_published_port_and_no_route_out(resolved):
    """Docker publishes no port from an internal network, so the UI joins one more network for
    its publication alone, and that network's bridge translates nothing on the way out."""
    networks = resolved["networks"]
    assert not networks["ui-port"].get("internal"), "a published port needs a routed network"
    assert networks["ui-port"]["driver_opts"][NO_MASQUERADE] == "false"
    services = resolved["services"]
    assert set(services["ui"]["networks"]) == {"core", "ui-port"}
    assert {n for n, s in services.items() if "ui-port" in s.get("networks", ())} == {"ui"}
    assert [n for n, s in services.items() if s.get("ports")] == ["ui"]


def test_nothing_with_the_broker_keys_shares_a_network_with_the_proxy(resolved):
    """Broker traffic must never leave through the VPN, so execution and reconcile have no
    route to the vpn's proxy at all, and the network that takes them out does not carry it."""
    services = resolved["services"]
    assert {n for n, s in services.items() if "outbound" in s.get("networks", ())} == {
        "pipeline",
        "execution",
        "reconcile",
    }
    vpn = set(services["vpn"]["networks"])
    for name in ("execution", "reconcile"):
        assert not set(services[name]["networks"]) & vpn, name


def test_backup_writes_to_a_host_directory_as_its_owner(resolved):
    backup = resolved["services"]["backup"]
    assert backup.get("user"), "the dumps must belong to the host user that owns the directory"
    [volume] = backup["volumes"]
    assert (volume["type"], volume["target"]) == ("bind", "/backups")
    assert backup["environment"]["AGB_BACKUP_DIR"] == "/backups"
    assert backup["environment"]["PGUSER"] == "backup"


def test_pipeline_writes_the_lake_and_panel_store_to_host_directories_as_their_owner(resolved):
    pipeline = resolved["services"]["pipeline"]
    assert pipeline.get("user"), "the lake must belong to the host user that owns the directory"
    volumes = {volume["target"]: volume["type"] for volume in pipeline["volumes"]}
    assert volumes == {"/lake": "bind", "/panel": "bind", "/models": "bind"}
    assert pipeline["environment"]["AGB_LAKE_DIR"] == "/lake"
    assert pipeline["environment"]["AGB_PANEL_DIR"] == "/panel"
    assert pipeline["environment"]["PGUSER"] == "pipeline"


def test_the_services_that_see_the_certified_models_only_read_them(resolved):
    """T21: the pipeline ranks with the promoted artifact, `agb promote` verifies it and the
    import records the one the operator copied in. None of the three may change it."""
    for name in MODEL_READERS:
        service = resolved["services"][name]
        [models] = [v for v in service["volumes"] if v["target"] == "/models"]
        assert (models["type"], models.get("read_only")) == ("bind", True), name
        assert service["environment"]["AGB_MODELS_DIR"] == "/models", name
    for name, service in resolved["services"].items():
        if name not in MODEL_READERS:
            assert "/models" not in {v["target"] for v in service.get("volumes", [])}, name


def test_the_import_is_the_one_service_that_runs_as_the_research_role(resolved):
    """Issue #81: recording a certification is the research profile's only write to the
    server's database, only that role may insert the row, and the database is reachable from
    the stack's own network alone. So the import is a service of its own, on core, from the
    app image, which carries the source without the research dependencies."""
    services = resolved["services"]
    importer = services["certification-import"]
    assert importer["environment"]["PGUSER"] == "research"
    assert [
        name for name, s in services.items() if s.get("environment", {}).get("PGUSER") == "research"
    ] == ["certification-import"]
    assert importer["image"] == services["migrate"]["image"], "the app image, not the research one"
    assert importer["entrypoint"] == ["agb", "import-certification"]
    assert "core" in importer["networks"]
    assert importer["profiles"] == ["cli"], "it runs only when the operator asks for it"
    assert "import <model id>" in (ROOT / "scripts/deploy/agb-stack").read_text()


def test_the_ui_is_published_on_the_loopback_address_alone(resolved):
    """T27 criterion (a): the UI answers on 127.0.0.1:8080 of the server itself, so the only
    way in is an SSH tunnel and SSH is the authentication. It reads the database as the `ui`
    role, mounts nothing, and holds no broker or Telegram credential."""
    ui = resolved["services"]["ui"]
    assert ui["ports"] == [
        {
            "mode": "ingress",
            "host_ip": "127.0.0.1",
            "target": 8080,
            "published": "8080",
            "protocol": "tcp",
        }
    ]
    assert ui["command"] == ["python", "-m", "agb.services.ui"]
    # Inside the container the socket binds every interface, since the published port arrives on
    # the container's own address; the publication above is what keeps it off the network.
    assert ui["environment"]["AGB_UI_HOST"] == "0.0.0.0"
    assert ui["image"] == resolved["services"]["migrate"]["image"]
    assert "egress" not in ui["networks"], "no proxy: the UI makes no outbound connection"
    assert not ui.get("volumes")
    env = ui["environment"]
    assert (env["PGHOST"], env["PGUSER"]) == ("db", "ui")
    assert not {key for key in env if key.startswith(("SHARADAR", "TELEGRAM", "ALPACA"))}


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
    # The server runs nothing of the research profile. Its one server-side piece is recording a
    # certification, which is a service of its own in the `cli` profile, so what agb-stack may
    # not do is enable the profile or run the `research` service, not name the role in a comment.
    stack = (ROOT / "scripts/deploy/agb-stack").read_text()
    profiles = re.findall(r"COMPOSE_PROFILES=(\S+)", stack)
    assert profiles and all("research" not in value for value in profiles)
    assert not re.search(r"(run|up|--profile)\s+(--\S+\s+)*research\b", stack)
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


# The remote commands promote.sh issues, with ssh replaced by a stub that logs the command it
# was given instead of connecting. `test -e` answers no, so the artifact is shipped, imported
# and promoted in one run, and the tar receiver drains its stdin so the sender sees no SIGPIPE.
FAKE_SSH = """#!/usr/bin/env bash
cmd=${!#}
case $cmd in
*"test -e"*) echo "$cmd" >> "$AGB_SSH_LOG"; exit 1 ;;
true|*"test -d"*|*"test -x"*) echo "$cmd" >> "$AGB_SSH_LOG"; exit 0 ;;
*"tar -xzf -"*) cat > /dev/null ;;
esac
echo "$cmd" >> "$AGB_SSH_LOG"
"""


def promote(tmp_path, *args, reason: str = "") -> subprocess.CompletedProcess:
    """promote.sh over a stub ssh and a stub artifact, with the remote commands it issued in
    `stdout` one per line."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(exist_ok=True)
    ssh = bin_dir / "ssh"
    ssh.write_text(FAKE_SSH)
    ssh.chmod(0o755)
    artifact = tmp_path / "certifications" / "ranker-2026.1"
    artifact.mkdir(parents=True, exist_ok=True)
    (artifact / "manifest.json").write_text("{}")
    log = tmp_path / "ssh.log"
    log.write_text("")
    done = subprocess.run(
        [str(ROOT / "scripts/deploy/promote.sh"), "ranker-2026.1", *args],
        cwd=ROOT,
        env={
            **os.environ,
            "PATH": f"{bin_dir}:{os.environ['PATH']}",
            "AGB_SSH_LOG": str(log),
            "AGB_RESEARCH_DIR": str(tmp_path),
            "AGB_DEPLOY_HOST": "server.test",
            "AGB_DEPLOY_DIR": "/deploy",
            "REASON": reason,
        },
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        timeout=60,
    )
    return subprocess.CompletedProcess(done.args, done.returncode, log.read_text(), done.stderr)


def test_promote_carries_the_paper_waiver_to_the_server(tmp_path):
    """The waiver is a flag on the server's `agb promote`, so the handoff has to pass it and the
    reason through; `make promote MODEL=... CMD=waive REASON=...` is the whole measurement
    handoff, not three steps by hand."""
    done = promote(tmp_path, "waive", reason="measuring the live limits, issue #45")
    assert done.returncode == 0, done.stderr
    [promotion] = [line for line in done.stdout.splitlines() if " agb promote " in line]
    # The reason is one word of prose, so the remote shell has to parse it back as one argument.
    assert shlex.split(promotion) == [
        "/deploy/agb-stack",
        "agb",
        "promote",
        "ranker-2026.1",
        "--waive-for-paper",
        "--reason",
        "measuring the live limits, issue #45",
    ]
    # The waiver changes the last step alone: the artifact ships and imports as any other.
    assert [line for line in done.stdout.splitlines() if line.endswith(" import ranker-2026.1")]


def test_promote_waives_nothing_unless_it_is_asked_to(tmp_path):
    done = promote(tmp_path)
    assert done.returncode == 0, done.stderr
    [promotion] = [line for line in done.stdout.splitlines() if " agb promote " in line]
    assert promotion == "/deploy/agb-stack agb promote ranker-2026.1"


def test_a_waiver_without_a_reason_is_refused_before_anything_is_shipped(tmp_path):
    done = promote(tmp_path, "waive")
    assert done.returncode == 1
    assert "a waiver needs a reason" in done.stderr
    assert done.stdout == "", "it refuses before it reaches the server"


# ssh for sizing.sh: the `pgrep` call is run for real, through a shell that carries the pattern
# in its own command line exactly as the remote one does, and every other remote command answers
# nothing. The pattern matching that shell is the whole bug, so the stub has to be able to make it.
FAKE_SSH_SIZING = """#!/usr/bin/env bash
cmd=${!#}
case $cmd in
*pgrep*) exec bash -c "$cmd" ;;
esac
"""


def test_sizing_does_not_mistake_its_own_pgrep_for_the_sampler(tmp_path):
    """`pgrep -f <path>` matches the shell that was handed the pattern, so sizing.sh read every
    server as already sampling: `start` refused to start a sampler that was never there, and
    `stop` aimed a pkill at the shell doing the asking."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    ssh = bin_dir / "ssh"
    ssh.write_text(FAKE_SSH_SIZING)
    ssh.chmod(0o755)

    done = subprocess.run(
        [str(ROOT / "scripts/deploy/sizing.sh"), "watch"],
        cwd=ROOT,
        env={
            **os.environ,
            "PATH": f"{bin_dir}:{os.environ['PATH']}",
            "AGB_DEPLOY_HOST": "server.test",
            "AGB_DEPLOY_DIR": "/deploy/agb-trader",
        },
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        timeout=60,
    )

    assert done.returncode == 0, done.stderr
    assert "the sampler is not running on server.test" in done.stderr
    assert "sampling on server.test" not in done.stdout


# ssh for sizing.sh `start`: it records every remote command and answers `pgrep` the way the
# server does, with nothing until the sampler has been launched and a pid after it.
FAKE_SSH_START = """#!/usr/bin/env bash
cmd=${!#}
echo "$cmd" >> "$AGB_SSH_LOG"
case $cmd in
*pgrep*) grep -q setsid "$AGB_SSH_LOG" && echo 4019410 ;;
*"cat > "*) cat > /dev/null ;;
esac
exit 0
"""


def test_sizing_start_closes_the_remote_shell_s_output(tmp_path):
    """`ssh` returns when the remote command's output pipes close, not when the command exits,
    and the shell that backgrounds the sampler goes on holding them: `start` launched the
    sampler and then hung until it ended. Only the sampler's own output is redirected, so the
    shell's has to be sent to /dev/null too.

    The hang itself needs the server's bash to reproduce (5.1.4 holds the pipes; the
    workstation's 5.3.9 does not), so what is checked here is the shape of the command.
    """
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    ssh = bin_dir / "ssh"
    ssh.write_text(FAKE_SSH_START)
    ssh.chmod(0o755)
    log = tmp_path / "ssh.log"
    log.write_text("")

    done = subprocess.run(
        [str(ROOT / "scripts/deploy/sizing.sh"), "start"],
        cwd=ROOT,
        env={
            **os.environ,
            "PATH": f"{bin_dir}:{os.environ['PATH']}",
            "AGB_SSH_LOG": str(log),
            "AGB_DEPLOY_HOST": "server.test",
            "AGB_DEPLOY_DIR": "/deploy/agb-trader",
        },
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        timeout=60,
    )

    assert done.returncode == 0, done.stderr
    assert "as pid 4019410" in done.stdout
    launch = [line for line in log.read_text().splitlines() if "setsid" in line]
    assert len(launch) == 1, log.read_text()
    assert launch[0].startswith("exec > /dev/null 2>&1;")
    # The sampler's own output still goes to the log it keeps on the server.
    assert ">> /deploy/agb-trader/measure.log 2>&1 < /dev/null &" in launch[0]
