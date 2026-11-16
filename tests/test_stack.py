"""The whole stack, end to end: acceptance criteria (a), (b) and (c) of T01.

Builds the app image and brings the stack up under a Compose project name unique to this
worktree, with the committed example env files and a free port for the UI, so it runs beside
a local stack that holds 127.0.0.1:8080. It tears the project down with its volume when the
tests finish, fail, are interrupted with Ctrl-C or are terminated with SIGTERM.
"""

import http.client
import os
import shutil
import signal
import socket
import subprocess
import time
from datetime import date

import pytest
from conftest import ROOT, worktree_id
from sharadar_fake import HEADERS, sep_row, to_csv

from agb import lake

pytestmark = pytest.mark.stack

# The services that write a heartbeat, so the ones `agb status` lists. The bot only runs with
# the telegram profile, and db and migrate write no heartbeat.
BEATING = ("scheduler", "ui", "backup", "pipeline", "execution")
# Short enough that a stopped service reads as stale inside a test's patience; the heartbeat
# interval is one second.
STALE_AFTER_SECONDS = 10


@pytest.fixture(scope="module")
def image_tag():
    return f"agb-t-{worktree_id()}"


@pytest.fixture(scope="module")
def lake_dir(tmp_path_factory):
    return tmp_path_factory.mktemp("lake")


def free_port() -> int:
    """A port nothing listens on at this moment, which Compose publishes the UI on."""
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


@pytest.fixture(scope="module")
def compose(tmp_path_factory, image_tag, lake_dir):
    project = image_tag
    env_dir = tmp_path_factory.mktemp("env")
    for example in (ROOT / "env").glob("*.env.example"):
        shutil.copy(example, env_dir / example.name.removesuffix(".example"))
    env = {
        **os.environ,
        "AGB_ENV_DIR": str(env_dir),
        "AGB_IMAGE_TAG": project,
        "AGB_EXPECTED_SERVICES": " ".join(BEATING),
        "AGB_HEARTBEAT_SECONDS": "1",
        "AGB_STALE_AFTER_SECONDS": str(STALE_AFTER_SECONDS),
        "AGB_UI_PORT": str(free_port()),
        "AGB_BACKUP_DIR": str(tmp_path_factory.mktemp("backups")),
        "AGB_BACKUP_USER": f"{os.getuid()}:{os.getgid()}",
        "AGB_LAKE_DIR": str(lake_dir),
        "AGB_PANEL_DIR": str(tmp_path_factory.mktemp("panel")),
        "AGB_MODELS_DIR": str(tmp_path_factory.mktemp("models")),
        "AGB_PIPELINE_USER": f"{os.getuid()}:{os.getgid()}",
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

    def terminated(signum, frame):
        raise KeyboardInterrupt

    # pytest's own handler covers Ctrl-C; a SIGTERM would otherwise end the process with the
    # project still up.
    previous = signal.signal(signal.SIGTERM, terminated)
    try:
        run("down", "--volumes", "--remove-orphans")
        up = run("up", "--detach", "--build", "--wait", check=False)
        assert up.returncode == 0, up.stderr[-4000:]
        yield run
    finally:
        signal.signal(signal.SIGTERM, previous)
        # An interrupt that arrives during this call must not leave the project behind.
        try:
            run("down", "--volumes", "--remove-orphans", check=False)
        except KeyboardInterrupt:
            run("down", "--volumes", "--remove-orphans", check=False)
            raise


def status(run) -> subprocess.CompletedProcess:
    """`agb status` in the cli container, which exits 1 when an expected service is stale."""
    return run("run", "--rm", "cli", "status", check=False)


def status_line(run, service) -> list[str]:
    out = status(run).stdout
    [line] = [line for line in out.splitlines() if line.split()[:1] == [service]]
    return line.split()


def test_one_command_brings_up_the_database_and_the_services(compose):
    ps = compose("ps", "--all", "--format", "{{.Service}} {{.State}} {{.ExitCode}}").stdout
    states = dict(line.split(" ", 1) for line in ps.splitlines())
    assert states["db"].startswith("running")
    for service in BEATING:
        assert states[service].startswith("running"), (service, states[service])
    assert states["migrate"] == "exited 0"


def test_duckdb_queries_the_lake_inside_the_pipeline_container(compose, lake_dir):
    rows = [
        sep_row("AAPL", "2026-09-14", "328.21"),
        sep_row("LEHMQ", "2008-09-12", "3.65"),
        sep_row("MSFT", "2026-09-14", "512.3"),
    ]
    table = lake.parse(lake.SEP, to_csv(HEADERS["stocks"], rows))
    lake.land(lake_dir, lake.SEP, table, date(2026, 9, 14))

    out = compose(
        "exec", "-T", "pipeline", "python", "-m", "agb.lake",
        "SELECT count(*) AS n, max(date) AS latest FROM sep",
    ).stdout  # fmt: skip

    assert out == "n\tlatest\n3\t2026-09-14\n"


@pytest.mark.parametrize("service", BEATING)
def test_cli_lists_the_heartbeat_and_it_advances(compose, service):
    first = status_line(compose, service)
    assert first[1] != "never", first
    deadline = time.monotonic() + 30
    while status_line(compose, service)[1:3] == first[1:3]:
        assert time.monotonic() < deadline, f"the {service} heartbeat did not advance"
        time.sleep(0.5)


def test_heartbeat_reports_the_image_version(compose, image_tag):
    # The image is built with AGB_VERSION set to its tag, and the scheduler reports it.
    assert status_line(compose, "scheduler")[-1] == image_tag


def test_agb_status_reports_every_service_and_passes(compose):
    result = status(compose)
    assert result.returncode == 0, result.stdout + result.stderr
    listed = [line.split()[0] for line in result.stdout.splitlines()[1:]]
    for service in BEATING:
        assert service in listed, result.stdout


# Issue #170. Opens a connection to an outside address by number, so no name has to resolve,
# and prints the errno's name when it fails. A container with no route off the host fails at
# once with ENETUNREACH, whatever the host's own connection to the internet is doing.
CONNECT_OUT = """
import errno, socket
try:
    socket.create_connection(("1.1.1.1", 443), timeout=5).close()
except OSError as exc:
    print(errno.errorcode.get(exc.errno, repr(exc)))
else:
    print("connected")
"""

# Prints whether the container's routing table holds a default route.
DEFAULT_ROUTE = """
with open("/proc/net/route") as table:
    print(any(line.split()[1] == "00000000" for line in list(table)[1:]))
"""


def in_container(run, service, script) -> str:
    return run("exec", "-T", service, "python", "-c", script).stdout.strip()


@pytest.mark.parametrize("service", ["scheduler", "backup"])
def test_a_service_on_core_alone_cannot_reach_the_internet(compose, service):
    assert in_container(compose, service, DEFAULT_ROUTE) == "False"
    assert in_container(compose, service, CONNECT_OUT) == "ENETUNREACH"


@pytest.mark.parametrize("service", ["pipeline", "execution"])
def test_the_services_that_call_out_have_a_route_off_the_host(compose, service):
    """The control for the test above: the same probe sees a route where outbound gives one.
    Only the route is checked, so the test does not depend on the host being online."""
    assert in_container(compose, service, DEFAULT_ROUTE) == "True"


def test_the_ui_answers_on_the_loopback_address(compose):
    """The UI is on the internal core network and on ui-port, which carries its publication.
    The request is addressed as the operator's tunnel addresses it."""
    address = compose("port", "ui", "8080").stdout.strip()
    host, port = address.rsplit(":", 1)
    assert host == "127.0.0.1", address
    connection = http.client.HTTPConnection(host, int(port), timeout=10)
    try:
        connection.request("HEAD", "/", headers={"Host": "127.0.0.1:8080"})
        assert connection.getresponse().status == 200
    finally:
        connection.close()
    # The landing page itself, not only the routing table that answers a HEAD.
    connection = http.client.HTTPConnection(host, int(port), timeout=10)
    try:
        connection.request("GET", "/", headers={"Host": "127.0.0.1:8080"})
        response = connection.getresponse()
        assert response.status == 200
        assert response.read()
    finally:
        connection.close()


# Last in the module, because it stops a service the tests above need. It shows that the
# assertions above can fail: with one service's process gone its heartbeat goes stale and
# `agb status` exits 1 and names it.
def test_stopping_a_service_makes_agb_status_fail(compose):
    compose("stop", "pipeline")
    deadline = time.monotonic() + STALE_AFTER_SECONDS + 30
    result = status(compose)
    while result.returncode == 0:
        assert time.monotonic() < deadline, "agb status still passes with pipeline stopped"
        time.sleep(1)
        result = status(compose)
    assert "pipeline is stale" in result.stderr, result.stderr
