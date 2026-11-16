"""The whole stack, end to end: acceptance criteria (a), (b) and (c) of T01.

Builds the app image and brings the stack up under a Compose project name unique to this
worktree, with the committed example env files, then tears it down with its volume.
"""

import http.client
import os
import shutil
import subprocess
import time
from datetime import date

import pytest
from conftest import ROOT, worktree_id
from sharadar_fake import HEADERS, sep_row, to_csv

from agb import lake

pytestmark = pytest.mark.stack


@pytest.fixture(scope="module")
def image_tag():
    return f"agb-t-{worktree_id()}"


@pytest.fixture(scope="module")
def lake_dir(tmp_path_factory):
    return tmp_path_factory.mktemp("lake")


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
        "AGB_HEARTBEAT_SECONDS": "1",
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

    run("down", "--volumes", "--remove-orphans")
    try:
        up = run("up", "--detach", "--build", "--wait", check=False)
        assert up.returncode == 0, up.stderr[-4000:]
        yield run
    finally:
        run("down", "--volumes", "--remove-orphans", check=False)


def status_line(run, service) -> list[str]:
    out = run("run", "--rm", "cli", "status").stdout
    [line] = [line for line in out.splitlines() if line.split()[:1] == [service]]
    return line.split()


def test_one_command_brings_up_the_database_and_the_services(compose):
    ps = compose("ps", "--all", "--format", "{{.Service}} {{.State}} {{.ExitCode}}").stdout
    states = dict(line.split(" ", 1) for line in ps.splitlines())
    assert states["db"].startswith("running")
    assert states["scheduler"].startswith("running")
    assert states["backup"].startswith("running")
    assert states["pipeline"].startswith("running")
    assert states["execution"].startswith("running")
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


@pytest.mark.parametrize("service", ["scheduler", "backup", "pipeline", "execution"])
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
