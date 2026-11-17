"""The test harness: a throwaway Postgres per pytest session, and a fresh database per test.

Built so any number of worktrees can run `make test` at once:
  - The Postgres container is named after this worktree and the pytest process, keeps its
    data on tmpfs, and publishes on a random localhost port, never a fixed one.
  - The container carries a label naming the pytest process. A session that is killed before it
    can remove its container leaves it behind, so each session starts by removing the labelled
    containers whose process has gone, and nothing else: a running session's container, and any
    container without our label, are left alone.
  - The schema is built once per session into a template database, then each test gets its
    own copy, which is dropped afterwards.
  - Role passwords are random for each session; no real env file is ever read.
"""

import hashlib
import os
import re
import secrets
import subprocess
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path

import psycopg
import pytest
import yaml
from psycopg import sql

from agb import migrate
from agb.roles import ALL_ROLES, OWNER

ROOT = Path(__file__).resolve().parents[1]
TEMPLATE = "agb_template"
SUPERUSER = "postgres"


def worktree_slug() -> str:
    return re.sub(r"[^a-z0-9]+", "-", ROOT.name.lower()).strip("-") or "agb"


def worktree_id() -> str:
    """Unique per checkout, even for two worktrees that share a directory name."""
    return f"{worktree_slug()}-{hashlib.sha256(str(ROOT).encode()).hexdigest()[:8]}"


def db_image() -> str:
    """The Postgres image the stack runs, so the tests never drift from it."""
    return yaml.safe_load((ROOT / "compose.yaml").read_text())["services"]["db"]["image"]


@dataclass
class Cluster:
    host: str
    port: int
    passwords: dict[str, str]
    # The Postgres container, for running the client tools inside it.
    container: str

    def connect(self, dbname: str, user: str) -> psycopg.Connection:
        return psycopg.connect(
            host=self.host,
            port=self.port,
            dbname=dbname,
            user=user,
            password=self.passwords[user],
            autocommit=True,
        )

    def pg_env(self, dbname: str, user: str) -> dict[str, str]:
        """The PG* variables a service container would get, for code that reads them."""
        return {
            "PGHOST": self.host,
            "PGPORT": str(self.port),
            "PGDATABASE": dbname,
            "PGUSER": user,
            "PGPASSWORD": self.passwords[user],
        }


def _docker(*args: str) -> str:
    return subprocess.run(
        ["docker", *args], check=True, capture_output=True, text=True
    ).stdout.strip()


LABEL = "agb.test"
PID_LABEL = "agb.test.pid"
NAME_PREFIX = "agb-test-"


def alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:  # the process exists and belongs to someone else
        return True
    return True


def stale_containers(listing: str, running=alive) -> list[str]:
    """The names to remove from `docker ps` output of `name<TAB>pid label` lines: containers our
    harness started (by label and by name) whose pytest process is no longer running. A line
    with no usable pid, such as a container from before the pid label, is read from the name's
    last field, and left alone if that is no pid either."""
    names = []
    for line in listing.splitlines():
        name, _, label = line.partition("\t")
        if not name.startswith(NAME_PREFIX):
            continue
        pid = label.strip() or name.rsplit("-", 1)[-1]
        if pid.isdigit() and not running(int(pid)):
            names.append(name)
    return names


def remove_stale_containers() -> list[str]:
    listing = _docker(
        "ps", "--all", "--filter", f"label={LABEL}=1",
        "--format", '{{.Names}}\t{{.Label "' + PID_LABEL + '"}}',
    )  # fmt: skip
    names = stale_containers(listing)
    if names:
        subprocess.run(["docker", "rm", "--force", *names], capture_output=True)
    return names


def _wait_for(cluster: Cluster, timeout: float = 60.0) -> None:
    # A TCP connection is the only reliable signal: during initdb the entrypoint runs a
    # temporary server on the Unix socket only, where pg_isready already reports success.
    deadline = time.monotonic() + timeout
    while True:
        try:
            cluster.connect("postgres", SUPERUSER).close()
            return
        except psycopg.OperationalError:
            if time.monotonic() > deadline:
                raise
            time.sleep(0.2)


# The tests run their baskets on sessions in the past, with the services' clocks injected, but
# the database judges an approval against its own clock (agb.approval_clock). This pins that
# clock to whatever a connection sets `agb.test_clock` to, and to the distant past when it
# sets nothing, so a basket is never too late unless a test says so. Only the template is
# changed; a deployed database keeps clock_timestamp().
_PINNED_CLOCK = """
    CREATE OR REPLACE FUNCTION agb.approval_clock() RETURNS timestamptz
    LANGUAGE sql VOLATILE AS $$
        SELECT coalesce(nullif(current_setting('agb.test_clock', true), '')::timestamptz,
                        '-infinity')
    $$
"""


def pin_approval_clock(owner: psycopg.Connection) -> None:
    owner.execute(_PINNED_CLOCK)


@pytest.fixture(scope="session")
def cluster() -> Iterator[Cluster]:
    passwords = {role: secrets.token_hex(16) for role in (SUPERUSER, *ALL_ROLES)}
    name = f"{NAME_PREFIX}{worktree_slug()}-{os.getpid()}"
    remove_stale_containers()
    _docker(
        "run", "--detach", "--rm", "--name", name,
        "--label", f"{LABEL}=1", "--label", f"{PID_LABEL}={os.getpid()}",
        "--publish", "127.0.0.1::5432",
        "--tmpfs", "/var/lib/postgresql/data",
        "--env", f"POSTGRES_PASSWORD={passwords[SUPERUSER]}",
        db_image(),
        # Durability is pointless for a throwaway database; turning it off saves seconds.
        "-c", "fsync=off", "-c", "full_page_writes=off", "-c", "synchronous_commit=off",
    )  # fmt: skip
    try:
        host, port = _docker("port", name, "5432/tcp").splitlines()[0].rsplit(":", 1)
        cluster = Cluster(host, int(port), passwords, name)
        _wait_for(cluster)
        with cluster.connect("postgres", SUPERUSER) as admin:
            admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(TEMPLATE)))
        with cluster.connect(TEMPLATE, SUPERUSER) as admin:
            migrate.bootstrap(admin, passwords)
        with cluster.connect(TEMPLATE, OWNER) as owner:
            migrate.apply(owner, migrate.migrations_dir())
            pin_approval_clock(owner)
        yield cluster
    finally:
        subprocess.run(["docker", "rm", "--force", name], capture_output=True)


@dataclass
class Database:
    cluster: Cluster
    name: str
    _open: list[psycopg.Connection] = field(default_factory=list)

    def connect(self, role: str) -> psycopg.Connection:
        """A connection as `role`, autocommit, closed when the test ends."""
        conn = self.cluster.connect(self.name, role)
        self._open.append(conn)
        return conn

    def pg_env(self, role: str) -> dict[str, str]:
        return self.cluster.pg_env(self.name, role)


def create_database(cluster: Cluster, template: str) -> str:
    name = f"t_{secrets.token_hex(6)}"
    with cluster.connect("postgres", SUPERUSER) as admin:
        admin.execute(
            sql.SQL("CREATE DATABASE {} TEMPLATE {}").format(
                sql.Identifier(name), sql.Identifier(template)
            )
        )
    return name


def drop_database(cluster: Cluster, name: str) -> None:
    with cluster.connect("postgres", SUPERUSER) as admin:
        admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))


@pytest.fixture
def db(cluster: Cluster) -> Iterator[Database]:
    """A fresh, fully migrated database for this test alone."""
    name = create_database(cluster, TEMPLATE)
    # The database-level grants are not copied from a template, so apply them as the
    # migrate service would. This also re-runs bootstrap, which must be idempotent.
    with cluster.connect(name, SUPERUSER) as admin:
        migrate.bootstrap(admin, cluster.passwords)
    database = Database(cluster, name)
    try:
        yield database
    finally:
        for conn in database._open:
            conn.close()
        drop_database(cluster, name)


@pytest.fixture
def blank_db(cluster: Cluster) -> Iterator[str]:
    """An empty database with no roles granted and no schema, as on first deploy."""
    name = create_database(cluster, "template0")
    try:
        yield name
    finally:
        drop_database(cluster, name)


# The research panel: a fake lake of two dozen names over several years, built once per
# session because the feature history over thirty month-ends takes a while. The lake is
# locked read-only once landed, so a test that writes into it fails where it writes. The built
# panel is shared too, and the session checks at its end that it never changed.


def _lock(root: Path, locked: bool) -> None:
    """Make every file and directory under `root` read-only, or writable again."""
    paths = [root, *sorted(root.rglob("*"), reverse=locked)]
    for path in paths:
        if path.is_dir():
            path.chmod(0o555 if locked else 0o755)
        else:
            path.chmod(0o444 if locked else 0o644)


@pytest.fixture(scope="session")
def research_lake(tmp_path_factory) -> Iterator[Path]:
    from research_fake import Market

    root = Market(tmp_path_factory.mktemp("research-lake")).names().benchmark().land()
    _lock(root, True)
    try:
        yield root
    finally:
        _lock(root, False)


@pytest.fixture(scope="session")
def research_panel(research_lake: Path):
    """The session's built panel. Read it, never change it: the session fails at its end if the
    frame no longer matches what was built, and a test that needs to change one copies it."""
    import pandas as pd
    from research_fake import END, PANEL_START

    from agb.research import panel

    built = panel.build(research_lake, PANEL_START, END)
    fingerprint = pd.util.hash_pandas_object(built.frame, index=True).sum()
    yield built
    assert pd.util.hash_pandas_object(built.frame, index=True).sum() == fingerprint, (
        "a test changed the session's shared research panel"
    )


@pytest.fixture(scope="module")
def certifiable() -> Iterator[None]:
    """A build version for a module that certifies, since `certify` refuses to run under `dev`.
    Module-scoped, because the certifications those modules share are."""
    with pytest.MonkeyPatch.context() as patch:
        patch.setenv("AGB_VERSION", "test-build")
        yield
