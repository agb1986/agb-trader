"""The test harness: a throwaway Postgres per pytest session, and a fresh database per test.

Built so any number of worktrees can run `make test` at once:
  - The Postgres container is named after this worktree and the pytest process, keeps its
    data on tmpfs, and publishes on a random localhost port, never a fixed one.
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
    name = f"agb-test-{worktree_slug()}-{os.getpid()}"
    _docker(
        "run", "--detach", "--rm", "--name", name,
        "--label", "agb.test=1",
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
# session because the feature history over thirty month-ends takes a while.


@pytest.fixture(scope="session")
def research_lake(tmp_path_factory) -> Path:
    from research_fake import Market

    return Market(tmp_path_factory.mktemp("research-lake")).names().benchmark().land()


@pytest.fixture(scope="session")
def research_panel(research_lake: Path):
    from research_fake import END, PANEL_START

    from agb.research import panel

    return panel.build(research_lake, PANEL_START, END)
