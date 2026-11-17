"""Database bootstrap and migrations. The one-shot `migrate` Compose service runs this module.

Two steps, both idempotent, and both under one advisory lock so that concurrent runs queue:

1. As the Postgres superuser, create or update the owner and service roles, and hand the
   database to the owner.
2. As the owner, apply every migration in `migrations/` that the ledger has not seen, each in
   its own transaction, and record its checksum.

A migration that was applied and has since changed on disk is an error: migrations are never
edited once they are on main. So is a pending migration that is older than the newest applied
one, because it would run out of order; `--allow-out-of-order` (or
`AGB_MIGRATE_ALLOW_OUT_OF_ORDER=1`) lets it through.
"""

import argparse
import hashlib
import logging
import os
import re
import sys
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

import psycopg
from psycopg import sql

from agb.roles import ALL_ROLES, BACKUP, OWNER, PASSWORD_VARS, SERVICE_ROLES

log = logging.getLogger(__name__)

FILENAME = re.compile(r"^(?P<version>\d{14})_(?P<name>[a-z0-9_]+)\.sql$")

# Serialises concurrent runs, e.g. two `up` commands racing. Any constant works.
_LOCK_KEY = 0x616762

ALLOW_OUT_OF_ORDER_VAR = "AGB_MIGRATE_ALLOW_OUT_OF_ORDER"


class MigrationError(Exception):
    pass


@dataclass(frozen=True)
class Migration:
    version: str
    name: str
    path: Path

    @property
    def sql(self) -> str:
        return self.path.read_text()

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.path.read_bytes()).hexdigest()


def migrations_dir() -> Path:
    default = Path(__file__).resolve().parents[2] / "migrations"
    return Path(os.environ.get("AGB_MIGRATIONS_DIR", default))


def discover(directory: Path) -> list[Migration]:
    migrations = []
    for path in sorted(directory.glob("*.sql")):
        match = FILENAME.match(path.name)
        if match is None:
            raise MigrationError(f"{path.name} does not match YYYYMMDDHHMMSS_lowercase_slug.sql")
        migrations.append(Migration(match["version"], match["name"], path))
    versions = [m.version for m in migrations]
    if len(versions) != len(set(versions)):
        raise MigrationError("two migrations share a version")
    return migrations


@contextmanager
def advisory_lock(conn: psycopg.Connection) -> Iterator[None]:
    """Hold the migration lock on `conn`, waiting for any other run to finish first.

    The lock belongs to the database, not to a role, so a run that holds it on its superuser
    connection keeps out a run that asks for it on its owner connection.
    """
    conn.execute("SELECT pg_advisory_lock(%s)", (_LOCK_KEY,))
    try:
        yield
    finally:
        conn.execute("SELECT pg_advisory_unlock(%s)", (_LOCK_KEY,))


def bootstrap(admin: psycopg.Connection, passwords: Mapping[str, str]) -> None:
    """Create or update every role and give the connected database to the owner.

    `admin` is a superuser connection to the target database. Each role logs in with its own
    password, is never a superuser, and sees UTC timestamps.

    No password reaches the server. Each one is turned into a SCRAM-SHA-256 verifier here, by
    libpq, as psql's `\\password` does, and the statement carries only the verifier, which
    Postgres stores as given. A statement log, or an error that echoes the statement, shows a
    salted hash a client cannot log in with.
    """
    dbname = admin.info.dbname
    with admin.transaction():
        for role in ALL_ROLES:
            ident = sql.Identifier(role)
            password = sql.Literal(verifier(admin, role, passwords[role]))
            exists = admin.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (role,))
            verb = sql.SQL("ALTER" if exists.fetchone() else "CREATE")
            admin.execute(
                sql.SQL(
                    "{} ROLE {} LOGIN PASSWORD {} NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS"
                ).format(verb, ident, password)
            )
            admin.execute(sql.SQL("ALTER ROLE {} SET timezone = 'UTC'").format(ident))
            admin.execute(sql.SQL("ALTER ROLE {} SET search_path = agb").format(ident))
        # pg_dump reads every table. Only a superuser can grant a predefined role, so this
        # lives here rather than in a migration, and it covers tables no migration names yet.
        admin.execute(sql.SQL("GRANT pg_read_all_data TO {}").format(sql.Identifier(BACKUP)))
        db = sql.Identifier(dbname)
        admin.execute(sql.SQL("ALTER DATABASE {} OWNER TO {}").format(db, sql.Identifier(OWNER)))
        admin.execute(sql.SQL("ALTER DATABASE {} SET timezone = 'UTC'").format(db))
        admin.execute(sql.SQL("REVOKE ALL ON DATABASE {} FROM PUBLIC").format(db))
        admin.execute(
            sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
                db, sql.SQL(", ").join(map(sql.Identifier, SERVICE_ROLES))
            )
        )
    log.info("bootstrapped roles %s on %s", ", ".join(ALL_ROLES), dbname)


def verifier(conn: psycopg.Connection, role: str, password: str) -> str:
    """The SCRAM-SHA-256 verifier for `role`'s `password`, computed on this side of `conn`.

    A fresh random salt each call, so the stored secret changes on every bootstrap even when
    the password does not; logins are unaffected.
    """
    encrypted = conn.pgconn.encrypt_password(password.encode(), role.encode(), b"scram-sha-256")
    return encrypted.decode()


def applied(owner: psycopg.Connection) -> dict[str, str]:
    """The ledger: version to checksum. Empty before the first migration creates it."""
    if owner.execute("SELECT to_regclass('agb.schema_migration')").fetchone()[0] is None:
        return {}
    rows = owner.execute("SELECT version, sha256 FROM agb.schema_migration").fetchall()
    return dict(rows)


def apply(
    owner: psycopg.Connection,
    directory: Path,
    *,
    allow_out_of_order: bool = False,
    locked: bool = False,
) -> list[Migration]:
    """Apply every pending migration in order, as the owner. Returns the ones applied.

    A pending migration older than the newest version in the ledger is refused unless
    `allow_out_of_order` is set. `locked` says the caller already holds the advisory lock, as
    `main` does from before the bootstrap; otherwise `apply` takes it on `owner`.
    """
    migrations = discover(directory)
    if locked:
        return _apply_locked(owner, migrations, allow_out_of_order)
    with advisory_lock(owner):
        return _apply_locked(owner, migrations, allow_out_of_order)


def _apply_locked(
    owner: psycopg.Connection, migrations: list[Migration], allow_out_of_order: bool
) -> list[Migration]:
    done = applied(owner)
    for m in migrations:
        if m.version in done and done[m.version] != m.sha256:
            raise MigrationError(
                f"{m.path.name} changed after it was applied; add a new migration instead"
            )
    # A version in the ledger with no file is not an error: an older image running
    # against a newer schema is expected after a rollback, and the additive-only rule
    # keeps it working. It still counts as the newest applied version below.
    pending = [m for m in migrations if m.version not in done]
    newest = max(done, default=None)
    if newest is not None and not allow_out_of_order:
        for m in pending:
            if m.version < newest:
                raise MigrationError(
                    f"{m.path.name} is older than the applied version {newest} and would run "
                    f"out of order; rename it with a newer timestamp, or run with "
                    f"--allow-out-of-order (or {ALLOW_OUT_OF_ORDER_VAR}=1) to apply it anyway"
                )
    for m in pending:
        with owner.transaction():
            owner.execute(m.sql)
            owner.execute(
                "INSERT INTO agb.schema_migration (version, name, sha256) VALUES (%s, %s, %s)",
                (m.version, m.name, m.sha256),
            )
        log.info("applied %s", m.path.name)
    return pending


def passwords_from_env() -> dict[str, str]:
    missing = [var for var in PASSWORD_VARS.values() if not os.environ.get(var)]
    if missing:
        raise MigrationError(f"missing role passwords: {', '.join(missing)}")
    return {role: os.environ[var] for role, var in PASSWORD_VARS.items()}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m agb.migrate", description=__doc__)
    parser.add_argument(
        "--allow-out-of-order",
        action="store_true",
        help="apply a pending migration even if it is older than the newest applied one "
        f"(also set by {ALLOW_OUT_OF_ORDER_VAR}=1)",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Bootstrap and migrate the database the PG* environment points at.

    PGUSER and PGPASSWORD are the superuser's; the owner's password comes from
    AGB_OWNER_PASSWORD. The advisory lock is taken on the superuser connection before the
    bootstrap and held until the last migration is recorded.
    """
    args = _parser().parse_args(argv)
    allow = args.allow_out_of_order or os.environ.get(ALLOW_OUT_OF_ORDER_VAR) == "1"
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    try:
        passwords = passwords_from_env()
        with psycopg.connect(autocommit=True) as admin, advisory_lock(admin):
            bootstrap(admin, passwords)
            with psycopg.connect(user=OWNER, password=passwords[OWNER], autocommit=True) as owner:
                pending = apply(owner, migrations_dir(), allow_out_of_order=allow, locked=True)
    except (MigrationError, psycopg.Error) as exc:
        log.error("%s", exc)
        return 1
    log.info("schema up to date (%d applied)", len(pending))
    return 0


if __name__ == "__main__":
    sys.exit(main())
