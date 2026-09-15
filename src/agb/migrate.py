"""Database bootstrap and migrations. The one-shot `migrate` Compose service runs this module.

Two steps, both idempotent:

1. As the Postgres superuser, create or update the owner and service roles, and hand the
   database to the owner.
2. As the owner, apply every migration in `migrations/` that the ledger has not seen, each in
   its own transaction, and record its checksum.

A migration that was applied and has since changed on disk is an error: migrations are never
edited once they are on main.
"""

import hashlib
import logging
import os
import re
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

import psycopg
from psycopg import sql

from agb.roles import ALL_ROLES, BACKUP, OWNER, PASSWORD_VARS, SERVICE_ROLES

log = logging.getLogger(__name__)

FILENAME = re.compile(r"^(?P<version>\d{14})_(?P<name>[a-z0-9_]+)\.sql$")

# Serialises concurrent runs, e.g. two `up` commands racing. Any constant works.
_LOCK_KEY = 0x616762


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


def bootstrap(admin: psycopg.Connection, passwords: Mapping[str, str]) -> None:
    """Create or update every role and give the connected database to the owner.

    `admin` is a superuser connection to the target database. Each role logs in with its own
    password, is never a superuser, and sees UTC timestamps.
    """
    dbname = admin.info.dbname
    with admin.transaction():
        for role in ALL_ROLES:
            ident, password = sql.Identifier(role), sql.Literal(passwords[role])
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


def applied(owner: psycopg.Connection) -> dict[str, str]:
    """The ledger: version to checksum. Empty before the first migration creates it."""
    if owner.execute("SELECT to_regclass('agb.schema_migration')").fetchone()[0] is None:
        return {}
    rows = owner.execute("SELECT version, sha256 FROM agb.schema_migration").fetchall()
    return dict(rows)


def apply(owner: psycopg.Connection, directory: Path) -> list[Migration]:
    """Apply every pending migration in order, as the owner. Returns the ones applied."""
    migrations = discover(directory)
    owner.execute("SELECT pg_advisory_lock(%s)", (_LOCK_KEY,))
    try:
        done = applied(owner)
        for m in migrations:
            if m.version in done and done[m.version] != m.sha256:
                raise MigrationError(
                    f"{m.path.name} changed after it was applied; add a new migration instead"
                )
        # A version in the ledger with no file is not an error: an older image running
        # against a newer schema is expected after a rollback, and the additive-only rule
        # keeps it working.
        pending = [m for m in migrations if m.version not in done]
        for m in pending:
            with owner.transaction():
                owner.execute(m.sql)
                owner.execute(
                    "INSERT INTO agb.schema_migration (version, name, sha256) VALUES (%s, %s, %s)",
                    (m.version, m.name, m.sha256),
                )
            log.info("applied %s", m.path.name)
        return pending
    finally:
        owner.execute("SELECT pg_advisory_unlock(%s)", (_LOCK_KEY,))


def passwords_from_env() -> dict[str, str]:
    missing = [var for var in PASSWORD_VARS.values() if not os.environ.get(var)]
    if missing:
        raise MigrationError(f"missing role passwords: {', '.join(missing)}")
    return {role: os.environ[var] for role, var in PASSWORD_VARS.items()}


def main() -> int:
    """Bootstrap and migrate the database the PG* environment points at.

    PGUSER and PGPASSWORD are the superuser's; the owner's password comes from
    AGB_OWNER_PASSWORD.
    """
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    try:
        passwords = passwords_from_env()
        with psycopg.connect(autocommit=True) as admin:
            bootstrap(admin, passwords)
        with psycopg.connect(user=OWNER, password=passwords[OWNER], autocommit=True) as owner:
            pending = apply(owner, migrations_dir())
    except (MigrationError, psycopg.Error) as exc:
        log.error("%s", exc)
        return 1
    log.info("schema up to date (%d applied)", len(pending))
    return 0


if __name__ == "__main__":
    sys.exit(main())
