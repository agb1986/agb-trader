"""History is append-only: acceptance criteria (d) and (e) of T01, and a catalog check that
keeps every later migration to the same rule."""

import psycopg
import pytest
from psycopg import errors

from agb.roles import OWNER, SERVICE_ROLES, SERVICES

# The spec's mutable exceptions: tables, and the operations on them, that are not
# append-only. On agb.job only the lease columns may change; tests/test_jobs.py covers that.
MUTABLE = {"agb.heartbeat": {"UPDATE"}, "agb.job": {"UPDATE"}}

CHANGES = {
    "UPDATE": "UPDATE {} SET note = 'edited'",
    "DELETE": "DELETE FROM {}",
    "TRUNCATE": "TRUNCATE {}",
}


@pytest.fixture
def history(db):
    """A history table built the way every migration builds one, holding one row."""
    owner = db.connect(OWNER)
    owner.execute("""
        CREATE TABLE agb.test_history (
            id     bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            note   text NOT NULL,
            actor  agb.actor NOT NULL
        );
        SELECT agb.make_append_only('agb.test_history');
        GRANT SELECT, INSERT ON agb.test_history TO pipeline;
    """)
    db.connect("pipeline").execute(
        "INSERT INTO agb.test_history (note, actor) VALUES ('first', 'pipeline')"
    )
    return "agb.test_history"


def test_service_can_append_and_read(db, history):
    pipeline = db.connect("pipeline")
    pipeline.execute(f"INSERT INTO {history} (note, actor) VALUES ('second', 'system')")
    assert pipeline.execute(f"SELECT note FROM {history} ORDER BY id").fetchall() == [
        ("first",),
        ("second",),
    ]


@pytest.mark.parametrize("op", CHANGES)
def test_service_cannot_change_history(db, history, op):
    with pytest.raises(errors.InsufficientPrivilege):
        db.connect("pipeline").execute(CHANGES[op].format(history))


@pytest.mark.parametrize("op", CHANGES)
def test_owner_cannot_change_history(db, history, op):
    with pytest.raises(errors.RaiseException, match=f"append-only: {op} refused"):
        db.connect(OWNER).execute(CHANGES[op].format(history))


@pytest.mark.parametrize(
    "statement",
    [
        "UPDATE agb.schema_migration SET name = 'edited'",
        "DELETE FROM agb.schema_migration",
        "TRUNCATE agb.schema_migration",
    ],
)
def test_owner_cannot_rewrite_the_migration_ledger(db, statement):
    with pytest.raises(errors.RaiseException, match="append-only"):
        db.connect(OWNER).execute(statement)


def test_service_cannot_write_outside_its_grants(db, history):
    with pytest.raises(errors.InsufficientPrivilege):
        db.connect("scheduler").execute(
            f"INSERT INTO {history} (note, actor) VALUES ('forged', 'scheduler')"
        )


@pytest.mark.parametrize("role", SERVICE_ROLES)
def test_service_cannot_write_the_migration_ledger(db, role):
    with pytest.raises(errors.InsufficientPrivilege):
        db.connect(role).execute(
            "INSERT INTO agb.schema_migration (version, name, sha256) VALUES ('0', 'x', 'x')"
        )


@pytest.mark.parametrize("role", SERVICE_ROLES)
@pytest.mark.parametrize("schema", ["agb", "public"])
def test_service_cannot_create_tables(db, role, schema):
    with pytest.raises(errors.InsufficientPrivilege):
        db.connect(role).execute(f"CREATE TABLE {schema}.sneaky (id int)")


@pytest.mark.parametrize("role", SERVICE_ROLES)
def test_service_cannot_create_temporary_tables(db, role):
    with pytest.raises(errors.InsufficientPrivilege):
        db.connect(role).execute("CREATE TEMPORARY TABLE sneaky (id int)")


def agb_tables(conn: psycopg.Connection) -> list[str]:
    return [
        name
        for (name,) in conn.execute("""
            SELECT format('%I.%I', n.nspname, c.relname)
            FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'agb' AND c.relkind IN ('r', 'p')
            ORDER BY 1
        """)
    ]


def refused_ops(conn: psycopg.Connection, table: str) -> set[str]:
    """The operations an enabled refuse_history_change trigger blocks on `table`."""
    # tgtype bits: 1 row, 2 before, 8 delete, 16 update, 32 truncate.
    rows = conn.execute(
        """
        SELECT tgtype FROM pg_trigger
        WHERE tgrelid = %s::regclass
          AND tgfoid = 'agb.refuse_history_change'::regproc
          AND tgenabled IN ('O', 'A')
        """,
        (table,),
    ).fetchall()
    ops = set()
    for (tgtype,) in rows:
        before, row = tgtype & 2, tgtype & 1
        if before and row and tgtype & 16:
            ops.add("UPDATE")
        if before and row and tgtype & 8:
            ops.add("DELETE")
        if before and not row and tgtype & 32:
            ops.add("TRUNCATE")
    return ops


def test_every_table_is_append_only_except_the_declared_exceptions(db):
    conn = db.connect(OWNER)
    tables = agb_tables(conn)
    assert set(MUTABLE) <= set(tables), "a declared exception no longer exists"
    for table in tables:
        expected = {"UPDATE", "DELETE", "TRUNCATE"} - MUTABLE.get(table, set())
        assert refused_ops(conn, table) == expected, table


def test_no_service_role_may_change_history(db):
    conn = db.connect(OWNER)
    for table in agb_tables(conn):
        for role in SERVICE_ROLES:
            held = {
                op
                for op, check in [
                    ("UPDATE", "has_any_column_privilege(%(role)s, %(table)s, 'UPDATE')"),
                    ("DELETE", "has_table_privilege(%(role)s, %(table)s, 'DELETE')"),
                    ("TRUNCATE", "has_table_privilege(%(role)s, %(table)s, 'TRUNCATE')"),
                ]
                if conn.execute(f"SELECT {check}", {"role": role, "table": table}).fetchone()[0]
            }
            allowed = MUTABLE.get(table, set()) if role in SERVICES else set()
            assert held <= allowed, f"{role} holds {held - allowed} on {table}"
