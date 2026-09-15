import shutil

import pytest

from agb import migrate
from agb.roles import OWNER, PASSWORD_VARS


def copy_migrations(tmp_path):
    directory = tmp_path / "migrations"
    shutil.copytree(migrate.migrations_dir(), directory)
    return directory


def test_every_migration_is_well_named():
    migrations = migrate.discover(migrate.migrations_dir())
    assert migrations, "no migrations found"
    assert [m.version for m in migrations] == sorted(m.version for m in migrations)


@pytest.mark.parametrize(
    "name", ["001_init.sql", "20260915120000-init.sql", "20260915120000_Init.sql"]
)
def test_badly_named_migration_is_refused(tmp_path, name):
    (tmp_path / name).write_text("SELECT 1;")
    with pytest.raises(migrate.MigrationError, match="does not match"):
        migrate.discover(tmp_path)


def test_duplicate_versions_are_refused(tmp_path):
    (tmp_path / "20260915120000_a.sql").write_text("SELECT 1;")
    (tmp_path / "20260915120000_b.sql").write_text("SELECT 1;")
    with pytest.raises(migrate.MigrationError, match="share a version"):
        migrate.discover(tmp_path)


def test_rerun_applies_nothing(db):
    assert migrate.apply(db.connect(OWNER), migrate.migrations_dir()) == []


def test_new_migration_is_applied_and_recorded(db, tmp_path):
    directory = copy_migrations(tmp_path)
    (directory / "29990101000000_extra.sql").write_text("CREATE TABLE agb.extra (id int);")
    owner = db.connect(OWNER)

    applied = migrate.apply(owner, directory)

    assert [m.name for m in applied] == ["extra"]
    row = owner.execute(
        "SELECT name, applied_by FROM agb.schema_migration WHERE version = '29990101000000'"
    ).fetchone()
    assert row == ("extra", OWNER)


def test_failed_migration_leaves_no_trace(db, tmp_path):
    directory = copy_migrations(tmp_path)
    (directory / "29990101000000_broken.sql").write_text(
        "CREATE TABLE agb.half (id int); SELECT no_such_function();"
    )
    owner = db.connect(OWNER)

    with pytest.raises(Exception, match="no_such_function"):
        migrate.apply(owner, directory)

    assert owner.execute("SELECT to_regclass('agb.half')").fetchone()[0] is None
    assert "29990101000000" not in migrate.applied(owner)


def test_edited_migration_is_refused(db, tmp_path):
    directory = copy_migrations(tmp_path)
    first = sorted(directory.glob("*.sql"))[0]
    first.write_text(first.read_text() + "\n-- an edit\n")

    with pytest.raises(migrate.MigrationError, match="changed after it was applied"):
        migrate.apply(db.connect(OWNER), directory)


def test_missing_passwords_are_reported(monkeypatch):
    for var in PASSWORD_VARS.values():
        monkeypatch.delenv(var, raising=False)
    with pytest.raises(migrate.MigrationError, match="AGB_OWNER_PASSWORD"):
        migrate.passwords_from_env()


def test_main_bootstraps_and_migrates_a_blank_database(cluster, blank_db, monkeypatch):
    for key, value in cluster.pg_env(blank_db, "postgres").items():
        monkeypatch.setenv(key, value)
    for role, var in PASSWORD_VARS.items():
        monkeypatch.setenv(var, cluster.passwords[role])

    assert migrate.main() == 0
    assert migrate.main() == 0  # and again, as every `make up` does

    with cluster.connect(blank_db, "scheduler") as conn:
        assert conn.execute("SELECT count(*) FROM agb.heartbeat").fetchone() == (0,)
        assert conn.execute("SHOW timezone").fetchone() == ("UTC",)
