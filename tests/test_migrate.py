import shutil
import threading
import time
from concurrent.futures import ThreadPoolExecutor

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

    assert migrate.main([]) == 0
    assert migrate.main([]) == 0  # and again, as every `make up` does

    with cluster.connect(blank_db, "scheduler") as conn:
        assert conn.execute("SELECT count(*) FROM agb.heartbeat").fetchone() == (0,)
        assert conn.execute("SHOW timezone").fetchone() == ("UTC",)


def add(directory, version, name="extra"):
    (directory / f"{version}_{name}.sql").write_text(f"CREATE TABLE agb.t{version} (id int);")


def newest_applied(owner):
    return max(migrate.applied(owner))


def test_several_pending_migrations_apply_in_order(db, tmp_path):
    directory = copy_migrations(tmp_path)
    add(directory, "29990101000002", "second")
    add(directory, "29990101000001", "first")

    applied = migrate.apply(db.connect(OWNER), directory)

    assert [m.version for m in applied] == ["29990101000001", "29990101000002"]


def test_out_of_order_migration_is_refused_naming_both_versions(db, tmp_path):
    directory = copy_migrations(tmp_path)
    owner = db.connect(OWNER)
    newest = newest_applied(owner)
    late = str(int(newest) - 1)
    add(directory, late, "late")

    with pytest.raises(migrate.MigrationError) as refused:
        migrate.apply(owner, directory)

    message = str(refused.value)
    assert f"{late}_late.sql" in message
    assert newest in message
    assert "--allow-out-of-order" in message
    assert owner.execute("SELECT to_regclass(%s)", (f"agb.t{late}",)).fetchone()[0] is None
    assert late not in migrate.applied(owner)


def test_out_of_order_migration_is_applied_when_allowed(db, tmp_path):
    directory = copy_migrations(tmp_path)
    owner = db.connect(OWNER)
    late = str(int(newest_applied(owner)) - 1)
    add(directory, late, "late")

    applied = migrate.apply(owner, directory, allow_out_of_order=True)

    assert [m.version for m in applied] == [late]
    assert late in migrate.applied(owner)


def test_ledger_version_without_a_file_is_allowed(db, tmp_path):
    # A rollback runs an older image, with fewer files, against the newer schema.
    directory = copy_migrations(tmp_path)
    owner = db.connect(OWNER)
    newest = sorted(directory.glob("*.sql"))[-1]
    newest.unlink()

    assert migrate.apply(owner, directory) == []
    assert newest.name.split("_")[0] in migrate.applied(owner)


def test_main_refuses_out_of_order_unless_flagged(cluster, blank_db, tmp_path, monkeypatch):
    use_blank_db(cluster, blank_db, monkeypatch)
    directory = copy_migrations(tmp_path)
    monkeypatch.setenv("AGB_MIGRATIONS_DIR", str(directory))
    assert migrate.main([]) == 0
    late = str(int(newest_applied_in(cluster, blank_db)) - 1)
    add(directory, late, "late")

    assert migrate.main([]) == 1
    assert migrate.main(["--allow-out-of-order"]) == 0
    assert late in applied_in(cluster, blank_db)


def test_main_reads_the_flag_from_the_environment(cluster, blank_db, tmp_path, monkeypatch):
    use_blank_db(cluster, blank_db, monkeypatch)
    directory = copy_migrations(tmp_path)
    monkeypatch.setenv("AGB_MIGRATIONS_DIR", str(directory))
    assert migrate.main([]) == 0
    late = str(int(newest_applied_in(cluster, blank_db)) - 1)
    add(directory, late, "late")
    monkeypatch.setenv(migrate.ALLOW_OUT_OF_ORDER_VAR, "1")

    assert migrate.main([]) == 0
    assert late in applied_in(cluster, blank_db)


def use_blank_db(cluster, blank_db, monkeypatch):
    for key, value in cluster.pg_env(blank_db, "postgres").items():
        monkeypatch.setenv(key, value)
    for role, var in PASSWORD_VARS.items():
        monkeypatch.setenv(var, cluster.passwords[role])


def applied_in(cluster, dbname):
    with cluster.connect(dbname, OWNER) as owner:
        return migrate.applied(owner)


def newest_applied_in(cluster, dbname):
    return max(applied_in(cluster, dbname))


def run_together(count, target):
    """Run `target` on `count` threads released at once; return their results in order."""
    barrier = threading.Barrier(count)

    def go():
        barrier.wait()
        return target()

    with ThreadPoolExecutor(count) as pool:
        futures = [pool.submit(go) for _ in range(count)]
        return [f.result(timeout=120) for f in futures]


def test_concurrent_migrators_apply_each_migration_once(db, tmp_path):
    directory = copy_migrations(tmp_path)
    add(directory, "29990101000001", "first")
    add(directory, "29990101000002", "second")

    def migrator():
        with db.cluster.connect(db.name, OWNER) as owner:
            return [m.version for m in migrate.apply(owner, directory)]

    results = run_together(4, migrator)

    assert sorted(v for applied in results for v in applied) == [
        "29990101000001",
        "29990101000002",
    ]
    owner = db.connect(OWNER)
    rows = owner.execute("SELECT version FROM agb.schema_migration").fetchall()
    assert len(rows) == len({version for (version,) in rows})
    assert set(migrate.applied(owner)) == {m.version for m in migrate.discover(directory)}


def test_concurrent_mains_on_a_blank_database_leave_one_applied_set(cluster, blank_db, monkeypatch):
    use_blank_db(cluster, blank_db, monkeypatch)

    assert run_together(3, lambda: migrate.main([])) == [0, 0, 0]

    expected = {m.version for m in migrate.discover(migrate.migrations_dir())}
    assert set(applied_in(cluster, blank_db)) == expected
    with cluster.connect(blank_db, OWNER) as owner:
        count = owner.execute("SELECT count(*) FROM agb.schema_migration").fetchone()[0]
    assert count == len(expected)


def waiting_for_the_lock(cluster, dbname):
    with cluster.connect(dbname, "postgres") as conn:
        return conn.execute(
            "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND NOT granted "
            "AND database = (SELECT oid FROM pg_database WHERE datname = %s)",
            (dbname,),
        ).fetchone()[0]


def test_main_waits_for_the_lock_before_it_bootstraps(cluster, blank_db, monkeypatch):
    use_blank_db(cluster, blank_db, monkeypatch)
    with cluster.connect(blank_db, "postgres") as holder, ThreadPoolExecutor(1) as pool:
        with migrate.advisory_lock(holder):
            run = pool.submit(migrate.main, [])
            deadline = time.monotonic() + 30
            while not waiting_for_the_lock(cluster, blank_db):
                assert time.monotonic() < deadline, "main never queued on the lock"
                assert not run.done()
                time.sleep(0.05)
            # Queued, so it has not reached the bootstrap, whose first statement hands the
            # database to the owner.
            owner_of = "SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname = %s"
            assert holder.execute(owner_of, (blank_db,)).fetchone()[0] == "postgres"
            assert not run.done()
        assert run.result(timeout=120) == 0
