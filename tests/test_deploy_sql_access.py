"""How the operator scripts reach the server's database: as `cli` (or `backup` for a row count),
never as the `postgres` superuser, in a read-only session unless the statement writes, and with
every value they were given passed to psql as a variable and never spliced into the SQL.

The scripts run against a stub `ssh` on PATH that records the remote command line and what the
script wrote to its stdin. Nothing here reaches a server, a database or a Docker daemon, so what
psql, the container's socket trust and the server's compose do with these lines is not tested
here."""

import os
import re
import subprocess
from pathlib import Path

import pytest
from conftest import ROOT

DEPLOY = ROOT / "scripts/deploy"
HOSTILE = "2026-10'; --"
READ_ONLY = "PGOPTIONS=-c default_transaction_read_only=on"

# Records "CMD <remote command>" for every call, and "STDIN <statement>" for what a psql call
# is sent, then answers the few commands the scripts read an answer from.
STUB_SSH = r"""#!/usr/bin/env bash
cmd=${*: -1}
echo "CMD $cmd" >> "$FAKE_SSH_LOG"
case $cmd in
*psql*)
    statement=$(cat)
    printf 'STDIN %s\n' "${statement//$'\n'/ }" >> "$FAKE_SSH_LOG"
    case $statement in
    *"FROM agb.panel_audit WHERE period"*) echo "100|100|0|0" ;;
    esac ;;
date*) echo "2026-11-17-drill-153012" ;;
esac
exit 0
"""


@pytest.fixture
def stub(tmp_path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    ssh = bin_dir / "ssh"
    ssh.write_text(STUB_SSH)
    ssh.chmod(0o755)
    log = tmp_path / "ssh.log"
    log.write_text("")
    env = {
        **os.environ,
        "PATH": f"{bin_dir}:{os.environ['PATH']}",
        "FAKE_SSH_LOG": str(log),
        "AGB_DEPLOY_HOST": "stub.invalid",
        "AGB_DEPLOY_DIR": "/srv/agb",
    }
    return env, log


def run(env, *argv):
    return subprocess.run(
        ["bash", *map(str, argv)], env=env, cwd=ROOT, capture_output=True, text=True, timeout=60
    )


def calls(log: Path):
    """(remote command, statement sent to it or None) for each ssh call the script made."""
    out: list[tuple[str, str | None]] = []
    for line in log.read_text().splitlines():
        kind, _, text = line.partition(" ")
        if kind == "CMD":
            out.append((text, None))
        else:
            out[-1] = (out[-1][0], text)
    return out


def psql_calls(log: Path):
    return [(cmd, sql) for cmd, sql in calls(log) if "psql" in cmd]


@pytest.mark.parametrize(
    "period",
    [
        HOSTILE,
        "2026-10-15'; DROP TABLE agb.job; --",
        "2026-11",
        "2026-13-01",
        "2026-02-30",
        "2026-11-15-Drill",
        "2026-11-15-",
        "2026-11-15 drill",
        "2026-11-15\n",
        "$(touch /tmp/x)",
    ],
)
def test_a_period_that_is_not_one_is_refused_before_anything_is_sent(stub, period):
    env, log = stub
    done = run(env, DEPLOY / "panel-check.sh", "run", period)
    assert done.returncode == 1
    assert "is not a period" in done.stderr
    assert log.read_text() == ""


@pytest.mark.parametrize("period", ["2026-11-15", "2026-11-15-again", "2026-02-28-a-b-1"])
def test_a_period_that_is_one_gets_past_the_check(stub, period):
    env, log = stub
    done = run(env, DEPLOY / "panel-check.sh", "run", period)
    assert "is not a period" not in done.stderr
    assert log.read_text() != ""


def test_run_sends_the_period_only_as_a_variable(stub):
    env, log = stub
    done = run(env, DEPLOY / "panel-check.sh", "run", "2026-11-15-again")
    assert done.returncode == 0, done.stderr
    sent = psql_calls(log)
    assert len(sent) >= 3  # the taken check, the request, the poll, then the last audits
    for cmd, sql in sent:
        assert "2026-11-15-again" not in (sql or ""), sql
        assert " -U cli " in cmd
        assert "-f -" in cmd
        assert "ON_ERROR_STOP=1" in cmd
    with_period = [c for c, _ in sent if "period=2026-11-15-again" in c]
    assert len(with_period) == 3
    # The three reads are read-only; the request is the one session that is not.
    request = [(c, s) for c, s in sent if s and "request_panel_audit" in s]
    assert len(request) == 1
    assert READ_ONLY not in request[0][0].replace("\\", "")
    assert request[0][1] == "SELECT agb.request_panel_audit(:'period')"
    for cmd, sql in sent:
        if sql and "request_panel_audit" not in sql:
            assert "default_transaction_read_only=on" in cmd


def test_the_default_period_comes_from_the_server_and_is_checked(stub):
    env, log = stub
    done = run(env, DEPLOY / "panel-check.sh", "run")
    assert done.returncode == 0, done.stderr
    assert any("period=2026-11-17-drill-153012" in cmd for cmd, _ in psql_calls(log))


def test_status_reads_in_a_read_only_session_as_cli(stub):
    env, log = stub
    done = run(env, DEPLOY / "panel-check.sh", "status")
    assert done.returncode == 0, done.stderr
    [(cmd, sql)] = psql_calls(log)
    assert " -U cli " in cmd and "default_transaction_read_only=on" in cmd
    assert "panel_audit" in sql


def test_promote_reads_as_cli_and_sends_the_model_only_as_a_variable(stub, tmp_path):
    env, log = stub
    artifact = tmp_path / "research/certifications/lgbm-2026-10"
    artifact.mkdir(parents=True)
    (artifact / "manifest.json").write_text('{"files": {}}')
    env = {**env, "AGB_RESEARCH_DIR": str(tmp_path / "research")}
    run(env, DEPLOY / "promote.sh", "lgbm-2026-10", "check")
    sent = psql_calls(log)
    assert sent, "promote check never queried the database"
    for cmd, sql in sent:
        assert " -U cli " in cmd and "default_transaction_read_only=on" in cmd
        assert "lgbm-2026-10" not in (sql or "")
    assert any("model=lgbm-2026-10" in cmd for cmd, _ in sent)


@pytest.mark.parametrize("script", ["panel-check.sh", "promote.sh"])
def test_no_script_reads_as_postgres(script):
    text = (DEPLOY / script).read_text()
    code = "\n".join(ln for ln in text.splitlines() if not ln.lstrip().startswith("#"))
    assert not re.search(r"-U\s+postgres|--username=postgres", code)


def test_agb_stack_names_the_superuser_only_where_it_is_needed():
    text = (DEPLOY / "agb-stack").read_text()
    users = re.findall(r"^.*(?:-U\s+postgres|--username=postgres).*$", text, re.MULTILINE)
    # db_psql, for CREATE/DROP DATABASE and the restore load, and the dropdb hint in a message.
    assert len(users) == 2, users
    counts = re.search(r"^counts\(\) \{\n(.*?)^\}", text, re.MULTILINE | re.DOTALL).group(1)
    assert "read_psql" in counts and "db_psql" not in counts
    assert "--username=backup" in text
