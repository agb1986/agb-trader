"""The lake sync and the research CLI: the workstation pulls the lake read-only, and never
pushes, deletes or reaches the vendor. `AGB_LIVE_RSYNC=1` runs one attended dry run against the
source named in the operator's own env/research.env."""

import ast
import os
import sys
from pathlib import Path

import pytest
from conftest import ROOT

from agb.research import __main__ as cli
from agb.research import sync

EXAMPLE = ROOT / "env" / "research.env.example"


def test_the_example_names_a_remote_lake_source():
    source = sync.source_from(EXAMPLE)
    assert sync.is_remote(source)
    assert source.endswith("/lake")
    assert "SHARADAR" not in EXAMPLE.read_text(), "the workstation never pulls from the vendor"


def test_the_command_pulls_from_the_server_into_a_local_directory(tmp_path):
    argv = sync.command("casaos.local:/mnt/data/AppData/agb-trader/lake", tmp_path / "lake")

    assert argv[0] == "rsync"
    assert argv[-2:] == ["casaos.local:/mnt/data/AppData/agb-trader/lake/", f"{tmp_path}/lake/"]
    assert "--archive" in argv
    assert "--exclude=/.query/" in argv
    assert not [flag for flag in argv if "delete" in flag], "nothing is deleted on either side"
    assert "--dry-run" not in argv
    assert not [flag for flag in argv if flag.startswith("--rsync-path")]


def test_a_dry_run_only_lists(tmp_path):
    argv = sync.command("host:/lake", tmp_path, dry_run=True)
    assert "--dry-run" in argv
    assert argv[-2:] == ["host:/lake/", f"{tmp_path}/"]


@pytest.mark.parametrize("source", ["/mnt/data/lake", "./lake", "lake", "/lake:with:colons"])
def test_a_local_source_is_refused(source, tmp_path):
    with pytest.raises(sync.SyncError, match="must be remote"):
        sync.command(source, tmp_path)


def test_a_remote_destination_is_refused():
    with pytest.raises(sync.SyncError, match="must be a local directory"):
        sync.command("host:/lake", Path("casaos.local:/mnt/data/AppData/agb-trader/lake"))


@pytest.mark.parametrize(
    ("spec", "remote"),
    [
        ("casaos.local:/mnt/data/lake", True),
        ("user@host:lake", True),
        ("host::module", True),
        ("rsync://host/module", True),
        ("/lake", False),
        ("./lake:a", False),
        ("lake/", False),
    ],
)
def test_rsync_reads_a_colon_before_any_slash_as_a_host(spec, remote):
    assert sync.is_remote(spec) is remote


def test_the_source_comes_from_the_env_file(tmp_path):
    env_file = tmp_path / "research.env"
    env_file.write_text("# comment\n\nAGB_LAKE_SOURCE = host:/lake \nOTHER=1\n")
    assert sync.read_env(env_file) == {"AGB_LAKE_SOURCE": "host:/lake", "OTHER": "1"}
    assert sync.source_from(env_file) == "host:/lake"

    env_file.write_text("AGB_LAKE_SOURCE=\n")
    with pytest.raises(sync.SyncError, match="AGB_LAKE_SOURCE is not set"):
        sync.source_from(env_file)
    with pytest.raises(sync.SyncError, match="does not exist"):
        sync.source_from(tmp_path / "missing.env")


def test_run_creates_the_lake_directory_and_hands_rsync_the_argv(tmp_path, capsys):
    env_file = tmp_path / "research.env"
    env_file.write_text("AGB_LAKE_SOURCE=host:/lake\n")
    lake = tmp_path / "lake"
    calls = []

    argv = sync.run(env_file, lake, dry_run=True, runner=lambda a, **kw: calls.append((a, kw)))

    assert lake.is_dir()
    assert calls == [(argv, {"check": True})]
    assert argv[-2:] == ["host:/lake/", f"{lake}/"]
    assert capsys.readouterr().out.startswith("+ rsync ")


def test_the_sync_needs_only_the_standard_library():
    """`make sync-lake` runs on the host, outside the image, so the module cannot pull in the
    research dependencies or the rest of the package."""
    tree = ast.parse((ROOT / "src/agb/research/sync.py").read_text())
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported |= {alias.name.split(".")[0] for alias in node.names}
        elif isinstance(node, ast.ImportFrom):
            imported.add((node.module or "").split(".")[0])
    assert imported, "the module imports something"
    assert imported <= sys.stdlib_module_names


def test_the_research_container_has_no_network_and_no_sync():
    """The container cannot rsync: the sync is a host command, run by make before it."""
    makefile = (ROOT / "Makefile").read_text()
    [line] = [line for line in makefile.splitlines() if "agb.research sync" in line]
    assert line.startswith("\tuv run python -m agb.research sync"), "the host runs the sync"
    assert "research: sync-lake" in makefile, "a research run syncs the lake first"


def test_the_cli_sync_stops_on_a_missing_env_file(tmp_path, capsys):
    with pytest.raises(SystemExit) as stop:
        cli.main(["sync", "--env-file", str(tmp_path / "research.env"), "--lake", str(tmp_path)])
    assert str(stop.value).startswith("sync: ")
    assert "does not exist" in str(stop.value)


def test_the_cli_lists_an_empty_ledger(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("AGB_RESEARCH_DIR", str(tmp_path))
    cli.main(["trials"])
    assert capsys.readouterr().out == "no trials yet\n"


@pytest.mark.live
@pytest.mark.skipif(os.environ.get("AGB_LIVE_RSYNC") != "1", reason="set AGB_LIVE_RSYNC=1")
def test_live_dry_run_reads_the_server_lake(tmp_path):
    """Attended: lists what `make sync-lake` would pull, over the operator's SSH, and copies
    nothing. Reads the operator's own env/research.env (or $AGB_ENV_DIR/research.env)."""
    env_file = Path(os.environ.get("AGB_ENV_DIR", ROOT / "env")) / "research.env"
    argv = sync.run(env_file, tmp_path / "lake", dry_run=True)
    assert "--dry-run" in argv
    assert not list((tmp_path / "lake").iterdir()), "a dry run transfers nothing"
