"""Certification end to end on the fake panel: a passing run's artifact directory and its
`model_certification` row with every field (acceptance criterion (c) of T20), a failing run
recorded rather than discarded (d), the hashed ledger in the artifact and a tampered one
refused at import (e), the deflated Sharpe using the ledger's own trial count (b) and every
output saying the gates certify stage 1 only (f). The gates' arithmetic and thresholds are
tests/test_gates.py.

`research_panel` in conftest is the session's fake panel of thirty months; a run here
cross-validates it in six groups and trades a six-name basket, which is what the fake market's
drift needs for the deflated Sharpe to clear its bar, and finishes in seconds. Five backtests
of plain signals go into the ledger first, as the candidates the overfitting gate ranks the
model against."""

import json
import shutil
from datetime import UTC, datetime

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from psycopg import errors
from psycopg.rows import dict_row

from agb.research import __main__ as cli
from agb.research import artifact, backtest, certify, cpcv, train, trials
from agb.research.panel import Panel
from agb.research.trials import Ledger
from agb.roles import RESEARCH

MODEL = train.Config(num_boost_round=20, min_data_in_leaf=5)
VALIDATION = cpcv.Options(groups=6, test_groups=2, embargo_months=1)
OPTIONS = certify.Options(model_id="ranker-2024.1", size=6, hold_through=10)
PRIORS = ("mom_12_1", "mom_6_1", "rev_1m", "vol_12m", "log_mktcap")
STAGE_1 = "stage 1 (classical ranker) only"
# The ledger of a workstation that has tried many things: enough declared trials that no
# thirty-month edge survives the deflated Sharpe.
MANY = 500


@pytest.fixture(scope="module")
def panel(research_panel) -> Panel:
    return research_panel


@pytest.fixture(scope="module")
def prior(panel, research_lake, tmp_path_factory) -> tuple[Ledger, list[str]]:
    """A research directory whose ledger already holds a backtest of each plain signal over
    the panel: the configurations the overfitting gate ranks the model against."""
    research_dir = tmp_path_factory.mktemp("research")
    ledger = Ledger(research_dir / trials.FILE)
    runs = [
        backtest.run(
            panel,
            backtest.Options(signal=signal, size=OPTIONS.size, hold_through=OPTIONS.hold_through),
            ledger,
            lake_dir=research_lake,
        )
        for signal in PRIORS
    ]
    return ledger, [run.trial_id for run in runs]


@pytest.fixture(scope="module")
def certified(panel, research_lake, prior) -> certify.Certification:
    ledger, _ = prior
    return certify.run(
        panel, MODEL, VALIDATION, OPTIONS, ledger,
        research_dir=ledger.path.parent, lake_dir=research_lake,
    )  # fmt: skip


@pytest.fixture(scope="module")
def failed(panel, research_lake, tmp_path_factory) -> certify.Certification:
    """The same configuration certified from a ledger that declares hundreds of trials."""
    research_dir = tmp_path_factory.mktemp("research-many")
    ledger = Ledger(research_dir / trials.FILE)
    stuff(ledger, panel, MANY)
    return certify.run(
        panel, MODEL, VALIDATION, OPTIONS, ledger,
        research_dir=research_dir, lake_dir=research_lake,
    )  # fmt: skip


def stuff(ledger: Ledger, panel: Panel, count: int) -> None:
    now = datetime.now(UTC)
    rows = [
        {
            "trial_id": f"{i:016x}", "kind": "scan", "status": "succeeded",
            "started_at": now, "finished_at": now, "config": "{}", "features": "[]",
            "train_start": panel.months[0].as_of, "train_end": panel.months[-1].as_of,
            "partition_hash": panel.partition_hash,
            "cost_model_version": panel.cost_model_version, "code_version": "test",
            "refits": 0, "results": None, "error": None,
        }
        for i in range(count)
    ]  # fmt: skip
    ledger.path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows, schema=trials.SCHEMA), ledger.path)


def copied(certification: certify.Certification, tmp_path) -> "Artifact":
    return Artifact(shutil.copytree(certification.directory, tmp_path / certification.model_id))


class Artifact:
    """A copy of an artifact directory, to tamper with."""

    def __init__(self, directory):
        self.directory = directory

    @property
    def manifest(self) -> dict:
        return json.loads((self.directory / artifact.MANIFEST).read_text())

    def ledger(self) -> Ledger:
        return Ledger(self.directory / trials.FILE)

    def drop_last_trial(self) -> None:
        rows = self.ledger().rows()[:-1]
        pq.write_table(pa.Table.from_pylist(rows, schema=trials.SCHEMA), self.ledger().path)

    def rewrite(self, **changes) -> None:
        """Rewrite the manifest as a forger would who knows the digest scheme."""
        manifest = {**self.manifest, **changes}
        files = {
            p.name: artifact.file_sha256(p)
            for p in sorted(self.directory.iterdir())
            if p.name != artifact.MANIFEST
        }
        manifest["files"] = files
        manifest["ledger_sha256"] = files[trials.FILE]
        manifest["artifact_sha256"] = artifact.artifact_sha256(files)
        (self.directory / artifact.MANIFEST).write_text(json.dumps(manifest))


def compounded(returns) -> float:
    total = 1.0
    for value in returns:
        total *= 1.0 + value
    return total


def imported(db, directory) -> dict:
    conn = db.connect(RESEARCH)
    row_id = artifact.import_artifact(conn, directory)
    conn.row_factory = dict_row
    return conn.execute("SELECT * FROM agb.model_certification WHERE id = %s", (row_id,)).fetchone()


# The artifact ---------------------------------------------------------------------------------


def test_a_passing_run_leaves_the_artifact_directory(certified, prior):
    ledger, _ = prior
    assert certified.passed
    assert certified.directory == ledger.path.parent / "certifications" / OPTIONS.model_id
    assert sorted(p.name for p in certified.directory.iterdir()) == [
        "manifest.json", "model.txt", "paths.json", "report.md", "trials.parquet",
    ]  # fmt: skip
    manifest = certified.manifest
    assert json.loads((certified.directory / artifact.MANIFEST).read_text()) == manifest
    assert manifest["files"] == {
        name: artifact.file_sha256(certified.directory / name)
        for name in ("model.txt", "paths.json", "report.md", "trials.parquet")
    }
    assert manifest["artifact_sha256"] == artifact.artifact_sha256(manifest["files"])
    assert manifest["ledger_sha256"] == manifest["files"]["trials.parquet"]


def test_the_artifact_carries_every_field_the_row_needs(certified, panel):
    manifest = certified.manifest
    assert set(artifact.FIELDS) <= set(manifest)
    assert manifest["model_id"] == OPTIONS.model_id
    assert manifest["certifies"] == STAGE_1
    assert manifest["features"] == list(MODEL.features)
    assert (manifest["train_start"], manifest["train_end"]) == (
        panel.months[0].as_of.isoformat(), panel.months[-1].as_of.isoformat(),
    )  # fmt: skip
    assert manifest["cost_model_version"] == panel.cost_model_version
    assert manifest["certified_capital"] == 100_000.0
    assert set(manifest["gates"]) >= {"dsr", "pbo", "sharpe_net", "dsr_2x", "thresholds"}
    assert all(manifest["gates"][g]["passed"] for g in ("dsr", "pbo", "sharpe_net", "dsr_2x"))
    for name in ("max_drawdown", "longest_losing_run", "tracking_dispersion"):
        assert set(manifest["path_quantiles"][name]) == {
            "min", "max", "q05", "q25", "q50", "q75", "q95",
        }  # fmt: skip
    assert manifest["participation_max"] >= manifest["participation_mean"] > 0.0
    assert manifest["break_even_multiple"] > 1.0
    assert manifest["gross_sharpe_flag"] is (manifest["sharpe_gross"] > 2.0)
    assert datetime.fromisoformat(manifest["certified_at"]).tzinfo is not None


def test_the_ledger_shipped_in_the_artifact_declares_the_trial_count(certified, prior):
    ledger, prior_ids = prior
    shipped = Ledger(certified.directory / trials.FILE)
    # The five prior backtests, the certification, its cross-validation and its backtest.
    declared = len(PRIORS) + 3
    assert shipped.count() == ledger.count() == certified.manifest["declared_trials"] == declared
    assert {row["kind"] for row in shipped.rows()} == {"backtest", "cpcv", "certification"}
    assert shipped.digest() == ledger.digest()
    own = shipped.get(certified.trial_id)
    assert own["kind"] == "certification" and own["status"] == "succeeded"
    results = json.loads(own["results"])
    assert results["declared_trials"] == declared and results["passed"]
    assert results["certifies"] == STAGE_1
    assert results["candidate_trials"] == prior_ids
    assert results["cpcv_trial_id"] != results["backtest_trial_id"]
    assert {r["status"] for r in shipped.rows()} == {"succeeded"}


def test_the_paths_are_traded_at_one_and_two_times_cost(certified, panel):
    paths = json.loads((certified.directory / certify.PATHS).read_text())
    assert [p["path"] for p in paths] == ["0", "1", "2", "3", "4"]
    for path in paths:
        assert len(path["monthly"]) == len(panel.months)
        for month in path["monthly"]:
            assert month["gross"] >= month["net"]
            assert month["as_of"]
        # Doubled costs shrink each position a little as well as each charge, so month by
        # month the two runs are not ordered; over the path they are.
        assert compounded(month["net_2x"] for month in path["monthly"]) < compounded(
            month["net"] for month in path["monthly"]
        )


def test_the_overfitting_gate_ranks_the_model_against_the_ledgers_backtests(certified):
    # The five prior backtests and the model itself.
    assert certified.manifest["gates"]["candidates"] == len(PRIORS) + 1


def test_the_report_says_what_the_gates_certify(certified):
    report = (certified.directory / certify.REPORT).read_text()
    assert report.startswith(f"# Certification of {OPTIONS.model_id}: PASSED")
    assert f"These gates certify {STAGE_1}." in report
    assert "unbacktested" in report
    assert "net of 1x modelled cost" in report
    assert "- dsr: " in report and "- dsr_2x: " in report and "- pbo: " in report
    assert "break-even cost multiple" in report
    assert "trials declared from the ledger" in report


def test_a_certification_is_never_overwritten(certified, prior, panel, research_lake):
    ledger, _ = prior
    before = ledger.count()
    with pytest.raises(artifact.CertificationError, match="never overwritten"):
        certify.run(
            panel, MODEL, VALIDATION, OPTIONS, ledger,
            research_dir=ledger.path.parent, lake_dir=research_lake,
        )  # fmt: skip
    assert ledger.count() == before


@pytest.mark.parametrize("bad", ["", "../x", "a b", ".hidden", "-x"])
def test_a_model_id_is_a_directory_name(bad):
    with pytest.raises(ValueError, match="model id"):
        certify.Options(model_id=bad)


# The row --------------------------------------------------------------------------------------


def test_the_import_writes_the_row_with_every_field(db, certified):
    manifest = certified.manifest
    row = imported(db, certified.directory)
    assert row["actor"] == "research" and row["at"] is not None
    assert row["artifact_path"] == manifest[artifact.CERTIFIED_PATH]
    assert row["certified_capital"] == 100_000
    assert row["certified_at"] == datetime.fromisoformat(manifest["certified_at"])
    assert row["train_start"].isoformat() == manifest["train_start"]
    assert row["train_end"].isoformat() == manifest["train_end"]
    for name in (
        "model_id", "certifies", "passed", "artifact_sha256", "ledger_sha256", "features",
        "declared_trials", "dsr", "pbo", "sharpe_net", "dsr_2x", "break_even_multiple",
        "sharpe_gross", "gross_sharpe_flag", "participation_max", "participation_mean",
        "cost_model_version", "path_quantiles", "gates",
    ):  # fmt: skip
        assert row[name] == manifest[name], name
    assert row["certifies"] == STAGE_1 and row["passed"]


def test_a_model_is_certified_once(db, certified):
    imported(db, certified.directory)
    with pytest.raises(artifact.CertificationError, match="already certified"):
        artifact.import_artifact(db.connect(RESEARCH), certified.directory)
    assert db.connect(RESEARCH).execute(
        "SELECT count(*) FROM agb.model_certification"
    ).fetchone() == (1,)


def test_the_row_is_history(db, certified):
    imported(db, certified.directory)
    with pytest.raises(errors.InsufficientPrivilege):
        db.connect(RESEARCH).execute("UPDATE agb.model_certification SET passed = false")
    with pytest.raises(errors.InsufficientPrivilege):
        db.connect(RESEARCH).execute("DELETE FROM agb.model_certification")


def test_only_research_imports_and_the_services_read(db, certified):
    imported(db, certified.directory)
    for role in ("pipeline", "cli", "ui"):
        assert db.connect(role).execute(
            "SELECT model_id, certifies FROM agb.model_certification"
        ).fetchall() == [(OPTIONS.model_id, STAGE_1)]
    # The `cli` role among them: the stack's cli service runs as `cli`, which is why the import
    # is a service of its own running as `research`.
    for role in ("pipeline", "cli"):
        with pytest.raises(errors.InsufficientPrivilege):
            artifact.import_artifact(db.connect(role), certified.directory, actor=role)


def test_the_row_records_where_the_certification_ran_not_where_it_was_read(db, certified, tmp_path):
    """On the server the artifact is a copy under the models directory and the import reads it
    there, but the column is provenance: the workstation path the certification wrote. The
    manifest carries it, so the copy records the same path the original would."""
    copy = copied(certified, tmp_path)
    assert copy.directory != certified.directory
    row = imported(db, copy.directory)
    assert row["artifact_path"] == certified.manifest[artifact.CERTIFIED_PATH]
    assert row["artifact_path"] == str(certified.directory.resolve())


def test_an_artifact_that_does_not_say_where_it_was_certified_is_refused(db, certified, tmp_path):
    """An artifact from before the manifest carried the path. Refusing it is the point: the
    alternative is a row whose provenance is the server's own copy."""
    copy = copied(certified, tmp_path)
    manifest = copy.manifest
    del manifest[artifact.CERTIFIED_PATH]
    (copy.directory / artifact.MANIFEST).write_text(json.dumps(manifest))
    with pytest.raises(artifact.CertificationError, match=f"lacks {artifact.CERTIFIED_PATH}"):
        artifact.import_artifact(db.connect(RESEARCH), copy.directory)


def test_a_row_must_say_what_it_certifies(db, certified, tmp_path):
    copy = copied(certified, tmp_path)
    copy.rewrite(certifies="stage 1 and the blend")
    with pytest.raises(errors.CheckViolation):
        artifact.import_artifact(db.connect(RESEARCH), copy.directory)


# A failed certification ------------------------------------------------------------------------


def test_a_failing_run_is_recorded_not_discarded(failed, certified, db):
    assert not failed.passed
    manifest = failed.manifest
    assert not manifest["gates"]["dsr"]["passed"]
    assert manifest["declared_trials"] == MANY + 3
    ledger = Ledger(failed.directory / trials.FILE)
    own = ledger.get(failed.trial_id)
    assert own["status"] == "succeeded" and not json.loads(own["results"])["passed"]
    report = (failed.directory / certify.REPORT).read_text()
    assert report.startswith(f"# Certification of {OPTIONS.model_id}: FAILED")
    assert f"These gates certify {STAGE_1}." in report
    row = imported(db, failed.directory)
    assert not row["passed"] and row["declared_trials"] == MANY + 3
    assert row["dsr"] == manifest["dsr"]


def test_declaring_more_trials_lowers_the_deflated_sharpe(failed, certified):
    # The same configuration over the same panel; only the ledger differs.
    assert failed.manifest["sharpe_net"] == pytest.approx(certified.manifest["sharpe_net"])
    assert failed.manifest["dsr"] < certified.manifest["dsr"]
    assert failed.manifest["dsr_2x"] < certified.manifest["dsr_2x"]


def test_a_run_that_raises_leaves_a_failed_trial_and_no_artifact(
    panel, research_lake, tmp_path, monkeypatch
):
    research_dir = tmp_path / "research"
    ledger = Ledger(research_dir / trials.FILE)

    def broken(*args, **kwargs):
        raise RuntimeError("the engine fell over")

    monkeypatch.setattr(backtest, "run", broken)
    with pytest.raises(RuntimeError, match="fell over"):
        certify.run(
            panel, MODEL, VALIDATION, OPTIONS, ledger,
            research_dir=research_dir, lake_dir=research_lake,
        )  # fmt: skip
    rows = {row["kind"]: row for row in ledger.rows()}
    assert rows["certification"]["status"] == "failed"
    assert "fell over" in rows["certification"]["error"]
    assert rows["cpcv"]["status"] == "succeeded"
    assert not (research_dir / "certifications").exists()


# A tampered artifact --------------------------------------------------------------------------


def test_a_tampered_ledger_is_detected(db, certified, tmp_path):
    copy = copied(certified, tmp_path)
    copy.drop_last_trial()
    with pytest.raises(artifact.CertificationError, match="trials.parquet has been altered"):
        artifact.import_artifact(db.connect(RESEARCH), copy.directory)


def test_a_ledger_that_disagrees_with_the_declared_count_is_refused(db, certified, tmp_path):
    copy = copied(certified, tmp_path)
    copy.drop_last_trial()
    copy.rewrite()
    with pytest.raises(
        artifact.CertificationError, match="holds 7 trials; the certification declared 8"
    ):
        artifact.import_artifact(db.connect(RESEARCH), copy.directory)


def test_a_declared_count_the_trial_row_disagrees_with_is_refused(db, certified, tmp_path):
    copy = copied(certified, tmp_path)
    copy.rewrite(declared_trials=7)
    with pytest.raises(
        artifact.CertificationError, match="holds 8 trials; the certification declared 7"
    ):
        artifact.import_artifact(db.connect(RESEARCH), copy.directory)
    copy = copied(certified, tmp_path / "again")
    copy.drop_last_trial()
    copy.rewrite(declared_trials=7)
    with pytest.raises(
        artifact.CertificationError, match="disagrees with the certification's own trial row"
    ):
        artifact.import_artifact(db.connect(RESEARCH), copy.directory)


def test_a_verdict_the_trial_row_disagrees_with_is_refused(db, certified, tmp_path):
    copy = copied(certified, tmp_path)
    copy.rewrite(passed=False)
    with pytest.raises(artifact.CertificationError, match="disagrees"):
        artifact.import_artifact(db.connect(RESEARCH), copy.directory)


def test_an_altered_report_or_an_extra_file_is_refused(db, certified, tmp_path):
    copy = copied(certified, tmp_path)
    (copy.directory / certify.REPORT).write_text("# all good\n")
    with pytest.raises(artifact.CertificationError, match="report.md has been altered"):
        artifact.import_artifact(db.connect(RESEARCH), copy.directory)
    copy = copied(certified, tmp_path / "again")
    (copy.directory / "notes.txt").write_text("trust me\n")
    with pytest.raises(artifact.CertificationError, match="unlisted files: notes.txt"):
        artifact.import_artifact(db.connect(RESEARCH), copy.directory)
    (copy.directory / "notes.txt").unlink()
    (copy.directory / certify.PATHS).unlink()
    with pytest.raises(artifact.CertificationError, match="paths.json is missing"):
        artifact.import_artifact(db.connect(RESEARCH), copy.directory)


def test_a_manifest_whose_digest_was_edited_is_refused(db, certified, tmp_path):
    copy = copied(certified, tmp_path)
    manifest = copy.manifest
    manifest["artifact_sha256"] = "0" * 64
    (copy.directory / artifact.MANIFEST).write_text(json.dumps(manifest))
    with pytest.raises(artifact.CertificationError, match="digest does not match"):
        artifact.import_artifact(db.connect(RESEARCH), copy.directory)
    del manifest["dsr"]
    (copy.directory / artifact.MANIFEST).write_text(json.dumps(manifest))
    with pytest.raises(artifact.CertificationError, match="lacks dsr"):
        artifact.import_artifact(db.connect(RESEARCH), copy.directory)
    with pytest.raises(artifact.CertificationError, match="holds no manifest.json"):
        artifact.import_artifact(db.connect(RESEARCH), tmp_path)
    assert db.connect(RESEARCH).execute(
        "SELECT count(*) FROM agb.model_certification"
    ).fetchone() == (0,)


# The command line -----------------------------------------------------------------------------


def test_the_cli_certifies_and_imports(
    db, panel, research_lake, certified, tmp_path, monkeypatch, capsys
):
    research_dir = tmp_path / "research"
    research_dir.mkdir()
    panel.save(research_dir / "panel.parquet")
    monkeypatch.setenv("AGB_RESEARCH_DIR", str(research_dir))
    monkeypatch.setenv("AGB_LAKE_DIR", str(research_lake))
    cli.main([
        "certify", "--model-id", "cli-ranker", "--rounds", "20", "--min-leaf", "5",
        "--groups", "6", "--size", "6", "--hold-through", "10",
    ])  # fmt: skip
    out = capsys.readouterr().out
    assert f"certification PASSED: {STAGE_1}; artifact " in out
    directory = research_dir / "certifications" / "cli-ranker"
    assert (directory / artifact.MANIFEST).exists()
    assert json.loads(out.split("\n", 2)[2])["model_id"] == "cli-ranker"
    for name, value in db.pg_env(RESEARCH).items():
        monkeypatch.setenv(name, value)
    cli.main(["import", str(directory)])
    assert capsys.readouterr().out == f"model_certification 1: {directory}\n"
    with pytest.raises(SystemExit, match="already certified"):
        cli.main(["import", str(directory)])
    with pytest.raises(SystemExit, match="never overwritten"):
        cli.main(["certify", "--model-id", "cli-ranker", "--rounds", "20", "--groups", "6"])
