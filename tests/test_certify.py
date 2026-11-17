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
from datetime import UTC, date, datetime, timedelta

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from psycopg import errors
from psycopg.rows import dict_row

from agb.research import __main__ as cli
from agb.research import artifact, backtest, certify, cpcv, gates, reproduce, train, trials
from agb.research.panel import Panel
from agb.research.trials import Ledger
from agb.roles import RESEARCH

# `certify` refuses to run without a build version.
pytestmark = pytest.mark.usefixtures("certifiable")

MODEL = train.Config(num_boost_round=20, min_data_in_leaf=5)
VALIDATION = cpcv.Options(groups=6, test_groups=2, embargo_months=1)
# The fake market's passing configuration: a six-name book held to the eighth session, ranked
# against three plain-signal priors. Since issue #187 the verdict also fails a Sharpe above 2.0
# on any basis, and the ten-session hold this used before cleared the deflated Sharpe only with a
# gross Sharpe above 2.0.
OPTIONS = certify.Options(model_id="ranker-2024.1", size=6, hold_through=8)
PRIORS = ("mom_12_1", "mom_6_1", "rev_1m")
STAGE_1 = "stage 1 (classical ranker) only"
# The ledger of a workstation that has tried many things: enough declared trials that no
# thirty-month edge survives the deflated Sharpe. Each row is a different configuration, so
# every one of them is a trial the count charges for.
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


def without_spy(panel, lake_dir, tmp_path_factory, name) -> certify.Certification:
    """The same configuration certified from a fresh ledger against `lake_dir`, a lake whose
    benchmark bars are missing or cut short."""
    research_dir = tmp_path_factory.mktemp(name)
    ledger = Ledger(research_dir / trials.FILE)
    return certify.run(
        panel, MODEL, VALIDATION, OPTIONS, ledger,
        research_dir=research_dir, lake_dir=lake_dir,
    )  # fmt: skip


@pytest.fixture(scope="module")
def unbenchmarked(panel, tmp_path_factory) -> certify.Certification:
    """Certified against a lake of the same names and no index fund, which is the real lake's
    shape today. The names come out of the seeded market exactly as in the research lake,
    because the benchmark is drawn after them."""
    from research_fake import Market

    lake_dir = Market(tmp_path_factory.mktemp("lake-without-spy")).names().land()
    return without_spy(panel, lake_dir, tmp_path_factory, "research-without-spy")


@pytest.fixture(scope="module")
def partly_benchmarked(panel, tmp_path_factory) -> certify.Certification:
    """Certified against a lake whose benchmark stops printing halfway through the panel."""
    from research_fake import BENCHMARK, Market

    market = Market(tmp_path_factory.mktemp("lake-partial-spy")).names()
    lake_dir = market.name(BENCHMARK, noise=0.002, category="ETF", until=date(2025, 6, 30)).land()
    return without_spy(panel, lake_dir, tmp_path_factory, "research-partial-spy")


def stuff(ledger: Ledger, panel: Panel, count: int) -> None:
    now = datetime.now(UTC)
    rows = [
        {
            "trial_id": f"{i:016x}", "kind": "scan", "status": "succeeded",
            "started_at": now, "finished_at": now,
            "config": trials.encode({"scan": i}), "features": "[]",
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

    def drop_first_trial(self, *, relink: bool = True) -> None:
        """Drop the first row, which is a prior backtest: a configuration the search counted,
        unlike the rows a certification nests. A forger who knows the link scheme relinks the
        rows after it, so the chain holds and only the checks behind it can refuse."""
        self.drop(0, relink=relink)

    def drop(self, index: int, *, relink: bool = False) -> None:
        rows = self.ledger().rows()
        del rows[index]
        if relink:
            for i, row in enumerate(rows):
                row["previous"] = trials.link(rows[i - 1]) if i else trials.GENESIS
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
        manifest["artifact_sha256"] = artifact.artifact_sha256(manifest)
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
    # The digest covers the manifest itself, every field but the digest, and through its
    # file digests every other file (issue #211).
    assert manifest["artifact_sha256"] == artifact.artifact_sha256(manifest)
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
    assert set(manifest["gates"]) >= {*certify.REPORTED_GATES, "thresholds"}
    assert all(manifest["gates"][g]["passed"] for g in certify.REPORTED_GATES)
    for name in ("max_drawdown", "longest_losing_run", "tracking_dispersion", "monthly_return"):
        assert set(manifest["path_quantiles"][name]) == {
            "min", "max", "q05", "q25", "q50", "q75", "q95",
        }  # fmt: skip
    assert manifest["participation_max"] >= manifest["participation_mean"] > 0.0
    assert manifest["break_even_multiple"] > 1.0
    assert manifest["gross_sharpe_flag"] is False and manifest["gates"]["start_date_stable"] is True
    assert datetime.fromisoformat(manifest["certified_at"]).tzinfo is not None


def test_the_ledger_shipped_in_the_artifact_declares_the_trial_count(certified, prior):
    ledger, prior_ids = prior
    shipped = Ledger(certified.directory / trials.FILE)
    # Eight rows: the five prior backtests, the certification, its cross-validation and its
    # backtest. Six configurations: the cross-validation and the backtest ran inside the
    # certification, so they are the mechanics of one evaluation rather than two more searched.
    rows = len(PRIORS) + 3
    declared = len(PRIORS) + 1
    assert shipped.count() == ledger.count() == rows
    assert shipped.declared() == ledger.declared() == declared
    assert certified.manifest["declared_trials"] == declared
    assert certified.manifest["ledger_rows"] == rows
    assert certified.manifest["declared_trials_rule"] == trials.RULE
    assert {row["kind"] for row in shipped.rows()} == {"backtest", "cpcv", "certification"}
    assert shipped.digest() == ledger.digest()
    own = shipped.get(certified.trial_id)
    assert own["kind"] == "certification" and own["status"] == "succeeded"
    results = json.loads(own["results"])
    assert results["declared_trials"] == declared and results["passed"]
    assert results["declared_trials_rule"] == trials.RULE
    assert results["ledger_rows"] == rows
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


# One column per configuration ------------------------------------------------------------------


def traded_labels(certification: certify.Certification) -> list[str]:
    paths = json.loads((certification.directory / certify.PATHS).read_text())
    return [month["as_of"] for month in paths[0]["monthly"]]


def again(row: dict, seconds: float, **changes) -> dict:
    """`row` run again `seconds` later, with `changes`: a new row with its own id. The row keeps
    the `parent` it had unless `changes` names another; `ledger_of` links it into the chain."""
    shift = timedelta(seconds=seconds)
    moved = {
        **row,
        "started_at": row["started_at"] + shift,
        "finished_at": row["finished_at"] + shift,
        **changes,
    }
    moved["trial_id"] = trials.link({**moved, "trial_id": None})[:16]
    return moved


def ledger_of(
    certification: certify.Certification, tmp_path, *extra: dict, legacy: bool = False
) -> Ledger:
    """The certification's shipped ledger with `extra` appended, each linked to the row before
    it so the chain holds. `legacy` writes every row as one from before the chain instead, with
    no `parent` and no `previous`."""
    ledger = Ledger(tmp_path / trials.FILE)
    rows = [*Ledger(certification.directory / trials.FILE).rows(), *extra]
    if legacy:
        rows = [{**row, "parent": None, "previous": None} for row in rows]
    else:
        for index in range(len(rows) - len(extra), len(rows)):
            rows[index] = {**rows[index], "previous": trials.link(rows[index - 1])}
    tmp_path.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows, schema=trials.SCHEMA), ledger.path)
    ledger.verify()
    return ledger


def test_the_certification_counts_each_prior_configuration_once(certified, prior):
    _, prior_ids = prior
    dropped = json.loads(
        Ledger(certified.directory / trials.FILE).get(certified.trial_id)["results"]
    )["candidates_dropped"]
    # Its own backtest is the model's column already, so it is not offered; nothing was dropped.
    assert dropped == certified.manifest["gates"]["candidates_dropped"] == []
    assert certified.manifest["gates"]["pbo"]["cause"] is None
    assert len(prior_ids) == len(set(prior_ids)) == len(PRIORS)


def test_a_rerun_of_a_configuration_adds_no_column(certified, prior, panel, tmp_path):
    _, prior_ids = prior
    shipped = Ledger(certified.directory / trials.FILE)
    first = shipped.get(prior_ids[0])
    rerun = again(first, 3600)
    ledger = ledger_of(certified, tmp_path, rerun)
    found = certify.candidates(
        ledger, panel, labels=traded_labels(certified), current=certified.trial_id
    )
    assert list(found.series) == prior_ids
    own = certified.manifest["backtest_trial_id"]
    assert {"trial_id": own, "reason": "this certification's own configuration"} in found.dropped
    reason = f"rerun of the configuration of {first['trial_id']}"
    assert {"trial_id": rerun["trial_id"], "reason": reason} in found.dropped
    assert found.missing == []
    # The ledger agrees that the rerun is not another configuration searched.
    assert ledger.declared() == shipped.declared()


def test_backtests_inside_different_certifications_are_different_configurations(
    certified, prior, panel, tmp_path
):
    _, prior_ids = prior
    shipped = Ledger(certified.directory / trials.FILE)
    own, inner = (
        shipped.get(certified.trial_id),
        shipped.get(certified.manifest["backtest_trial_id"]),
    )
    # Another model certified later: its backtest's own row reads exactly like this one's.
    other = again(own, 3600, config=own["config"].replace(OPTIONS.model_id, "ranker-2024.2"))
    nested = again(inner, 3600, parent=other["trial_id"])
    assert trials.configuration(nested) == trials.configuration(inner)
    ledger = ledger_of(certified, tmp_path, other, nested)
    asked = certify.candidates(
        ledger, panel, labels=traded_labels(certified), current=certified.trial_id
    )
    assert list(asked.series) == [*prior_ids, nested["trial_id"]]
    # Certifying the other model: this model's backtest is a candidate, its own is not.
    theirs = certify.candidates(
        ledger, panel, labels=traded_labels(certified), current=other["trial_id"]
    )
    assert list(theirs.series) == [*prior_ids, inner["trial_id"]]


def test_overlapping_certifications_each_keep_their_own_column(certified, prior, panel, tmp_path):
    """Two models certified at once from two terminals: the second starts just after the first
    and its backtest runs while the first is still open, so by the clock that backtest sits
    inside the first certification. Each backtest names the certification that opened it as
    its parent, and that is whose configuration it is."""
    _, prior_ids = prior
    shipped = Ledger(certified.directory / trials.FILE)
    own, inner = (
        shipped.get(certified.trial_id),
        shipped.get(certified.manifest["backtest_trial_id"]),
    )
    tick = timedelta(microseconds=1)
    other = {
        **again(own, 0, config=own["config"].replace(OPTIONS.model_id, "ranker-2024.2")),
        "started_at": own["started_at"] + tick,
        "finished_at": own["finished_at"] + timedelta(hours=1),
    }
    theirs = again(inner, tick.total_seconds(), parent=other["trial_id"])
    assert own["started_at"] < theirs["started_at"] and theirs["finished_at"] <= own["finished_at"]
    ledger = ledger_of(certified, tmp_path, other, theirs)
    assert theirs["trial_id"] in trials.nested(ledger.rows())
    labels = traded_labels(certified)
    mine = certify.candidates(ledger, panel, labels=labels, current=certified.trial_id)
    assert list(mine.series) == [*prior_ids, theirs["trial_id"]]
    assert mine.missing == []
    other_side = certify.candidates(ledger, panel, labels=labels, current=other["trial_id"])
    assert list(other_side.series) == [*prior_ids, inner["trial_id"]]
    # Asked from outside either, both backtests are columns.
    both = certify.candidates(ledger, panel, labels=labels)
    assert list(both.series) == [*prior_ids, inner["trial_id"], theirs["trial_id"]]


def test_a_backtest_from_before_the_chain_is_still_read_by_its_interval(
    certified, prior, panel, tmp_path
):
    """Rows written before the `parent` column name no parent, so the run around a legacy
    backtest is the one whose interval holds it, as it always was."""
    _, prior_ids = prior
    shipped = Ledger(certified.directory / trials.FILE)
    own, inner = (
        shipped.get(certified.trial_id),
        shipped.get(certified.manifest["backtest_trial_id"]),
    )
    other = again(own, 3600, config=own["config"].replace(OPTIONS.model_id, "ranker-2024.2"))
    nested = again(inner, 3600)
    ledger = ledger_of(certified, tmp_path, other, nested, legacy=True)
    assert all(row["previous"] is None and row["parent"] is None for row in ledger.rows())
    labels = traded_labels(certified)
    mine = certify.candidates(ledger, panel, labels=labels, current=certified.trial_id)
    assert list(mine.series) == [*prior_ids, nested["trial_id"]]
    own_backtest = {
        "trial_id": inner["trial_id"],
        "reason": "this certification's own configuration",
    }
    assert own_backtest in mine.dropped
    other_side = certify.candidates(ledger, panel, labels=labels, current=other["trial_id"])
    assert list(other_side.series) == [*prior_ids, inner["trial_id"]]


def test_a_candidate_that_cannot_be_read_is_reported_and_fails_the_gate(
    certified, prior, panel, tmp_path
):
    _, prior_ids = prior
    first = Ledger(certified.directory / trials.FILE).get(prior_ids[0])
    results = json.loads(first["results"])
    results["monthly"][2]["net"] = None
    broken = again(
        first,
        3600,
        config=first["config"].replace(PRIORS[0], "mom_9_1"),
        results=trials.encode(results),
    )
    ledger = ledger_of(certified, tmp_path, broken)
    found = certify.candidates(
        ledger, panel, labels=traded_labels(certified), current=certified.trial_id
    )
    assert list(found.series) == prior_ids
    assert {"trial_id": broken["trial_id"], "reason": "a month that is not finite"} in found.dropped
    assert found.missing == [f"{broken['trial_id']}: a month that is not finite"]
    # A short series is reported the same way.
    results["monthly"] = results["monthly"][1:]
    short = again(broken, 60, results=trials.encode(results))
    found = certify.candidates(
        ledger_of(certified, tmp_path / "short", short),
        panel,
        labels=traded_labels(certified),
        current=certified.trial_id,
    )
    months = len(traded_labels(certified))
    assert found.missing == [f"{short['trial_id']}: {months - 1} months, {months} traded"]


def test_the_report_says_what_the_gates_certify(certified):
    report = (certified.directory / certify.REPORT).read_text()
    assert report.startswith(f"# Certification of {OPTIONS.model_id}: PASSED")
    assert f"These gates certify {STAGE_1}." in report
    assert "unbacktested" in report
    assert "net of 1x modelled cost" in report
    assert "- dsr: " in report and "- dsr_2x: " in report and "- pbo: " in report
    assert "break-even cost multiple" in report
    assert f"{len(PRIORS) + 1} trials declared from the {len(PRIORS) + 3} rows of the" in report
    assert f"counted as {trials.RULE}" in report


def test_the_report_lists_the_binding_suspicion_and_start_date_gates(certified):
    """Issue #187: the gross-Sharpe suspicion check and the start-date check are gates, so the
    report lists them with the rest; a manifest written before them still renders, and a gate
    with no value to read says so rather than failing to format."""
    report = (certified.directory / certify.REPORT).read_text()
    assert "- gross_sharpe: " in report and "at most 2.0000: pass" in report
    assert "- start_date: " in report
    older = {**certified.manifest}
    older["gates"] = {
        key: gate
        for key, gate in certified.manifest["gates"].items()
        if key not in ("gross_sharpe", "start_date")
    }
    assert "- gross_sharpe: " not in certify.report(older)
    empty = {**certified.manifest}
    empty["gates"] = {
        **certified.manifest["gates"],
        "start_date": {**certified.manifest["gates"]["start_date"], "value": None},
    }
    assert "- start_date: none, above " in certify.report(empty)


def test_the_report_names_the_excess_return_definition(certified):
    """Issue #167: the report says the Sharpe gates read returns in excess of SPY, defines the
    excess return, and gives the total-return Sharpe beside them."""
    manifest = certified.manifest
    report = (certified.directory / certify.REPORT).read_text()
    assert manifest["sharpe_basis"] == gates.EXCESS == "excess_spy"
    assert manifest["excess"] == {
        "benchmark": "SPY", "definition": gates.EXCESS_DEFINITION, "defined": True, "cause": None,
    }  # fmt: skip
    assert "read returns in excess of SPY" in report
    assert gates.EXCESS_DEFINITION in report
    assert "ranks the candidates on total returns" in report
    assert f"total-return Sharpe {manifest['sharpe_total']:.4f}" in report
    assert manifest["sharpe_net"] < manifest["sharpe_total"], "the benchmark rose"
    for key in ("dsr", "sharpe_net", "dsr_2x"):
        assert manifest["gates"][key]["basis"] == gates.EXCESS
    assert manifest["gates"]["pbo"]["basis"] == gates.TOTAL
    assert "sharpe_basis" not in artifact.FIELDS, "the basis is the manifest's, not a column"


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


def test_a_row_must_say_what_it_certifies(db, certified, tmp_path, monkeypatch):
    copy = copied(certified, tmp_path)
    copy.rewrite(certifies="stage 1 and the blend")
    with pytest.raises(artifact.CertificationError, match="this code certifies only"):
        artifact.import_artifact(db.connect(RESEARCH), copy.directory)
    # The table's own check holds behind the artifact's, for a row that reaches it unchecked.
    monkeypatch.setattr(artifact, "load", lambda directory: copy.manifest)
    with pytest.raises(errors.CheckViolation):
        artifact.import_artifact(db.connect(RESEARCH), copy.directory)


# A failed certification ------------------------------------------------------------------------


def test_a_failing_run_is_recorded_not_discarded(failed, certified, db):
    assert not failed.passed
    manifest = failed.manifest
    assert not manifest["gates"]["dsr"]["passed"]
    # Every stuffed row is its own configuration, and the certification is one more; the two
    # rows it nests are not.
    assert manifest["declared_trials"] == MANY + 1
    assert manifest["ledger_rows"] == MANY + 3
    ledger = Ledger(failed.directory / trials.FILE)
    assert ledger.count() == MANY + 3
    own = ledger.get(failed.trial_id)
    assert own["status"] == "succeeded" and not json.loads(own["results"])["passed"]
    report = (failed.directory / certify.REPORT).read_text()
    assert report.startswith(f"# Certification of {OPTIONS.model_id}: FAILED")
    assert f"These gates certify {STAGE_1}." in report
    row = imported(db, failed.directory)
    assert not row["passed"] and row["declared_trials"] == MANY + 1
    assert row["dsr"] == manifest["dsr"]
    # The scans carry no monthly returns, so the model is the only candidate and the
    # overfitting gate has nothing to measure: it fails and says so (issue #166), and the
    # row still imports, with the undefined PBO recorded as 1.0.
    assert row["pbo"] == manifest["pbo"] == 1.0


def test_a_one_candidate_ledger_fails_certification_with_the_cause_named(failed):
    gate = failed.manifest["gates"]["pbo"]
    cause = "PBO undefined: 1 distinct candidate, at least two needed"
    assert not gate["passed"] and gate["cause"] == cause
    assert gate["threshold"] == certify.gates.Thresholds().pbo
    assert failed.manifest["gates"]["candidates"] == 1
    report = (failed.directory / certify.REPORT).read_text()
    assert f"- pbo: 1.0000, at most 0.5000: FAIL ({cause})" in report


def test_declaring_more_trials_lowers_the_deflated_sharpe(failed, certified):
    # The same configuration over the same panel; only the ledger differs.
    assert failed.manifest["sharpe_net"] == pytest.approx(certified.manifest["sharpe_net"])
    assert failed.manifest["dsr"] < certified.manifest["dsr"]
    assert failed.manifest["dsr_2x"] < certified.manifest["dsr_2x"]


# The benchmark missing (issue #167) -------------------------------------------------------------

SHARPE_GATES = ("dsr", "sharpe_net", "dsr_2x")


def missing_benchmark(certification: certify.Certification, cause: str) -> None:
    """The three Sharpe gates failed with the cause named in the gate, the manifest and the
    report, finite fail values recorded, and the artifact written and importable."""
    manifest = certification.manifest
    assert not certification.passed and not manifest["passed"]
    assert manifest["sharpe_basis"] == gates.EXCESS
    assert manifest["excess"]["defined"] is False and manifest["excess"]["cause"] == cause
    for key in SHARPE_GATES:
        gate = manifest["gates"][key]
        assert not gate["passed"] and gate["defined"] is False and gate["cause"] == cause
    assert (manifest["dsr"], manifest["sharpe_net"], manifest["dsr_2x"]) == (
        gates.UNDEFINED_DSR, gates.UNDEFINED_SHARPE, gates.UNDEFINED_DSR,
    )  # fmt: skip
    assert manifest["sharpe_total"] > 0, "the total-return Sharpe is still measured"
    report = (certification.directory / certify.REPORT).read_text()
    assert report.startswith(f"# Certification of {OPTIONS.model_id}: FAILED")
    assert f"Those three gates fail ({cause})." in report
    assert "- sharpe_net: undefined, above 0.0000: FAIL" in report
    assert artifact.load(certification.directory) == manifest, "written, and it verifies"


def test_a_lake_without_the_benchmark_fails_the_sharpe_gates_and_writes_the_artifact(
    unbenchmarked, db
):
    """The coordinator's rule: a missing benchmark is a failed gate rather than an error, so
    the artifact exists for the operator's paper-only waiver to waive."""
    paths = json.loads((unbenchmarked.directory / certify.PATHS).read_text())
    total = sum(len(path["monthly"]) for path in paths)
    assert total and all(month["benchmark"] is None for path in paths for month in path["monthly"])
    cause = f"excess Sharpe undefined: the lake holds no SPY return for {total} of {total}"
    missing_benchmark(unbenchmarked, cause + " traded path-months")
    row = imported(db, unbenchmarked.directory)
    assert not row["passed"] and row["sharpe_net"] == gates.UNDEFINED_SHARPE


def test_a_benchmark_that_covers_part_of_the_run_fails_closed(partly_benchmarked, unbenchmarked):
    """Partial coverage is not partly measured: no month is dropped or zero-filled, and the
    certification fails exactly as with no benchmark, naming how many months it lacked."""
    cause = partly_benchmarked.manifest["excess"]["cause"]
    absent, total = (int(word) for word in cause.split("return for ")[1].split()[0:3:2])
    assert 0 < absent < total, "some months carry the benchmark and some do not"
    missing_benchmark(partly_benchmarked, cause)
    assert partly_benchmarked.manifest["sharpe_total"] == pytest.approx(
        unbenchmarked.manifest["sharpe_total"]
    ), "the same book either way"


def test_an_artifact_without_a_basis_is_refused_and_its_report_reads_total(certified, tmp_path):
    """Certifications up to lgbm-2026-10 recorded no basis. The import recomputes the basis, so
    one without it is refused and has to be certified again (issue #211); a report rendered
    from such a manifest still says it gated total returns."""
    copy = copied(certified, tmp_path)
    manifest = {
        key: value
        for key, value in copy.manifest.items()
        if key not in ("sharpe_basis", "excess", "sharpe_total")
    }
    manifest["gates"] = {
        key: (
            {k: v for k, v in gate.items() if k not in ("basis", "defined", "cause")}
            if key in SHARPE_GATES
            else gate
        )
        for key, gate in manifest["gates"].items()
    }
    (copy.directory / artifact.MANIFEST).write_text(json.dumps(manifest))
    copy.rewrite()
    with pytest.raises(
        artifact.CertificationError, match="lacks sharpe_basis, excess, sharpe_total; re-certify"
    ):
        artifact.load(copy.directory)
    report = certify.report(manifest)
    assert "The deflated Sharpe and Sharpe gates read total returns." in report
    assert f"total-return Sharpe {manifest['sharpe_net']:.4f}" in report


def test_an_unknown_basis_is_refused(certified, tmp_path):
    copy = copied(certified, tmp_path)
    copy.rewrite(sharpe_basis="excess_cash")
    with pytest.raises(
        artifact.CertificationError,
        match="differs from manifest.json in 1 field.*sharpe_basis: manifest 'excess_cash',"
        " recomputed 'excess_spy'",
    ):
        artifact.load(copy.directory)


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


def test_a_run_under_no_build_version_is_refused_before_it_starts(
    panel, research_lake, tmp_path, monkeypatch
):
    """`dev` names no commit, so the artifact and the cache it keys could not say what ran."""
    monkeypatch.delenv("AGB_VERSION")
    research_dir = tmp_path / "research"
    ledger = Ledger(research_dir / trials.FILE)
    with pytest.raises(certify.CertificationError, match="names no build"):
        certify.run(
            panel, MODEL, VALIDATION, OPTIONS, ledger,
            research_dir=research_dir, lake_dir=research_lake,
        )  # fmt: skip
    assert ledger.count() == 0
    assert not (research_dir / "certifications").exists()


def test_a_run_refuses_a_ledger_a_row_was_taken_out_of(panel, research_lake, tmp_path):
    """A ledger whose chain is broken declares a count nobody can vouch for, so the run refuses
    it before adding a row of its own or writing anything."""
    research_dir = tmp_path / "research"
    ledger = Ledger(research_dir / trials.FILE)
    for i in range(3):
        with ledger.trial(
            "scan", {"scan": i}, features=PRIORS,
            train_start=panel.months[0].as_of, train_end=panel.months[-1].as_of,
            partition_hash=panel.partition_hash, cost_model_version=panel.cost_model_version,
        ):  # fmt: skip
            pass
    rows = ledger.rows()
    del rows[1]
    pq.write_table(pa.Table.from_pylist(rows, schema=trials.SCHEMA), ledger.path)
    with pytest.raises(
        certify.CertificationError,
        match="the trial ledger cannot be trusted: .* row 1, .* does not link to the row before it",
    ):
        certify.run(
            panel, MODEL, VALIDATION, OPTIONS, ledger,
            research_dir=research_dir, lake_dir=research_lake,
        )  # fmt: skip
    assert ledger.rows() == rows
    assert not (research_dir / "certifications").exists()


# A tampered artifact --------------------------------------------------------------------------


def test_a_tampered_ledger_is_detected(db, certified, tmp_path):
    copy = copied(certified, tmp_path)
    copy.drop_last_trial()
    with pytest.raises(artifact.CertificationError, match="trials.parquet has been altered"):
        artifact.import_artifact(db.connect(RESEARCH), copy.directory)


def test_a_ledger_with_the_wrong_number_of_rows_is_refused(db, certified, tmp_path):
    copy = copied(certified, tmp_path)
    copy.drop_last_trial()
    copy.rewrite()
    with pytest.raises(
        artifact.CertificationError,
        match=f"holds {len(PRIORS) + 2} rows; the certification shipped {len(PRIORS) + 3}",
    ):
        artifact.import_artifact(db.connect(RESEARCH), copy.directory)


def test_a_ledger_that_disagrees_with_the_declared_count_is_refused(db, certified, tmp_path):
    # A forger who drops a configuration that was searched and covers the row count over it.
    # The recount under the manifest's own rule is what refuses the artifact.
    copy = copied(certified, tmp_path)
    copy.drop_first_trial()
    copy.rewrite(ledger_rows=len(PRIORS) + 2)
    with pytest.raises(
        artifact.CertificationError,
        match=f"holds {len(PRIORS)} trials counted as {trials.RULE};"
        f" the certification declared {len(PRIORS) + 1}",
    ):
        artifact.import_artifact(db.connect(RESEARCH), copy.directory)


@pytest.mark.parametrize("index", [0, 2, -3])
def test_a_ledger_a_row_was_taken_out_of_is_refused(db, certified, tmp_path, index):
    # A row taken out of the middle, with the row count and every digest covered over it, still
    # breaks the chain: the row after it names a link no row left in the ledger has.
    copy = copied(certified, tmp_path)
    copy.drop(index)
    copy.rewrite(ledger_rows=len(PRIORS) + 2)
    with pytest.raises(
        artifact.CertificationError,
        match="trials.parquet cannot be trusted: .* does not link to the row before it",
    ):
        artifact.import_artifact(db.connect(RESEARCH), copy.directory)


def test_a_declared_count_the_trial_row_disagrees_with_is_refused(db, certified, tmp_path):
    copy = copied(certified, tmp_path)
    copy.rewrite(declared_trials=7)
    with pytest.raises(
        artifact.CertificationError,
        match=f"holds {len(PRIORS) + 1} trials counted as {trials.RULE};"
        " the certification declared 7",
    ):
        artifact.import_artifact(db.connect(RESEARCH), copy.directory)
    # Dropping a configuration and lowering the declared count to match leaves the recount and
    # the manifest agreeing. The count the run itself charged for, which is in its own trial
    # row under the ledger's digest, is what refuses it.
    copy = copied(certified, tmp_path / "again")
    copy.drop_first_trial()
    copy.rewrite(declared_trials=len(PRIORS), ledger_rows=len(PRIORS) + 2)
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
    with pytest.raises(artifact.CertificationError, match="manifest.json has been altered"):
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


def test_a_trial_count_by_any_other_rule_is_refused(db, certified, tmp_path):
    """The rule is fixed in the code (issue #211). A manifest that names `rows` and declares
    the ledger's row count, which agrees with itself, is refused before anything is counted;
    so is one that names no rule, as every certification before the fix does."""
    copy = copied(certified, tmp_path)
    rows = copy.ledger().rows()
    copy.rewrite(
        declared_trials_rule="rows",
        declared_trials=trials.declared(rows, rule="rows"),
    )
    with pytest.raises(
        artifact.CertificationError,
        match=r"counted its trials as 'rows', and the only rule this code accepts is 'started';"
        " re-certify",
    ):
        artifact.import_artifact(db.connect(RESEARCH), copy.directory)
    manifest = copy.manifest
    del manifest["declared_trials_rule"]
    (copy.directory / artifact.MANIFEST).write_text(json.dumps(manifest))
    with pytest.raises(
        artifact.CertificationError, match="names no trial count rule.*issue #211.*re-certify"
    ):
        artifact.import_artifact(db.connect(RESEARCH), copy.directory)
    assert db.connect(RESEARCH).execute(
        "SELECT count(*) FROM agb.model_certification"
    ).fetchone() == (0,)


def test_a_gate_value_edited_with_every_digest_recomputed_is_refused(db, certified, tmp_path):
    """A forger who knows the digest scheme can make every digest agree; the gates run again
    over the shipped paths and ledger cannot be made to agree with an edited value."""
    copy = copied(certified, tmp_path)
    dsr = copy.manifest["dsr"]
    copy.rewrite(dsr=dsr + 0.01)
    with pytest.raises(
        artifact.CertificationError,
        match=r"differs from manifest.json in 1 field\(s\): dsr: manifest",
    ):
        artifact.import_artifact(db.connect(RESEARCH), copy.directory)
    copy = copied(certified, tmp_path / "gate")
    gated = copy.manifest["gates"]
    copy.rewrite(gates={**gated, "pbo": {**gated["pbo"], "value": 0.0, "passed": True}})
    with pytest.raises(artifact.CertificationError, match=r"gates\.pbo\.value: manifest 0\.0"):
        artifact.import_artifact(db.connect(RESEARCH), copy.directory)
    assert db.connect(RESEARCH).execute(
        "SELECT count(*) FROM agb.model_certification"
    ).fetchone() == (0,)


def test_a_difference_within_the_tolerance_is_not_one(certified, tmp_path):
    """Floating-point noise in the last digits is not a forgery: a relative 1e-12 passes, a
    relative 1e-6 does not."""
    copy = copied(certified, tmp_path)
    dsr = copy.manifest["dsr"]
    copy.rewrite(dsr=dsr * (1 + 1e-12))
    assert artifact.load(copy.directory)["dsr"] == dsr * (1 + 1e-12)
    copy.rewrite(dsr=dsr * (1 + 1e-6))
    with pytest.raises(artifact.CertificationError, match="dsr: manifest"):
        artifact.load(copy.directory)


def test_a_digest_over_the_files_alone_is_refused_by_name(certified, tmp_path):
    """The digest certifications up to lgbm-2026-10 carry covers the files and not the
    manifest; such an artifact is refused as one to certify again, not as a forgery."""
    copy = copied(certified, tmp_path)
    manifest = copy.manifest
    manifest["artifact_sha256"] = artifact._files_sha256(manifest["files"])
    (copy.directory / artifact.MANIFEST).write_text(json.dumps(manifest))
    with pytest.raises(
        artifact.CertificationError,
        match="covers its files and not manifest.json.*issue #211.*re-certify",
    ):
        artifact.load(copy.directory)


# The command line -----------------------------------------------------------------------------


def test_an_import_with_no_database_says_where_the_import_runs(certified, monkeypatch):
    """The research service sets no PG* variables and runs with `network_mode: none`, so an
    unguarded connect fails on a local socket, complaining about a Unix account that does not
    exist. The guard names the two commands that can do the write instead."""
    for name in cli._DATABASE_ENV:
        monkeypatch.delenv(name, raising=False)
    with pytest.raises(SystemExit, match="runs on the server as `./agb-stack import ranker"):
        cli.main(["import", str(certified.directory)])


def test_the_cli_certifies_and_imports(
    db, panel, research_lake, prior, certified, tmp_path, monkeypatch, capsys
):
    research_dir = tmp_path / "research"
    research_dir.mkdir()
    # The prior backtests give the overfitting gate something to rank the model against; an
    # empty ledger leaves one candidate and fails it. Only the priors are copied: the module's
    # own certification also sits in that ledger, and counting it would declare one more trial
    # than the certified run did.
    ledger, prior_ids = prior
    Ledger(research_dir / trials.FILE)._write(
        [row for row in ledger.rows() if row["trial_id"] in prior_ids]
    )
    panel.save(research_dir / "panel.parquet")
    monkeypatch.setenv("AGB_RESEARCH_DIR", str(research_dir))
    monkeypatch.setenv("AGB_LAKE_DIR", str(research_lake))
    cli.main([
        "certify", "--model-id", "cli-ranker", "--rounds", "20", "--min-leaf", "5",
        "--groups", "6", "--size", "6", "--hold-through", "8",
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


def test_the_cli_exits_1_on_a_failed_certification_after_writing_it(
    panel, research_lake, tmp_path, monkeypatch, capsys
):
    """A failed certification is a record, so it is written, but the command failed: a script
    reading the exit status must not take the artifact for a promotable one. An empty ledger
    leaves the overfitting gate one candidate, which fails it."""
    research_dir = tmp_path / "research"
    research_dir.mkdir()
    panel.save(research_dir / "panel.parquet")
    monkeypatch.setenv("AGB_RESEARCH_DIR", str(research_dir))
    monkeypatch.setenv("AGB_LAKE_DIR", str(research_lake))
    with pytest.raises(SystemExit) as exited:
        cli.main([
            "certify", "--model-id", "cli-failed", "--rounds", "20", "--min-leaf", "5",
            "--groups", "6", "--size", "6", "--hold-through", "8",
        ])  # fmt: skip
    assert exited.value.code == 1
    out = capsys.readouterr().out
    assert f"certification FAILED: {STAGE_1}; artifact " in out
    manifest = json.loads(
        (research_dir / "certifications" / "cli-failed" / artifact.MANIFEST).read_text()
    )
    assert manifest["passed"] is False


def test_the_nesting_sweep_agrees_with_asking_every_row_of_every_other():
    """`trials.nested` sweeps the rows by start (issue #204) where it once compared each row with
    every other. Over ties, zombies, the open run and plain overlaps, the two must agree."""
    import random

    base = datetime(2026, 1, 1, tzinfo=UTC)
    generator = random.Random(211)
    for _ in range(200):
        rows = []
        for i in range(generator.randint(1, 12)):
            start = base + timedelta(minutes=generator.randint(0, 20))
            finished = generator.choice([None, start + timedelta(minutes=generator.randint(0, 20))])
            rows.append({"trial_id": f"t{i}", "started_at": start, "finished_at": finished})
        current = generator.choice([None, *(row["trial_id"] for row in rows)])
        pairwise = {
            row["trial_id"]
            for row in rows
            if any(
                other["started_at"] < row["started_at"]
                and trials._closes_at(other, current) >= trials._closes_at(row, current)
                for other in rows
                if other["trial_id"] != row["trial_id"]
            )
        }
        assert trials.nested(rows, current=current) == pairwise


# Reproduction (issue #212) --------------------------------------------------------------------


def ledger_copy(certified: certify.Certification, tmp_path) -> Ledger:
    """The module's ledger, copied: a reproduction appends to the ledger it reads, and the
    module's other tests read the original."""
    path = tmp_path / "research" / trials.FILE
    path.parent.mkdir()
    shutil.copyfile(certified.directory.parent.parent / trials.FILE, path)
    return Ledger(path)


def test_a_certification_reproduces_to_tolerance_and_is_no_new_trial(
    panel, research_lake, certified, tmp_path
):
    ledger = ledger_copy(certified, tmp_path)
    before = ledger.rows()
    rows = len(before)
    declared = {rule: ledger.declared(rule=rule) for rule in ("configurations", "started")}

    result = reproduce.reproduce(ledger, certified.trial_id, panel, lake_dir=research_lake)

    assert result.differences == ()
    assert result.matches
    assert result.rerun.trial_id != certified.trial_id
    assert result.rerun.results["passed"] == certified.passed
    added = ledger.rows()[rows:]
    assert {row["kind"] for row in added} == {artifact.KIND, cpcv.KIND, backtest.KIND}
    assert all(row["reproduces"] == certified.trial_id for row in added)
    for rule, count in declared.items():
        assert ledger.declared(rule=rule) == count, f"{rule}: a reproduction is not a new trial"
    ledger.verify()
    assert not (ledger.path.parent / "certifications").exists(), "its artifact is scratch"

    # The same rerun against a recorded path that moved by a cent of net return a month.
    moved = dict(ledger.get(certified.trial_id))
    results = json.loads(moved["results"])
    results["paths"][0]["monthly"][0]["net"] += 0.01
    moved["results"] = trials.encode(results)
    found = reproduce._compare(moved, result.rerun, before, ledger.rows())
    assert any(line.startswith("paths[0].monthly[0].net: original") for line in found), found


def test_the_model_score_backtest_of_a_certification_reproduces(
    panel, research_lake, certified, tmp_path
):
    """A backtest nested in a certification has no options of its own to rerun from: the
    certification reruns and its backtest is compared with the original's."""
    ledger = ledger_copy(certified, tmp_path)
    backtest_id = certified.manifest["backtest_trial_id"]

    result = reproduce.reproduce(ledger, backtest_id, panel, lake_dir=research_lake)

    assert result.matches, result.differences
    rerun = ledger.get(result.rerun.trial_id)
    assert rerun["kind"] == backtest.KIND
    assert rerun["reproduces"] == backtest_id
    assert rerun["parent"] != ledger.get(backtest_id)["parent"]
