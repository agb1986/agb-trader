"""The certified-model registry (T21) with a real certified artifact: `agb promote` making it
live and recording the promotion (acceptance criterion 1), every refusal recorded as a refused
command (2), a cost-model bump voiding the certifications made under the old one (3), the
pipeline refusing a model it cannot trust (4), the gate halting a basket ranked by anything but
the promoted artifact (5), a paper rebalance end to end on the promoted booster (6) and the
placeholder ranker gone (7). The paper-only measurement waiver is here too: a certification
that genuinely failed its gates, promoted for a paper rebalance and refused everywhere else.
tests/test_gate.py holds the gate's model check to every case on a stand-in model.

The artifact is certified once for the module, as tests/test_certify.py certifies one: the
session's fake research panel, cross-validated in six groups, with five plain-signal
backtests in the ledger first. Each test copies it into a models directory of its own, as the
operator copies it to the server, and imports its row into a fresh database. The rebalance
runs on the research lake, whose last session is the signal session of September 2026."""

import importlib.util
import json
import logging
import re
import shutil
import threading
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import registry_fake
from account_fake import read_book

from agb import cli, cost, gate, lake, registry, schedule, universe
from agb.approval import Source
from agb.rebalance import SIZE
from agb.research import artifact, backtest, certify, cpcv, gates, train, trials
from agb.research.trials import Ledger
from agb.roles import RESEARCH
from agb.schedule import Expected, Planned
from agb.services.pipeline import Pipeline

# `certify` refuses to run without a build version.
pytestmark = pytest.mark.usefixtures("certifiable")

MODEL_ID = "ranker-2026.1"
MODEL = train.Config(num_boost_round=20, min_data_in_leaf=5)
VALIDATION = cpcv.Options(groups=6, test_groups=2, embargo_months=1)
OPTIONS = certify.Options(model_id=MODEL_ID, size=6, hold_through=10)
PRIORS = ("mom_12_1", "mom_6_1", "rev_1m", "vol_12m", "log_mktcap")

# The research lake's last session, and the rebalance it signals.
SIGNAL = date(2026, 8, 31)
TRADE = date(2026, 9, 1)
PERIOD = "2026-09"
EVENING = datetime(2026, 8, 31, 22, 0, tzinfo=UTC)
CAPITAL = Decimal("100000")
# The research lake is small, so the gate's universe band is widened to fit it.
LIMITS = gate.Limits(universe_min=20)
# A cost model other than the one in force, as after a release that changes it.
NEXT = "next"
# A second certification of the same model, judged where no model can reach: the DSR gate
# fails below its threshold, and a DSR is never one. It fails its gates and nothing else,
# which is the failure the paper-only measurement waiver covers.
FAILED_ID = "ranker-2026.1-failed"
FAILED = certify.Options(
    model_id=FAILED_ID, size=6, hold_through=10, thresholds=gates.Thresholds(dsr=1.0)
)
WAIVER = "measuring the live limits, issue #45"

SRC = Path(__file__).resolve().parent.parent / "src" / "agb"


@pytest.fixture(scope="module")
def certified(research_panel, research_lake, tmp_path_factory) -> certify.Certification:
    research_dir = tmp_path_factory.mktemp("research")
    ledger = Ledger(research_dir / trials.FILE)
    for signal in PRIORS:
        backtest.run(
            research_panel,
            backtest.Options(signal=signal, size=OPTIONS.size, hold_through=OPTIONS.hold_through),
            ledger,
            lake_dir=research_lake,
        )
    certified = certify.run(
        research_panel, MODEL, VALIDATION, OPTIONS, ledger,
        research_dir=research_dir, lake_dir=research_lake,
    )  # fmt: skip
    assert certified.passed
    return certified


@pytest.fixture
def models(certified, tmp_path) -> Path:
    """A models directory holding a copy of the artifact, as on the server."""
    directory = tmp_path / "models"
    shutil.copytree(certified.directory, directory / MODEL_ID)
    return directory


@pytest.fixture
def certification_id(db, certified) -> int:
    """The artifact's row, imported as the research profile imports it."""
    return artifact.import_artifact(db.connect(RESEARCH), certified.directory)


@pytest.fixture(scope="module")
def failed(research_panel, research_lake, tmp_path_factory) -> certify.Certification:
    """The same configuration certified against a DSR threshold no model can reach, so the
    gates fail and the artifact is as real as the certified one's."""
    research_dir = tmp_path_factory.mktemp("research-failed")
    run = certify.run(
        research_panel, MODEL, VALIDATION, FAILED, Ledger(research_dir / trials.FILE),
        research_dir=research_dir, lake_dir=research_lake,
    )  # fmt: skip
    assert not run.passed
    return run


@pytest.fixture
def waivable(failed, models) -> Path:
    """A models directory holding the failed artifact beside the certified one."""
    shutil.copytree(failed.directory, models / FAILED_ID)
    return models


@pytest.fixture
def failed_id(db, failed) -> int:
    """The failed certification's row, imported as the research profile imports it. A failed
    certification is recorded like any other."""
    return artifact.import_artifact(db.connect(RESEARCH), failed.directory)


def source(verb: str, actor: str = "operator") -> Source:
    return Source(actor, "cli", f"agb {verb}")


def promote(db, models: Path, model_id: str = MODEL_ID) -> str:
    return registry.promote(db.connect("cli"), model_id, source(f"promote {model_id}"), models)


def waive(db, models: Path, model_id: str = FAILED_ID, reason: str = WAIVER) -> str:
    return registry.promote(
        db.connect("cli"),
        model_id,
        source(f"promote {model_id} --waive-for-paper --reason {reason!r}"),
        models,
        waive_for_paper=reason,
    )


def go_live(db) -> None:
    """Move the mode to live as `agb mode live` records it, which the ladder allows only once
    the paper record has earned it (tests/test_ladder.py)."""
    conn = db.connect("agb_owner")
    [(command_id,)] = conn.execute(
        "INSERT INTO agb.command (actor, surface, raw_text, verb, outcome)"
        " VALUES ('operator', 'cli', 'agb mode live', 'mode', 'accepted') RETURNING id"
    ).fetchall()
    conn.execute(
        "INSERT INTO agb.mode_event (mode, actor, reason, attestation, command_id)"
        " VALUES ('live', 'cli', 'earned', 'the paper record is sound', %s)",
        (command_id,),
    )


def bump(monkeypatch) -> None:
    """Put the next cost model in force, as a release that changes it does. Its constants are
    the current one's; only the version, which is all the registry reads, differs."""
    fingerprint = cost.VERSIONS[cost.CURRENT.version]
    monkeypatch.setattr(cost, "VERSIONS", {**cost.VERSIONS, NEXT: fingerprint})
    monkeypatch.setattr(cost, "CURRENT", replace(cost.CURRENT, version=NEXT))


def rows(db, query, *params):
    return db.connect("cli").execute(query, params).fetchall()


def commands(db):
    return rows(
        db, "SELECT verb, args, outcome, reason, actor, surface FROM agb.command ORDER BY id"
    )


def plan_rebalance(db) -> None:
    start = datetime.now(UTC) - timedelta(hours=1)
    job = Planned(
        schedule.REBALANCE,
        PERIOD,
        TRADE,
        start,
        (Expected(schedule.PROPOSAL, PERIOD, start + timedelta(hours=15), True),),
    )
    schedule.write(db.connect("scheduler"), [job])


def rebalance(db, lake_dir: Path, models: Path, **kwargs) -> None:
    """One pipeline cycle: the planned rebalance, ranked by the promoted model unless a ranker
    is given, with the account read at the signal session's close."""
    kwargs = {"limits": LIMITS, "clock": lambda: EVENING, "capital": CAPITAL, **kwargs}
    read_book(db, SIGNAL)
    Pipeline(lake_dir, "", models_dir=models, **kwargs)(db.connect("pipeline"))


def test_the_registry_reads_the_layout_the_research_profile_writes(models):
    assert (registry.MANIFEST, registry.MODEL, registry.LEDGER) == (
        artifact.MANIFEST,
        artifact.MODEL,
        trials.FILE,
    )
    # The server recounts the artifact's trials itself, without importing the research
    # profile, so its counter has to agree with the one that wrote the number.
    ledger = models / MODEL_ID / trials.FILE
    assert registry.RULES == trials.RULES
    for rule in trials.RULES:
        assert registry.declared_trials_of(ledger, rule) == Ledger(ledger).declared(rule=rule)
    with pytest.raises(registry.RegistryError, match="unknown rule 'guesses'"):
        registry.declared_trials_of(ledger, "guesses")


def test_the_registry_counts_failed_nested_and_old_rows_as_the_ledger_does(tmp_path):
    """The certification's own ledger holds only runs that succeeded, so the counters are also
    held together over one holding everything the rules treat differently: rows from before the
    chain, a failure, a rerun, a certification's nested runs and a scan that merely overlapped
    it from another thread."""
    window = {
        "features": ("mom_12_1",), "train_start": date(2024, 1, 31),
        "train_end": date(2025, 12, 31), "cost_model_version": "v1",
    }  # fmt: skip
    old = Ledger(tmp_path / "old.parquet")
    with old.trial("certification", {"seed": 0}, partition_hash="old", **window):
        with old.trial("cpcv", {"seed": 0}, partition_hash="old", **window):
            pass
    with pytest.raises(RuntimeError):
        with old.trial("scan", {"seed": 1}, partition_hash="old", **window):
            raise RuntimeError("boom")
    path = tmp_path / trials.FILE
    columns = [f for f in trials.SCHEMA if f.name not in ("parent", "previous")]
    pq.write_table(pa.Table.from_pylist(old.rows(), schema=pa.schema(columns)), path)
    ledger = Ledger(path)

    def scan():
        with ledger.trial("scan", {"seed": 2}, partition_hash="new", **window):
            pass

    with pytest.raises(RuntimeError):
        with ledger.trial("backtest", {"seed": 3}, partition_hash="new", **window):
            raise RuntimeError("boom")
    with ledger.trial("backtest", {"seed": 3}, partition_hash="new", **window):
        pass
    with ledger.trial("certification", {"seed": 4}, partition_hash="new", **window):
        with ledger.trial("cpcv", {"seed": 4}, partition_hash="new", **window):
            pass
        thread = threading.Thread(target=scan)
        thread.start()
        thread.join()
    ledger.verify()
    counts = {rule: registry.declared_trials_of(path, rule) for rule in trials.RULES}
    assert counts == {rule: ledger.declared(rule=rule) for rule in trials.RULES}
    assert counts == {"rows": 8, "configurations": 3, "started": 5}


# Promoting ------------------------------------------------------------------------------------


def test_promote_makes_the_certification_live_and_records_the_promotion(
    db, models, certification_id, certified
):
    digest = certified.manifest["artifact_sha256"]

    message = promote(db, models)

    assert message == (
        f"model {MODEL_ID} promoted: certification {certification_id}, artifact {digest[:12]}."
        " The next rebalance ranks with it."
    )
    [(command_id, verb, args, outcome)] = rows(
        db, "SELECT id, verb, args, outcome FROM agb.command"
    )
    assert (verb, outcome) == ("promote", "accepted")
    assert args == {
        "model_id": MODEL_ID,
        "certification_id": certification_id,
        "artifact_sha256": digest,
    }
    assert rows(db, "SELECT certification_id, command_id, actor FROM agb.promotion_event") == [
        (certification_id, command_id, "operator")
    ]
    assert rows(
        db,
        "SELECT certification_id, model_id, artifact_sha256, voided, promoted_by"
        " FROM agb.promoted_model",
    ) == [(certification_id, MODEL_ID, digest, False, "operator")]


def test_the_promoted_model_loads_as_the_certified_booster(db, models, certification_id, certified):
    promote(db, models)

    ranker = registry.load_promoted(db.connect("pipeline"), models)

    assert (ranker.certification_id, ranker.name, ranker.artifact_sha256) == (
        certification_id,
        MODEL_ID,
        certified.manifest["artifact_sha256"],
    )
    assert ranker.features == tuple(MODEL.features)
    assert ranker.booster.feature_name() == list(MODEL.features)


def _missing(db, models):
    return "nonesuch", "no certification of model nonesuch"


def _voided(db, models):
    registry.void(db.connect("cli"), MODEL_ID, "leaked labels", source("void"))
    return MODEL_ID, rf"model {MODEL_ID} was voided on \S+ by operator: leaked labels"


def _failed(db, models):
    registry_fake.certify(db, "failed-model", passed=False)
    return "failed-model", "model failed-model failed certification"


def _no_artifact(db, models):
    shutil.rmtree(models / MODEL_ID)
    return MODEL_ID, f"model {MODEL_ID} has no artifact at {models / MODEL_ID}"


def _fewer_trials(db, models):
    """The artifact's ledger with its first trial dropped: a prior backtest, which is a
    configuration the search counted, so the count the ledger ships no longer matches the count
    the certification declared. The last row would be the backtest the certification nests,
    which the count never charged for."""
    ledger = Ledger(models / MODEL_ID / trials.FILE)
    rows = ledger.rows()
    pq.write_table(pa.Table.from_pylist(rows[1:], schema=trials.SCHEMA), ledger.path)
    return MODEL_ID, (
        f"the artifact's ledger holds {trials.declared(rows[1:])} trials counted as"
        f" {trials.RULE}; the certification declared {trials.declared(rows)}"
    )


def _altered(db, models):
    with (models / MODEL_ID / "report.md").open("a") as report:
        report.write("\nbetter than it looks\n")
    return MODEL_ID, "report.md in .* has been altered since certification"


def _swapped(db, models):
    """An altered report with a manifest listing it, as a forger would write: every file
    matches the manifest, but the digest over them is not the certification's."""
    directory = models / MODEL_ID
    with (directory / "report.md").open("a") as report:
        report.write("\nbetter than it looks\n")
    path = directory / registry.MANIFEST
    files = {
        p.name: registry._sha256(p)
        for p in sorted(directory.iterdir())
        if p.name != registry.MANIFEST
    }
    path.write_text(json.dumps({**json.loads(path.read_text()), "files": files}))
    return MODEL_ID, "the artifact in .* has digest [0-9a-f]{12}; the certification's is"


def _unlisted(db, models):
    (models / MODEL_ID / "notes.txt").write_text("tried a few more things\n")
    return MODEL_ID, "holds unlisted files: notes.txt"


def _already_live(db, models):
    promote(db, models)
    return MODEL_ID, rf"model {MODEL_ID} is already live, promoted \S+ by operator"


@pytest.mark.parametrize(
    "refuse",
    [
        pytest.param(_missing, id="missing"),
        pytest.param(_voided, id="voided"),
        pytest.param(_failed, id="failed"),
        pytest.param(_no_artifact, id="no_artifact"),
        pytest.param(_fewer_trials, id="trial_count_mismatch"),
        pytest.param(_altered, id="altered"),
        pytest.param(_swapped, id="digest_mismatch"),
        pytest.param(_unlisted, id="unlisted_file"),
        pytest.param(_already_live, id="already_live"),
    ],
)
def test_promote_refuses_and_records_the_refusal(db, models, certification_id, refuse):
    model_id, why = refuse(db, models)
    promotions = rows(db, "SELECT id FROM agb.promotion_event")
    live = rows(db, "SELECT certification_id FROM agb.promoted_model")

    with pytest.raises(registry.Refused, match=why) as refused:
        promote(db, models, model_id)

    *_, last = commands(db)
    assert last == (
        "promote",
        {"model_id": model_id},
        "refused",
        str(refused.value),
        "operator",
        "cli",
    )
    assert rows(db, "SELECT id FROM agb.promotion_event") == promotions
    assert rows(db, "SELECT certification_id FROM agb.promoted_model") == live


# Waiving --------------------------------------------------------------------------------------


def test_a_waiver_promotes_the_failed_certification_and_records_the_waiver(
    db, waivable, failed_id, failed
):
    digest = failed.manifest["artifact_sha256"]

    message = waive(db, waivable)

    assert message == (
        f"model {FAILED_ID} promoted under a paper-only measurement waiver: certification"
        f" {failed_id}, artifact {digest[:12]}. It failed certification, so the next rebalance"
        " ranks with an uncertified model, and a move to live is refused until a model that"
        " passed is promoted."
    )
    [(command_id, verb, args, outcome)] = rows(
        db, "SELECT id, verb, args, outcome FROM agb.command"
    )
    assert (verb, outcome) == ("promote", "accepted")
    assert args == {
        "model_id": FAILED_ID,
        "waive_for_paper": WAIVER,
        "certification_id": failed_id,
        "artifact_sha256": digest,
    }
    assert rows(
        db,
        "SELECT certification_id, event, reason, cost_model_version, command_id, actor"
        " FROM agb.certification_event",
    ) == [(failed_id, "waived", WAIVER, cost.CURRENT.version, command_id, "operator")]
    assert rows(
        db,
        "SELECT certification_id, model_id, passed, waived, waived_reason FROM agb.promoted_model",
    ) == [(failed_id, FAILED_ID, False, True, WAIVER)]


def test_a_certification_is_waived_once(db, waivable, failed_id):
    waive(db, waivable)

    with pytest.raises(registry.Refused, match=f"model {FAILED_ID} is already live"):
        waive(db, waivable)

    assert len(rows(db, "SELECT id FROM agb.certification_event WHERE event = 'waived'")) == 1


def _passed_its_gates(db, models, monkeypatch):
    return MODEL_ID, WAIVER, f"model {MODEL_ID} passed certification and needs no waiver"


def _no_reason(db, models, monkeypatch):
    return FAILED_ID, "   ", "a measurement waiver needs a reason"


def _live_mode(db, models, monkeypatch):
    go_live(db)
    return (
        FAILED_ID,
        WAIVER,
        (
            "a measurement waiver promotes a model that failed certification, so it is refused in"
            " live mode; it is for paper only"
        ),
    )


def _waiving_the_voided(db, models, monkeypatch):
    registry.void(db.connect("cli"), FAILED_ID, "fitted on the test months", source("void"))
    return (
        FAILED_ID,
        WAIVER,
        (rf"model {FAILED_ID} was voided on \S+ by operator: fitted on the test months"),
    )


def _waiving_the_stale(db, models, monkeypatch):
    """A cost-model bump leaves the failed certification stale, and promotion voids the stale
    before it reads the candidate, so the waiver meets the voiding its own promotion wrote."""
    bump(monkeypatch)
    return FAILED_ID, WAIVER, f"certified under cost model .*, superseded by {NEXT}"


def _waiving_without_the_artifact(db, models, monkeypatch):
    shutil.rmtree(models / FAILED_ID)
    return FAILED_ID, WAIVER, f"model {FAILED_ID} has no artifact at {models / FAILED_ID}"


@pytest.mark.parametrize(
    "refuse",
    [
        pytest.param(_passed_its_gates, id="passed"),
        pytest.param(_no_reason, id="no_reason"),
        pytest.param(_live_mode, id="live_mode"),
        pytest.param(_waiving_the_voided, id="voided"),
        pytest.param(_waiving_the_stale, id="stale"),
        pytest.param(_waiving_without_the_artifact, id="no_artifact"),
    ],
)
def test_a_waiver_refuses_and_records_the_refusal(
    db, waivable, certification_id, failed_id, monkeypatch, refuse
):
    model_id, reason, why = refuse(db, waivable, monkeypatch)

    with pytest.raises(registry.Refused, match=why) as refused:
        waive(db, waivable, model_id, reason)

    *_, last = commands(db)
    assert last == (
        "promote",
        {"model_id": model_id, "waive_for_paper": reason},
        "refused",
        str(refused.value),
        "operator",
        "cli",
    )
    assert rows(db, "SELECT id FROM agb.promotion_event") == []
    assert rows(db, "SELECT id FROM agb.certification_event WHERE event = 'waived'") == []


def test_the_waived_model_loads_in_paper_carrying_the_waiver_and_never_in_live(
    db, waivable, failed_id, failed
):
    waive(db, waivable)

    ranker = registry.load_promoted(db.connect("pipeline"), waivable)

    assert (ranker.certification_id, ranker.name, ranker.artifact_sha256) == (
        failed_id,
        FAILED_ID,
        failed.manifest["artifact_sha256"],
    )
    assert re.fullmatch(
        rf"model {re.escape(FAILED_ID)} failed certification and is live under the paper-only"
        rf" measurement waiver recorded on \d{{4}}-\d\d-\d\d by operator:"
        rf" {re.escape(WAIVER)}",
        ranker.waiver,
    )

    # The waiver is paper's alone: the same promotion in live mode is the failed certification
    # it has always been. The ladder refuses the move while it is live; this is the floor under
    # that refusal.
    go_live(db)
    with pytest.raises(
        registry.RegistryError, match=f"the promoted model {FAILED_ID} failed certification"
    ):
        registry.load_promoted(db.connect("pipeline"), waivable)


def test_a_paper_rebalance_on_the_waived_model_runs_and_says_it_ranked_under_the_waiver(
    db, research_lake, waivable, failed_id, failed, caplog
):
    """What the waiver is for: a real rebalance to measure, with every run that ranks by an
    uncertified model saying so in the log."""
    plan_rebalance(db)
    waive(db, waivable)

    with caplog.at_level(logging.WARNING):
        rebalance(db, research_lake, waivable)

    assert rows(db, "SELECT state FROM agb.job_state WHERE kind = 'rebalance'") == [("completed",)]
    [(ranker, certification, digest)] = rows(
        db, "SELECT ranker, model_certification_id, model_sha256 FROM agb.basket"
    )
    assert (ranker, certification, digest) == (
        FAILED_ID,
        failed_id,
        failed.manifest["artifact_sha256"],
    )
    assert rows(db, "SELECT name, measured, threshold FROM agb.gate_check WHERE NOT passed") == []
    assert rows(db, "SELECT state, actor FROM agb.basket_state") == [("approved", "system")]
    [under] = [
        record.getMessage() for record in caplog.records if "ranks under a waiver" in record.message
    ]
    assert under.startswith(
        f"rebalance {PERIOD} ranks under a waiver: model {FAILED_ID} failed certification"
    )
    assert under.endswith(WAIVER)


# Voiding --------------------------------------------------------------------------------------


def test_voiding_the_live_model_records_it_and_stops_the_next_rebalance_loading_it(
    db, models, certification_id
):
    promote(db, models)

    message = registry.void(db.connect("cli"), MODEL_ID, "leaked labels", source("void"))

    assert message == (
        f"model {MODEL_ID} voided. It is the live model: rebalances fail until another is promoted."
    )
    [(command_id, args, outcome)] = rows(
        db, "SELECT id, args, outcome FROM agb.command WHERE verb = 'void'"
    )
    assert (args, outcome) == (
        {"model_id": MODEL_ID, "reason": "leaked labels", "certification_id": certification_id},
        "accepted",
    )
    assert rows(
        db,
        "SELECT certification_id, event, reason, cost_model_version, command_id, actor"
        " FROM agb.certification_event",
    ) == [
        (certification_id, "voided", "leaked labels", cost.CURRENT.version, command_id, "operator")
    ]
    assert rows(db, "SELECT voided FROM agb.promoted_model") == [(True,)]
    with pytest.raises(registry.RegistryError, match=f"the promoted model {MODEL_ID} was voided"):
        registry.load_promoted(db.connect("pipeline"), models)


@pytest.mark.parametrize(
    ("model_id", "reason", "why"),
    [
        ("nonesuch", "no reason", "no certification of model nonesuch"),
        (MODEL_ID, " ", "a voiding needs a reason"),
    ],
)
def test_void_refuses_and_records_the_refusal(db, certification_id, model_id, reason, why):
    with pytest.raises(registry.Refused, match=why):
        registry.void(db.connect("cli"), model_id, reason, source("void"))

    assert commands(db) == [
        ("void", {"model_id": model_id, "reason": reason}, "refused", why, "operator", "cli")
    ]
    assert rows(db, "SELECT id FROM agb.certification_event") == []


def test_a_certification_is_voided_once(db, certification_id):
    registry.void(db.connect("cli"), MODEL_ID, "leaked labels", source("void"))

    with pytest.raises(registry.Refused, match=f"model {MODEL_ID} was already voided on"):
        registry.void(db.connect("cli"), MODEL_ID, "again", source("void"))

    assert [outcome for _, _, outcome, *_ in commands(db)] == ["accepted", "refused"]
    assert len(rows(db, "SELECT id FROM agb.certification_event")) == 1


def test_a_cost_model_bump_voids_every_certification_made_under_the_old_one(
    db, models, certification_id, monkeypatch
):
    old = cost.CURRENT.version
    other = registry_fake.certify(db, "other-model")
    # Certified by research code that carries the next cost model, before its release: refused
    # until then, but not stale, so neither promotion's voiding nor the deploy's touches it.
    registry_fake.certify(db, "next-model", cost_model_version=NEXT)
    promote(db, models)
    with pytest.raises(
        registry.Refused,
        match=f"model next-model was certified under cost model {NEXT}; the cost model is now"
        f" {old}",
    ):
        promote(db, models, "next-model")
    bump(monkeypatch)

    voided = registry.void_stale(db.connect("cli"), source("void-stale", "deploy"))

    assert voided == [MODEL_ID, "other-model"]
    [(command_id, args, outcome, actor)] = rows(
        db, "SELECT id, args, outcome, actor FROM agb.command WHERE verb = 'void_stale'"
    )
    assert (args, outcome, actor) == (
        {"cost_model_version": NEXT, "model_ids": [MODEL_ID, "other-model"]},
        "accepted",
        "deploy",
    )
    reason = f"certified under cost model {old}, superseded by {NEXT}"
    assert rows(
        db,
        "SELECT certification_id, event, reason, cost_model_version, command_id, actor"
        " FROM agb.certification_event ORDER BY certification_id",
    ) == [
        (certification_id, "voided", reason, NEXT, command_id, "deploy"),
        (other, "voided", reason, NEXT, command_id, "deploy"),
    ]
    # The live model is among them, so the next rebalance has no model to rank with.
    with pytest.raises(registry.RegistryError, match=f"the promoted model {MODEL_ID} was voided"):
        registry.load_promoted(db.connect("pipeline"), models)

    # Nothing more is stale, so a second run records nothing.
    assert registry.void_stale(db.connect("cli"), source("void-stale")) == []
    assert [(verb, outcome) for verb, _, outcome, *_ in commands(db)] == [
        ("promote", "accepted"),
        ("promote", "refused"),
        ("void_stale", "accepted"),
    ]


def test_promotion_voids_stale_certifications_first(db, models, certification_id, monkeypatch):
    bump(monkeypatch)

    with pytest.raises(
        registry.Refused, match=f"certified under cost model .*, superseded by {NEXT}"
    ):
        promote(db, models)

    assert [(verb, outcome) for verb, _, outcome, *_ in commands(db)] == [
        ("void_stale", "accepted"),
        ("promote", "refused"),
    ]


def test_the_cli_records_the_certification_the_operator_copied_in(db, models, monkeypatch, capsys):
    """Issue #81: on the server the row is inserted by `agb import-certification <model id>`,
    which reads the artifact from the models directory. It runs as `research`, the one role
    granted the insert, which is why it is a Compose service of its own."""
    for key, value in db.pg_env(RESEARCH).items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv(registry.MODELS_ENV, str(models))

    assert cli.main(["import-certification", MODEL_ID]) == 0
    assert capsys.readouterr().out == f"model_certification 1: {models / MODEL_ID}\n"
    assert rows(db, "SELECT model_id, actor FROM agb.model_certification") == [(MODEL_ID, RESEARCH)]

    # A certification is never replaced, so a second run of the same command refuses.
    assert cli.main(["import-certification", MODEL_ID]) == 1
    assert "is already certified" in capsys.readouterr().err
    assert len(rows(db, "SELECT id FROM agb.model_certification")) == 1

    # And the promotion the import unblocks, which is the ordinary `cli` service again.
    for key, value in db.pg_env("cli").items():
        monkeypatch.setenv(key, value)
    assert cli.main(["promote", MODEL_ID, "--actor", "alice"]) == 0


def test_the_cli_role_cannot_record_a_certification(db, models, monkeypatch, capsys):
    """The ordinary `cli` service runs as `cli`, which holds no insert on the table. The error
    names the role the operator needs rather than leaving a privilege message to read."""
    for key, value in db.pg_env("cli").items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv(registry.MODELS_ENV, str(models))

    assert cli.main(["import-certification", MODEL_ID]) == 1
    assert "use PGUSER=research" in capsys.readouterr().err
    assert rows(db, "SELECT id FROM agb.model_certification") == []


def test_a_refusal_names_the_database_it_asked(db, models, monkeypatch, capsys):
    """A certification lives in the database it was imported into, so the same promotion run
    against another database refuses a model that is certified on the server. The second line
    says which database answered, which is what tells those two refusals apart."""
    env = db.pg_env("cli")
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv(registry.MODELS_ENV, str(models))

    assert cli.main(["promote", MODEL_ID, "--actor", "alice"]) == 1
    err = capsys.readouterr().err.splitlines()
    assert err[0] == f"agb: refused: no certification of model {MODEL_ID}"
    assert err[1] == f"agb: asked database {env['PGDATABASE']} on {env['PGHOST']} as cli"


def test_the_cli_promotes_voids_and_voids_the_stale(
    db, models, certification_id, monkeypatch, capsys
):
    for key, value in db.pg_env("cli").items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv(registry.MODELS_ENV, str(models))

    assert cli.main(["promote", MODEL_ID, "--actor", "alice"]) == 0
    assert f"model {MODEL_ID} promoted" in capsys.readouterr().out
    assert cli.main(["promote", MODEL_ID, "--actor", "alice"]) == 1
    assert f"agb: refused: model {MODEL_ID} is already live" in capsys.readouterr().err
    assert cli.main(["void-stale", "--actor", "alice"]) == 0
    assert "nothing to void" in capsys.readouterr().out

    # A release with a new cost model: the deploy's void-stale voids the live certification.
    bump(monkeypatch)
    assert cli.main(["void-stale", "--actor", "deploy"]) == 0
    assert capsys.readouterr().out == f"voided under cost model {NEXT}: {MODEL_ID}\n"
    assert cli.main(["void", MODEL_ID, "--reason", "stale anyway", "--actor", "alice"]) == 1
    assert f"agb: refused: model {MODEL_ID} was already voided on" in capsys.readouterr().err

    assert [
        (verb, outcome, actor, text)
        for verb, outcome, actor, text in rows(
            db, "SELECT verb, outcome, actor, raw_text FROM agb.command ORDER BY id"
        )
    ] == [
        ("promote", "accepted", "alice", f"agb promote {MODEL_ID}"),
        ("promote", "refused", "alice", f"agb promote {MODEL_ID}"),
        ("void_stale", "accepted", "deploy", "agb void-stale"),
        ("void", "refused", "alice", f"agb void {MODEL_ID} --reason 'stale anyway'"),
    ]


# The pipeline and the gate --------------------------------------------------------------------


def test_a_paper_rebalance_runs_end_to_end_on_the_promoted_model(
    db, research_lake, models, certification_id, certified
):
    plan_rebalance(db)
    promote(db, models)

    rebalance(db, research_lake, models)

    assert rows(db, "SELECT state FROM agb.job_state WHERE kind = 'rebalance'") == [("completed",)]
    [(basket_id, ranker, certification, digest, snapshot)] = rows(
        db,
        "SELECT id, ranker, model_certification_id, model_sha256, snapshot_date FROM agb.basket",
    )
    assert (ranker, certification, digest, snapshot) == (
        MODEL_ID,
        certification_id,
        certified.manifest["artifact_sha256"],
        SIGNAL,
    )
    failed = rows(db, "SELECT name, measured, threshold FROM agb.gate_check WHERE NOT passed")
    assert failed == []
    [(measured, threshold)] = rows(
        db, "SELECT measured, threshold FROM agb.gate_check WHERE name = 'model'"
    )
    assert measured == threshold == f"{MODEL_ID} {digest[:12]}"
    assert rows(db, "SELECT state, actor FROM agb.basket_state") == [("approved", "system")]

    # The targets are the booster's top twenty.
    booster = registry.load_promoted(db.connect("pipeline"), models)
    with lake.connect(research_lake) as lake_db:
        ranked = booster.rank(universe.snapshot(SIGNAL, research_lake, db=lake_db), lake_db)
    assert len({r.score for r in ranked}) > 1
    top = sorted((r.rank, r.symbol) for r in ranked if r.rank <= SIZE)
    assert rows(
        db, "SELECT rank, symbol FROM agb.basket_target WHERE basket_id = %s ORDER BY rank",
        basket_id,
    ) == top  # fmt: skip


def _nothing_promoted(db, models):
    return "no model is promoted; `agb promote <model id>` promotes one"


def _promoted_then_voided(db, models):
    promote(db, models)
    registry.void(db.connect("cli"), MODEL_ID, "leaked labels", source("void"))
    return f"the promoted model {MODEL_ID} was voided"


def _promoted_then_altered(db, models):
    promote(db, models)
    _altered(db, models)
    return "report.md in .* has been altered since certification"


def _promoted_then_swapped(db, models):
    promote(db, models)
    _swapped(db, models)
    return "the artifact in .* has digest"


@pytest.mark.parametrize(
    "arrange",
    [
        pytest.param(_nothing_promoted, id="uncertified"),
        pytest.param(_promoted_then_voided, id="voided"),
        pytest.param(_promoted_then_altered, id="altered"),
        pytest.param(_promoted_then_swapped, id="hash_mismatch"),
    ],
)
def test_the_pipeline_refuses_a_model_it_cannot_trust(
    db, research_lake, models, certification_id, arrange
):
    why = arrange(db, models)
    with pytest.raises(registry.RegistryError, match=why):
        registry.load_promoted(db.connect("pipeline"), models)
    plan_rebalance(db)

    rebalance(db, research_lake, models)

    assert rows(db, "SELECT state FROM agb.job_state WHERE kind = 'rebalance'") == [("failed",)]
    [(detail,)] = rows(db, "SELECT detail FROM agb.job_event WHERE event = 'failed'")
    assert re.search(why, detail)
    assert rows(db, "SELECT id FROM agb.basket") == []


def test_the_pipeline_refuses_a_promoted_model_certified_under_an_old_cost_model(
    db, models, certification_id, monkeypatch
):
    promote(db, models)
    bump(monkeypatch)

    with pytest.raises(
        registry.RegistryError,
        match=f"the promoted model {MODEL_ID} was certified under cost model .*; the cost model"
        f" is now {NEXT}",
    ):
        registry.load_promoted(db.connect("pipeline"), models)


def test_the_gate_halts_a_basket_ranked_by_anything_but_the_promoted_artifact(
    db, research_lake, models, certification_id, certified
):
    plan_rebalance(db)
    promote(db, models)
    impostor = replace(
        registry.load_promoted(db.connect("pipeline"), models), artifact_sha256="ef" * 32
    )

    rebalance(db, research_lake, models, ranker=impostor)

    digest = certified.manifest["artifact_sha256"]
    assert rows(db, "SELECT name, measured, threshold FROM agb.gate_check WHERE NOT passed") == [
        ("model", f"{MODEL_ID} {'ef' * 6}", f"{MODEL_ID} {digest[:12]}")
    ]
    assert rows(db, "SELECT state FROM agb.basket_state") == [("halted",)]


# The placeholder ------------------------------------------------------------------------------


def test_the_placeholder_ranker_is_gone():
    assert importlib.util.find_spec("agb.placeholder_ranker") is None
    mentions = [
        str(path.relative_to(SRC))
        for path in SRC.rglob("*.py")
        if "placeholder" in path.read_text().lower()
    ]
    assert mentions == []
