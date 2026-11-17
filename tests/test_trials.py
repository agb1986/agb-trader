"""The trial ledger: the hash chain, the way a broken chain is named, the trial statuses, the
parent a nested trial names, and the rules a certification counts its trials by.

The ledger is a parquet file in a temporary directory; nothing here needs a database. The
rules are exercised on hand-built rows so the intervals are exact, and the `Ledger` on real
runs."""

import threading
from datetime import UTC, date, datetime, timedelta

import pytest

from agb.research import trials
from agb.research.trials import GENESIS, Ledger, LedgerError

T0 = datetime(2026, 10, 1, 12, 0, tzinfo=UTC)


def minutes(n):
    return T0 + timedelta(minutes=n)


def row(trial_id, start, end="open", *, config="a", status="succeeded", parent=None, **extra):
    """A ledger row by hand. `end` is a minute offset, or None for a run that never ended."""
    return {
        "trial_id": trial_id,
        "kind": "fit",
        "status": status,
        "started_at": None if start is None else minutes(start),
        "finished_at": None if end in (None, "open") else minutes(end),
        "config": config,
        "features": "[]",
        "train_start": date(2020, 1, 1),
        "train_end": date(2025, 12, 31),
        "partition_hash": "p",
        "cost_model_version": "c1",
        "code_version": "v1",
        "refits": 0,
        "results": None,
        "error": None,
        "parent": parent,
        "previous": None,
        **extra,
    }


def chained(rows):
    """The same rows with each one's `previous` set to the link of the row before it."""
    out = []
    for index, r in enumerate(rows):
        out.append({**r, "previous": trials.link(out[-1]) if index else GENESIS})
    return out


def run(ledger, config=None, **overrides):
    """Open a trial on `ledger` with the usual arguments and return its context manager."""
    arguments = {
        "features": ("f1", "f2"),
        "train_start": date(2020, 1, 1),
        "train_end": date(2025, 12, 31),
        "partition_hash": "digest",
        "cost_model_version": "c1",
        **overrides,
    }
    return ledger.trial("fit", config or {"depth": 3}, **arguments)


@pytest.fixture
def ledger(tmp_path):
    return Ledger(tmp_path / "ledgers" / trials.FILE)


# -- encoding and identity ------------------------------------------------------------------


def test_equal_configurations_encode_equal_whatever_the_key_order():
    assert trials.encode({"b": 1, "a": 2}) == trials.encode({"a": 2, "b": 1}) == '{"a":2,"b":1}'


def test_dates_tuples_and_sets_encode_as_json():
    encoded = trials.encode({"d": date(2026, 1, 2), "t": ("x", "y"), "s": frozenset({"z"})})
    assert encoded == '{"d":"2026-01-02","s":["z"],"t":["x","y"]}'


def test_a_value_json_cannot_hold_is_refused_rather_than_stringified():
    with pytest.raises(TypeError, match="object is not JSON serialisable"):
        trials.encode({"x": object()})


def test_the_code_version_is_the_build_version_or_dev(monkeypatch):
    monkeypatch.setenv("AGB_VERSION", "abc123")
    assert trials.code_version() == "abc123"
    monkeypatch.setenv("AGB_VERSION", "")
    assert trials.code_version() == trials.UNVERSIONED == "dev"
    monkeypatch.delenv("AGB_VERSION")
    assert trials.code_version() == "dev"


def test_a_configuration_leaves_out_when_and_how_the_run_went():
    one = row("1", 0, 5, config="x")
    other = row("2", 10, 12, config="x", status="failed", error="boom")
    assert trials.configuration(one) == trials.configuration(other)
    assert trials.configuration(one) != trials.configuration(row("3", 0, 5, config="y"))
    assert trials.configuration(one) != trials.configuration(row("4", 0, 5, partition_hash="q"))


def test_a_link_covers_what_was_tried_and_not_how_the_run_ended():
    base = row("1", 0, 5)
    finished = {**base, "status": "failed", "error": "boom", "refits": 4, "results": "{}"}
    assert trials.link(base) == trials.link(finished)
    assert trials.link(base) != trials.link({**base, "config": "other"})
    assert trials.link(base) != trials.link({**base, "previous": "x"})


# -- the chain ------------------------------------------------------------------------------


def test_an_empty_ledger_and_a_sound_chain_have_nothing_broken():
    assert trials.broken([]) is None
    assert trials.broken(chained([row("1", 0, 1), row("2", 2, 3), row("3", 4, 5)])) is None


def test_a_row_taken_out_of_the_middle_is_named():
    rows = chained([row("1", 0, 1), row("2", 2, 3), row("3", 4, 5)])
    problem = trials.broken([rows[0], rows[2]])
    assert problem is not None
    assert "row 1, trial 3" in problem and "removed, inserted or reordered" in problem


def test_rows_reordered_or_a_row_put_in_are_named():
    rows = chained([row("1", 0, 1), row("2", 2, 3), row("3", 4, 5)])
    assert "row 1, trial 3" in trials.broken([rows[0], rows[2], rows[1]])
    extra = chained([row("9", 0, 1)])[0]
    assert "does not link" in trials.broken([rows[0], extra, rows[1], rows[2]])


def test_a_first_row_that_does_not_start_at_genesis_is_named():
    first = {**chained([row("1", 0, 1)])[0], "previous": "f" * 64}
    assert "row 0, trial 1" in trials.broken([first])


def test_a_trial_id_that_appears_twice_is_named():
    rows = chained([row("1", 0, 1)])
    assert trials.broken([rows[0], rows[0]]) == "trial 1 appears twice"


def test_a_row_without_a_link_after_rows_that_have_one_is_named():
    rows = chained([row("1", 0, 1), row("2", 2, 3)])
    problem = trials.broken([*rows, row("3", 4, 5)])
    assert problem == "row 2, trial 3, has no link but follows rows that do"


def test_rows_from_before_the_chain_may_come_first():
    old = [row("1", 0, 1), row("2", 2, 3)]
    first_linked = {**row("3", 4, 5), "previous": trials.link(old[1])}
    assert trials.broken([*old, first_linked]) is None


def test_a_parent_must_have_been_appended_earlier():
    child = row("2", 1, 2, parent="1")
    parent = row("1", 0, 5)
    assert "names parent 1, which is not before it" in trials.broken(chained([child, parent]))
    assert trials.broken(chained([parent, child])) is None
    stray = row("3", 6, 7, parent="nope")
    assert "names parent nope" in trials.broken(chained([parent, stray]))


# -- nesting and the declared count ---------------------------------------------------------


def test_a_run_that_began_inside_another_is_nested():
    rows = [row("outer", 0, 10), row("inner", 2, 4), row("after", 11, 12)]
    assert trials.nested(rows) == {"inner"}


def test_rows_that_start_at_the_same_instant_are_siblings():
    rows = [row("a", 0, 10), row("b", 0, 5)]
    assert trials.nested(rows) == set()


def test_a_run_that_overlaps_another_without_ending_inside_it_is_not_nested():
    assert trials.nested([row("a", 0, 5), row("b", 3, 8)]) == set()


def test_a_run_never_finished_is_closed_at_its_start_unless_it_is_the_one_asking():
    zombie = row("zombie", 0, None, status="running")
    after = row("after", 5, 6)
    assert trials.nested([zombie, after]) == set()
    assert trials.nested([zombie, after], current="zombie") == {"after"}


def test_a_row_with_no_start_time_is_refused():
    with pytest.raises(LedgerError, match="1 trials with no start time, such as lost"):
        trials.nested([row("lost", None), row("ok", 0, 1)])


def test_rows_rule_counts_every_row():
    rows = [row("1", 0, 10), row("2", 2, 4), row("3", 11, 12)]
    assert trials.declared(rows, rule="rows") == 3


def test_an_unknown_rule_is_refused():
    with pytest.raises(LedgerError, match=r"unknown trial count rule 'lucky'"):
        trials.declared([], rule="lucky")


def test_the_default_rule_is_started():
    assert trials.RULE == "started"
    assert trials.declared([]) == 0


def test_configurations_rule_counts_succeeded_distinct_outer_runs():
    rows = [
        row("1", 0, 10, config="a"),
        row("2", 2, 4, config="inner"),  # nested: the mechanics of "1"
        row("3", 11, 12, config="a"),  # a repeat of "1"
        row("4", 13, 14, config="b", status="failed"),  # never succeeded
        row("5", 15, 16, config="c"),
    ]
    assert trials.declared(rows, rule="configurations") == 2


def test_configurations_rule_counts_the_run_asking_though_it_has_not_succeeded():
    rows = [row("1", 0, 1, config="a"), row("2", 5, None, config="b", status="running")]
    assert trials.declared(rows, rule="configurations") == 1
    assert trials.declared(rows, rule="configurations", current="2") == 2


def test_started_rule_counts_failed_and_abandoned_tries():
    rows = [
        row("1", 0, 1, config="a"),
        row("2", 2, 3, config="b", status="failed"),
        row("3", 4, 5, config="c", status="abandoned"),
        row("4", 6, 7, config="a"),
    ]
    assert trials.declared(rows) == 3


def test_started_rule_folds_a_chained_child_into_the_parent_it_names():
    rows = chained(
        [
            row("cert", 0, 10, config="cert"),
            row("cv", 1, 4, config="cv", parent="cert"),
            row("bt", 5, 9, config="bt", parent="cert"),
        ]
    )
    assert trials.declared(rows) == 1
    assert trials.declared(rows, rule="rows") == 3


def test_started_rule_counts_a_chained_run_that_merely_overlapped_another():
    rows = chained([row("a", 0, 10, config="a"), row("b", 2, 4, config="b")])  # no parent
    assert trials.declared(rows) == 2
    assert trials.declared(rows, rule="configurations") == 1


def test_started_rule_counts_a_child_whose_parent_does_not_hold_it():
    rows = chained([row("p", 0, 3, config="p"), row("c", 5, 6, config="c", parent="p")])
    assert trials.declared(rows) == 2


def test_started_rule_still_counts_the_children_of_a_run_killed_before_it_wrote_its_end():
    legacy = [row("p", 0, None, status="running"), row("c", 2, 3, config="c")]
    # The killed run closes at its own start, so nothing is inside it: both count, as they did.
    assert trials.declared(legacy) == 2
    assert trials.declared(legacy, rule="configurations") == 1


def test_roots_follow_parents_up_for_chained_rows():
    rows = chained(
        [
            row("root", 0, 10),
            row("mid", 1, 9, parent="root", config="m"),
            row("leaf", 2, 3, parent="mid", config="l"),
            row("other", 2, 4, config="o"),
        ]
    )
    found = trials.roots(rows)
    assert found["leaf"]["trial_id"] == "root"
    assert found["mid"]["trial_id"] == "root"
    assert found["root"]["trial_id"] == "root"
    assert found["other"]["trial_id"] == "other"


def test_roots_read_unlinked_rows_by_their_interval():
    rows = [row("outer", 0, 10), row("inner", 2, 4, config="i"), row("alone", 11, 12, config="z")]
    found = trials.roots(rows)
    assert found["inner"]["trial_id"] == "outer"
    assert found["alone"]["trial_id"] == "alone"


def test_a_parent_cycle_in_an_unverified_ledger_does_not_spin():
    a = row("a", 0, 5, parent="b", previous="x")
    b = row("b", 1, 4, parent="a", previous="y")
    assert set(trials.roots([a, b])) == {"a", "b"}


# -- the ledger on disk ---------------------------------------------------------------------


def test_a_missing_file_is_an_empty_ledger(ledger):
    assert ledger.rows() == []
    assert ledger.count() == 0
    assert ledger.declared() == 0
    ledger.verify()
    assert not ledger.path.exists()


def test_a_run_leaves_one_row_with_its_configuration_and_outcome(ledger):
    with run(ledger, {"depth": 3, "lr": 0.1}) as trial:
        trial.refit()
        trial.refit()
        trial.results = {"auc": 0.61}
    (stored,) = ledger.rows()
    assert stored["trial_id"] == trial.trial_id and len(trial.trial_id) == 16
    assert (stored["kind"], stored["status"], stored["refits"]) == ("fit", "succeeded", 2)
    assert stored["config"] == '{"depth":3,"lr":0.1}'
    assert stored["features"] == '["f1","f2"]'
    assert stored["results"] == '{"auc":0.61}'
    assert (stored["train_start"], stored["train_end"]) == (date(2020, 1, 1), date(2025, 12, 31))
    assert (stored["partition_hash"], stored["cost_model_version"]) == ("digest", "c1")
    assert stored["previous"] == GENESIS and stored["parent"] is None and stored["error"] is None
    assert stored["started_at"] <= stored["finished_at"]
    assert ledger.get(trial.trial_id) == stored


def test_the_row_is_on_disk_as_running_before_the_body_ends(ledger):
    with run(ledger) as trial:
        (during,) = ledger.rows()
        assert during["trial_id"] == trial.trial_id
        assert during["status"] == "running" and during["finished_at"] is None
    assert ledger.get(trial.trial_id)["status"] == "succeeded"


def test_a_run_with_no_results_stores_none(ledger):
    with run(ledger) as trial:
        pass
    assert ledger.get(trial.trial_id)["results"] is None


def test_a_run_that_raises_is_marked_failed_with_its_error_and_the_error_propagates(ledger):
    with pytest.raises(ValueError, match="bad fold"):
        with run(ledger) as trial:
            trial.refit()
            raise ValueError("bad fold")
    stored = ledger.get(trial.trial_id)
    assert stored["status"] == "failed"
    assert stored["error"] == "ValueError: bad fold"
    assert stored["refits"] == 1 and stored["finished_at"] is not None


def test_an_interrupted_run_is_abandoned(ledger):
    with pytest.raises(KeyboardInterrupt):
        with run(ledger) as trial:
            raise KeyboardInterrupt
    stored = ledger.get(trial.trial_id)
    assert (stored["status"], stored["error"]) == ("abandoned", "interrupted")
    assert stored["status"] in trials.STATUSES


def test_every_status_a_run_can_end_with_is_a_declared_status():
    assert set(trials.STATUSES) == {"running", "succeeded", "failed", "abandoned"}


def test_each_row_links_to_the_one_before_and_the_chain_verifies(ledger):
    for depth in (1, 2, 3):
        with run(ledger, {"depth": depth}):
            pass
    rows = ledger.rows()
    assert rows[0]["previous"] == GENESIS
    assert rows[1]["previous"] == trials.link(rows[0])
    assert rows[2]["previous"] == trials.link(rows[1])
    ledger.verify()
    assert ledger.count() == 3


def test_a_trial_opened_inside_another_on_the_same_ledger_names_it_as_parent(ledger):
    with run(ledger, {"kind": "certify"}) as outer:
        with run(ledger, {"kind": "cv"}) as inner:
            assert trials._OPEN.get()[-1][1] == inner.trial_id
        with run(ledger, {"kind": "backtest"}) as second:
            pass
    rows = {r["trial_id"]: r for r in ledger.rows()}
    assert rows[outer.trial_id]["parent"] is None
    assert rows[inner.trial_id]["parent"] == outer.trial_id
    assert rows[second.trial_id]["parent"] == outer.trial_id
    ledger.verify()
    assert ledger.declared() == 1
    assert ledger.count() == 3


def test_a_trial_on_another_ledger_is_nobodys_child(tmp_path):
    one, two = Ledger(tmp_path / "one.parquet"), Ledger(tmp_path / "two.parquet")
    with run(one):
        with run(two) as other:
            pass
    assert two.get(other.trial_id)["parent"] is None


def test_the_open_trials_are_closed_again_however_the_body_ends(ledger):
    assert trials._OPEN.get() == ()
    with pytest.raises(RuntimeError):
        with run(ledger):
            raise RuntimeError("x")
    assert trials._OPEN.get() == ()
    with run(ledger):
        assert len(trials._OPEN.get()) == 1
    assert trials._OPEN.get() == ()


def test_the_same_configuration_tried_twice_gets_two_rows_and_counts_once(ledger):
    with run(ledger, {"depth": 3}) as first:
        pass
    with run(ledger, {"depth": 3}) as second:
        pass
    assert first.trial_id != second.trial_id
    assert ledger.count() == 2
    assert ledger.declared() == 1
    assert ledger.declared(rule="rows") == 2
    assert ledger.declared(rule="configurations") == 1


def test_a_different_code_version_is_a_different_configuration(ledger, monkeypatch):
    monkeypatch.setenv("AGB_VERSION", "v1")
    with run(ledger):
        pass
    monkeypatch.setenv("AGB_VERSION", "v2")
    with run(ledger):
        pass
    assert {r["code_version"] for r in ledger.rows()} == {"v1", "v2"}
    assert ledger.declared() == 2


def test_getting_a_trial_the_ledger_does_not_have_raises(ledger):
    with run(ledger):
        pass
    with pytest.raises(LedgerError, match="no trial 'missing'"):
        ledger.get("missing")


def test_verify_names_a_row_taken_out_of_the_middle(ledger):
    for depth in (1, 2, 3):
        with run(ledger, {"depth": depth}):
            pass
    kept = ledger.rows()
    ledger._write([kept[0], kept[2]])
    with pytest.raises(LedgerError, match="the chain through .* is broken: row 1"):
        ledger.verify()


def test_verify_accepts_a_row_taken_off_the_end(ledger):
    for depth in (1, 2):
        with run(ledger, {"depth": depth}):
            pass
    ledger._write(ledger.rows()[:1])
    ledger.verify()


def test_the_digest_follows_the_rows(ledger):
    empty = ledger.digest()
    with run(ledger):
        pass
    one = ledger.digest()
    assert one != empty and len(one) == 64
    assert ledger.digest() == one
    with run(ledger, {"depth": 9}):
        pass
    assert ledger.digest() != one


def test_a_write_leaves_no_temporary_file_and_keeps_the_mode(ledger):
    with run(ledger):
        pass
    ledger.path.chmod(0o640)
    with run(ledger, {"depth": 2}):
        pass
    assert ledger.path.stat().st_mode & 0o777 == 0o640
    names = {p.name for p in ledger.path.parent.iterdir()}
    assert names == {trials.FILE, trials.FILE + ".lock"}


def test_a_failed_write_cleans_up_its_temporary_file(ledger, monkeypatch):
    with run(ledger):
        pass

    def refuse(table, handle):
        raise OSError("disk full")

    monkeypatch.setattr(trials.pq, "write_table", refuse)
    with pytest.raises(OSError, match="disk full"):
        ledger._write(ledger.rows())
    assert {p.name for p in ledger.path.parent.iterdir()} == {
        trials.FILE,
        trials.FILE + ".lock",
    }


def test_two_writers_appending_at_once_both_land_and_the_chain_holds(ledger):
    barrier = threading.Barrier(4)
    errors = []

    def write(n):
        try:
            barrier.wait()
            with run(ledger, {"worker": n}):
                pass
        except BaseException as error:  # noqa: BLE001 - reported below
            errors.append(error)

    threads = [threading.Thread(target=write, args=(n,)) for n in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == []
    assert ledger.count() == 4
    assert {r["status"] for r in ledger.rows()} == {"succeeded"}
    ledger.verify()
    assert ledger.declared() == 4
