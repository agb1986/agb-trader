"""Job leases: one winner per job, skip-don't-block, visible expiry, and a logged lifecycle."""

import threading
import time
from datetime import timedelta

import pytest
from psycopg import errors

from agb import jobs
from agb.roles import OWNER

LEASE = timedelta(minutes=5)


def add_job(db, kind="rebalance", period="2026-10", due="now() - interval '1 minute'"):
    scheduler = db.connect("scheduler")
    (job_id,) = scheduler.execute(
        "INSERT INTO agb.job (kind, period, session_date, not_before, created_by) "
        f"VALUES (%s, %s, current_date, {due}, 'scheduler') RETURNING id",
        (kind, period),
    ).fetchone()
    scheduler.execute(
        "INSERT INTO agb.job_event (job_id, event, actor) VALUES (%s, 'created', 'scheduler')",
        (job_id,),
    )
    return job_id


def events(db, job_id):
    return (
        db.connect("cli")
        .execute("SELECT event, actor FROM agb.job_event WHERE job_id = %s ORDER BY id", (job_id,))
        .fetchall()
    )


def state(db, job_id):
    return (
        db.connect("cli")
        .execute(
            "SELECT state, claimed_by, lease_expired FROM agb.job_state WHERE id = %s", (job_id,)
        )
        .fetchone()
    )


def test_claim_complete_lifecycle(db):
    job_id = add_job(db)
    pipeline = db.connect("pipeline")

    claim = jobs.claim(pipeline, ["rebalance"], "pipeline", LEASE)
    assert (claim.job_id, claim.kind, claim.period, claim.reclaimed) == (
        job_id,
        "rebalance",
        "2026-10",
        False,
    )
    assert state(db, job_id) == ("claimed", "pipeline", False)

    renewed = jobs.renew(pipeline, claim, timedelta(hours=1))
    assert renewed.lease_until > claim.lease_until

    jobs.complete(pipeline, renewed)
    assert state(db, job_id) == ("completed", None, None)
    assert jobs.claim(pipeline, ["rebalance"], "pipeline", LEASE) is None
    assert events(db, job_id) == [
        ("created", "scheduler"),
        ("claimed", "pipeline"),
        ("completed", "pipeline"),
    ]


def test_a_failed_job_is_not_claimed_again(db):
    job_id = add_job(db)
    pipeline = db.connect("pipeline")
    jobs.fail(pipeline, jobs.claim(pipeline, ["rebalance"], "pipeline", LEASE), "vendor 503")

    assert state(db, job_id)[0] == "failed"
    assert jobs.claim(pipeline, ["rebalance"], "pipeline", LEASE) is None
    assert db.connect("cli").execute(
        "SELECT detail FROM agb.job_event WHERE event = 'failed'"
    ).fetchone() == ("vendor 503",)


def test_only_due_jobs_of_the_asked_kinds_are_claimed(db):
    add_job(db, "rebalance", "2026-11", due="now() + interval '1 day'")
    add_job(db, "data_pull", "2026-09-30")
    pipeline = db.connect("pipeline")
    assert jobs.claim(pipeline, ["rebalance"], "pipeline", LEASE) is None
    assert jobs.claim(pipeline, ["rebalance", "data_pull"], "pipeline", LEASE).kind == "data_pull"


def test_two_workers_racing_for_one_job_exactly_one_wins(db):
    workers = [db.connect("pipeline"), db.connect("pipeline")]
    for round_ in range(20):
        job_id = add_job(db, period=f"race-{round_}")
        barrier = threading.Barrier(len(workers))
        results = [None] * len(workers)

        def race(i, barrier=barrier, results=results):
            barrier.wait()
            results[i] = jobs.claim(workers[i], ["rebalance"], f"worker-{i}", LEASE)

        threads = [threading.Thread(target=race, args=(i,)) for i in range(len(workers))]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        winners = [r for r in results if r is not None]
        assert len(winners) == 1, results
        assert winners[0].job_id == job_id
        assert state(db, job_id)[1] == winners[0].worker
        assert [e for e, _ in events(db, job_id)] == ["created", "claimed"]
        jobs.complete(workers[results.index(winners[0])], winners[0])


def test_a_locked_job_is_skipped_not_waited_for(db):
    first = add_job(db, period="2026-10")
    second = add_job(db, period="2026-11")
    holder, other = db.connect("pipeline"), db.connect("pipeline")
    other.execute("SET lock_timeout = '2s'")

    with holder.transaction():
        held = jobs.claim(holder, ["rebalance"], "holder", LEASE)
        assert held.job_id == first
        # The holder's transaction is still open, so its row lock on the first job is live.
        started = time.monotonic()
        taken = jobs.claim(other, ["rebalance"], "other", LEASE)
        assert time.monotonic() - started < 1
        assert taken.job_id == second
        assert jobs.claim(other, ["rebalance"], "other", LEASE) is None


def expire(db, job_id):
    """Wait out a zero-length lease."""
    time.sleep(0.05)
    assert state(db, job_id)[2] is True


def test_an_expired_lease_is_visible_and_not_reclaimed_by_default(db):
    job_id = add_job(db, kind="submission")
    execution = db.connect("execution")
    claim = jobs.claim(execution, ["submission"], "execution", timedelta(0))
    expire(db, job_id)

    assert state(db, job_id) == ("lease_expired", "execution", True)
    assert jobs.claim(db.connect("execution"), ["submission"], "execution-2", LEASE) is None
    with pytest.raises(jobs.LeaseLost):
        jobs.renew(execution, claim, LEASE)
    # The expiry is recorded once, by the scheduler, and nothing else changes.
    scheduler = db.connect("scheduler")
    assert jobs.record_expiries(scheduler, "scheduler") == 1
    assert jobs.record_expiries(scheduler, "scheduler") == 0
    assert events(db, job_id)[-1] == ("lease_expired", "scheduler")
    assert state(db, job_id) == ("lease_expired", "execution", True)


def test_a_pipeline_stage_with_an_expired_lease_is_reclaimed(db):
    job_id = add_job(db, kind="features")
    stale, fresh = db.connect("pipeline"), db.connect("pipeline")
    lost = jobs.claim(stale, ["features"], "pipeline-a", timedelta(0))
    expire(db, job_id)

    claim = jobs.claim(fresh, ["features"], "pipeline-b", LEASE, reclaim_expired=True)
    assert claim.reclaimed and claim.job_id == job_id
    assert state(db, job_id) == ("claimed", "pipeline-b", False)

    # The worker that lost the lease can neither renew nor finish.
    with pytest.raises(jobs.LeaseLost):
        jobs.renew(stale, lost, LEASE)
    with pytest.raises(jobs.LeaseLost):
        jobs.complete(stale, lost)
    jobs.complete(fresh, claim)

    assert events(db, job_id) == [
        ("created", "scheduler"),
        ("claimed", "pipeline-a"),
        ("lease_expired", "pipeline-b"),
        ("reclaimed", "pipeline-b"),
        ("completed", "pipeline-b"),
    ]


def test_an_expiry_already_recorded_is_not_recorded_again_on_reclaim(db):
    job_id = add_job(db, kind="features")
    jobs.claim(db.connect("pipeline"), ["features"], "pipeline-a", timedelta(0))
    expire(db, job_id)
    jobs.record_expiries(db.connect("scheduler"), "scheduler")
    jobs.claim(db.connect("pipeline"), ["features"], "pipeline-b", LEASE, reclaim_expired=True)
    assert [e for e in events(db, job_id) if e[0] == "lease_expired"] == [
        ("lease_expired", "scheduler")
    ]


def test_every_job_event_has_an_actor(db):
    job_id = add_job(db)
    for actor in (None, ""):
        with pytest.raises((errors.NotNullViolation, errors.CheckViolation)):
            db.connect("pipeline").execute(
                "INSERT INTO agb.job_event (job_id, event, actor) VALUES (%s, 'failed', %s)",
                (job_id, actor),
            )


def test_a_job_ends_once(db):
    job_id = add_job(db)
    pipeline = db.connect("pipeline")
    pipeline.execute(
        "INSERT INTO agb.job_event (job_id, event, actor) VALUES (%s, 'completed', 'pipeline')",
        (job_id,),
    )
    with pytest.raises(errors.UniqueViolation):
        pipeline.execute(
            "INSERT INTO agb.job_event (job_id, event, actor) VALUES (%s, 'failed', 'pipeline')",
            (job_id,),
        )


def test_workers_may_change_only_the_lease_columns(db):
    job_id = add_job(db)
    with pytest.raises(errors.InsufficientPrivilege):
        db.connect("pipeline").execute(
            "UPDATE agb.job SET not_before = now() WHERE id = %s", (job_id,)
        )


@pytest.mark.parametrize("role", ["scheduler", "bot", "ui", "cli"])
def test_other_roles_cannot_take_leases(db, role):
    job_id = add_job(db)
    with pytest.raises(errors.InsufficientPrivilege):
        db.connect(role).execute(
            "UPDATE agb.job SET claimed_by = 'x', lease_until = now() WHERE id = %s", (job_id,)
        )


def test_the_owner_cannot_rewrite_a_job_outside_its_lease(db):
    job_id = add_job(db)
    owner = db.connect(OWNER)
    with pytest.raises(errors.RaiseException, match="outside its lease columns"):
        owner.execute("UPDATE agb.job SET period = '2026-11' WHERE id = %s", (job_id,))
    owner.execute(
        "UPDATE agb.job SET claimed_by = 'owner', lease_until = now() WHERE id = %s", (job_id,)
    )


@pytest.mark.parametrize("statement", ["DELETE FROM agb.job", "TRUNCATE agb.job CASCADE"])
def test_the_owner_cannot_remove_jobs(db, statement):
    add_job(db)
    with pytest.raises(errors.RaiseException, match="append-only"):
        db.connect(OWNER).execute(statement)


def test_a_lease_must_name_its_holder(db):
    job_id = add_job(db)
    with pytest.raises(errors.CheckViolation):
        db.connect("pipeline").execute(
            "UPDATE agb.job SET lease_until = now() WHERE id = %s", (job_id,)
        )
