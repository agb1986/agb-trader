-- Scheduler work: jobs with leases, their append-only lifecycle, and expected events.
--
-- The scheduler writes a job row for each piece of dated work and an expected_event for
-- everything it expects to happen by a deadline. Pollers claim jobs with
-- SELECT ... FOR UPDATE SKIP LOCKED and a conditional update of the lease columns.

-- One row per piece of scheduled work. (kind, period) is unique, so scheduling the same work
-- twice fails on the constraint instead of producing a duplicate.
--
-- The lease columns are the spec's mutable exception 1: claimed_by and lease_until change as
-- workers claim, renew and release. Every other column is fixed at insert, and the row is
-- never deleted. What happened to the job is told by job_event, not by these columns.
CREATE TABLE agb.job (
    id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    kind          text NOT NULL CHECK (kind ~ '^[a-z][a-z0-9_]*$'),
    period        text NOT NULL CHECK (period <> ''),
    session_date  date NOT NULL,
    not_before    timestamptz NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now(),
    created_by    agb.actor NOT NULL,
    claimed_by    agb.actor,
    lease_until   timestamptz,
    UNIQUE (kind, period),
    CHECK ((claimed_by IS NULL) = (lease_until IS NULL))
);

-- Refuses an update that touches anything but the lease columns.
CREATE FUNCTION agb.job_lease_only() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF to_jsonb(NEW) - 'claimed_by' - 'lease_until'
            IS DISTINCT FROM to_jsonb(OLD) - 'claimed_by' - 'lease_until' THEN
        RAISE EXCEPTION '%.% is append-only outside its lease columns: UPDATE refused',
            TG_TABLE_SCHEMA, TG_TABLE_NAME
            USING HINT = 'Only claimed_by and lease_until may change.';
    END IF;
    RETURN NEW;
END;
$$;
REVOKE EXECUTE ON FUNCTION agb.job_lease_only() FROM PUBLIC;

CREATE TRIGGER lease_only BEFORE UPDATE ON agb.job
    FOR EACH ROW EXECUTE FUNCTION agb.job_lease_only();
CREATE TRIGGER no_delete BEFORE DELETE ON agb.job
    FOR EACH ROW EXECUTE FUNCTION agb.refuse_history_change();
CREATE TRIGGER no_truncate BEFORE TRUNCATE ON agb.job
    FOR EACH STATEMENT EXECUTE FUNCTION agb.refuse_history_change();

CREATE INDEX job_claimable ON agb.job (not_before, id) WHERE claimed_by IS NULL;

-- A job's lifecycle. Lease renewals are not events; everything else is.
CREATE TABLE agb.job_event (
    id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_id       bigint NOT NULL REFERENCES agb.job,
    event        text NOT NULL CHECK (
                     event IN ('created', 'claimed', 'completed', 'failed',
                               'lease_expired', 'reclaimed')),
    actor        agb.actor NOT NULL,
    at           timestamptz NOT NULL DEFAULT now(),
    lease_until  timestamptz,
    detail       text,
    CHECK ((event IN ('claimed', 'reclaimed', 'lease_expired')) = (lease_until IS NOT NULL))
);
SELECT agb.make_append_only('agb.job_event');
CREATE INDEX job_event_job ON agb.job_event (job_id, id);
-- A job is created once and ends once. An expired lease is recorded once, by whichever of the
-- scheduler or a reclaiming worker sees it first.
CREATE UNIQUE INDEX job_event_created_once ON agb.job_event (job_id) WHERE event = 'created';
CREATE UNIQUE INDEX job_event_ends_once ON agb.job_event (job_id)
    WHERE event IN ('completed', 'failed');
CREATE UNIQUE INDEX job_event_expiry_once ON agb.job_event (job_id, lease_until)
    WHERE event = 'lease_expired';

-- Something the system expects to happen by a deadline, written ahead of time by the
-- scheduler. job_id names the job whose completion fulfils it, when there is one.
CREATE TABLE agb.expected_event (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    kind        text NOT NULL CHECK (kind ~ '^[a-z][a-z0-9_]*$'),
    period      text NOT NULL CHECK (period <> ''),
    due_at      timestamptz NOT NULL,
    job_id      bigint REFERENCES agb.job,
    created_at  timestamptz NOT NULL DEFAULT now(),
    created_by  agb.actor NOT NULL,
    UNIQUE (kind, period)
);
SELECT agb.make_append_only('agb.expected_event');
CREATE INDEX expected_event_due ON agb.expected_event (kind, due_at);
CREATE INDEX expected_event_job ON agb.expected_event (job_id) WHERE job_id IS NOT NULL;

-- Completion is appended, never written onto the expected event: a reference to the row that
-- fulfilled it, such as the job_event that completed its job.
CREATE TABLE agb.expected_event_completion (
    id                 bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    expected_event_id  bigint NOT NULL UNIQUE REFERENCES agb.expected_event,
    ref_table          text NOT NULL CHECK (ref_table ~ '^[a-z][a-z0-9_]*$'),
    ref_id             bigint NOT NULL,
    actor              agb.actor NOT NULL,
    completed_at       timestamptz NOT NULL DEFAULT now()
);
SELECT agb.make_append_only('agb.expected_event_completion');

-- Each job with its current state, derived from its events and its lease.
CREATE VIEW agb.job_state AS
SELECT j.id, j.kind, j.period, j.session_date, j.not_before, j.claimed_by, j.lease_until,
       CASE
           WHEN ended.event IS NOT NULL THEN ended.event
           WHEN j.lease_until < now() THEN 'lease_expired'
           WHEN j.claimed_by IS NOT NULL THEN 'claimed'
           ELSE 'pending'
       END AS state,
       j.lease_until < now() AS lease_expired,
       ended.at AS ended_at
FROM agb.job j
LEFT JOIN agb.job_event ended
       ON ended.job_id = j.id AND ended.event IN ('completed', 'failed');

-- How far ahead a rebalance must be scheduled before its absence is flagged. Configure it
-- with ALTER DATABASE ... SET agb.rebalance_horizon = '...'; the default covers the longest
-- gap between two first sessions of a month, about five weeks, with a little to spare.
CREATE FUNCTION agb.rebalance_horizon() RETURNS interval
LANGUAGE sql STABLE AS $$
    SELECT coalesce(nullif(current_setting('agb.rebalance_horizon', true), ''), '40 days')::interval
$$;

-- What the system expected by `at` and did not get:
--   past_due                     an expected event whose deadline has passed with no completion;
--   no_rebalance_within_horizon  no rebalance submission is expected in (at, at + horizon],
--                                which is how a dead scheduler shows: it stops writing rows.
CREATE FUNCTION agb.overdue_at(at timestamptz)
RETURNS TABLE (expected_event_id bigint, kind text, period text, due_at timestamptz, reason text)
LANGUAGE sql STABLE AS $$
    SELECT e.id, e.kind, e.period, e.due_at, 'past_due'
    FROM agb.expected_event e
    WHERE e.due_at < at
      AND NOT EXISTS (
          SELECT 1 FROM agb.expected_event_completion c WHERE c.expected_event_id = e.id)
    UNION ALL
    SELECT NULL, 'submission', NULL, at + agb.rebalance_horizon(), 'no_rebalance_within_horizon'
    WHERE NOT EXISTS (
        SELECT 1 FROM agb.expected_event e
        WHERE e.kind = 'submission'
          AND e.due_at > at
          AND e.due_at <= at + agb.rebalance_horizon())
    ORDER BY 4, 1
$$;

CREATE VIEW agb.overdue AS SELECT * FROM agb.overdue_at(now());

GRANT SELECT ON agb.job, agb.job_event, agb.expected_event, agb.expected_event_completion,
    agb.job_state, agb.overdue TO scheduler, pipeline, execution, bot, ui, cli;
GRANT INSERT ON agb.job, agb.expected_event TO scheduler;
-- Claiming locks the row FOR UPDATE, which needs UPDATE on at least one column.
GRANT UPDATE (claimed_by, lease_until) ON agb.job TO pipeline, execution;
GRANT INSERT ON agb.job_event TO scheduler, pipeline, execution;
GRANT INSERT ON agb.expected_event_completion TO scheduler, pipeline, execution, bot, ui;
