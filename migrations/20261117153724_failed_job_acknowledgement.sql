-- A failed job leaves agb.overdue once someone acknowledges it, or once it is older than
-- agb.failed_overdue_age(). Before this, a failed job's expected events stayed overdue for
-- good: nothing ever fulfils them, and a job is never failed twice.

-- One acknowledgement per failed job: who looked at the failure, and what they decided.
CREATE TABLE agb.job_acknowledgement (
    id      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_id  bigint NOT NULL UNIQUE REFERENCES agb.job,
    actor   agb.actor NOT NULL,
    reason  text NOT NULL CHECK (reason <> ''),
    at      timestamptz NOT NULL DEFAULT now()
);
SELECT agb.make_append_only('agb.job_acknowledgement');

-- Only a failed job is acknowledged: one still running, or completed, has nothing to clear.
CREATE FUNCTION agb.acknowledge_failed_only() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM agb.job_event WHERE job_id = NEW.job_id AND event = 'failed'
    ) THEN
        RAISE EXCEPTION 'job % has not failed: only a failed job is acknowledged', NEW.job_id
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER acknowledge_failed_only BEFORE INSERT ON agb.job_acknowledgement
    FOR EACH ROW EXECUTE FUNCTION agb.acknowledge_failed_only();

-- How long a failed job's expected events stay overdue with no acknowledgement. Configure it
-- with ALTER DATABASE ... SET agb.failed_overdue_age = '...'.
CREATE FUNCTION agb.failed_overdue_age() RETURNS interval
LANGUAGE sql STABLE AS $$
    SELECT coalesce(nullif(current_setting('agb.failed_overdue_age', true), ''), '14 days')::interval
$$;

-- As before, except that a past-due event whose job failed is left out once the failure was
-- acknowledged by `at`, or happened longer than agb.failed_overdue_age() before it. Inside
-- the subqueries `at` is written overdue_at.at, since a bare `at` names their own column.
CREATE OR REPLACE FUNCTION agb.overdue_at(at timestamptz)
RETURNS TABLE (expected_event_id bigint, kind text, period text, due_at timestamptz, reason text)
LANGUAGE sql STABLE AS $$
    SELECT e.id, e.kind, e.period, e.due_at, 'past_due'
    FROM agb.expected_event e
    WHERE e.due_at < at
      AND NOT EXISTS (
          SELECT 1 FROM agb.expected_event_completion c WHERE c.expected_event_id = e.id)
      AND NOT EXISTS (
          SELECT 1 FROM agb.job_event f
          WHERE f.job_id = e.job_id
            AND f.event = 'failed'
            AND (f.at < overdue_at.at - agb.failed_overdue_age()
                 OR EXISTS (
                     SELECT 1 FROM agb.job_acknowledgement a
                     WHERE a.job_id = f.job_id AND a.at <= overdue_at.at)))
    UNION ALL
    SELECT NULL, 'submission', NULL, at + agb.rebalance_horizon(), 'no_rebalance_within_horizon'
    WHERE NOT EXISTS (
        SELECT 1 FROM agb.expected_event e
        WHERE e.kind = 'submission'
          AND e.due_at > at
          AND e.due_at <= at + agb.rebalance_horizon())
    ORDER BY 4, 1
$$;

-- Every role that reads agb.overdue now reads these through agb.overdue_at, which runs as its
-- caller. The operator acknowledges through the CLI (`agb acknowledge`).
GRANT SELECT ON agb.job_acknowledgement
    TO scheduler, pipeline, execution, bot, ui, backup, cli, research;
GRANT SELECT ON agb.job_event TO research;
GRANT INSERT ON agb.job_acknowledgement TO cli;
