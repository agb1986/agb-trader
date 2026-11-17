-- How the operator asks for a panel audit without a superuser or a write grant on the queue.
--
-- scripts/deploy/panel-check.sh drove the drill by inserting into agb.job and agb.job_event as
-- the `postgres` superuser, because no operator role could. Granting `cli` INSERT on those two
-- tables would let it enqueue any kind of job and write any lifecycle event, including a
-- `completed` for work that never ran. This function does only the one thing the drill needs:
-- it enqueues one `panel_audit` for a period shaped as agb.panel_audit's own check admits (a day,
-- then an optional lowercase suffix), created by `operator`, and records that it was created.
--
-- It runs as its owner, which owns the tables, so the triggers and constraints on them apply
-- as they do to the scheduler's own inserts. A period that is taken fails on UNIQUE (kind,
-- period), as it does for the scheduler.

CREATE FUNCTION agb.request_panel_audit(requested text) RETURNS bigint
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE
    new_job  bigint;
BEGIN
    IF requested IS NULL OR requested !~ '^\d{4}-\d{2}-\d{2}(-[a-z0-9][a-z0-9-]*)?$' THEN
        RAISE EXCEPTION 'request_panel_audit: % is not a day with an optional suffix', requested
            USING ERRCODE = 'check_violation';
    END IF;
    INSERT INTO agb.job (kind, period, session_date, not_before, created_by)
    VALUES ('panel_audit', requested, left(requested, 10)::date, now(), 'operator')
    RETURNING id INTO new_job;
    INSERT INTO agb.job_event (job_id, event, actor) VALUES (new_job, 'created', 'operator');
    RETURN new_job;
END;
$$;
REVOKE EXECUTE ON FUNCTION agb.request_panel_audit(text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION agb.request_panel_audit(text) TO cli;
