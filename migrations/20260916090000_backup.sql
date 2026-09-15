-- The backup service: it takes the scheduler's nightly backup job and runs pg_dump.
--
-- It reads every table through pg_read_all_data, which migrate.bootstrap grants because only a
-- superuser can. The grants here are the ones every job poller holds, plus its own heartbeat.
-- It connects as NOBYPASSRLS and dumps with --enable-row-security, so every table with
-- row-level security must let it read every row (see README.md in this directory).

GRANT USAGE ON SCHEMA agb TO backup;

GRANT SELECT, INSERT, UPDATE ON agb.heartbeat TO backup;

GRANT SELECT ON agb.job, agb.job_event, agb.expected_event, agb.expected_event_completion,
    agb.job_state, agb.overdue TO backup;
-- Claiming locks the row FOR UPDATE, which needs UPDATE on at least one column.
GRANT UPDATE (claimed_by, lease_until) ON agb.job TO backup;
GRANT INSERT ON agb.job_event, agb.expected_event_completion TO backup;
