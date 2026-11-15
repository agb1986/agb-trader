-- An audit the operator drives, which the scheduler's date-shaped periods cannot name.
--
-- The scheduler plans a `panel_audit` job for every night 45 days ahead, so every date is
-- already taken in agb.job (UNIQUE (kind, period)). The drill (scripts/deploy/panel-check.sh)
-- therefore inserts a job whose period is a date plus a suffix, e.g. 2026-11-15-drill-143012,
-- which the scheduler never plans. The check below admitted only a bare date, so the audit ran
-- and its row was refused. It admits the suffixed form too; a bare date still means the
-- nightly job for that day, and the suffix never contains anything but lowercase letters,
-- digits and hyphens.
--
-- Loosening a check keeps every existing row valid and every older image working.
ALTER TABLE agb.panel_audit DROP CONSTRAINT panel_audit_period_check;
ALTER TABLE agb.panel_audit ADD CONSTRAINT panel_audit_period_check
    CHECK (period ~ '^\d{4}-\d{2}-\d{2}(-[a-z0-9][a-z0-9-]*)?$');
