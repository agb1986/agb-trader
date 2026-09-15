-- Foundation: the agb schema, the append-only machinery, the migration ledger and heartbeats.
--
-- Conventions every later migration follows (see README.md in this directory):
--   money and share quantities are numeric; timestamps are timestamptz in UTC; trading dates
--   are date on the NYSE calendar; every history row carries an agb.actor; nothing is deleted.

CREATE SCHEMA agb;
GRANT USAGE ON SCHEMA agb TO scheduler, pipeline, execution, bot, ui, cli;

-- Who caused a row: a service, the operator's identity, or 'system'.
CREATE DOMAIN agb.actor AS text CHECK (VALUE <> '');

-- Refuses any change to a history table. It fires for every role, the owner included, so an
-- accidental fix-up in psql fails as well as a bug in a service.
CREATE FUNCTION agb.refuse_history_change() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '%.% is append-only: % refused', TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_OP
        USING HINT = 'Correct history by appending a correcting event, never by editing.';
END;
$$;

-- Makes a table append-only. Row triggers do not fire on TRUNCATE, hence the statement trigger.
CREATE FUNCTION agb.make_append_only(t regclass) RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    EXECUTE format(
        'CREATE TRIGGER append_only_row BEFORE UPDATE OR DELETE ON %s '
        'FOR EACH ROW EXECUTE FUNCTION agb.refuse_history_change()', t);
    EXECUTE format(
        'CREATE TRIGGER append_only_truncate BEFORE TRUNCATE ON %s '
        'FOR EACH STATEMENT EXECUTE FUNCTION agb.refuse_history_change()', t);
END;
$$;

REVOKE EXECUTE ON FUNCTION agb.refuse_history_change() FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION agb.make_append_only(regclass) FROM PUBLIC;

-- The migration ledger, written by agb.migrate in the same transaction as each migration.
CREATE TABLE agb.schema_migration (
    version     text PRIMARY KEY,
    name        text NOT NULL,
    sha256      text NOT NULL,
    applied_at  timestamptz NOT NULL DEFAULT now(),
    applied_by  agb.actor NOT NULL DEFAULT CURRENT_USER
);
SELECT agb.make_append_only('agb.schema_migration');

-- Liveness: one row per service, upserted every cycle. This is one of the spec's two mutable
-- exceptions, so it takes UPDATE but still refuses DELETE and TRUNCATE. Row-level security
-- keys each row to the role that writes it, so a service can only ever report itself.
CREATE TABLE agb.heartbeat (
    service     text PRIMARY KEY,
    started_at  timestamptz NOT NULL,
    beat_at     timestamptz NOT NULL DEFAULT now(),
    version     text
);
CREATE TRIGGER no_delete BEFORE DELETE ON agb.heartbeat
    FOR EACH ROW EXECUTE FUNCTION agb.refuse_history_change();
CREATE TRIGGER no_truncate BEFORE TRUNCATE ON agb.heartbeat
    FOR EACH STATEMENT EXECUTE FUNCTION agb.refuse_history_change();

ALTER TABLE agb.heartbeat ENABLE ROW LEVEL SECURITY;
CREATE POLICY heartbeat_read ON agb.heartbeat FOR SELECT USING (true);
CREATE POLICY heartbeat_insert_own ON agb.heartbeat FOR INSERT
    WITH CHECK (service = CURRENT_USER);
CREATE POLICY heartbeat_update_own ON agb.heartbeat FOR UPDATE
    USING (service = CURRENT_USER) WITH CHECK (service = CURRENT_USER);

GRANT SELECT ON agb.heartbeat TO scheduler, pipeline, execution, bot, ui, cli;
GRANT INSERT, UPDATE ON agb.heartbeat TO scheduler, pipeline, execution, bot, ui;
