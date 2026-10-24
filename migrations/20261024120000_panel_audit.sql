-- The nightly panel-store integrity check (docs/architecture/07, "Postgres durability").
--
-- The panel store holds the bodies the database only refers to by hash: filing documents and
-- texts, model requests and responses, scrubbed extracts. Postgres is backed up nightly and
-- its backup is verified by restoring it, but the bodies live on the data disk beside it, and
-- a disk that quietly loses or rewrites a file would not show up anywhere until a human went
-- looking for the evidence behind a score. So each night the pipeline reads a sample of the
-- referenced bodies back through the store, which checks each one against the hash it is
-- named by, and records what it found here. The bot raises an alert on any body that is
-- missing or no longer matches.
--
-- A sample, not the whole store: the check runs on the deploy machine's one slow disk beside
-- everything else, and a few hundred bodies a night covers the store many times over in the
-- time between rebalances while costing minutes rather than hours.

-- One night's check. `bodies` is how many distinct hashes the database referred to when the
-- sample was drawn, `checked` how many of them were read back; `missing` has no file at all,
-- `corrupt` has one whose content no longer matches its hash. Both are kept as hashes rather
-- than counted, because the point of the alert is to say which body to go and look for.
CREATE TABLE agb.panel_audit (
    id       bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    -- The calendar day checked, as the nightly job names its period.
    period   text NOT NULL UNIQUE CHECK (period ~ '^\d{4}-\d{2}-\d{2}$'),
    job_id   bigint NOT NULL REFERENCES agb.job,
    bodies   integer NOT NULL CHECK (bodies >= 0),
    checked  integer NOT NULL CHECK (checked >= 0 AND checked <= bodies),
    sound    integer NOT NULL CHECK (sound >= 0),
    missing  text[] NOT NULL DEFAULT '{}',
    corrupt  text[] NOT NULL DEFAULT '{}',
    at       timestamptz NOT NULL DEFAULT now(),
    actor    agb.actor NOT NULL,
    -- Every body read back is sound, missing or corrupt, and nothing is counted twice.
    CHECK (checked = sound + cardinality(missing) + cardinality(corrupt))
);
SELECT agb.make_append_only('agb.panel_audit');

GRANT SELECT ON agb.panel_audit TO scheduler, pipeline, execution, bot, ui, cli;
-- The pipeline runs the check, as it is the only service that mounts the panel store.
GRANT INSERT ON agb.panel_audit TO pipeline;
