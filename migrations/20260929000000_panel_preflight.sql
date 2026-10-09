-- The panel pre-flight suite: whether the panel can be trusted under one `panel_config`,
-- recorded check by check (docs/architecture/03, "Pre-flight validation"). "The panel
-- pre-flight has passed" is one of the paper-to-live criteria (docs/architecture/04), and the
-- eligibility check reads agb.panel_preflight_ready.
--
-- The four checks are retrieval fidelity (every Analyst quote found verbatim in the document
-- it cites), known-disclosure recall (a committed fixture set of names with known impairment
-- disclosures, each caught in its rubric dimension), the operator's spot check over about 20
-- names, and the dispersion gate (scores spread far enough apart to rank for a reason).
--
-- Results belong to the configuration they were measured under: a new `panel_config` has none
-- and reads as not yet pre-flighted, so changing the models, prompts, rubric or scrubber means
-- running the suite again. Nothing here is ever updated; a check re-run appends a row and the
-- views read the newest.

-- A spot check names the score it reviewed, and this key makes that score agree with the
-- retrieval it belongs to.
ALTER TABLE agb.panel_score ADD UNIQUE (id, retrieval_id);

-- One check's result. `measured` and `threshold` are text, as the pre-trade gate's are, so a
-- count, a ratio and a standard deviation read the same way; `detail` carries what the check
-- found, such as the quotes rejected or the disclosures missed by dimension. `run_id` is the
-- panel run measured, and the composite key makes that run belong to this configuration;
-- recall is the one check with no single run, because the fixture set is read as of the date
-- each disclosure was on file, so it names its runs in `detail`.
CREATE TABLE agb.panel_preflight (
    id               bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    panel_config_id  bigint NOT NULL REFERENCES agb.panel_config,
    name             text NOT NULL CHECK (name IN ('retrieval_fidelity', 'disclosure_recall',
                                                   'spot_check', 'dispersion')),
    run_id           bigint,
    passed           boolean NOT NULL,
    measured         text NOT NULL CHECK (measured <> ''),
    threshold        text NOT NULL CHECK (threshold <> ''),
    detail           jsonb NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(detail) = 'object'),
    actor            agb.actor NOT NULL,
    at               timestamptz NOT NULL DEFAULT now(),
    FOREIGN KEY (run_id, panel_config_id) REFERENCES agb.panel_run (id, panel_config_id),
    CHECK (name = 'disclosure_recall' OR run_id IS NOT NULL)
);
SELECT agb.make_append_only('agb.panel_preflight');
CREATE INDEX panel_preflight_config ON agb.panel_preflight (panel_config_id, name, id DESC);

-- One name the operator reviewed, with the verdict: 'agree' when the panel's reading of the
-- name holds up, 'disagree' when it does not, which says why. One verdict per name per
-- operator, and a second operator may review the same name.
CREATE TABLE agb.panel_spot_check (
    id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id        bigint NOT NULL,
    retrieval_id  bigint NOT NULL,
    score_id      bigint NOT NULL,
    verdict       text NOT NULL CHECK (verdict IN ('agree', 'disagree')),
    note          text CHECK (btrim(note) <> ''),
    actor         agb.actor NOT NULL,
    at            timestamptz NOT NULL DEFAULT now(),
    UNIQUE (retrieval_id, actor),
    FOREIGN KEY (retrieval_id, run_id) REFERENCES agb.panel_retrieval (id, run_id),
    FOREIGN KEY (score_id, retrieval_id) REFERENCES agb.panel_score (id, retrieval_id),
    CHECK (verdict = 'agree' OR note IS NOT NULL)
);
SELECT agb.make_append_only('agb.panel_spot_check');

-- Every configuration against all four checks, newest result each: `passed` is null for a
-- check never run, so a configuration that has never been pre-flighted reads as four null
-- rows.
CREATE VIEW agb.panel_preflight_status AS
SELECT c.id AS panel_config_id,
       n.seq,
       n.name,
       p.id AS preflight_id,
       p.run_id,
       p.passed,
       p.measured,
       p.threshold,
       p.detail,
       p.actor,
       p.at
FROM agb.panel_config c
CROSS JOIN (VALUES (1, 'retrieval_fidelity'),
                   (2, 'disclosure_recall'),
                   (3, 'spot_check'),
                   (4, 'dispersion')) AS n(seq, name)
LEFT JOIN LATERAL (
    SELECT p.*
    FROM agb.panel_preflight p
    WHERE p.panel_config_id = c.id AND p.name = n.name
    ORDER BY p.id DESC
    LIMIT 1
) p ON true;

-- One row per configuration: whether the whole suite passed, and which checks are
-- outstanding, meaning failed or never run.
CREATE VIEW agb.panel_preflight_ready AS
SELECT panel_config_id,
       bool_and(passed IS TRUE) AS passed,
       count(*) FILTER (WHERE passed IS TRUE) AS checks_passed,
       array_agg(name ORDER BY seq) FILTER (WHERE passed IS NOT TRUE) AS outstanding,
       max(at) AS at
FROM agb.panel_preflight_status
GROUP BY panel_config_id;

GRANT SELECT ON agb.panel_preflight, agb.panel_spot_check, agb.panel_preflight_status,
    agb.panel_preflight_ready
    TO scheduler, pipeline, execution, bot, ui, cli;
-- The operator records every check from the CLI; recall is the one that drives the panel
-- itself, so it runs as the pipeline role, which may insert the runs it measures.
GRANT INSERT ON agb.panel_preflight TO cli, pipeline;
GRANT INSERT ON agb.panel_spot_check TO cli;
