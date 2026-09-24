-- The LLM panel's judging half: the rubric score for each name the Analyst extracted, after
-- the Prosecutor and Defender have argued over the scrubbed extract and the Judge has scored
-- it (docs/architecture/03). The debate's model calls are logged in agb.panel_model_call like
-- the Analyst's, and their bodies are in the panel store. Nothing here is ever updated.

-- A score names the run and the panel_config it was built with, and the keys below make both
-- agree with the retrieval it scores.
ALTER TABLE agb.panel_run ADD UNIQUE (id, panel_config_id);
ALTER TABLE agb.panel_retrieval ADD UNIQUE (id, run_id);

-- One row per debated name: 'scored', with the rubric total (0 to 24) and its normalisation to
-- [0, 1], higher meaning safer, both computed in code from the dimension rows; or
-- 'debate_failure', with the reason and no score. A name that was never debated (an EDGAR
-- failure, no readable filings, an Analyst failure) has no row: the blend gives it the neutral
-- 0.5. `sector` is what the debate roles were told.
CREATE TABLE agb.panel_score (
    id                  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    retrieval_id        bigint NOT NULL UNIQUE,
    run_id              bigint NOT NULL,
    panel_config_id     bigint NOT NULL REFERENCES agb.panel_config,
    sector              text NOT NULL CHECK (sector <> ''),
    prosecutor_call_id  bigint REFERENCES agb.panel_model_call,
    defender_call_id    bigint REFERENCES agb.panel_model_call,
    judge_call_id       bigint REFERENCES agb.panel_model_call,
    outcome             text NOT NULL CHECK (outcome IN ('scored', 'debate_failure')),
    detail              text,
    total               smallint CHECK (total BETWEEN 0 AND 24),
    score               double precision CHECK (score BETWEEN 0 AND 1),
    at                  timestamptz NOT NULL DEFAULT now(),
    FOREIGN KEY (retrieval_id, run_id) REFERENCES agb.panel_retrieval (id, run_id),
    FOREIGN KEY (run_id, panel_config_id) REFERENCES agb.panel_run (id, panel_config_id),
    CHECK ((outcome = 'scored') = (detail IS NULL)),
    CHECK ((outcome = 'scored') = (total IS NOT NULL)),
    CHECK ((outcome = 'scored') = (score IS NOT NULL)),
    CHECK (outcome <> 'scored' OR judge_call_id IS NOT NULL)
);
SELECT agb.make_append_only('agb.panel_score');

-- The six rubric dimensions of a scored name, each 0 (severe concern) to 4 (clean) with its
-- one-sentence reason. `source` is 'judge' when the score is the Judge's, and 'rule' when a
-- filing rule set it (a delinquent filer's cap on dimensions 1 and 2, a recent IPO's neutral
-- 2 on dimension 6); the Judge's own score and reason are kept either way.
CREATE TABLE agb.panel_dimension (
    id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    score_id      bigint NOT NULL REFERENCES agb.panel_score,
    dimension     smallint NOT NULL CHECK (dimension BETWEEN 1 AND 6),
    score         smallint NOT NULL CHECK (score BETWEEN 0 AND 4),
    reason        text NOT NULL CHECK (btrim(reason) <> ''),
    source        text NOT NULL CHECK (source IN ('judge', 'rule')),
    judge_score   smallint NOT NULL CHECK (judge_score BETWEEN 0 AND 4),
    judge_reason  text NOT NULL CHECK (btrim(judge_reason) <> ''),
    UNIQUE (score_id, dimension),
    CHECK (source = 'rule' OR (score = judge_score AND reason = judge_reason))
);
SELECT agb.make_append_only('agb.panel_dimension');

GRANT SELECT ON agb.panel_score, agb.panel_dimension
    TO scheduler, pipeline, execution, bot, ui, cli;
GRANT INSERT ON agb.panel_score, agb.panel_dimension TO pipeline;
