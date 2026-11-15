-- How each pass over a panel run ended (issue #157). The panel writes each name as its calls
-- finish, so a pass that dies part-way leaves the names it finished, and a retry of the same
-- job resumes that run from them instead of paying for every call again. agb.panel_run is
-- append-only, so the outcome is a row here rather than a column there: the run's state is its
-- newest row.
--
-- `stage` is the half of the panel the pass was in: 'evidence' (retrieval and the Analyst,
-- panel.run) or 'score' (the debate, panel.score). `state` is 'completed', or 'interrupted'
-- with what stopped it. A run with no row at all is one whose first pass is still going, or
-- whose process was killed outright before it could say so.
CREATE TABLE agb.panel_run_state (
    id      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id  bigint NOT NULL REFERENCES agb.panel_run,
    stage   text NOT NULL CHECK (stage IN ('evidence', 'score')),
    state   text NOT NULL CHECK (state IN ('completed', 'interrupted')),
    detail  text,
    at      timestamptz NOT NULL DEFAULT now(),
    actor   agb.actor NOT NULL,
    CHECK ((state = 'interrupted') = (detail IS NOT NULL))
);
SELECT agb.make_append_only('agb.panel_run_state');
CREATE INDEX panel_run_state_run ON agb.panel_run_state (run_id, id);

GRANT SELECT ON agb.panel_run_state TO scheduler, pipeline, execution, bot, ui, cli;
GRANT INSERT ON agb.panel_run_state TO pipeline;
