-- Stage 2: the blend that mixes the panel's safety score into the stage-1 ranking, and the
-- shadow basket that records the counterfactual (docs/architecture/02, 03).
--
-- The blend is final_score = stage1_rank_pct + w * llm_score_pct over the shortlist, with w
-- taken from the immutable panel_config the basket names. Nothing here is fitted: the panel
-- knows what happened to every name it reads, so a weight learnt from history would be
-- learning the outcome. One row per shortlisted name records both terms exactly as they were
-- computed, so a basket's selection can be reproduced from the database alone.

-- Which panel run, under which config and at which weight, the basket was blended with. All
-- three are null together on a basket proposed with no panel at all.
ALTER TABLE agb.basket
    ADD COLUMN panel_config_id bigint REFERENCES agb.panel_config,
    ADD COLUMN panel_run_id    bigint,
    ADD COLUMN panel_w         numeric CHECK (panel_w >= 0),
    ADD FOREIGN KEY (panel_run_id, panel_config_id) REFERENCES agb.panel_run (id, panel_config_id),
    ADD CHECK (
        (panel_run_id IS NULL) = (panel_config_id IS NULL)
        AND (panel_run_id IS NULL) = (panel_w IS NULL));

-- One row per shortlisted name: its stage-1 rank and that rank's percentile among the
-- shortlist, the panel's normalised score and that score's percentile among the names the
-- panel scored, and the blend of the two. `neutral` is true for a name the panel did not
-- score (an EDGAR failure, no readable filings, an Analyst or debate failure): it takes the
-- neutral 0.5 and has no panel_score_id. The percentiles are tie-averaged, the same
-- normalisation stage 1 applies to every feature.
CREATE TABLE agb.basket_blend (
    id               bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    basket_id        bigint NOT NULL REFERENCES agb.basket,
    symbol           text NOT NULL CHECK (symbol <> ''),
    stage1_rank      integer NOT NULL CHECK (stage1_rank >= 1),
    stage1_rank_pct  double precision NOT NULL CHECK (stage1_rank_pct BETWEEN 0 AND 1),
    panel_score_id   bigint REFERENCES agb.panel_score,
    llm_total        smallint CHECK (llm_total BETWEEN 0 AND 24),
    llm_score        double precision CHECK (llm_score BETWEEN 0 AND 1),
    llm_score_pct    double precision NOT NULL CHECK (llm_score_pct BETWEEN 0 AND 1),
    neutral          boolean NOT NULL,
    final_score      double precision NOT NULL,
    UNIQUE (basket_id, symbol),
    CHECK (neutral = (panel_score_id IS NULL)),
    CHECK (neutral = (llm_total IS NULL)),
    CHECK (neutral = (llm_score IS NULL))
);
SELECT agb.make_append_only('agb.basket_blend');

GRANT SELECT ON agb.basket_blend TO scheduler, pipeline, execution, bot, ui, cli;
GRANT INSERT ON agb.basket_blend TO pipeline;

-- The shadow basket is a record, not a position: it is proposed and then left alone. The
-- trigger gains that rule, so no surface can approve, submit or otherwise move one however
-- its role is granted. Everything else is as it was.
CREATE OR REPLACE FUNCTION agb.basket_event_check() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    latest_seq    integer;
    latest_state  text;
    basket_kind   text;
BEGIN
    SELECT e.seq, e.to_state INTO latest_seq, latest_state
    FROM agb.basket_event e
    WHERE e.basket_id = NEW.basket_id
    ORDER BY e.seq DESC
    LIMIT 1;

    IF coalesce(latest_seq, 0) + 1 <> NEW.seq THEN
        RAISE EXCEPTION 'basket %: seq % does not follow its latest event (seq %)',
            NEW.basket_id, NEW.seq, coalesce(latest_seq, 0)
            USING ERRCODE = 'check_violation';
    END IF;
    IF NEW.from_state IS DISTINCT FROM latest_state THEN
        RAISE EXCEPTION 'basket %: it is % and not %',
            NEW.basket_id, coalesce(latest_state, 'unproposed'), coalesce(NEW.from_state, 'unproposed')
            USING ERRCODE = 'check_violation';
    END IF;

    SELECT b.kind INTO basket_kind FROM agb.basket b WHERE b.id = NEW.basket_id;
    IF basket_kind = 'shadow' AND NEW.seq > 1 THEN
        RAISE EXCEPTION 'basket %: a shadow basket is never traded, so it does not move to %',
            NEW.basket_id, NEW.to_state
            USING ERRCODE = 'insufficient_privilege';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM agb.basket_transition t
        WHERE t.from_state IS NOT DISTINCT FROM NEW.from_state
          AND t.to_state = NEW.to_state
          AND t.role = current_user
    ) THEN
        RAISE EXCEPTION 'basket_transition: % may not move basket % from % to %',
            current_user, NEW.basket_id, coalesce(NEW.from_state, 'unproposed'), NEW.to_state
            USING ERRCODE = 'insufficient_privilege';
    END IF;
    RETURN NEW;
END;
$$;
REVOKE EXECUTE ON FUNCTION agb.basket_event_check() FROM PUBLIC;
