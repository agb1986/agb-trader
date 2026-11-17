-- A model's refusal is its own outcome, not an unusable answer.
--
-- The Analyst or a debate role can decline to answer (stop_reason "refusal"). That was stored
-- as 'analyst_failure' or 'debate_failure', the same as an answer cut off or not in the shape
-- asked for, so a run could not say how many names the model had declined, and a reviewer had
-- to read the detail text to find out. A refusal now has its own outcome:
-- 'analyst_refusal' on agb.panel_extract and 'debate_refusal' on agb.panel_score. Like a
-- failure it carries the reason in `detail` and no extract or score, and the name is scored
-- neutral downstream.
--
-- Only the two CHECK lists widen. Existing rows keep their outcomes, and the other checks on
-- both tables already say what a non-extracted or non-scored outcome carries.

ALTER TABLE agb.panel_extract DROP CONSTRAINT panel_extract_outcome_check;
ALTER TABLE agb.panel_extract ADD CONSTRAINT panel_extract_outcome_check
    CHECK (outcome IN ('extracted', 'analyst_failure', 'analyst_refusal'));

ALTER TABLE agb.panel_score DROP CONSTRAINT panel_score_outcome_check;
ALTER TABLE agb.panel_score ADD CONSTRAINT panel_score_outcome_check
    CHECK (outcome IN ('scored', 'debate_failure', 'debate_refusal'));
