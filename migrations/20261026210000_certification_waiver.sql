-- A paper-only measurement waiver: promoting a certification that failed its gates, in paper
-- mode, so the stack can be exercised end to end when no model has passed.
--
-- The sizing run (docs/build/measured-limits.md) needs a real rebalance to measure, and a
-- rebalance with nothing promoted fails before it proposes a basket. A waiver lets the
-- operator promote a failed certification for that purpose alone: `agb promote <model id>
-- --waive-for-paper --reason <why>` refuses unless the trading mode is paper, and a move to
-- live is refused while a waived model is promoted (`agb mode live`). Everything else about
-- the certification still holds: the artifact is verified as it always is, and a voided or
-- stale certification is never waivable.
--
-- A waiver is an event on the certification, like a voiding: at most one per certification,
-- append-only, naming the command that asked for it. It is never withdrawn; promoting a
-- model that has passed replaces it.
ALTER TABLE agb.certification_event DROP CONSTRAINT certification_event_event_check;
ALTER TABLE agb.certification_event ADD CONSTRAINT certification_event_event_check
    CHECK (event IN ('voided', 'waived'));
CREATE UNIQUE INDEX certification_event_waived_once ON agb.certification_event (certification_id)
    WHERE event = 'waived';

-- The live model gains its waiver, so the readout, the gate and the mode interlock can all
-- see it without joining the event table themselves. Appended, so an older image reading the
-- view by name is unaffected.
CREATE OR REPLACE VIEW agb.promoted_model AS
SELECT p.id AS promotion_id, c.id AS certification_id, c.model_id, c.artifact_sha256,
       c.artifact_path, c.declared_trials, c.passed, c.cost_model_version, c.features,
       p.actor AS promoted_by, p.at AS promoted_at,
       v.id IS NOT NULL AS voided, v.at AS voided_at, v.reason AS voided_reason,
       w.id IS NOT NULL AS waived, w.at AS waived_at, w.reason AS waived_reason
FROM (SELECT * FROM agb.promotion_event ORDER BY id DESC LIMIT 1) p
JOIN agb.model_certification c ON c.id = p.certification_id
LEFT JOIN LATERAL (
    SELECT e.id, e.at, e.reason FROM agb.certification_event e
    WHERE e.certification_id = c.id AND e.event = 'voided'
) v ON true
LEFT JOIN LATERAL (
    SELECT e.id, e.at, e.reason FROM agb.certification_event e
    WHERE e.certification_id = c.id AND e.event = 'waived'
) w ON true;
