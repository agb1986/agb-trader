-- The autonomy ladder and the move from paper to live (docs/architecture/04, "The autonomy
-- ladder" and "From paper to live"). Three levels: L0 asks the human before every rebalance,
-- L1 asks only when the gate raises a flag or the turnover is unusual and otherwise proceeds
-- unless rejected inside a window, L2 notifies and never asks. The pre-trade gate and the
-- kill-switch are not part of the ladder: they apply at every level, permanently.
--
-- Loosening is manual and tightening is not. Moving up is a human act through the CLI
-- (`agb level`, `agb mode`), refused unless the eligibility view is satisfied, and the
-- criteria that are human judgement are recorded as an attestation on the event. A rejection
-- or a gate breach at L1 or L2 drops the system back to L0 by itself, written by the
-- scheduler as the system, naming the basket that caused it.
--
-- Eligibility is a set of views, never a stored counter (docs/architecture/08): the clean
-- rebalances are counted from the recorded history each time they are read, and a rejection,
-- a breach or a missed approval ends the run by being the newest settled rebalance that was
-- not filled.

-- A move up or down the ladder. The latest row is the level in force. Every upward move
-- comes from the CLI with an attestation and the command that asked for it; every downward
-- move is to L0, and the automatic ones name the basket that caused them.
CREATE TABLE agb.ladder_event (
    id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    level        text NOT NULL CHECK (level IN ('L0', 'L1', 'L2')),
    actor        agb.actor NOT NULL,
    at           timestamptz NOT NULL DEFAULT now(),
    reason       text NOT NULL CHECK (btrim(reason) <> ''),
    -- What the human attests to that no view can measure: that they have read the evidence
    -- and accept the looser default.
    attestation  text CHECK (btrim(attestation) <> ''),
    command_id   bigint REFERENCES agb.command,
    -- The rebalance whose rejection or breach demoted the system, where there was one.
    basket_id    bigint REFERENCES agb.basket,
    CHECK (level = 'L0' OR (attestation IS NOT NULL AND command_id IS NOT NULL)),
    CHECK (basket_id IS NULL OR level = 'L0')
);
SELECT agb.make_append_only('agb.ladder_event');

INSERT INTO agb.ladder_event (level, actor, reason)
    VALUES ('L0', 'system', 'live trading starts at L0, asking before every rebalance');

-- The CLI moves up; the scheduler and execution only ever drop to L0 as the system.
ALTER TABLE agb.ladder_event ENABLE ROW LEVEL SECURITY;
CREATE POLICY ladder_event_read ON agb.ladder_event FOR SELECT USING (true);
CREATE POLICY ladder_event_up ON agb.ladder_event FOR INSERT TO cli WITH CHECK (true);
CREATE POLICY ladder_event_down ON agb.ladder_event FOR INSERT TO scheduler, execution
    WITH CHECK (level = 'L0' AND actor = 'system');

-- The level in force: the latest event's.
CREATE VIEW agb.current_level AS
SELECT e.level, e.at AS since, e.actor, e.reason, e.attestation
FROM agb.ladder_event e
ORDER BY e.id DESC
LIMIT 1;

-- Going live is the same kind of act, so a mode_event that goes live carries the same
-- attestation and names the same kind of command. Dropping back to paper, like dropping to
-- L0, needs neither.
ALTER TABLE agb.mode_event
    ADD COLUMN attestation text CHECK (btrim(attestation) <> ''),
    ADD COLUMN command_id bigint REFERENCES agb.command,
    ADD CHECK (mode = 'paper' OR (attestation IS NOT NULL AND command_id IS NOT NULL));
CREATE POLICY mode_event_up ON agb.mode_event FOR INSERT TO cli WITH CHECK (true);
GRANT INSERT ON agb.mode_event TO cli;

-- The paper-to-live checks that are arithmetic, not judgement: that realised fills matched
-- the official closing price within tolerance, that the fee arithmetic agrees with the cost
-- model each fill was priced under, and that partial fills were handled correctly.
-- `measured` and `threshold` are text, as the gate's and the pre-flight suite's are. Nothing
-- is ever updated: a re-run appends a row and the view reads the newest of each name.
CREATE TABLE agb.paper_check (
    id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name       text NOT NULL CHECK (name IN ('fill_tolerance', 'fee_arithmetic',
                                             'partial_fills')),
    basket_id  bigint REFERENCES agb.basket,
    passed     boolean NOT NULL,
    measured   text NOT NULL CHECK (measured <> ''),
    threshold  text NOT NULL CHECK (threshold <> ''),
    detail     jsonb NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(detail) = 'object'),
    actor      agb.actor NOT NULL,
    at         timestamptz NOT NULL DEFAULT now()
);
SELECT agb.make_append_only('agb.paper_check');
CREATE INDEX paper_check_name ON agb.paper_check (name, id DESC);

-- Every settled live rebalance with the mode it was gated under. A rebalance is settled once
-- it is filled, rejected, missed or halted; 'filled' is the clean outcome and the other three
-- each end a run of clean ones.
CREATE VIEW agb.ladder_rebalance AS
SELECT s.id AS basket_id, s.reference, s.period, s.session_date, s.state, s.since, r.mode
FROM agb.basket_state s
JOIN LATERAL (
    SELECT g.mode FROM agb.gate_run g WHERE g.basket_id = s.id ORDER BY g.id DESC LIMIT 1
) r ON true
WHERE s.kind = 'live' AND s.state IN ('filled', 'rejected', 'missed', 'halted');

-- The consecutive clean live rebalances since the last rejection, breach or missed approval,
-- counted from the history every time it is read. One row, 0 when there have been none.
CREATE VIEW agb.ladder_clean AS
SELECT count(*) AS clean_rebalances, max(since) AS at
FROM agb.ladder_rebalance
WHERE mode = 'live' AND state = 'filled'
  AND basket_id > coalesce((SELECT max(basket_id) FROM agb.ladder_rebalance
                            WHERE mode = 'live' AND state <> 'filled'), 0);

-- What the next level up asks for, criterion by criterion, for the level in force: six clean
-- live rebalances to reach L1, then a year at L1 with nothing rejected. L2 is the top of the
-- ladder and has no rows.
CREATE VIEW agb.ladder_status AS
WITH l AS (SELECT level, since FROM agb.current_level)
SELECT 'L1'::text AS target, 1 AS seq, 'clean_rebalances'::text AS name,
       c.clean_rebalances::text AS measured, '>= 6'::text AS threshold,
       c.clean_rebalances >= 6 AS met
FROM l, agb.ladder_clean c
WHERE l.level = 'L0'
UNION ALL
SELECT 'L2', 1, 'days_at_l1',
       (extract(epoch FROM now() - l.since) / 86400)::bigint::text, '>= 365',
       now() - l.since >= interval '365 days'
FROM l
WHERE l.level = 'L1'
UNION ALL
SELECT 'L2', 2, 'rejections_at_l1', count(r.basket_id)::text, '= 0', count(r.basket_id) = 0
FROM l
LEFT JOIN agb.ladder_rebalance r
    ON r.mode = 'live' AND r.state = 'rejected' AND r.since >= l.since
WHERE l.level = 'L1'
GROUP BY l.level;

-- One row for the next level up: whether every criterion is met, and which are not.
CREATE VIEW agb.ladder_ready AS
SELECT target,
       bool_and(met) AS ready,
       array_agg(name ORDER BY seq) FILTER (WHERE NOT met) AS outstanding
FROM agb.ladder_status
GROUP BY target;

-- The paper-to-live criteria, each measured from the recorded history: twelve settled paper
-- rebalances, the three arithmetic checks, no undiagnosed gate breach, the panel pre-flight
-- suite passed under the newest configuration, and pause, liquidate and the approval flow
-- each exercised at least once. A breach counts as diagnosed once a clear of the gate_breach
-- cause carries a reason.
CREATE VIEW agb.live_status AS
SELECT 1 AS seq, 'paper_rebalances'::text AS name,
       count(*)::text AS measured, '>= 12'::text AS threshold, count(*) >= 12 AS met
FROM agb.ladder_rebalance
WHERE mode = 'paper' AND state IN ('filled', 'halted')
UNION ALL
SELECT n.seq, n.name,
       coalesce(p.measured, 'not run'), coalesce(p.threshold, 'a recorded pass'),
       coalesce(p.passed, false)
FROM (VALUES (2, 'fill_tolerance'),
             (3, 'fee_arithmetic'),
             (4, 'partial_fills')) AS n(seq, name)
LEFT JOIN LATERAL (
    SELECT c.passed, c.measured, c.threshold FROM agb.paper_check c
    WHERE c.name = n.name ORDER BY c.id DESC LIMIT 1
) p ON true
UNION ALL
SELECT 5, 'undiagnosed_breaches', count(*)::text, '= 0', count(*) = 0
FROM agb.halt_event h
WHERE h.cause = 'gate_breach' AND h.action = 'set'
  AND NOT EXISTS (SELECT 1 FROM agb.halt_event c
                  WHERE c.cause = 'gate_breach' AND c.action = 'clear' AND c.id > h.id
                    AND btrim(coalesce(c.reason, '')) <> '')
UNION ALL
SELECT 6, 'panel_preflight',
       CASE WHEN r.panel_config_id IS NULL THEN 'no panel configuration'
            WHEN r.passed THEN format('config %s, all four checks', r.panel_config_id)
            ELSE format('config %s, outstanding: %s', r.panel_config_id,
                        array_to_string(r.outstanding, ', ')) END,
       'the suite passed', coalesce(r.passed, false)
FROM (SELECT max(id) AS id FROM agb.panel_config) newest
LEFT JOIN agb.panel_preflight_ready r ON r.panel_config_id = newest.id
UNION ALL
SELECT n.seq, n.name,
       CASE WHEN count(c.id) = 0 THEN 'never' ELSE format('%s, last %s', count(c.id),
            to_char(max(c.at) AT TIME ZONE 'America/New_York', 'YYYY-MM-DD')) END,
       '>= 1', count(c.id) >= 1
FROM (VALUES (7, 'pause_exercised', 'pause'),
             (8, 'liquidate_exercised', 'liquidate'),
             (9, 'approval_exercised', 'approve')) AS n(seq, name, verb)
LEFT JOIN agb.command c ON c.verb = n.verb AND c.outcome = 'accepted'
GROUP BY n.seq, n.name;

-- One row: whether paper has earned live, and which criteria are outstanding.
CREATE VIEW agb.live_ready AS
SELECT bool_and(met) AS ready,
       count(*) FILTER (WHERE met) AS criteria_met,
       array_agg(name ORDER BY seq) FILTER (WHERE NOT met) AS outstanding
FROM agb.live_status;

-- At L1 the scheduler proceeds with a clean basket as the system once the opt-out window
-- closes, which is the one transition the ladder adds.
INSERT INTO agb.basket_transition (from_state, to_state, role) VALUES
    ('pending', 'approved', 'scheduler');

-- Refuses an approval the ladder does not allow. The human's own answer to a pending basket
-- is left alone; every approval the system makes needs a gate run that recorded all of its
-- checks with none failed, and then:
--
--   * proposed to approved, unasked, in paper mode, which `agb.paper_approval` governs;
--   * proposed to approved in live at L2, where the human is notified, not asked;
--   * pending to approved in live at L1, where the basket was put to the human with a
--     countdown and the opt-out window closed with no rejection.
--
-- Named as before so it fires after transition_allowed, which refuses a role that has no
-- such transition at all.
CREATE OR REPLACE FUNCTION agb.basket_approval_check() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    mode_now   text;
    level_now  text;
    declared   integer;
    recorded   integer;
    failed     integer;
    why        text;
BEGIN
    IF NEW.to_state <> 'approved'
       OR NEW.from_state IS NULL
       OR NEW.from_state NOT IN ('proposed', 'pending') THEN
        RETURN NEW;
    END IF;
    IF NEW.actor <> 'system' AND NEW.from_state = 'pending' THEN
        RETURN NEW;
    END IF;
    SELECT m.mode INTO mode_now FROM agb.current_mode m;
    SELECT l.level INTO level_now FROM agb.current_level l;
    SELECT r.checks, count(c.id), count(c.id) FILTER (WHERE NOT c.passed)
        INTO declared, recorded, failed
    FROM agb.gate_run r
    LEFT JOIN agb.gate_check c ON c.run_id = r.id
    WHERE r.basket_id = NEW.basket_id
    GROUP BY r.checks;

    IF NEW.actor <> 'system' THEN
        why := format('the gate approves as system, not %s', NEW.actor);
    ELSIF declared IS NULL THEN
        why := 'the gate has not run';
    ELSIF recorded < declared THEN
        why := format('the gate recorded %s of its %s checks', recorded, declared);
    ELSIF failed > 0 THEN
        why := format('%s gate checks failed', failed);
    ELSIF NEW.from_state = 'proposed' AND mode_now IS DISTINCT FROM 'paper'
          AND level_now IS DISTINCT FROM 'L2' THEN
        why := format('the system approves a live rebalance unasked only at L2, and the'
                      ' level is %s', level_now);
    ELSIF NEW.from_state = 'pending'
          AND (mode_now IS DISTINCT FROM 'live' OR level_now IS DISTINCT FROM 'L1') THEN
        why := format('the system proceeds with a pending rebalance only at L1 in live, and'
                      ' the mode is %s at %s', mode_now, level_now);
    END IF;
    IF why IS NOT NULL THEN
        RAISE EXCEPTION 'basket_gate: % may not move basket % from % to approved: %',
            current_user, NEW.basket_id, NEW.from_state, why
            USING ERRCODE = 'insufficient_privilege';
    END IF;
    RETURN NEW;
END;
$$;

-- The approval loop refused every `pending` answer the system gave, because below L1 a
-- pending basket is the human's to decide. At L1 the system does answer one: it proceeds
-- with a clean basket when the opt-out window closes. A rejection is still never the
-- system's to make, and `agb.basket_approval_check` holds the proceed itself to live, L1 and
-- a passed gate run, so this leaves `pending` to `approved` to it.
CREATE OR REPLACE FUNCTION agb.basket_policy_check() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    asks  boolean;
    why   text;
BEGIN
    IF NEW.from_state = 'proposed' AND NEW.to_state = 'approved' THEN
        SELECT p.required INTO asks FROM agb.paper_approval p;
        IF asks THEN
            why := 'paper mode is set to ask the human first';
        END IF;
    ELSIF NEW.from_state = 'pending' AND NEW.to_state = 'rejected'
          AND NEW.actor = 'system' THEN
        why := 'only a human rejects a pending basket';
    END IF;
    IF why IS NOT NULL THEN
        RAISE EXCEPTION 'basket_policy: % may not move basket % from % to %: %',
            current_user, NEW.basket_id, NEW.from_state, NEW.to_state, why
            USING ERRCODE = 'insufficient_privilege';
    END IF;
    RETURN NEW;
END;
$$;

GRANT SELECT ON agb.ladder_event, agb.current_level, agb.paper_check, agb.ladder_rebalance,
    agb.ladder_clean, agb.ladder_status, agb.ladder_ready, agb.live_status, agb.live_ready
    TO scheduler, pipeline, execution, bot, ui, cli;
GRANT INSERT ON agb.ladder_event TO cli, scheduler, execution;
GRANT INSERT ON agb.paper_check TO cli;

-- The audit log gains the ladder, one branch more. The twelve branches are recreated here
-- whole, as every migration that adds one must: a view is replaced, never extended.
CREATE OR REPLACE VIEW agb.audit_log AS
SELECT e.at, 'basket_event'::text AS source, e.id AS row_id, e.actor::text AS actor,
       'basket ' || e.to_state AS event,
       b.kind || ' basket ' || coalesce(b.reference, b.id::text) AS subject,
       e.reason AS detail
FROM agb.basket_event e
JOIN agb.basket b ON b.id = e.basket_id
UNION ALL
SELECT h.at, 'halt_event', h.id, h.actor,
       'halt ' || h.action || ' ' || h.cause,
       CASE WHEN b.id IS NULL THEN NULL
            ELSE b.kind || ' basket ' || coalesce(b.reference, b.id::text) END,
       h.reason
FROM agb.halt_event h
LEFT JOIN agb.basket b ON b.id = h.basket_id
UNION ALL
SELECT m.at, 'mode_event', m.id, m.actor, 'mode ' || m.mode, NULL, m.reason
FROM agb.mode_event m
UNION ALL
SELECT e.at, 'order_event', e.id, e.actor,
       'order ' || e.kind || ' ' || e.status,
       o.side || ' ' || o.qty || ' ' || o.symbol || ' (' || o.client_order_id || ')',
       e.detail
FROM agb.order_event e
JOIN agb.broker_order o ON o.id = e.order_id
UNION ALL
SELECT c.at, 'command', c.id, c.actor,
       'command ' || c.outcome,
       c.surface || ' ' || left(c.raw_text, 200),
       c.reason
FROM agb.command c
UNION ALL
SELECT e.at, 'alert_event', e.id, e.actor,
       'alert ' || e.event,
       a.kind || ' ' || a.subject || ' ' || a.period,
       e.detail
FROM agb.alert_event e
JOIN agb.alert a ON a.id = e.alert_id
UNION ALL
SELECT e.at, 'job_event', e.id, e.actor,
       'job ' || e.event,
       j.kind || ' ' || j.period,
       e.detail
FROM agb.job_event e
JOIN agb.job j ON j.id = e.job_id
UNION ALL
SELECT p.at, 'paper_approval_event', p.id, p.actor,
       CASE WHEN p.required THEN 'paper approval required' ELSE 'paper approval unattended' END,
       NULL, p.reason
FROM agb.paper_approval_event p
UNION ALL
SELECT p.at, 'promotion_event', p.id, p.actor,
       'model promoted',
       'model ' || c.model_id,
       NULL
FROM agb.promotion_event p
JOIN agb.model_certification c ON c.id = p.certification_id
UNION ALL
SELECT e.at, 'certification_event', e.id, e.actor,
       'model ' || e.event,
       'model ' || c.model_id,
       e.reason
FROM agb.certification_event e
JOIN agb.model_certification c ON c.id = e.certification_id
UNION ALL
-- The token itself stays out of the log.
SELECT l.at, 'liquidation', l.id, l.actor,
       'liquidation ' || l.kind,
       CASE WHEN l.follows IS NULL THEN NULL ELSE 'liquidation ' || l.follows END,
       l.detail
FROM agb.liquidation l
UNION ALL
SELECT e.at, 'ladder_event', e.id, e.actor,
       'ladder ' || e.level,
       CASE WHEN b.id IS NULL THEN NULL
            ELSE b.kind || ' basket ' || coalesce(b.reference, b.id::text) END,
       CASE WHEN e.attestation IS NULL THEN e.reason
            ELSE e.reason || ' — attested: ' || e.attestation END
FROM agb.ladder_event e
LEFT JOIN agb.basket b ON b.id = e.basket_id;

GRANT SELECT ON agb.audit_log TO scheduler, pipeline, execution, bot, ui, cli;
