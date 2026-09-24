-- The approval loop: putting a gated basket to the human, recording the decision, the miss at
-- the cutoff, and the rerun the human may ask for afterwards.
--
-- A live basket that passes the gate waits for the human. The bot moves it from proposed to
-- pending when it sends the approval request, and `/approve <reference>` or
-- `/reject <reference> [reason]` moves it on; the compare-and-set on basket_event decides a
-- race between two surfaces. If no answer arrives by the session's MOC cutoff the scheduler
-- marks the basket missed, as the system. A miss is not a failure and opens no halt cause.
-- Afterwards the operator may ask, through the CLI, for the same basket to trade at the next
-- session's close: a new basket, '2026-10r1', whose deviation_of names the one it reruns. The
-- pipeline builds it from the rerun command and gates it like any other.

-- Whether paper mode asks the human too. Paper runs unattended by default, and live mode
-- always asks at L0; this lets the operator rehearse the loop in paper. The operator sets it
-- through the CLI, and the latest row is in force.
CREATE TABLE agb.paper_approval_event (
    id        bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    required  boolean NOT NULL,
    actor     agb.actor NOT NULL,
    at        timestamptz NOT NULL DEFAULT now(),
    reason    text CHECK (reason <> '')
);
SELECT agb.make_append_only('agb.paper_approval_event');

INSERT INTO agb.paper_approval_event (required, actor, reason)
    VALUES (false, 'system', 'paper runs unattended');

CREATE VIEW agb.paper_approval AS
SELECT e.required, e.at AS since, e.actor, e.reason
FROM agb.paper_approval_event e
ORDER BY e.id DESC
LIMIT 1;

-- The command a human decision or a rerun came from, so the event reads back to the message
-- or the CLI invocation that caused it. System and service events have none.
ALTER TABLE agb.basket_event ADD COLUMN command_id bigint REFERENCES agb.command;

-- Refuses what the loop does not allow, on top of basket_transition's roles and the gate's
-- own check (transition_gated). Named to fire after transition_allowed and transition_gated.
--   * proposed to approved while paper mode asks the human: the gate may not approve alone.
--   * pending to approved or rejected by the system: only a human decides a pending basket.
-- A basket that fails the gate is halted by it, so only a passed one is left to put to the
-- human; the bot puts only gated baskets, and agb.approval says why.
CREATE FUNCTION agb.basket_policy_check() RETURNS trigger
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
    ELSIF NEW.from_state = 'pending' AND NEW.to_state IN ('approved', 'rejected')
          AND NEW.actor = 'system' THEN
        why := 'only a human decides a pending basket';
    END IF;
    IF why IS NOT NULL THEN
        RAISE EXCEPTION 'basket_policy: % may not move basket % from % to %: %',
            current_user, NEW.basket_id, NEW.from_state, NEW.to_state, why
            USING ERRCODE = 'insufficient_privilege';
    END IF;
    RETURN NEW;
END;
$$;
REVOKE EXECUTE ON FUNCTION agb.basket_policy_check() FROM PUBLIC;

CREATE TRIGGER transition_policy BEFORE INSERT ON agb.basket_event
    FOR EACH ROW EXECUTE FUNCTION agb.basket_policy_check();

-- The reruns the operator asked for that the pipeline has not yet built and gated, oldest
-- first. The CLI records the rerun as a command naming the missed basket, the new reference
-- and the session it trades on; the pipeline builds the basket and gates it, and a gate run
-- on the basket under that reference is what marks the command done. Building and gating are
-- both safe to redo, so one interrupted between the two is finished on the next cycle.
CREATE VIEW agb.rerun_pending AS
SELECT c.id AS command_id, c.actor, c.at,
       (c.args->>'basket_id')::bigint AS basket_id,
       c.args->>'rerun' AS reference,
       (c.args->>'session_date')::date AS session_date
FROM agb.command c
WHERE c.verb = 'rerun'
  AND c.surface = 'cli'
  AND c.outcome = 'accepted'
  AND NOT EXISTS (SELECT 1 FROM agb.basket b JOIN agb.gate_run g ON g.basket_id = b.id
                  WHERE b.reference = c.args->>'rerun')
ORDER BY c.id;

GRANT SELECT ON agb.paper_approval_event, agb.paper_approval, agb.rerun_pending
    TO scheduler, pipeline, execution, bot, ui, cli;
GRANT INSERT ON agb.paper_approval_event TO cli;

-- The audit log gains the paper-approval setting, one branch more, as the view says.
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
FROM agb.paper_approval_event p;

GRANT SELECT ON agb.audit_log TO scheduler, pipeline, execution, bot, ui, cli;
