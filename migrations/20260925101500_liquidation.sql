-- Two-step liquidate: the token, the confirmation, and the broker's per-symbol result.
--
-- `/liquidate` is two commands. The first prints what will happen and issues a short random
-- token, valid for five minutes: a `token` row. The second types the token back. A match in
-- time is a `confirm` row, written in one transaction with the `liquidation` halt cause, so
-- the system is paused before anything reaches the broker. A wrong or expired token is a
-- `refused` row against the token it spent, and the operator starts again from the first
-- step. Either way the token is used once: each row that follows a token names it, and a
-- token can be named only once.
--
-- Only execution holds the broker's keys, so it picks up each confirmation that has no
-- result, calls Alpaca's close-all (`DELETE /v2/positions?cancel_orders=true`), and records
-- the 207 Multi-Status answer symbol by symbol as a `result` row, failures included. The
-- `liquidation` cause stays open afterwards, until `/resume`.
CREATE TABLE agb.liquidation (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    kind        text NOT NULL CHECK (kind IN ('token', 'confirm', 'refused', 'result')),
    -- The row this one follows: a confirm or refusal names its token, a result its confirm.
    follows     bigint UNIQUE REFERENCES agb.liquidation,
    -- The command that issued, confirmed or refused; a result is execution's, not a command.
    command_id  bigint UNIQUE REFERENCES agb.command,
    token       text CHECK (token ~ '^[a-z0-9]{4,12}$'),
    expires_at  timestamptz,
    -- A result's per-symbol answer: [{symbol, status, order_id, message}], one per position.
    symbols     jsonb CHECK (jsonb_typeof(symbols) = 'array'),
    -- What the row says: the summary a token was issued with, why a token was refused, or
    -- the result as the operator was told it.
    detail      text NOT NULL CHECK (detail <> ''),
    at          timestamptz NOT NULL DEFAULT now(),
    actor       agb.actor NOT NULL,
    CHECK ((kind = 'token') = (follows IS NULL)),
    CHECK ((kind = 'token') = (token IS NOT NULL)),
    CHECK ((kind = 'token') = (expires_at IS NOT NULL)),
    CHECK ((kind = 'result') = (command_id IS NULL)),
    CHECK ((kind = 'result') = (symbols IS NOT NULL))
);
SELECT agb.make_append_only('agb.liquidation');

-- A confirmation or refusal follows a token, and a result follows a confirmation.
CREATE FUNCTION agb.liquidation_check() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    prior  text;
BEGIN
    IF NEW.follows IS NULL THEN
        RETURN NEW;
    END IF;
    SELECT l.kind INTO prior FROM agb.liquidation l WHERE l.id = NEW.follows;
    IF NEW.kind IN ('confirm', 'refused') AND prior IS DISTINCT FROM 'token' THEN
        RAISE EXCEPTION 'liquidation: a % follows a token, not a %', NEW.kind, prior
            USING ERRCODE = 'check_violation';
    END IF;
    IF NEW.kind = 'result' AND prior IS DISTINCT FROM 'confirm' THEN
        RAISE EXCEPTION 'liquidation: a result follows a confirmation, not a %', prior
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END;
$$;
REVOKE EXECUTE ON FUNCTION agb.liquidation_check() FROM PUBLIC;

CREATE TRIGGER follows_in_order BEFORE INSERT ON agb.liquidation
    FOR EACH ROW EXECUTE FUNCTION agb.liquidation_check();

-- Who writes which row: the human's surfaces issue, confirm and refuse; execution, which
-- alone holds the broker's keys, records the result.
ALTER TABLE agb.liquidation ENABLE ROW LEVEL SECURITY;
CREATE POLICY liquidation_read ON agb.liquidation FOR SELECT USING (true);
CREATE POLICY liquidation_human ON agb.liquidation FOR INSERT TO bot, ui
    WITH CHECK (kind IN ('token', 'confirm', 'refused'));
CREATE POLICY liquidation_execution ON agb.liquidation FOR INSERT TO execution
    WITH CHECK (kind = 'result' AND actor = 'execution');

-- The confirmations execution has not yet carried to the broker, oldest first.
CREATE VIEW agb.liquidation_pending AS
SELECT l.id, l.actor, l.at, l.command_id
FROM agb.liquidation l
WHERE l.kind = 'confirm'
  AND NOT EXISTS (SELECT 1 FROM agb.liquidation r WHERE r.follows = l.id)
ORDER BY l.id;

GRANT SELECT ON agb.liquidation, agb.liquidation_pending
    TO scheduler, pipeline, execution, bot, ui, cli;
GRANT INSERT ON agb.liquidation TO bot, ui, execution;

-- The audit log gains its liquidation branch. The token itself stays out of it.
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
SELECT l.at, 'liquidation', l.id, l.actor,
       'liquidation ' || l.kind,
       CASE WHEN l.follows IS NULL THEN NULL ELSE 'liquidation ' || l.follows END,
       l.detail
FROM agb.liquidation l;

GRANT SELECT ON agb.audit_log TO scheduler, pipeline, execution, bot, ui, cli;
