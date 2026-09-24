-- Where each command came from, and the audit view the bot reads back.
--
-- `agb.command` records every message a human surface receives, whatever became of it.
-- The bot also records messages from a chat that is not the configured one, refused, so
-- the chat and the Telegram message it came from are kept alongside. A refusal says why,
-- and an accepted command has nothing to explain.
ALTER TABLE agb.command
    ADD COLUMN chat_id              text,
    ADD COLUMN telegram_message_id  bigint,
    ADD CONSTRAINT command_reason CHECK (reason <> ''),
    ADD CONSTRAINT command_reason_iff_refused CHECK ((outcome = 'refused') = (reason IS NOT NULL));

-- The audit log: one row per recorded event across the event tables, with the time, the
-- actor, what happened, the fields that identify what it happened to, and any detail. It
-- is what `/log` reads. `system` is an actor like any other. Each later event table
-- (the ladder, promotion, certification, liquidation) is added here when it exists, by a
-- migration that replaces this view with one more branch.
CREATE VIEW agb.audit_log AS
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
JOIN agb.job j ON j.id = e.job_id;

GRANT SELECT ON agb.audit_log TO scheduler, pipeline, execution, bot, ui, cli;
