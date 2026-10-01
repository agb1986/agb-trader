-- The audit log, carrying the branches of both parallel migrations.
--
-- The two-step liquidate (20260925101500) and the certified-model registry (20260926090000)
-- were built side by side, and each recreated agb.audit_log from the definition it had
-- branched from. The registry's file sorts later, so it applied last and its view, which
-- never knew about agb.liquidation, is the one in force: liquidation tokens, confirmations,
-- refusals and results stopped appearing in the log. The git merge was clean, because the
-- two migrations touch different files; only applying them in order shows the loss.
--
-- Migrations are immutable once applied (src/agb/migrate.py compares the recorded sha256),
-- so the earlier file is left as it is and the view is recreated here with all eleven
-- branches: the nine of the liquidation migration plus the registry's promotions and
-- voidings.
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
FROM agb.liquidation l;

GRANT SELECT ON agb.audit_log TO scheduler, pipeline, execution, bot, ui, cli;
