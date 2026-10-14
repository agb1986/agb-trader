-- Execution crash recovery: the operator's reconcile, and what it appends.
--
-- An expired submission lease is never retried. Execution opens the
-- `execution_lease_expired` halt cause instead, and nothing trades until a human has
-- reconciled the basket against the broker and cleared the cause. This migration is what
-- the reconcile writes.
--
-- `agb reconcile REFERENCE` walks every planned order of the basket and looks the broker up
-- by `client_order_id`. What it learns is appended as an order event, never written onto a
-- row: `reconciled` when the broker holds the order, carrying its id, status and fill, and
-- `unsent` when the broker has never seen the client_order_id, which is an order written
-- before the crash that never reached it. An order with no `broker_order` row at all has
-- nothing to append an event to; `agb.reconcile_state` classifies it as `unwritten` from the
-- absence itself.
--
-- The run row records that the walk happened and what it found, so a reconcile that
-- classified nothing still shows in the log. The operator then decides what to do; the
-- system does not.

-- Two more kinds the broker's view can take. A view of the check is replaced, never
-- extended, so the whole list is restated here.
ALTER TABLE agb.order_event DROP CONSTRAINT order_event_kind_check;
ALTER TABLE agb.order_event ADD CONSTRAINT order_event_kind_check CHECK (kind IN (
    'ack', 'lookup', 'rejected', 'status',
    'cancel_requested', 'cancelled', 'cancel_failed',
    'reconciled', 'unsent'));

-- One more thing a job can have happen to it. An expired submission lease is held open for a
-- human to see; when the operator has reconciled the basket and cleared the halt cause, the
-- lease is given up, and `released` is that act on the record. The whole list is restated
-- because a check is replaced, never extended.
ALTER TABLE agb.job_event DROP CONSTRAINT job_event_event_check;
ALTER TABLE agb.job_event ADD CONSTRAINT job_event_event_check CHECK (event IN (
    'created', 'claimed', 'completed', 'failed',
    'lease_expired', 'reclaimed', 'released'));

-- One reconcile of one basket: how many planned orders it walked and the one-line finding it
-- reported to the operator. The per-order truth is in the events it appended.
CREATE TABLE agb.reconcile_run (
    id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    basket_id  bigint NOT NULL REFERENCES agb.basket,
    orders     integer NOT NULL CHECK (orders >= 0),
    detail     text NOT NULL CHECK (detail <> ''),
    at         timestamptz NOT NULL DEFAULT now(),
    actor      agb.actor NOT NULL
);
SELECT agb.make_append_only('agb.reconcile_run');
CREATE INDEX reconcile_run_basket ON agb.reconcile_run (basket_id, id);

-- Every planned order of a basket with where it stands at the broker:
--   unwritten   no order row was ever written, so nothing was sent;
--   unanswered  a row was written and the broker has said nothing about it, which is how a
--               crash between the write and the answer leaves it until a reconcile;
--   unsent      the reconcile asked and the broker has never seen the client_order_id;
--   filled      the broker filled it;
--   closed      the broker is done with it without a fill: cancelled, expired or refused;
--   open        the broker holds it and may still fill it.
CREATE VIEW agb.reconcile_state AS
SELECT bo.basket_id, bo.id AS basket_order_id, bo.client_order_id, bo.symbol, bo.side, bo.qty,
       s.order_id, s.kind, s.status, s.filled_qty, s.broker_order_id,
       CASE
           WHEN o.id IS NULL THEN 'unwritten'
           WHEN s.kind IS NULL THEN 'unanswered'
           WHEN s.kind = 'unsent' THEN 'unsent'
           WHEN s.status = 'filled' THEN 'filled'
           WHEN s.status IN ('canceled', 'expired', 'rejected', 'done_for_day') THEN 'closed'
           ELSE 'open'
       END AS outcome
FROM agb.basket_order bo
LEFT JOIN agb.broker_order o ON o.basket_order_id = bo.id
LEFT JOIN agb.order_state s ON s.order_id = o.id;

GRANT SELECT ON agb.reconcile_run, agb.reconcile_state
    TO scheduler, pipeline, execution, bot, ui, cli;
-- The reconcile runs as the execution role: it holds the broker's keys and already owns the
-- order events the walk appends.
GRANT INSERT ON agb.reconcile_run TO execution;

-- The audit log gains the reconcile, one branch more. The thirteen branches are recreated
-- here whole, as every migration that adds one must: a view is replaced, never extended.
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
LEFT JOIN agb.basket b ON b.id = e.basket_id
UNION ALL
SELECT r.at, 'reconcile_run', r.id, r.actor,
       'basket reconciled',
       b.kind || ' basket ' || coalesce(b.reference, b.id::text),
       r.detail
FROM agb.reconcile_run r
JOIN agb.basket b ON b.id = r.basket_id;

GRANT SELECT ON agb.audit_log TO scheduler, pipeline, execution, bot, ui, cli;
