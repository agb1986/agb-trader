-- Forced corporate events: what the market did to a holding without the system asking.
--
-- Four cases the ranker defines (docs/architecture/02-classical-ranker.md), handled the same
-- way in the backtest and in live trading:
--   cash_acquisition  the position terminates at the deal price and the cash waits for the
--                     next rebalance; it is not redeployed mid-month;
--   stock_merger      acquirer shares arrive in the account; they are sold at the next
--                     rebalance whatever the acquirer's rank, because the position was never
--                     chosen;
--   delisting         the name leaves the listed market; the position exits at the last
--                     listed print, modelled rather than dropped;
--   halt              trading is suspended across the rebalance; the position is carried, its
--                     replacement is skipped, and the basket runs one name short.
--
-- A recorded event is what makes the shortfall legitimate. The pre-trade gate accepts fewer
-- than the full twenty positions only where a row here explains each carried name, and an
-- unexplained carry halts: a name that has quietly stopped printing looks exactly like a
-- halted one, and the difference is a human's judgement, not an inference.
--
-- Nothing here is written by the system. An operator records the event with `agb corporate`
-- once the broker's statement or the exchange notice is in hand, which is why every row
-- carries an actor, a surface and a note saying where the fact came from.

CREATE TABLE agb.forced_event (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    -- The rebalance period the event bears on, as agb.basket names it.
    period          text NOT NULL CHECK (period ~ '^\d{4}-\d{2}$'),
    symbol          text NOT NULL CHECK (symbol <> ''),
    kind            text NOT NULL CHECK (
                        kind IN ('cash_acquisition', 'stock_merger', 'delisting', 'halt')),
    -- The session the action took effect on, by the exchange's notice.
    effective_date  date NOT NULL,
    -- The holding before the action, and what the action itself left: a cash acquisition and a
    -- stock-for-stock merger terminate the position without a fill, while a delisting exit and
    -- a halt take nothing away, so for those two the two numbers are equal and the exit, when
    -- it comes, is an ordinary fill.
    shares_before   numeric NOT NULL CHECK (shares_before > 0),
    shares_after    numeric NOT NULL CHECK (shares_after >= 0),
    -- The price the action settled at: the deal price for a cash acquisition, the last listed
    -- print for a delisting. A halt and a stock-for-stock merger settle in no cash.
    price_usd       numeric CHECK (price_usd > 0),
    -- What arrived instead, for a stock-for-stock merger: the acquirer and how many shares.
    into_symbol     text CHECK (into_symbol <> ''),
    into_shares     numeric CHECK (into_shares > 0),
    -- Where the operator learned the fact, kept for the audit.
    detail          text NOT NULL CHECK (detail <> ''),
    at              timestamptz NOT NULL DEFAULT now(),
    actor           agb.actor NOT NULL,
    surface         text NOT NULL CHECK (surface IN ('cli', 'ui', 'telegram')),
    -- The command that recorded it, so the typed text is one join away.
    command_id      bigint NOT NULL REFERENCES agb.command,
    -- One row per event, so the share deltas below are never counted twice.
    UNIQUE (period, symbol, kind),
    CHECK ((into_symbol IS NULL) = (into_shares IS NULL)),
    CHECK ((kind = 'stock_merger') = (into_symbol IS NOT NULL)),
    CHECK (kind NOT IN ('cash_acquisition', 'stock_merger') OR shares_after = 0),
    CHECK (kind NOT IN ('delisting', 'halt') OR shares_after = shares_before),
    CHECK (kind <> 'cash_acquisition' OR price_usd IS NOT NULL),
    CHECK (kind <> 'halt' OR price_usd IS NULL)
);
SELECT agb.make_append_only('agb.forced_event');
CREATE INDEX forced_event_period ON agb.forced_event (period, id);
CREATE INDEX forced_event_effective ON agb.forced_event (effective_date, id);

GRANT SELECT ON agb.forced_event TO scheduler, pipeline, execution, bot, ui, cli, research;
-- The operator records one; no service does.
GRANT INSERT ON agb.forced_event TO cli, ui;

-- The daily snapshot check, with the recorded forced events folded in. Without them a
-- corporate action fires the mismatch alert, as it should: the broker's holdings changed and
-- no fill explains it. Once the event is on the record the share delta it caused is part of
-- what the snapshot is expected to show, so the alert goes quiet for that symbol and stays
-- loud for every other unexplained change. The view is recomputed from the record, so
-- recording the event after the snapshot was read quiets the alert then too.
CREATE OR REPLACE VIEW agb.position_mismatch AS
WITH snap AS (
    SELECT s.id, s.snapshot_date, s.observed_at,
           lag(s.id) OVER w AS prev_id,
           lag(s.snapshot_date) OVER w AS prev_snapshot_date,
           lag(s.observed_at) OVER w AS prev_observed_at
    FROM agb.position_snapshot s
    WINDOW w AS (ORDER BY s.snapshot_date)
),
expected AS (
    SELECT snap.id AS snapshot_id, x.symbol, sum(x.qty) AS qty
    FROM snap
    CROSS JOIN LATERAL (
        SELECT p.symbol, p.qty FROM agb.position p WHERE p.snapshot_id = snap.prev_id
        UNION ALL
        SELECT o.symbol, CASE WHEN o.side = 'buy' THEN f.qty ELSE -f.qty END
        FROM agb.fill f
        JOIN agb.broker_order o ON o.id = f.order_id
        WHERE f.filled_at > snap.prev_observed_at AND f.filled_at <= snap.observed_at
        UNION ALL
        SELECT e.symbol, e.shares_after - e.shares_before
        FROM agb.forced_event e
        WHERE e.effective_date > snap.prev_snapshot_date
          AND e.effective_date <= snap.snapshot_date
        UNION ALL
        SELECT e.into_symbol, e.into_shares
        FROM agb.forced_event e
        WHERE e.into_symbol IS NOT NULL
          AND e.effective_date > snap.prev_snapshot_date
          AND e.effective_date <= snap.snapshot_date
    ) x
    WHERE snap.prev_id IS NOT NULL
    GROUP BY snap.id, x.symbol
),
observed AS (
    SELECT p.snapshot_id, p.symbol, p.qty
    FROM agb.position p
    JOIN snap ON snap.id = p.snapshot_id
    WHERE snap.prev_id IS NOT NULL
)
SELECT s.id AS snapshot_id, s.snapshot_date,
       coalesce(e.symbol, o.symbol) AS symbol,
       coalesce(e.qty, 0) AS expected_qty,
       coalesce(o.qty, 0) AS observed_qty
FROM expected e
FULL JOIN observed o ON o.snapshot_id = e.snapshot_id AND o.symbol = e.symbol
JOIN agb.position_snapshot s ON s.id = coalesce(e.snapshot_id, o.snapshot_id)
WHERE coalesce(e.qty, 0) <> coalesce(o.qty, 0);

GRANT SELECT ON agb.position_mismatch TO scheduler, pipeline, execution, bot, ui, cli;

-- The audit log gains the forced corporate event, one branch more. The fifteen branches are
-- recreated here whole, as every migration that adds one must: a view is replaced, never
-- extended.
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
JOIN agb.basket b ON b.id = r.basket_id
UNION ALL
SELECT e.at, 'forced_event', e.id, e.actor,
       'forced ' || e.kind,
       e.symbol || ' ' || e.period,
       e.detail
FROM agb.forced_event e;

GRANT SELECT ON agb.audit_log TO scheduler, pipeline, execution, bot, ui, cli;
