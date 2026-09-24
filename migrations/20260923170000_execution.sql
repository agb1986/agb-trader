-- Paper MOC execution: broker checks, orders, fills, position snapshots and the observed
-- broker state.
--
-- Execution is the only service that talks to the broker. It picks up an approved basket
-- late in its trade session, checks the broker's own data on every name (tradable, and
-- fractionable where the quantity needs it) and on the account (nothing blocked, not
-- suspended), and only then submits one market-on-close order per planned order, writing
-- the order's row before each broker call so a crash between the call and the answer is
-- recovered by looking the order up under its client_order_id, never by sending it again.
-- After the close it appends every broker state change it observes, records each fill on
-- its own row with the regulatory fees the shared schedule charges, and moves the basket
-- to filled. Once a session it snapshots the broker's positions and the account flags it
-- reads, so the halt latch can be compared with the broker's `suspend_trade` and a
-- snapshot with the previous one plus the day's fills.

-- Broker checks halt an approved basket the way gate checks halt a proposed one.
INSERT INTO agb.basket_transition (from_state, to_state, role) VALUES
    ('approved', 'halted', 'execution');

-- Execution opens gate_breach for a broker-check failure; its own lease cause stays.
CREATE POLICY halt_event_broker ON agb.halt_event FOR INSERT TO execution
    WITH CHECK (cause = 'gate_breach' AND action = 'set');

-- One run of the broker checks per basket, and how many checks it ran. A basket is checked
-- once: a later attempt reads the verdict back.
CREATE TABLE agb.broker_check_run (
    id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    basket_id  bigint NOT NULL UNIQUE REFERENCES agb.basket,
    checks     integer NOT NULL CHECK (checks > 0),
    at         timestamptz NOT NULL DEFAULT now(),
    actor      agb.actor NOT NULL
);
SELECT agb.make_append_only('agb.broker_check_run');

-- The broker checks' result, check by check, in the gate's shape: what the broker reported
-- and what it had to be, both as text. An account-level check has no symbol; an order-level
-- check names the order's.
CREATE TABLE agb.broker_check (
    id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id     bigint NOT NULL REFERENCES agb.broker_check_run,
    level      text NOT NULL CHECK (level IN ('account', 'order')),
    name       text NOT NULL CHECK (name ~ '^[a-z][a-z_]*$'),
    symbol     text CHECK (symbol <> ''),
    measured   text NOT NULL,
    threshold  text NOT NULL,
    passed     boolean NOT NULL,
    UNIQUE NULLS NOT DISTINCT (run_id, level, name, symbol),
    CHECK ((level = 'order') = (symbol IS NOT NULL))
);
SELECT agb.make_append_only('agb.broker_check');

-- Order intent, written before the broker is called. One row per planned order the service
-- decided to send: the deterministic client_order_id the broker deduplicates on, what is
-- ordered, the modelled cost frozen at proposal under its cost-model version, and the NBBO
-- reference quote read just before submission, which the shortfall view measures fills
-- against. The row's creation is the moment before the HTTP call; the broker's answer is
-- an order_event.
CREATE TABLE agb.broker_order (
    id                  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    basket_id           bigint NOT NULL REFERENCES agb.basket,
    basket_order_id     bigint NOT NULL UNIQUE REFERENCES agb.basket_order,
    client_order_id     text NOT NULL UNIQUE
                        CHECK (client_order_id <> '' AND length(client_order_id) <= 128),
    symbol              text NOT NULL CHECK (symbol <> ''),
    side                text NOT NULL CHECK (side IN ('buy', 'sell')),
    qty                 numeric NOT NULL CHECK (qty > 0),
    tif                 text NOT NULL DEFAULT 'cls' CHECK (tif = 'cls'),
    cost_model_version  text NOT NULL CHECK (cost_model_version <> ''),
    modelled_cost_bps   numeric NOT NULL,
    nbbo_bid            numeric NOT NULL CHECK (nbbo_bid > 0),
    nbbo_ask            numeric NOT NULL CHECK (nbbo_ask > 0),
    nbbo_mid            numeric NOT NULL CHECK (nbbo_mid > 0),
    nbbo_at             timestamptz NOT NULL,
    created_at          timestamptz NOT NULL DEFAULT now(),
    actor               agb.actor NOT NULL,
    CHECK (nbbo_mid = (nbbo_bid + nbbo_ask) / 2)
);
SELECT agb.make_append_only('agb.broker_order');
CREATE INDEX broker_order_basket ON agb.broker_order (basket_id, id);

-- What the broker said about an order, in the order it was heard: the acknowledgement that
-- carries the broker's own order id (`ack`, or `lookup` when it was found under its
-- client_order_id after the broker refused a duplicate or a crash lost the answer), each
-- status change seen afterwards, a refusal, and each cancel request with its result.
CREATE TABLE agb.order_event (
    id                bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id          bigint NOT NULL REFERENCES agb.broker_order,
    kind              text NOT NULL CHECK (kind IN (
                          'ack', 'lookup', 'rejected', 'status',
                          'cancel_requested', 'cancelled', 'cancel_failed')),
    broker_order_id   text CHECK (broker_order_id <> ''),
    -- The broker's status word as it reported it.
    status            text NOT NULL CHECK (status ~ '^[a-z][a-z_]*$'),
    filled_qty        numeric NOT NULL DEFAULT 0 CHECK (filled_qty >= 0),
    filled_avg_price  numeric CHECK (filled_avg_price > 0),
    detail            text,
    at                timestamptz NOT NULL DEFAULT now(),
    actor             agb.actor NOT NULL,
    CHECK ((kind IN ('ack', 'lookup')) <= (broker_order_id IS NOT NULL))
);
SELECT agb.make_append_only('agb.order_event');
CREATE INDEX order_event_order ON agb.order_event (order_id, id);

-- Each fill the broker reported, one row per execution, so a partial fill is several rows.
-- The fees are the regulatory schedule's charge on this fill alone, under the fee model
-- version that computed them; buys carry none.
CREATE TABLE agb.fill (
    id                 bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id           bigint NOT NULL REFERENCES agb.broker_order,
    -- The broker's id for the execution.
    execution_id       text NOT NULL UNIQUE CHECK (execution_id <> ''),
    qty                numeric NOT NULL CHECK (qty > 0),
    price              numeric NOT NULL CHECK (price > 0),
    filled_at          timestamptz NOT NULL,
    sec_fee_usd        numeric NOT NULL DEFAULT 0 CHECK (sec_fee_usd >= 0),
    taf_fee_usd        numeric NOT NULL DEFAULT 0 CHECK (taf_fee_usd >= 0),
    cat_fee_usd        numeric NOT NULL DEFAULT 0 CHECK (cat_fee_usd >= 0),
    commission_usd     numeric NOT NULL DEFAULT 0 CHECK (commission_usd >= 0),
    fee_model_version  text NOT NULL CHECK (fee_model_version <> ''),
    recorded_at        timestamptz NOT NULL DEFAULT now(),
    actor              agb.actor NOT NULL
);
SELECT agb.make_append_only('agb.fill');
CREATE INDEX fill_order ON agb.fill (order_id, id);

-- The broker's positions as they stood once a session, read after the close. The snapshot
-- row says the read happened, so an empty account is a snapshot with no positions rather
-- than no snapshot.
CREATE TABLE agb.position_snapshot (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    snapshot_date  date NOT NULL UNIQUE,
    observed_at    timestamptz NOT NULL DEFAULT now(),
    positions      integer NOT NULL CHECK (positions >= 0),
    actor          agb.actor NOT NULL
);
SELECT agb.make_append_only('agb.position_snapshot');

CREATE TABLE agb.position (
    id                bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    snapshot_id       bigint NOT NULL REFERENCES agb.position_snapshot,
    symbol            text NOT NULL CHECK (symbol <> ''),
    -- Signed: a short, should one ever appear, is negative.
    qty               numeric NOT NULL CHECK (qty <> 0),
    avg_entry_price   numeric CHECK (avg_entry_price > 0),
    market_value_usd  numeric,
    UNIQUE (snapshot_id, symbol)
);
SELECT agb.make_append_only('agb.position');

-- The account state the broker reported, each time execution read it: the flags that
-- mirror the halt latch, and the balances.
CREATE TABLE agb.broker_state (
    id                bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    observed_at       timestamptz NOT NULL DEFAULT now(),
    account_status    text NOT NULL CHECK (account_status <> ''),
    suspend_trade     boolean NOT NULL,
    no_shorting       boolean NOT NULL,
    trading_blocked   boolean NOT NULL,
    account_blocked   boolean NOT NULL,
    equity_usd        numeric,
    cash_usd          numeric,
    buying_power_usd  numeric,
    actor             agb.actor NOT NULL
);
SELECT agb.make_append_only('agb.broker_state');

-- The latest account state read.
CREATE VIEW agb.broker_state_now AS
SELECT s.observed_at, s.account_status, s.suspend_trade, s.no_shorting, s.trading_blocked,
       s.account_blocked, s.equity_usd, s.cash_usd, s.buying_power_usd
FROM agb.broker_state s
ORDER BY s.id DESC
LIMIT 1;

-- Each order's latest word from the broker.
CREATE VIEW agb.order_state AS
SELECT o.id AS order_id, o.basket_id, o.client_order_id, o.symbol, o.side, o.qty,
       latest.kind, latest.status, latest.filled_qty, latest.filled_avg_price, latest.at,
       ids.broker_order_id
FROM agb.broker_order o
LEFT JOIN LATERAL (
    SELECT e.kind, e.status, e.filled_qty, e.filled_avg_price, e.at
    FROM agb.order_event e
    WHERE e.order_id = o.id
    ORDER BY e.id DESC
    LIMIT 1
) latest ON true
LEFT JOIN LATERAL (
    SELECT e.broker_order_id
    FROM agb.order_event e
    WHERE e.order_id = o.id AND e.broker_order_id IS NOT NULL
    ORDER BY e.id
    LIMIT 1
) ids ON true;

-- Shortfall: the fill-weighted average price against the reference mid, signed so that a
-- worse fill is positive on either side, in basis points, beside the modelled cost.
CREATE VIEW agb.shortfall AS
SELECT o.id AS order_id, o.basket_id, o.client_order_id, o.symbol, o.side, o.qty,
       o.nbbo_mid AS reference_mid,
       sum(f.qty) AS filled_qty,
       sum(f.qty * f.price) / sum(f.qty) AS avg_fill_price,
       CASE WHEN o.side = 'buy' THEN 1 ELSE -1 END
           * (sum(f.qty * f.price) / sum(f.qty) - o.nbbo_mid) / o.nbbo_mid * 10000
           AS shortfall_bps,
       o.modelled_cost_bps,
       o.cost_model_version,
       sum(f.sec_fee_usd + f.taf_fee_usd + f.cat_fee_usd + f.commission_usd) AS fees_usd,
       count(f.id) AS fills,
       max(f.filled_at) AS last_fill_at
FROM agb.broker_order o
JOIN agb.fill f ON f.order_id = o.id
GROUP BY o.id;

-- Each snapshot against the previous one plus the fills between the two reads: every
-- symbol whose quantity is not what the fills explain. The first snapshot has nothing to
-- compare with. A corporate action fires this, and should.
CREATE VIEW agb.position_mismatch AS
WITH snap AS (
    SELECT s.id, s.snapshot_date, s.observed_at,
           lag(s.id) OVER w AS prev_id,
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

GRANT SELECT ON agb.broker_check_run, agb.broker_check, agb.broker_order, agb.order_event,
    agb.fill, agb.position_snapshot, agb.position, agb.broker_state, agb.broker_state_now,
    agb.order_state, agb.shortfall, agb.position_mismatch
    TO scheduler, pipeline, execution, bot, ui, cli;
GRANT INSERT ON agb.broker_check_run, agb.broker_check, agb.broker_order, agb.order_event,
    agb.fill, agb.position_snapshot, agb.position, agb.broker_state TO execution;
