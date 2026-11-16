-- A liquidation's closing orders and their fills, so the next snapshot can account for them.
--
-- Alpaca's close-all answers with one closing order per symbol. Those orders belong to no
-- basket, so they cannot go in `agb.broker_order`, and until now their fills were skipped
-- when the fills were recorded. The next snapshot then expected the closed positions to
-- still be held and flagged every one of them in `agb.position_mismatch`.
--
-- `agb.liquidation_order` is the broker's answer, one row per symbol, written as soon as
-- close-all returns and before anything else that could fail. It is the record a retry
-- reads back, so a second close-all is never sent for a confirmation that already has an
-- answer. `agb.liquidation_fill` holds the fills on those orders, in the shape of `agb.fill`.
CREATE TABLE agb.liquidation_order (
    id               bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    -- The operator's confirmation this close-all carried out.
    confirm_id       bigint NOT NULL REFERENCES agb.liquidation,
    symbol           text NOT NULL CHECK (symbol <> ''),
    -- The HTTP status the broker answered for this symbol: 2xx is a closing order.
    status           integer NOT NULL,
    -- The closing order's id at the broker; a symbol that failed to close has none.
    broker_order_id  text UNIQUE CHECK (broker_order_id <> ''),
    message          text,
    at               timestamptz NOT NULL DEFAULT now(),
    actor            agb.actor NOT NULL
);
-- No uniqueness on the symbol: an entry the broker sent without one is recorded as `?`, and
-- the answer is kept whole rather than refused.
CREATE INDEX liquidation_order_confirm ON agb.liquidation_order (confirm_id, id);
SELECT agb.make_append_only('agb.liquidation_order');

-- A closing order answers an operator's confirmation, not a token, refusal or result.
CREATE FUNCTION agb.liquidation_order_check() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    prior  text;
BEGIN
    SELECT l.kind INTO prior FROM agb.liquidation l WHERE l.id = NEW.confirm_id;
    IF prior IS DISTINCT FROM 'confirm' THEN
        RAISE EXCEPTION 'liquidation_order: a closing order answers a confirmation, not a %',
            prior
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END;
$$;
REVOKE EXECUTE ON FUNCTION agb.liquidation_order_check() FROM PUBLIC;

CREATE TRIGGER answers_a_confirmation BEFORE INSERT ON agb.liquidation_order
    FOR EACH ROW EXECUTE FUNCTION agb.liquidation_order_check();

CREATE TABLE agb.liquidation_fill (
    id                 bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id           bigint NOT NULL REFERENCES agb.liquidation_order,
    -- The broker's id for the execution.
    execution_id       text NOT NULL UNIQUE CHECK (execution_id <> ''),
    -- A closing order sells a long and buys back a short; the broker's `sell_short` is a sale.
    side               text NOT NULL CHECK (side IN ('buy', 'sell')),
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
SELECT agb.make_append_only('agb.liquidation_fill');
CREATE INDEX liquidation_fill_order ON agb.liquidation_fill (order_id, id);

GRANT SELECT ON agb.liquidation_order, agb.liquidation_fill
    TO scheduler, pipeline, execution, bot, ui, cli;
GRANT INSERT ON agb.liquidation_order, agb.liquidation_fill TO execution;

-- The position check counts a liquidation's fills with the basket fills, so a liquidation
-- between two snapshots is not reported as a mismatch.
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
        SELECT o.symbol, CASE WHEN f.side = 'buy' THEN f.qty ELSE -f.qty END
        FROM agb.liquidation_fill f
        JOIN agb.liquidation_order o ON o.id = f.order_id
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
