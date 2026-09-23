-- Baskets: what the pipeline proposes each rebalance, and the append-only event log that
-- moves a basket through its states.
--
-- A basket's content is frozen at proposal: its target weights, the stage-1 ranks it was built
-- from, its order plan with each order's modelled cost under the cost-model version of the
-- day, and the provenance that ties it to the lake (snapshot date, partition digest) and to the
-- ranker that produced the ranks. Nothing here is ever updated. What happened to a basket
-- afterwards is told by basket_event.

-- One row per proposed basket. `reference` is the human handle ('2026-10' for the rebalance
-- traded in October 2026; a rerun is '2026-10r1' and names the basket it deviates from). A
-- shadow basket, built for comparison and never traded, has no reference.
CREATE TABLE agb.basket (
    id                  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    kind                text NOT NULL DEFAULT 'live' CHECK (kind IN ('live', 'shadow')),
    reference           text UNIQUE CHECK (reference ~ '^\d{4}-\d{2}(r\d+)?$'),
    period              text NOT NULL CHECK (period ~ '^\d{4}-\d{2}$'),
    deviation_of        bigint REFERENCES agb.basket,
    job_id              bigint REFERENCES agb.job,
    -- The trade session the basket is for, and the session whose close it was signalled from.
    session_date        date NOT NULL,
    signal_date         date NOT NULL,
    -- Provenance: the session the universe's prices came from, the digest of the lake
    -- partitions the universe was built from, how many names it held, and the ranker.
    snapshot_date       date NOT NULL,
    partition_hash      text NOT NULL CHECK (partition_hash ~ '^[0-9a-f]{64}$'),
    universe_size       integer NOT NULL CHECK (universe_size >= 0),
    ranker              text NOT NULL CHECK (ranker <> ''),
    -- The cost-model version every order's modelled cost was frozen under.
    cost_model_version  text NOT NULL CHECK (cost_model_version <> ''),
    -- The capital the target weights were sized against, in dollars.
    capital_usd         numeric NOT NULL CHECK (capital_usd > 0),
    created_at          timestamptz NOT NULL DEFAULT now(),
    actor               agb.actor NOT NULL,
    CHECK ((kind = 'shadow') = (reference IS NULL)),
    CHECK (signal_date < session_date),
    CHECK (snapshot_date <= signal_date)
);
SELECT agb.make_append_only('agb.basket');
CREATE INDEX basket_period ON agb.basket (period, id);

-- The stage-1 ranking the basket was selected from: every name in the universe, best first.
CREATE TABLE agb.basket_rank (
    id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    basket_id  bigint NOT NULL REFERENCES agb.basket,
    symbol     text NOT NULL CHECK (symbol <> ''),
    rank       integer NOT NULL CHECK (rank >= 1),
    score      numeric,
    UNIQUE (basket_id, symbol),
    UNIQUE (basket_id, rank)
);
SELECT agb.make_append_only('agb.basket_rank');

-- The basket's targets: each name it holds after the rebalance and its weight. `held` is
-- true for an incumbent kept from the book, false for a name bought this rebalance. A carried
-- name is an incumbent that could not trade on the signal session (a halt spanning the
-- rebalance); it keeps its slot and gets no order.
CREATE TABLE agb.basket_target (
    id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    basket_id  bigint NOT NULL REFERENCES agb.basket,
    symbol     text NOT NULL CHECK (symbol <> ''),
    weight     numeric NOT NULL CHECK (weight > 0 AND weight <= 1),
    -- The position after the rebalance, in whole shares: what the next rebalance's book
    -- reads once the basket has filled.
    shares     numeric NOT NULL CHECK (shares > 0),
    held       boolean NOT NULL,
    carried    boolean NOT NULL DEFAULT false,
    rank       integer,
    UNIQUE (basket_id, symbol),
    CHECK (NOT carried OR held)
);
SELECT agb.make_append_only('agb.basket_target');

-- The order plan: one market-on-close order per name entering or leaving the basket, with
-- its modelled cost frozen at proposal. client_order_id is deterministic from the basket's
-- reference and the symbol, so a resubmission cannot duplicate an order at the broker.
CREATE TABLE agb.basket_order (
    id                  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    basket_id           bigint NOT NULL REFERENCES agb.basket,
    client_order_id     text NOT NULL UNIQUE CHECK (client_order_id <> ''),
    symbol              text NOT NULL CHECK (symbol <> ''),
    side                text NOT NULL CHECK (side IN ('buy', 'sell')),
    qty                 numeric NOT NULL CHECK (qty > 0),
    tif                 text NOT NULL DEFAULT 'cls' CHECK (tif = 'cls'),
    -- The close the order was sized at, as traded, and the notional it implies.
    reference_price     numeric NOT NULL CHECK (reference_price > 0),
    notional_usd        numeric NOT NULL CHECK (notional_usd > 0),
    -- The modelled cost, under the version named on the basket.
    cost_model_version  text NOT NULL CHECK (cost_model_version <> ''),
    floor_bps           numeric NOT NULL CHECK (floor_bps >= 0),
    impact_bps          numeric NOT NULL CHECK (impact_bps >= 0),
    fees_usd            numeric NOT NULL CHECK (fees_usd >= 0),
    modelled_cost_bps   numeric NOT NULL CHECK (modelled_cost_bps >= 0),
    UNIQUE (basket_id, symbol)
);
SELECT agb.make_append_only('agb.basket_order');

-- Which role may move a basket from one state to the next. The first event of a basket has
-- no from_state. A transition not in this table is refused for every role, the owner
-- included. Later migrations add rows; none is ever removed.
CREATE TABLE agb.basket_transition (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    from_state  text CHECK (from_state ~ '^[a-z][a-z_]*$'),
    to_state    text NOT NULL CHECK (to_state ~ '^[a-z][a-z_]*$'),
    role        name NOT NULL,
    UNIQUE NULLS NOT DISTINCT (from_state, to_state, role)
);
SELECT agb.make_append_only('agb.basket_transition');

INSERT INTO agb.basket_transition (from_state, to_state, role) VALUES
    -- The pipeline proposes, and halts a proposal at the pre-trade gate.
    (NULL,       'proposed',  'pipeline'),
    ('proposed', 'halted',    'pipeline'),
    ('proposed', 'halted',    'execution'),
    -- The approval request puts the basket to the human; only the human's surfaces decide.
    ('proposed', 'pending',   'bot'),
    ('proposed', 'pending',   'ui'),
    ('pending',  'approved',  'bot'),
    ('pending',  'approved',  'ui'),
    ('pending',  'rejected',  'bot'),
    ('pending',  'rejected',  'ui'),
    -- Only the scheduler marks a basket missed, at whatever stage the deadline passed it.
    ('proposed', 'missed',    'scheduler'),
    ('pending',  'missed',    'scheduler'),
    ('approved', 'missed',    'scheduler'),
    -- Only execution submits an approved basket, and reports it filled.
    ('approved', 'submitted', 'execution'),
    ('submitted', 'filled',   'execution');

-- A basket's lifecycle: every state change, who made it and through which surface. `seq`
-- runs from 1 per basket, and unique (basket_id, seq) is the compare-and-set: an insert
-- names the seq it expects to be next, so two writers acting on the same state cannot both
-- succeed.
CREATE TABLE agb.basket_event (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    basket_id   bigint NOT NULL REFERENCES agb.basket,
    seq         integer NOT NULL CHECK (seq >= 1),
    from_state  text CHECK (from_state ~ '^[a-z][a-z_]*$'),
    to_state    text NOT NULL CHECK (to_state ~ '^[a-z][a-z_]*$'),
    actor       agb.actor NOT NULL,
    surface     text NOT NULL CHECK (
                    surface IN ('telegram', 'ui', 'cli', 'scheduler', 'pipeline', 'execution')),
    at          timestamptz NOT NULL DEFAULT now(),
    reason      text,
    UNIQUE (basket_id, seq),
    CHECK ((seq = 1) = (from_state IS NULL))
);
SELECT agb.make_append_only('agb.basket_event');

-- Refuses an event that does not follow the basket's latest one, or a transition that
-- basket_transition does not allow the inserting role. Runs as the inserting role, so
-- current_user is the service that connected.
CREATE FUNCTION agb.basket_event_check() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    latest_seq    integer;
    latest_state  text;
BEGIN
    SELECT e.seq, e.to_state INTO latest_seq, latest_state
    FROM agb.basket_event e
    WHERE e.basket_id = NEW.basket_id
    ORDER BY e.seq DESC
    LIMIT 1;

    IF coalesce(latest_seq, 0) + 1 <> NEW.seq THEN
        RAISE EXCEPTION 'basket %: seq % does not follow its latest event (seq %)',
            NEW.basket_id, NEW.seq, coalesce(latest_seq, 0)
            USING ERRCODE = 'check_violation';
    END IF;
    IF NEW.from_state IS DISTINCT FROM latest_state THEN
        RAISE EXCEPTION 'basket %: it is % and not %',
            NEW.basket_id, coalesce(latest_state, 'unproposed'), coalesce(NEW.from_state, 'unproposed')
            USING ERRCODE = 'check_violation';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM agb.basket_transition t
        WHERE t.from_state IS NOT DISTINCT FROM NEW.from_state
          AND t.to_state = NEW.to_state
          AND t.role = current_user
    ) THEN
        RAISE EXCEPTION 'basket_transition: % may not move basket % from % to %',
            current_user, NEW.basket_id, coalesce(NEW.from_state, 'unproposed'), NEW.to_state
            USING ERRCODE = 'insufficient_privilege';
    END IF;
    RETURN NEW;
END;
$$;
REVOKE EXECUTE ON FUNCTION agb.basket_event_check() FROM PUBLIC;

CREATE TRIGGER transition_allowed BEFORE INSERT ON agb.basket_event
    FOR EACH ROW EXECUTE FUNCTION agb.basket_event_check();

-- Each basket with its current state: the to_state of its latest event.
CREATE VIEW agb.basket_state AS
SELECT b.id, b.kind, b.reference, b.period, b.session_date, b.job_id,
       latest.to_state AS state, latest.seq, latest.at AS since, latest.actor, latest.surface
FROM agb.basket b
JOIN LATERAL (
    SELECT e.to_state, e.seq, e.at, e.actor, e.surface
    FROM agb.basket_event e
    WHERE e.basket_id = b.id
    ORDER BY e.seq DESC
    LIMIT 1
) latest ON true;

GRANT SELECT ON agb.basket, agb.basket_rank, agb.basket_target, agb.basket_order,
    agb.basket_transition, agb.basket_event, agb.basket_state
    TO scheduler, pipeline, execution, bot, ui, cli;
GRANT INSERT ON agb.basket, agb.basket_rank, agb.basket_target, agb.basket_order TO pipeline;
GRANT INSERT ON agb.basket_event TO scheduler, pipeline, execution, bot, ui;
