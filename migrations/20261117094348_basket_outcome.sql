-- How a submitted basket came out at the close: filled in full, filled in part, or not filled
-- at all.
--
-- Until now reconcile moved every submitted basket to 'filled' once the broker was done with
-- each order, so a basket whose twenty orders all expired unfilled read as filled, and the
-- next rebalance's book was built from targets that were never bought. A basket that filled
-- nothing now ends in its own state, 'unfilled'. A basket that filled some of its orders, or
-- some of an order, still ends 'filled', since the book is read from what the broker holds;
-- its outcome row says it filled in part.

INSERT INTO agb.basket_transition (from_state, to_state, role) VALUES
    ('submitted', 'unfilled', 'execution');

-- One row per reconciled basket, written by execution in the same transaction as the event
-- that settled the basket, which it names. Each order the broker is done with is counted
-- once: filled in full, filled in part, or unfilled (expired, rejected or cancelled with
-- nothing filled). The outcome follows from the counts, so the two cannot disagree.
CREATE TABLE agb.basket_outcome (
    id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    basket_id  bigint NOT NULL UNIQUE REFERENCES agb.basket,
    event_id   bigint NOT NULL UNIQUE REFERENCES agb.basket_event,
    outcome    text NOT NULL CHECK (outcome IN ('filled', 'partly_filled', 'unfilled')),
    orders     integer NOT NULL CHECK (orders >= 0),
    in_full    integer NOT NULL CHECK (in_full >= 0),
    in_part    integer NOT NULL CHECK (in_part >= 0),
    unfilled   integer NOT NULL CHECK (unfilled >= 0),
    actor      agb.actor NOT NULL,
    at         timestamptz NOT NULL DEFAULT now(),
    CHECK (in_full + in_part + unfilled = orders),
    CHECK (outcome = CASE WHEN in_part = 0 AND unfilled = 0 THEN 'filled'
                          WHEN in_full = 0 AND in_part = 0 THEN 'unfilled'
                          ELSE 'partly_filled' END)
);
SELECT agb.make_append_only('agb.basket_outcome');

-- A basket that filled nothing is a settled rebalance and not a clean one: it ends a run of
-- clean live rebalances as a rejection, a missed approval or a halt does. The columns are
-- unchanged; only the settled states gain 'unfilled'.
CREATE OR REPLACE VIEW agb.ladder_rebalance AS
SELECT s.id AS basket_id, s.reference, s.period, s.session_date, s.state, s.since, r.mode
FROM agb.basket_state s
JOIN LATERAL (
    SELECT g.mode FROM agb.gate_run g WHERE g.basket_id = s.id ORDER BY g.id DESC LIMIT 1
) r ON true
WHERE s.kind = 'live' AND s.state IN ('filled', 'unfilled', 'rejected', 'missed', 'halted');

GRANT SELECT ON agb.basket_outcome TO scheduler, pipeline, execution, bot, ui, cli;
GRANT INSERT ON agb.basket_outcome TO execution;
