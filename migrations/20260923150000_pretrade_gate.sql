-- The pre-trade gate, the halt latch and the trading mode.
--
-- The gate is the last step of the rebalance: it checks the proposed basket's provenance, the
-- basket as a whole and every order, and records each check with what it measured. A single
-- failure halts the rebalance (the basket moves to halted and the latch's gate_breach cause
-- opens), so the book stays as last month left it. In paper mode a basket that passes every
-- check is approved by the system; in live mode it waits for the human.
--
-- Once the gate has run, the basket's content is frozen: no rank, target or order can be
-- added, and the gate's own result cannot be extended.

-- One row per basket the gate has run on, and how many checks it ran. A basket is gated once:
-- a rerun of the rebalance finds the run and reads its verdict back.
CREATE TABLE agb.gate_run (
    id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    basket_id  bigint NOT NULL UNIQUE REFERENCES agb.basket,
    checks     integer NOT NULL CHECK (checks > 0),
    -- The trading mode the gate read, which decided whether a pass approves the basket.
    mode       text NOT NULL CHECK (mode IN ('paper', 'live')),
    at         timestamptz NOT NULL DEFAULT now(),
    actor      agb.actor NOT NULL
);
SELECT agb.make_append_only('agb.gate_run');

-- The gate's result, check by check. `measured` is what the check found and `threshold` what
-- it had to meet, both as text, so a failure reads without the code that produced it. A
-- basket-level check has no symbol; an order-level check names the order's.
CREATE TABLE agb.gate_check (
    id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id     bigint NOT NULL REFERENCES agb.gate_run,
    level      text NOT NULL CHECK (level IN ('provenance', 'basket', 'order')),
    name       text NOT NULL CHECK (name ~ '^[a-z][a-z_]*$'),
    symbol     text CHECK (symbol <> ''),
    measured   text NOT NULL,
    threshold  text NOT NULL,
    passed     boolean NOT NULL,
    UNIQUE NULLS NOT DISTINCT (run_id, level, name, symbol),
    CHECK ((level = 'order') = (symbol IS NOT NULL))
);
SELECT agb.make_append_only('agb.gate_check');

-- The gate runs on a proposed basket only.
CREATE FUNCTION agb.gate_run_check() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    current_state  text;
BEGIN
    SELECT e.to_state INTO current_state
    FROM agb.basket_event e
    WHERE e.basket_id = NEW.basket_id
    ORDER BY e.seq DESC
    LIMIT 1;
    IF current_state IS DISTINCT FROM 'proposed' THEN
        RAISE EXCEPTION 'basket %: the gate runs on a proposed basket, and it is %',
            NEW.basket_id, coalesce(current_state, 'unproposed')
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END;
$$;
REVOKE EXECUTE ON FUNCTION agb.gate_run_check() FROM PUBLIC;

CREATE TRIGGER gate_on_proposed BEFORE INSERT ON agb.gate_run
    FOR EACH ROW EXECUTE FUNCTION agb.gate_run_check();

-- A run records exactly the checks it declared, and nothing is added once they are in.
CREATE FUNCTION agb.gate_check_check() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    declared  integer;
    recorded  integer;
BEGIN
    SELECT r.checks INTO declared FROM agb.gate_run r WHERE r.id = NEW.run_id;
    SELECT count(*) INTO recorded FROM agb.gate_check c WHERE c.run_id = NEW.run_id;
    IF recorded >= declared THEN
        RAISE EXCEPTION 'gate run %: its % checks are recorded, and the result is frozen',
            NEW.run_id, declared
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END;
$$;
REVOKE EXECUTE ON FUNCTION agb.gate_check_check() FROM PUBLIC;

CREATE TRIGGER result_frozen BEFORE INSERT ON agb.gate_check
    FOR EACH ROW EXECUTE FUNCTION agb.gate_check_check();

-- Refuses new content for a basket the gate has run on: what the gate passed is what trades.
CREATE FUNCTION agb.basket_content_check() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM agb.gate_run r WHERE r.basket_id = NEW.basket_id) THEN
        RAISE EXCEPTION 'basket %: content is frozen once the gate has run: % refused',
            NEW.basket_id, TG_TABLE_NAME
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END;
$$;
REVOKE EXECUTE ON FUNCTION agb.basket_content_check() FROM PUBLIC;

CREATE TRIGGER content_frozen BEFORE INSERT ON agb.basket_rank
    FOR EACH ROW EXECUTE FUNCTION agb.basket_content_check();
CREATE TRIGGER content_frozen BEFORE INSERT ON agb.basket_target
    FOR EACH ROW EXECUTE FUNCTION agb.basket_content_check();
CREATE TRIGGER content_frozen BEFORE INSERT ON agb.basket_order
    FOR EACH ROW EXECUTE FUNCTION agb.basket_content_check();

-- The halt latch. Each row sets or clears one named cause, and trading is blocked while any
-- cause is open. A cause stays open until a clear is recorded for it; `/resume` writes one
-- clear per open cause, so each is attributed on its own.
CREATE TABLE agb.halt_event (
    id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    cause      text NOT NULL CHECK (
                   cause IN ('gate_breach', 'execution_lease_expired', 'pause', 'liquidation')),
    action     text NOT NULL CHECK (action IN ('set', 'clear')),
    -- The basket that tripped the cause, where there is one.
    basket_id  bigint REFERENCES agb.basket,
    actor      agb.actor NOT NULL,
    at         timestamptz NOT NULL DEFAULT now(),
    reason     text
);
SELECT agb.make_append_only('agb.halt_event');

-- Who may write which cause: the pipeline sets gate_breach, execution sets
-- execution_lease_expired, and the human's surfaces pause, liquidate and clear.
ALTER TABLE agb.halt_event ENABLE ROW LEVEL SECURITY;
CREATE POLICY halt_event_read ON agb.halt_event FOR SELECT USING (true);
CREATE POLICY halt_event_gate ON agb.halt_event FOR INSERT TO pipeline
    WITH CHECK (cause = 'gate_breach' AND action = 'set');
CREATE POLICY halt_event_lease ON agb.halt_event FOR INSERT TO execution
    WITH CHECK (cause = 'execution_lease_expired' AND action = 'set');
CREATE POLICY halt_event_human ON agb.halt_event FOR INSERT TO bot, ui
    WITH CHECK (cause IN ('pause', 'liquidation') OR action = 'clear');

-- The open halt causes: every cause whose latest event sets it, with that event.
CREATE VIEW agb.halt_open AS
SELECT latest.cause, latest.at, latest.basket_id, latest.actor, latest.reason
FROM (
    SELECT DISTINCT ON (e.cause) e.cause, e.action, e.at, e.basket_id, e.actor, e.reason
    FROM agb.halt_event e
    ORDER BY e.cause, e.id DESC
) latest
WHERE latest.action = 'set';

-- The trading mode. Moving up to live is a human act through the CLI, which a later
-- migration grants along with the eligibility view it checks; moving down to paper is the
-- system's, as the scheduler or execution.
CREATE TABLE agb.mode_event (
    id      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    mode    text NOT NULL CHECK (mode IN ('paper', 'live')),
    actor   agb.actor NOT NULL,
    at      timestamptz NOT NULL DEFAULT now(),
    reason  text
);
SELECT agb.make_append_only('agb.mode_event');

INSERT INTO agb.mode_event (mode, actor, reason)
    VALUES ('paper', 'system', 'the system starts in paper');

ALTER TABLE agb.mode_event ENABLE ROW LEVEL SECURITY;
CREATE POLICY mode_event_read ON agb.mode_event FOR SELECT USING (true);
CREATE POLICY mode_event_down ON agb.mode_event FOR INSERT TO scheduler, execution
    WITH CHECK (mode = 'paper' AND actor = 'system');

-- The mode in force: the latest event's.
CREATE VIEW agb.current_mode AS
SELECT e.mode, e.at AS since, e.actor
FROM agb.mode_event e
ORDER BY e.id DESC
LIMIT 1;

-- In paper mode the pipeline approves a basket that passed the gate, as the system.
INSERT INTO agb.basket_transition (from_state, to_state, role) VALUES
    ('proposed', 'approved', 'pipeline');

-- Refuses proposed to approved unless the system makes the move in paper mode, for a basket
-- whose gate run recorded all of its checks and none of them failed. Named to fire after
-- transition_allowed, so a role with no such transition is refused as any other.
CREATE FUNCTION agb.basket_approval_check() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    mode_now  text;
    declared  integer;
    recorded  integer;
    failed    integer;
    why       text;
BEGIN
    IF NEW.from_state IS DISTINCT FROM 'proposed' OR NEW.to_state <> 'approved' THEN
        RETURN NEW;
    END IF;
    SELECT m.mode INTO mode_now FROM agb.current_mode m;
    SELECT r.checks, count(c.id), count(c.id) FILTER (WHERE NOT c.passed)
        INTO declared, recorded, failed
    FROM agb.gate_run r
    LEFT JOIN agb.gate_check c ON c.run_id = r.id
    WHERE r.basket_id = NEW.basket_id
    GROUP BY r.checks;

    IF NEW.actor <> 'system' THEN
        why := format('the gate approves as system, not %s', NEW.actor);
    ELSIF mode_now IS DISTINCT FROM 'paper' THEN
        why := format('the system approves only in paper, and the mode is %s', mode_now);
    ELSIF declared IS NULL THEN
        why := 'the gate has not run';
    ELSIF recorded < declared THEN
        why := format('the gate recorded %s of its %s checks', recorded, declared);
    ELSIF failed > 0 THEN
        why := format('%s gate checks failed', failed);
    END IF;
    IF why IS NOT NULL THEN
        RAISE EXCEPTION 'basket_gate: % may not move basket % from proposed to approved: %',
            current_user, NEW.basket_id, why
            USING ERRCODE = 'insufficient_privilege';
    END IF;
    RETURN NEW;
END;
$$;
REVOKE EXECUTE ON FUNCTION agb.basket_approval_check() FROM PUBLIC;

CREATE TRIGGER transition_gated BEFORE INSERT ON agb.basket_event
    FOR EACH ROW EXECUTE FUNCTION agb.basket_approval_check();

GRANT SELECT ON agb.gate_run, agb.gate_check, agb.halt_event, agb.halt_open, agb.mode_event,
    agb.current_mode TO scheduler, pipeline, execution, bot, ui, cli;
GRANT INSERT ON agb.gate_run, agb.gate_check TO pipeline;
GRANT INSERT ON agb.halt_event TO pipeline, execution, bot, ui;
GRANT INSERT ON agb.mode_event TO scheduler, execution;
