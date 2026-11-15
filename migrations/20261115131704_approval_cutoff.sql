-- The approval cutoff, held in the database as well as in agb.approval.
--
-- A rebalance can be approved only before its session's MOC cutoff, which is when the
-- submission window closes. agb.approval refuses a late answer in Python, but any role that
-- may insert a basket event could approve after it, so this trigger refuses one too.
--
-- The database has no NYSE calendar, so it holds the regular cutoff: 15:45 New York time on
-- the basket's session date, fifteen minutes before a 16:00 close (MOC_SUBMISSION_LEAD in
-- agb.nyse). On an early-close day the true cutoff is earlier, and agb.approval, which reads
-- the calendar, is the stricter of the two; this bound still stops an approval from the
-- evening onwards, or days later.

-- The time an approval is judged at: the moment of the insert, not the start of its
-- transaction, so a transaction held open across the cutoff cannot slip one through. The
-- tests replace it to pin the clock; nothing else does, and only the owner could.
CREATE FUNCTION agb.approval_clock() RETURNS timestamptz
LANGUAGE sql VOLATILE AS $$ SELECT clock_timestamp() $$;

-- A session's regular MOC cutoff.
CREATE FUNCTION agb.basket_cutoff(session date) RETURNS timestamptz
LANGUAGE sql IMMUTABLE AS $$
    SELECT (session + time '15:45') AT TIME ZONE 'America/New_York'
$$;

GRANT EXECUTE ON FUNCTION agb.approval_clock(), agb.basket_cutoff(date)
    TO scheduler, pipeline, execution, bot, ui, cli;

-- Refuses a move to approved, from any state and by any role, at or after the basket's
-- cutoff. Named to fire after transition_allowed, transition_gated and transition_policy,
-- so a refusal they would give is given first. check_violation, like a lost race, because
-- to the caller it is one: the basket is past being approved.
CREATE FUNCTION agb.basket_cutoff_check() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    cutoff  timestamptz;
BEGIN
    IF NEW.to_state <> 'approved' THEN
        RETURN NEW;
    END IF;
    SELECT agb.basket_cutoff(b.session_date) INTO cutoff
    FROM agb.basket b
    WHERE b.id = NEW.basket_id;
    IF agb.approval_clock() >= cutoff THEN
        RAISE EXCEPTION 'basket_cutoff: % may not approve basket % at or after its cutoff, %',
            current_user, NEW.basket_id, cutoff
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END;
$$;
REVOKE EXECUTE ON FUNCTION agb.basket_cutoff_check() FROM PUBLIC;

CREATE TRIGGER transition_timely BEFORE INSERT ON agb.basket_event
    FOR EACH ROW EXECUTE FUNCTION agb.basket_cutoff_check();

