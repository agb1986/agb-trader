-- Who a role may say it is, and the ladder's upward checks in the database
-- (docs/architecture/04, "The autonomy ladder" and "From paper to live").
--
-- Until now the actor on a basket event was whatever the writing role put there. The bot or
-- the UI could write an approval as `system`, and at L1 in live mode a system approval of a
-- pending basket is the one the scheduler makes unattended: a compromised surface could
-- approve a live basket as if the system had. The surfaces are now bound to the actors they
-- speak for:
--
--   bot        surface telegram; the Telegram operator who answered (`telegram:<user id>`),
--              or `bot` itself when it puts a basket to the human (proposed to pending).
--   ui         surface ui; the UI operator (`ui:operator`).
--   system     only the roles that act unattended: pipeline (the gate's paper approval),
--              scheduler (the L1 proceed and the missed deadline) and execution.
--
-- Moving up the ladder or to live was a policy that let the CLI write any row
-- (`WITH CHECK (true)`); the eligibility checks lived only in src/agb/ladder.py. They are
-- repeated here, so a role that skips the Python is refused all the same: a level is climbed
-- one step at a time and only once agb.ladder_ready says the step is earned, and live is
-- reached only once agb.live_ready says paper earned it and no waived model is promoted.
-- `system` on either table is written only by the scheduler and execution, which already
-- may only drop to L0 or paper.
--
-- The table owner is exempt from the ladder and mode checks, as it is from row-level
-- security: it runs the migrations, no service connects as it, and it could drop the trigger
-- anyway. The basket check needs no exemption, because transition_allowed refuses the owner
-- every transition already.

-- Refuses a basket event whose actor or surface the inserting role may not write. Named to
-- fire second, after transition_allowed, so a role with no such transition is refused as any
-- other first.
CREATE FUNCTION agb.basket_actor_check() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    why  text;
BEGIN
    IF current_user = 'bot' THEN
        IF NEW.surface <> 'telegram' THEN
            why := 'the bot writes from telegram';
        ELSIF NEW.actor ~ '^telegram:[0-9]+$' THEN
            why := NULL;
        ELSIF NEW.actor = 'bot' AND NEW.from_state = 'proposed' AND NEW.to_state = 'pending' THEN
            why := NULL;
        ELSE
            why := 'the bot writes as the Telegram operator who answered';
        END IF;
    ELSIF current_user = 'ui' THEN
        IF NEW.surface <> 'ui' OR NEW.actor <> 'ui:operator' THEN
            why := 'the UI writes as ui:operator from ui';
        END IF;
    ELSIF NEW.actor = 'system' AND current_user NOT IN ('pipeline', 'scheduler', 'execution') THEN
        why := 'only the roles that act unattended write as system';
    END IF;
    IF why IS NOT NULL THEN
        RAISE EXCEPTION 'basket_actor: % may not write basket % as % from %: %',
            current_user, NEW.basket_id, NEW.actor, NEW.surface, why
            USING ERRCODE = 'insufficient_privilege';
    END IF;
    RETURN NEW;
END;
$$;
REVOKE EXECUTE ON FUNCTION agb.basket_actor_check() FROM PUBLIC;

CREATE TRIGGER transition_attributed BEFORE INSERT ON agb.basket_event
    FOR EACH ROW EXECUTE FUNCTION agb.basket_actor_check();

-- Refuses a ladder move ladder.py would refuse on the way up: `system` from a role that
-- does not act unattended, a skipped level, or a step agb.ladder_ready does not call earned.
-- A move down, to the level in force or below, is left to the policies.
CREATE FUNCTION agb.ladder_event_check() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    levels    text[] := ARRAY['L0', 'L1', 'L2'];
    now_at    text;
    target    text;
    earned    boolean;
    missing   text[];
    why       text;
BEGIN
    IF EXISTS (SELECT 1 FROM pg_class c
               WHERE c.oid = TG_RELID AND pg_get_userbyid(c.relowner) = current_user) THEN
        RETURN NEW;
    END IF;
    SELECT l.level INTO now_at FROM agb.current_level l;
    IF NEW.actor = 'system' AND current_user NOT IN ('scheduler', 'execution') THEN
        why := 'only the roles that act unattended write as system';
    ELSIF array_position(levels, NEW.level) <= array_position(levels, now_at) THEN
        why := NULL;
    ELSIF array_position(levels, NEW.level) > array_position(levels, now_at) + 1 THEN
        why := format('the ladder is climbed one level at a time; %s comes after %s',
                      levels[array_position(levels, now_at) + 1], now_at);
    ELSE
        SELECT r.target, r.ready, r.outstanding INTO target, earned, missing
        FROM agb.ladder_ready r;
        IF target IS DISTINCT FROM NEW.level THEN
            why := format('nothing measures eligibility for %s from %s', NEW.level, now_at);
        ELSIF NOT coalesce(earned, false) THEN
            why := format('%s is not earned yet: %s outstanding',
                          NEW.level, array_to_string(missing, ', '));
        END IF;
    END IF;
    IF why IS NOT NULL THEN
        RAISE EXCEPTION 'ladder_event: % may not move the level from % to % as %: %',
            current_user, now_at, NEW.level, NEW.actor, why
            USING ERRCODE = 'insufficient_privilege';
    END IF;
    RETURN NEW;
END;
$$;
REVOKE EXECUTE ON FUNCTION agb.ladder_event_check() FROM PUBLIC;

CREATE TRIGGER climb_earned BEFORE INSERT ON agb.ladder_event
    FOR EACH ROW EXECUTE FUNCTION agb.ladder_event_check();

-- Refuses a move from paper to live that ladder.py would refuse: `system` from a role that
-- does not act unattended, a paper record agb.live_ready does not call sound, or a promoted
-- model that is running under the certification waiver.
CREATE FUNCTION agb.mode_event_check() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    was      text;
    earned   boolean;
    missing  text[];
    waived   text;
    why      text;
BEGIN
    IF EXISTS (SELECT 1 FROM pg_class c
               WHERE c.oid = TG_RELID AND pg_get_userbyid(c.relowner) = current_user) THEN
        RETURN NEW;
    END IF;
    SELECT m.mode INTO was FROM agb.current_mode m;
    IF NEW.actor = 'system' AND current_user NOT IN ('scheduler', 'execution') THEN
        why := 'only the roles that act unattended write as system';
    ELSIF NEW.mode = 'live' AND was IS DISTINCT FROM 'live' THEN
        SELECT r.ready, r.outstanding INTO earned, missing FROM agb.live_ready r;
        SELECT p.model_id INTO waived FROM agb.promoted_model p
        WHERE p.waived AND NOT p.voided
        LIMIT 1;
        IF NOT coalesce(earned, false) THEN
            why := format('live is not earned yet: %s outstanding in the paper record',
                          array_to_string(missing, ', '));
        ELSIF waived IS NOT NULL THEN
            why := format('the live model %s is promoted under the paper-only measurement'
                          ' waiver', waived);
        END IF;
    END IF;
    IF why IS NOT NULL THEN
        RAISE EXCEPTION 'mode_event: % may not move the mode from % to % as %: %',
            current_user, was, NEW.mode, NEW.actor, why
            USING ERRCODE = 'insufficient_privilege';
    END IF;
    RETURN NEW;
END;
$$;
REVOKE EXECUTE ON FUNCTION agb.mode_event_check() FROM PUBLIC;

CREATE TRIGGER live_earned BEFORE INSERT ON agb.mode_event
    FOR EACH ROW EXECUTE FUNCTION agb.mode_event_check();
