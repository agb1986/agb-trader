-- Acknowledging an alert (issue #209): the operator says they have seen it, from Telegram
-- (`/ack ALERT_ID`) or from the UI's Alerts page. The schema already allowed the event, an
-- `alert_event` row with event 'acknowledged'; nothing wrote one. Two rules now hold in the
-- database as well as in src/agb/alerts.py, so a role that skips the Python is refused alike.
--
-- An alert is acknowledged once. The first acknowledgement stands, and a second one, from
-- either surface or from a race between the two, inserts nothing.
CREATE UNIQUE INDEX alert_event_acknowledged_once ON agb.alert_event (alert_id)
    WHERE event = 'acknowledged';

-- Only the two surfaces a human drives acknowledge, and each speaks for its operator, as it
-- does for a basket (20261116221827_actor_binding.sql):
--
--   bot   channel telegram, `telegram:<user id>`, and the chat the command came in.
--   ui    channel ui, `ui:operator`.
--
-- The scheduler and execution raise alerts and record deliveries; neither acknowledges one on
-- anybody's behalf. The table owner is exempt, as it is from the other actor checks: it runs
-- the migrations and no service connects as it.
CREATE FUNCTION agb.alert_acknowledged_by() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    why  text;
BEGIN
    IF NEW.event <> 'acknowledged'
       OR EXISTS (SELECT 1 FROM pg_class c
                  WHERE c.oid = TG_RELID AND pg_get_userbyid(c.relowner) = current_user) THEN
        RETURN NEW;
    END IF;
    IF current_user = 'bot' THEN
        IF NEW.channel <> 'telegram' OR NEW.chat_id IS NULL THEN
            why := 'the bot acknowledges from telegram, naming the chat';
        ELSIF NEW.actor !~ '^telegram:[0-9]+$' THEN
            why := 'the bot acknowledges as the Telegram operator who asked';
        END IF;
    ELSIF current_user = 'ui' THEN
        IF NEW.channel <> 'ui' OR NEW.actor <> 'ui:operator' THEN
            why := 'the UI acknowledges as ui:operator from ui';
        END IF;
    ELSE
        why := 'only the bot and the UI acknowledge an alert';
    END IF;
    IF why IS NOT NULL THEN
        RAISE EXCEPTION 'alert_actor: % may not acknowledge alert % as % from %: %',
            current_user, NEW.alert_id, NEW.actor, NEW.channel, why
            USING ERRCODE = 'insufficient_privilege';
    END IF;
    RETURN NEW;
END;
$$;
REVOKE EXECUTE ON FUNCTION agb.alert_acknowledged_by() FROM PUBLIC;

CREATE TRIGGER acknowledged_attributed BEFORE INSERT ON agb.alert_event
    FOR EACH ROW EXECUTE FUNCTION agb.alert_acknowledged_by();
