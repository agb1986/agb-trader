-- Delivery of a long alert, one part at a time, and an alert with nothing to send.
--
-- Telegram refuses a message longer than 4096 characters, so the bot sends a longer alert in
-- parts. It recorded only the alert's delivery, once every part had gone, so an alert that
-- failed part-way was sent again from its first part on every retry, and the chat read the
-- same opening parts repeatedly. Each part that Telegram accepts is now a row here, written
-- as it is sent; a retry resumes from the first part with no row. A part can still go twice
-- if the bot dies between Telegram accepting it and the row being written, which is the
-- price of not being able to make the two one step.
--
-- A reply the bot could not send is retried as an alert of kind `reply`, so its parts are
-- recorded here too and a retry never repeats what the chat already has.
--
-- An alert whose body is blank has no part to send. Its delivery is recorded without a
-- Telegram message, with the detail saying why, so it leaves the undelivered list instead of
-- being retried for ever. `alert_event` required a message id on every Telegram delivery;
-- that check is loosened to allow none when the event names its reason. Loosening a check
-- keeps every existing row valid and every older image working.
CREATE TABLE agb.alert_part (
    alert_id             bigint NOT NULL REFERENCES agb.alert,
    -- Counted from 1, in the order `split` cuts the body.
    part                 integer NOT NULL CHECK (part >= 1),
    channel              text NOT NULL CHECK (channel ~ '^[a-z][a-z0-9_]*$'),
    chat_id              text,
    telegram_message_id  bigint NOT NULL,
    at                   timestamptz NOT NULL DEFAULT now(),
    -- Only the bot delivers to Telegram.
    actor                agb.actor NOT NULL CHECK (actor = 'bot'),
    PRIMARY KEY (alert_id, channel, part)
);
SELECT agb.make_append_only('agb.alert_part');

GRANT SELECT ON agb.alert_part TO scheduler, pipeline, execution, bot, ui, cli;
GRANT INSERT ON agb.alert_part TO bot;

-- The check carries no name of its own, so find it by what it says.
DO $$
DECLARE
    found  record;
BEGIN
    FOR found IN
        SELECT conname FROM pg_constraint
        WHERE conrelid = 'agb.alert_event'::regclass AND contype = 'c'
          AND pg_get_constraintdef(oid) LIKE '%telegram_message_id IS NOT NULL%'
    LOOP
        EXECUTE format('ALTER TABLE agb.alert_event DROP CONSTRAINT %I', found.conname);
    END LOOP;
END;
$$;
ALTER TABLE agb.alert_event ADD CONSTRAINT alert_event_delivery_names_a_message
    CHECK (event <> 'delivered' OR channel <> 'telegram'
           OR telegram_message_id IS NOT NULL OR detail IS NOT NULL);
