-- The Telegram updates the bot has handled, one row each, so each is handled at most once.
--
-- The bot used to keep its Telegram offset in memory, and Telegram forgets an update only
-- when the next poll asks from past it. A restart between handling a batch and the next poll
-- read the batch again, and `/pause`, `/resume` and a bare `/liquidate` ran a second time.
--
-- A row here is written in the same transaction as the `command` row and whatever the
-- command did, so either both are on record or neither is. The primary key makes handling
-- idempotent on Telegram's `update_id`: a replayed update finds its row and is skipped, and
-- the bot asks Telegram from one past the highest `update_id` here when it starts.
-- Updates that are not messages (an edit, say) and messages from another chat get a row too,
-- since they move the offset as well.
CREATE TABLE agb.telegram_update (
    update_id            bigint PRIMARY KEY,
    -- The chat and message the update carried, where it carried one.
    chat_id              text CHECK (chat_id <> ''),
    telegram_message_id  bigint,
    at                   timestamptz NOT NULL DEFAULT now(),
    actor                agb.actor NOT NULL
);
SELECT agb.make_append_only('agb.telegram_update');

GRANT SELECT, INSERT ON agb.telegram_update TO bot;
GRANT SELECT ON agb.telegram_update TO cli;
