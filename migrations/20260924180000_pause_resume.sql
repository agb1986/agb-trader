-- Pause and resume: the command record, and the broker half of both latches.
--
-- `/pause` and `/resume` act on two latches. The database latch is the `pause` halt cause
-- (and, for `/resume`, every open cause), which the surface the operator typed on writes
-- itself. The broker latch is Alpaca's `suspend_trade`, which only execution may touch
-- because only execution holds the broker's keys: the surface records the accepted command,
-- and execution picks it up on its next cycle, cancels whatever a pause catches in flight,
-- sets or unsets `suspend_trade`, reads the account back and records the result against the
-- command. The confirmation the operator receives is built from that row.

-- Every inbound command from Telegram, the UI or the CLI, refused ones included: who sent
-- it, where, the text as typed, the verb and arguments it parsed to, and the outcome. A
-- message that parsed to no verb has none. A refusal says why.
CREATE TABLE agb.command (
    id        bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    actor     agb.actor NOT NULL,
    surface   text NOT NULL CHECK (surface IN ('telegram', 'ui', 'cli')),
    raw_text  text NOT NULL,
    verb      text CHECK (verb ~ '^[a-z][a-z_]*$'),
    args      jsonb NOT NULL DEFAULT '{}'::jsonb,
    outcome   text NOT NULL CHECK (outcome IN ('accepted', 'refused')),
    reason    text,
    at        timestamptz NOT NULL DEFAULT now(),
    CHECK (outcome <> 'refused' OR reason IS NOT NULL)
);
SELECT agb.make_append_only('agb.command');
CREATE INDEX command_verb ON agb.command (verb, id);

-- What execution did at the broker for an accepted pause or resume: the `suspend_trade` it
-- asked for, the value the account reported when read back straight afterwards, and what
-- else it did (orders cancelled, orders left live). One row per command, so a command
-- without one is still waiting for execution.
CREATE TABLE agb.broker_latch (
    id                  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    command_id          bigint NOT NULL UNIQUE REFERENCES agb.command,
    suspend_trade       boolean NOT NULL,
    observed_suspend    boolean NOT NULL,
    detail              text NOT NULL CHECK (detail <> ''),
    at                  timestamptz NOT NULL DEFAULT now(),
    actor               agb.actor NOT NULL
);
SELECT agb.make_append_only('agb.broker_latch');

-- The accepted pauses and resumes execution has not yet carried to the broker, oldest first.
CREATE VIEW agb.latch_pending AS
SELECT c.id, c.verb, c.actor, c.at, c.args
FROM agb.command c
WHERE c.verb IN ('pause', 'resume')
  AND c.outcome = 'accepted'
  AND NOT EXISTS (SELECT 1 FROM agb.broker_latch l WHERE l.command_id = c.id)
ORDER BY c.id;

-- A pause that lands while a submitted basket's orders can still be cancelled halts it.
INSERT INTO agb.basket_transition (from_state, to_state, role) VALUES
    ('submitted', 'halted', 'execution');

GRANT SELECT ON agb.command, agb.broker_latch, agb.latch_pending
    TO scheduler, pipeline, execution, bot, ui, cli;
GRANT INSERT ON agb.command TO bot, ui, cli;
GRANT INSERT ON agb.broker_latch TO execution;
