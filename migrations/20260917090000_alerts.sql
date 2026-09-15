-- Alerts and their delivery. Written by bot, ui, execution and scheduler.
--
-- An alert is raised once: (kind, subject, period) is unique, so a condition that is still
-- true on the next check adds nothing. Every attempt to deliver an alert is an alert_event,
-- and so is its acknowledgement, so an alert that never reached the operator stays visible.

CREATE TABLE agb.alert (
    id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    kind       text NOT NULL CHECK (kind ~ '^[a-z][a-z0-9_]*$'),
    subject    text NOT NULL CHECK (subject <> ''),
    period     text NOT NULL CHECK (period <> ''),
    -- The message itself, computed from database state when the alert was raised.
    body       text NOT NULL CHECK (body <> ''),
    raised_at  timestamptz NOT NULL DEFAULT now(),
    raised_by  agb.actor NOT NULL,
    UNIQUE (kind, subject, period)
);
SELECT agb.make_append_only('agb.alert');

CREATE TABLE agb.alert_event (
    id                   bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    alert_id             bigint NOT NULL REFERENCES agb.alert,
    event                text NOT NULL CHECK (event IN ('delivered', 'failed', 'acknowledged')),
    channel              text NOT NULL CHECK (channel ~ '^[a-z][a-z0-9_]*$'),
    chat_id              text,
    telegram_message_id  bigint,
    detail               text,
    actor                agb.actor NOT NULL,
    at                   timestamptz NOT NULL DEFAULT now(),
    -- A Telegram delivery names the message it became, and a failure says why.
    CHECK (event <> 'delivered' OR channel <> 'telegram' OR telegram_message_id IS NOT NULL),
    CHECK (event <> 'failed' OR detail IS NOT NULL)
);
SELECT agb.make_append_only('agb.alert_event');
CREATE INDEX alert_event_alert ON agb.alert_event (alert_id, id);
CREATE UNIQUE INDEX alert_event_delivered_once ON agb.alert_event (alert_id, channel)
    WHERE event = 'delivered';

-- Alerts not yet delivered to Telegram, with their failed attempts. The bot works through
-- this list, and an alert that stays on it is itself something the operator should see.
CREATE VIEW agb.undelivered_alert AS
SELECT a.id, a.kind, a.subject, a.period, a.body, a.raised_at, a.raised_by,
       count(f.id) AS failures,
       max(f.at) AS last_failed_at,
       (array_agg(f.detail ORDER BY f.id DESC))[1] AS last_failure
FROM agb.alert a
LEFT JOIN agb.alert_event f
       ON f.alert_id = a.id AND f.channel = 'telegram' AND f.event = 'failed'
WHERE NOT EXISTS (
    SELECT 1 FROM agb.alert_event d
    WHERE d.alert_id = a.id AND d.channel = 'telegram' AND d.event = 'delivered')
GROUP BY a.id;

GRANT SELECT ON agb.alert, agb.alert_event, agb.undelivered_alert
    TO scheduler, pipeline, execution, bot, ui, cli;
GRANT INSERT ON agb.alert, agb.alert_event TO scheduler, execution, bot, ui;
