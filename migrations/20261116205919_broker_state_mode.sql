-- The trading mode each account reading was taken in (#156). Paper and live are two accounts,
-- and the newest reading of one says nothing about the other's equity: a basket sized in live
-- mode on the paper account's last reading asks for capital the live account may not have.
-- Execution stamps the mode in force on every reading from now on. Readings taken before
-- this column existed stay NULL: broker_state is append-only and is not backfilled, and a NULL
-- mode matches neither mode, so a reader that needs the mode waits for the next reading.
ALTER TABLE agb.broker_state
    ADD COLUMN mode text CHECK (mode IN ('paper', 'live'));

-- The newest reading, now with its mode. Appended as the last column, which is all CREATE OR
-- REPLACE VIEW allows; the view keeps its grants.
CREATE OR REPLACE VIEW agb.broker_state_now AS
SELECT s.observed_at, s.account_status, s.suspend_trade, s.no_shorting, s.trading_blocked,
       s.account_blocked, s.equity_usd, s.cash_usd, s.buying_power_usd, s.mode
FROM agb.broker_state s
ORDER BY s.id DESC
LIMIT 1;
