-- Performance and cost monitoring (docs/architecture/04): what the alerts read.
--
-- Nothing here acts. Every object below is a reading the bot turns into an alert that says
-- what was not done, and the two levers stay the operator's: /pause and /liquidate.
--
-- The thresholds are not round numbers. They are the quantiles of the promoted
-- certification's own CPCV path distribution, so each model brings its own, and a live
-- figure past one of them means the live world is outside the range validation explored.
-- agb.alert_threshold lifts them out of the certification's path_quantiles; a quantile the
-- certification did not record reads as NULL, and the check that needs it stays quiet
-- rather than inventing a threshold of its own.
--
-- The operator records the dividends the paper broker does not credit. Without them a paper
-- month looks worse than the backtest's expectation by exactly the dividend yield, and the
-- tracking alert would fire on the broker's bookkeeping rather than on the strategy.

-- Dividends paid on a holding, recorded by hand. Alpaca paper credits none, so a paper month
-- compared against the backtest's expectation must add them back (docs/architecture/01);
-- live equity already includes them and nothing is added to it. Nothing writes this but a
-- person, which is why the row carries an actor, a surface and the command that recorded it.
CREATE TABLE agb.dividend (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    -- The rebalance period the payment falls in, as agb.basket names one.
    period      text NOT NULL CHECK (period ~ '^\d{4}-\d{2}$'),
    symbol      text NOT NULL CHECK (symbol <> ''),
    amount_usd  numeric NOT NULL CHECK (amount_usd > 0),
    -- The session the broker's statement shows the cash on.
    paid_on     date NOT NULL,
    -- Where the operator read the figure, kept for the audit.
    detail      text NOT NULL CHECK (detail <> ''),
    at          timestamptz NOT NULL DEFAULT now(),
    actor       agb.actor NOT NULL,
    surface     text NOT NULL CHECK (surface IN ('cli', 'ui', 'telegram')),
    command_id  bigint NOT NULL REFERENCES agb.command,
    -- One row per payment, so a month's total is never counted twice.
    UNIQUE (period, symbol, paid_on)
);
SELECT agb.make_append_only('agb.dividend');
CREATE INDEX dividend_period ON agb.dividend (period, id);

GRANT SELECT ON agb.dividend TO scheduler, pipeline, execution, bot, ui, cli, research;
-- The operator records one; no service does.
GRANT INSERT ON agb.dividend TO cli, ui;

-- Shortfall per fill, where agb.shortfall measures it per order: each fill against the NBBO
-- mid read just before its order was submitted, signed so a worse fill is positive on either
-- side, in basis points, beside the cost the model had frozen for that order. `mode` is the
-- mode in force when the order was created, because paper simulates neither impact nor
-- slippage: a paper fill measures the broker's simulator rather than the market, so the cost
-- alert reads live fills only.
CREATE VIEW agb.fill_shortfall AS
SELECT f.id AS fill_id, o.id AS order_id, o.basket_id, o.symbol, o.side,
       f.qty, f.price, f.filled_at,
       o.nbbo_mid AS reference_mid,
       CASE WHEN o.side = 'buy' THEN 1 ELSE -1 END
           * (f.price - o.nbbo_mid) / o.nbbo_mid * 10000 AS shortfall_bps,
       o.modelled_cost_bps,
       o.cost_model_version,
       mode.mode
FROM agb.fill f
JOIN agb.broker_order o ON o.id = f.order_id
LEFT JOIN LATERAL (
    SELECT m.mode FROM agb.mode_event m
    WHERE m.at <= o.created_at
    ORDER BY m.id DESC
    LIMIT 1
) mode ON true;

-- The live cost reading the alert is drawn from: every live fill on the record, its mean
-- shortfall beside the mean cost the model charged for the same orders, and the standard
-- error of the per-fill excess. One row always, counting zero when no live fill has printed.
-- The alert needs both a count and a size: fewer than fifty fills measures nothing, and an
-- excess inside two standard errors is noise (docs/architecture/04).
CREATE VIEW agb.live_cost AS
SELECT count(*) AS fills,
       avg(s.shortfall_bps) AS mean_shortfall_bps,
       avg(s.modelled_cost_bps) AS mean_modelled_bps,
       avg(s.shortfall_bps - s.modelled_cost_bps) AS mean_excess_bps,
       stddev_samp(s.shortfall_bps - s.modelled_cost_bps) AS excess_sd_bps,
       stddev_samp(s.shortfall_bps - s.modelled_cost_bps) / sqrt(count(*)::numeric)
           AS excess_se_bps,
       max(s.filled_at) AS last_fill_at
FROM agb.fill_shortfall s
WHERE s.mode = 'live';

-- Each New York month's realised return, from the account equity execution observed: from
-- the last reading before the month, or the month's own first reading when there is none, to
-- its last. Those are the two readings /pnl measures a month between. `dividends_usd` is
-- what the operator recorded for the month, and total_return_fraction adds it back in paper
-- and only in paper, so a paper month is comparable with the backtest. The performance
-- alerts read the total return; /pnl shows both.
CREATE VIEW agb.equity_month AS
WITH reading AS (
    SELECT s.observed_at, s.equity_usd,
           to_char(s.observed_at AT TIME ZONE 'America/New_York', 'YYYY-MM') AS period
    FROM agb.broker_state s
    WHERE s.equity_usd IS NOT NULL
),
bounds AS (
    SELECT r.period, min(r.observed_at) AS first_at, count(*) AS readings
    FROM reading r
    GROUP BY r.period
)
SELECT b.period, b.readings,
       opening.observed_at AS opening_at, opening.equity_usd AS opening_usd,
       closing.observed_at AS closing_at, closing.equity_usd AS closing_usd,
       coalesce(paid.dividends_usd, 0) AS dividends_usd,
       mode.mode,
       closing.equity_usd / nullif(opening.equity_usd, 0) - 1 AS return_fraction,
       (closing.equity_usd
            + CASE WHEN mode.mode = 'paper' THEN coalesce(paid.dividends_usd, 0) ELSE 0 END)
           / nullif(opening.equity_usd, 0) - 1 AS total_return_fraction
FROM bounds b
-- The last reading before the month, and the month's own first when the month opens the
-- record: the same opening /pnl uses.
JOIN LATERAL (
    SELECT r.observed_at, r.equity_usd FROM reading r
    WHERE r.observed_at <= b.first_at
    ORDER BY (r.observed_at = b.first_at), r.observed_at DESC
    LIMIT 1
) opening ON true
JOIN LATERAL (
    SELECT r.observed_at, r.equity_usd FROM reading r
    WHERE r.period = b.period
    ORDER BY r.observed_at DESC
    LIMIT 1
) closing ON true
LEFT JOIN LATERAL (
    SELECT sum(d.amount_usd) AS dividends_usd FROM agb.dividend d WHERE d.period = b.period
) paid ON true
LEFT JOIN LATERAL (
    SELECT m.mode FROM agb.mode_event m
    WHERE m.at <= closing.observed_at
    ORDER BY m.id DESC
    LIMIT 1
) mode ON true;

-- The promoted certification's alert thresholds, lifted out of its path distribution: the
-- quantile each performance alert compares the live figure against, and the extreme of the
-- same distribution, which the alert quotes so the operator sees the range rather than one
-- number. A drawdown is a negative fraction, so the far end of its distribution is the 5th
-- percentile; a losing run and a tracking dispersion are the other way up, so theirs is the
-- 95th. `monthly_return` is the distribution of each path's mean month, which the tracking
-- alert and /pnl use as the backtest's expectation. No model promoted, or no such quantile
-- recorded, reads NULL rather than a default.
CREATE VIEW agb.alert_threshold AS
SELECT c.id AS certification_id, c.model_id, c.certified_capital,
       (c.path_quantiles ->> 'paths')::integer AS paths,
       (c.path_quantiles #>> '{max_drawdown,q05}')::double precision AS drawdown_q05,
       (c.path_quantiles #>> '{max_drawdown,min}')::double precision AS drawdown_min,
       (c.path_quantiles #>> '{longest_losing_run,q95}')::double precision AS losing_run_q95,
       (c.path_quantiles #>> '{longest_losing_run,max}')::double precision AS losing_run_max,
       (c.path_quantiles #>> '{tracking_dispersion,q95}')::double precision AS tracking_q95,
       (c.path_quantiles #>> '{tracking_dispersion,max}')::double precision AS tracking_max,
       (c.path_quantiles #>> '{monthly_return,q50}')::double precision AS monthly_return
FROM agb.promoted_model p
JOIN agb.model_certification c ON c.id = p.certification_id;

GRANT SELECT ON agb.fill_shortfall, agb.live_cost, agb.equity_month, agb.alert_threshold
    TO scheduler, pipeline, execution, bot, ui, cli;

-- The audit log gains the recorded dividend, one branch more. The sixteen branches are
-- restated here whole, as every migration that adds one must: a view is replaced, never
-- extended.
CREATE OR REPLACE VIEW agb.audit_log AS
SELECT e.at, 'basket_event'::text AS source, e.id AS row_id, e.actor::text AS actor,
       'basket ' || e.to_state AS event,
       b.kind || ' basket ' || coalesce(b.reference, b.id::text) AS subject,
       e.reason AS detail
FROM agb.basket_event e
JOIN agb.basket b ON b.id = e.basket_id
UNION ALL
SELECT h.at, 'halt_event', h.id, h.actor,
       'halt ' || h.action || ' ' || h.cause,
       CASE WHEN b.id IS NULL THEN NULL
            ELSE b.kind || ' basket ' || coalesce(b.reference, b.id::text) END,
       h.reason
FROM agb.halt_event h
LEFT JOIN agb.basket b ON b.id = h.basket_id
UNION ALL
SELECT m.at, 'mode_event', m.id, m.actor, 'mode ' || m.mode, NULL, m.reason
FROM agb.mode_event m
UNION ALL
SELECT e.at, 'order_event', e.id, e.actor,
       'order ' || e.kind || ' ' || e.status,
       o.side || ' ' || o.qty || ' ' || o.symbol || ' (' || o.client_order_id || ')',
       e.detail
FROM agb.order_event e
JOIN agb.broker_order o ON o.id = e.order_id
UNION ALL
SELECT c.at, 'command', c.id, c.actor,
       'command ' || c.outcome,
       c.surface || ' ' || left(c.raw_text, 200),
       c.reason
FROM agb.command c
UNION ALL
SELECT e.at, 'alert_event', e.id, e.actor,
       'alert ' || e.event,
       a.kind || ' ' || a.subject || ' ' || a.period,
       e.detail
FROM agb.alert_event e
JOIN agb.alert a ON a.id = e.alert_id
UNION ALL
SELECT e.at, 'job_event', e.id, e.actor,
       'job ' || e.event,
       j.kind || ' ' || j.period,
       e.detail
FROM agb.job_event e
JOIN agb.job j ON j.id = e.job_id
UNION ALL
SELECT p.at, 'paper_approval_event', p.id, p.actor,
       CASE WHEN p.required THEN 'paper approval required' ELSE 'paper approval unattended' END,
       NULL, p.reason
FROM agb.paper_approval_event p
UNION ALL
SELECT p.at, 'promotion_event', p.id, p.actor,
       'model promoted',
       'model ' || c.model_id,
       NULL
FROM agb.promotion_event p
JOIN agb.model_certification c ON c.id = p.certification_id
UNION ALL
SELECT e.at, 'certification_event', e.id, e.actor,
       'model ' || e.event,
       'model ' || c.model_id,
       e.reason
FROM agb.certification_event e
JOIN agb.model_certification c ON c.id = e.certification_id
UNION ALL
-- The token itself stays out of the log.
SELECT l.at, 'liquidation', l.id, l.actor,
       'liquidation ' || l.kind,
       CASE WHEN l.follows IS NULL THEN NULL ELSE 'liquidation ' || l.follows END,
       l.detail
FROM agb.liquidation l
UNION ALL
SELECT e.at, 'ladder_event', e.id, e.actor,
       'ladder ' || e.level,
       CASE WHEN b.id IS NULL THEN NULL
            ELSE b.kind || ' basket ' || coalesce(b.reference, b.id::text) END,
       CASE WHEN e.attestation IS NULL THEN e.reason
            ELSE e.reason || ' — attested: ' || e.attestation END
FROM agb.ladder_event e
LEFT JOIN agb.basket b ON b.id = e.basket_id
UNION ALL
SELECT r.at, 'reconcile_run', r.id, r.actor,
       'basket reconciled',
       b.kind || ' basket ' || coalesce(b.reference, b.id::text),
       r.detail
FROM agb.reconcile_run r
JOIN agb.basket b ON b.id = r.basket_id
UNION ALL
SELECT e.at, 'forced_event', e.id, e.actor,
       'forced ' || e.kind,
       e.symbol || ' ' || e.period,
       e.detail
FROM agb.forced_event e
UNION ALL
SELECT d.at, 'dividend', d.id, d.actor,
       'dividend recorded',
       d.symbol || ' ' || d.period,
       d.detail
FROM agb.dividend d;

GRANT SELECT ON agb.audit_log TO scheduler, pipeline, execution, bot, ui, cli;
