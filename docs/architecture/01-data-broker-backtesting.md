# 01 — Data, Broker and Backtesting

**Sources:** [#2 Select broker platform](https://github.com/agb1986/agb-trader/issues/2) · [#3 Select market data vendor](https://github.com/agb1986/agb-trader/issues/3) · [#4 Select event-driven backtesting engine](https://github.com/agb1986/agb-trader/issues/4) · [#11 Define transaction cost model for backtest realism](https://github.com/agb1986/agb-trader/issues/11) · validation-gate and lake decisions from [#10](https://github.com/agb1986/agb-trader/issues/10) and [#12](https://github.com/agb1986/agb-trader/issues/12)
**Parent:** [#1 AGB Trader — Architecture Spec Map](https://github.com/agb1986/agb-trader/issues/1)
**Status:** Settled design.
**Date:** 2026-09-10

## Broker: Alpaca

Alpaca is the broker for both paper and live trading. It was chosen for the best paper/live API parity of the candidates (the same order-placement interface serves both, so moving to live is a credential change, not a redesign), for least-privilege access control by construction (API keys that can trade but cannot withdraw), and for needing no gateway process. Runner-up was Tradier. The full comparison is in [`docs/research/002-broker-platform-selection.md`](../research/002-broker-platform-selection.md).

Properties of Alpaca that later decisions depend on:

- Market-on-close orders via time-in-force `cls`, with a submission cutoff around 15:50 ET. The system submits before 15:45 ET to leave a pad.
- Fractional shares, which remove any capital-scale constraint on the 20-position portfolio.
- Account configuration flags `no_shorting` and `suspend_trade`, used as broker-side backstops for the pre-trade gate and the pause lever respectively.
- `DELETE /v2/positions?cancel_orders=true`, the liquidate primitive, returning HTTP 207 with per-symbol status.
- Deterministic `client_order_id` (up to 128 characters), looked up by `GET /v2/orders:by_client_order_id`; a duplicate `client_order_id` on an active order is rejected with HTTP 422.
- Multiple simultaneous paper accounts are supported but deliberately not used (see [03 — LLM panel](03-llm-panel.md)).
- The paper environment does not simulate market impact, slippage, price improvement, regulatory fees or dividends, and randomly partial-fills roughly 10% of eligible orders. This shapes what paper trading can and cannot validate, below.

Commission on self-directed equities is $0.

## Market data: Sharadar

Sharadar (via Nasdaq Data Link) supplies daily prices (SEP) and fundamentals (SF1). It was the only candidate with true point-in-time data on both, which is what makes an honest backtest possible: SF1 rows carry `datekey`, the SEC filing date, so every fundamental feature is gated on when it became public rather than the period it describes. SEP coverage starts in 1998 and includes delisted tickers, so the backtest models delistings rather than silently dropping them. Runner-up was Norgate. The full comparison is in [`docs/research/003-market-data-vendor-selection.md`](../research/003-market-data-vendor-selection.md).

Sharadar carries daily OHLCV but no bid/ask and no closing-auction volume. The cost model below is built to estimate spreads from what SEP has rather than pretend to measure them.

### The Parquet lake

Market data does not live in Postgres. It is a Parquet lake on the server's `/mnt/data` volume, read with DuckDB. **The server owns the lake and performs the only vendor pull.** The workstation `rsync`s the lake down over the LAN before a research run, so the backtest reads exactly the bytes live reads. Independent pulls on both machines would diverge on a Sharadar restatement or a failed fetch with nothing comparing them, producing a backtest that cannot be reproduced against live behaviour. This is a correctness property, not a convenience.

Every basket records the Sharadar snapshot date and a hash of the lake partition it read, so a rebalance can be tied to the data it saw.

## Backtesting engine: NautilusTrader

NautilusTrader is the event-driven backtesting engine, chosen for execution realism, pure Python, and a native live/paper bridge. Runner-up was QuantConnect LEAN. The full comparison is in [`docs/research/backtesting-engine-selection.md`](../research/backtesting-engine-selection.md).

NautilusTrader provides no cross-sectional ranking or rebalancing abstraction. Universe ranking, the rank buffer, tie-breaks and the forced-event handling in [02 — Classical ranker](02-classical-ranker.md) are hand-rolled in the strategy layer, and that document is written concretely enough to hand-roll against. vectorbt is used for fast scans alongside; the NautilusTrader run is the validation run.

**The scan and the validation run are not expected to agree on real data, and the scan is the one that is wrong on purpose.** The scan exists to be fast, so it models three frictions fewer than the engine, each of which the engine models because the live basket has it:

- **Delisting exits.** The engine sells a held name at its last listed print, the forced exit [02 — Classical ranker](02-classical-ranker.md) requires. The scan carries the last price forward and sells the name at the next rebalance that drops it out of the ranking.
- **Trims.** The engine trims a buy the account cannot fund whole, and drops one that rounds to no shares at all. vectorbt's shared cash group does neither.
- **Whole shares.** The engine sizes whole shares at the signal session's close, as the live basket sizes them; vectorbt sizes fractional shares at the fill.

On a synthetic market none of the three ever fires and the two agree exactly, which is what `tests/test_backtest.py` asserts. On the real lake they fire tens of times over a few dozen months, so what is checked there is that the divergence is only those frictions: `tests/test_backtest_live.py` reconciles the two order sets through the engine's own delisting-exit and trim counts and bounds the gap in the scale-free return statistics. Teaching the scan about delistings and trims would make it a second engine and remove the reason it exists. How large a gap can be tolerated before the scan stops being a usable filter on candidates is open, and is tracked separately.

Both use the same feature definitions and the same cost function from the shared library (see [07 — Services and deployment](07-services-deployment.md)), which is what keeps the backtest and the live path from drifting.

## The transaction cost model

The backtest charges every trade a per-trade cost computed from the name's own liquidity on the trade date, fixed before any backtest is run, certified at a stated capital, and stress-tested at twice its own estimate.

### Certification capital: $100,000

Market impact scales with order size relative to liquidity, so a cost model is only meaningful at a stated capital. Gates are certified at **$100k**, twice the largest book expected in the first year of live trading. The margin is deliberate: a strategy certified at exactly the capital it runs at is outside its validation the first time returns compound.

At $100k each equal-weight position is $5,000. Against the universe floor of $1M median daily dollar volume, the largest possible order is 0.5% of daily volume.

The certified capital is stored with the certified model. When the live book's value crosses it, an alert fires. The alert notifies and never acts: halting rebalancing because the book grew would itself be an unvalidated modification of the strategy. Re-certifying at a higher capital is a deliberate operator act.

### The per-trade cost

Every trade, buy or sell, including the small trims and top-ups that restore equal weight, is charged:

```
cost_bps = floor + k · σ_daily · √(order_$ / ADV_$)          (both sides)
         + regulatory fees                                   (sells only)
```

- **`floor`** is half the name's estimated effective spread: the [Abdi-Ranaldo (2017)](https://doi.org/10.1093/rfs/hhx084) estimator computed from daily close, high and low, taken as the trailing 60-trading-day median of two-day estimates, with negative estimates set to zero, and a minimum of 2 bps.
- **`k = 1`**, the conservative end of the 0.5–1 range the square-root impact literature reports.
- **`σ_daily`** is the standard deviation of the name's daily returns over the trailing 60 trading days.
- **`ADV_$`** is the 60-day median daily dollar volume, the same quantity the universe filter computes.
- **`order_$`** is the trade's notional, not the position's.

A MOC order crosses no spread, but it is not free. It joins the auction's imbalance and moves the clearing price against itself, and the official close is not the mid a trader could otherwise have transacted at. Charging the estimated half-spread on modern auction fills slightly overstates their cost, and that is the direction to be wrong in.

Illustrative magnitudes at $100k: the smallest eligible name ($1M ADV, 3% daily volatility, a modern effective spread around 30 bps) costs roughly 15 bps of floor plus 21 bps of impact, about 36 bps per side. A typical universe name ($20M ADV, 2% volatility, a spread around 10 bps) costs roughly 8 bps per side. At the roughly 8.4× annual book turnover implied by seven buys and seven sells a month at 5% each, every 10 bps of average per-side cost is about 0.84% a year off returns. Cost is concentrated in exactly the small, illiquid names where the documented cross-sectional edge is claimed to live, which is why it is modelled per trade rather than as a constant.

### Pre-2001 and pre-2004 data

The floor term handles the era problem without a hand-picked constant. The spread estimate is computed per name per date from that period's own prices, so a 1998 small cap quoted in sixteenths is charged a 1998 spread and a 2026 small cap a 2026 spread.

Two structural facts make this matter more than decimalisation alone:

- NYSE decimalised on 29 January 2001 and Nasdaq on 9 April 2001. Before then the minimum tick was 1/16 of a dollar.
- The Nasdaq Closing Cross launched on 29 March 2004. Before that date, Nasdaq-listed names had no closing auction; a MOC order filled against the last trade and did pay the spread. For 1998–2004 the "MOC crosses no spread" premise is false for a large part of the small-cap universe.

Charging the estimated half-spread in every era is correct in both regimes: the actual cost before 2004 and a conservative overstatement after. The start-date sensitivity check from [02 — Classical ranker](02-classical-ranker.md) runs against this same cost model.

### Regulatory fees

Applied to sells only, at the current schedule across all of history:

- **SEC Section 31:** $20.60 per $1M of principal, effective 4 April 2026, rounded up to the cent.
- **FINRA TAF:** $0.000195 per share, capped at $9.79 per trade, rounded to the cent.
- **CAT fee:** $0.000001 per executed share.
- **Commission:** $0.

Historical Section 31 rates varied year to year; the difference is under 0.3 bps a side and does not justify a dated rate table. The whole fee stack is roughly 0.2 bps per sell at typical position sizes, material only as a correctness check against broker statements.

### Constants are fixed ex ante

`k`, the 60-day windows, the 2 bps minimum and the choice of estimator are fixed now, before any backtest exists. They are a versioned cost model, not parameters. Changing any of them after results have been seen is a trial, logged against the Deflated Sharpe trial budget like any other. **Any change to the cost model invalidates existing certifications**, because a certified model is certified against a specific cost-model version (recorded as a `certification_event` voiding; see [08 — Database schema](08-database-schema.md)).

### One implementation

The cost function is written once in the shared library and used by the vectorbt scan, the NautilusTrader validation run (as its fee and fill model), the certification gates, and live monitoring. A backtest and live monitoring that disagree about what a trade should have cost is a bug class this design does not permit.

## The validation gates and certification

The gates certify **stage 1 of the ranker only** (standing constraint 2). They run on the workstation as part of the research profile.

- **All gates run on returns net of 1× modelled cost:** the Deflated Sharpe Ratio (fail below ~0.9), the Probability of Backtest Overfitting (fail above ~0.5), and the cost-inclusive Sharpe.
- **DSR must also pass at 2× modelled cost.** This is the stress requirement, because the largest remaining uncertainty is whether `k = 1` and the half-spread estimate are still too low for the smallest names. A strategy whose certification depends on the cost model being exactly right is not certified. The 2× run is one extra backtest pass with fixed constants and adds no trials.
- **The break-even cost multiple is reported for every run** — the multiple of modelled cost at which net Sharpe reaches zero. A diagnostic, not a gate.
- **The Sharpe > ~2 suspicion flag is checked on gross returns as well as net.** An implausible gross Sharpe points at leakage regardless of what costs do to it.
- **Maximum and mean participation (`order_$ / ADV_$`) are reported for every run**, so a certification's liquidity footprint is visible alongside its returns.
- **Cross-validation is purged and embargoed combinatorial (CPCV).** Labels are forward 1-month returns, so the final month of any training fold overlaps the first month of the adjacent test fold; that overlap is purged and an additional month embargoed on either side. CPCV refits the model roughly 120 times per split-set, which is why research runs on the workstation.
- **A start-date sensitivity check** belongs in the suite. If results only hold when 1998–2002 is included, that is a red flag rather than a longer backtest.
- **Every configuration tried is a trial.** Feature-set variants, hyperparameter searches and cost-model changes all count against the DSR trial budget. The effective sample is roughly 320 monthly cross-sections, not a million rows, so the budget is small.

A model that passes becomes a **certification**: a versioned artifact directory plus an immutable `model_certification` row carrying the artifact's sha256 and path, the feature list, the training window, the declared trial count, DSR, PBO and cost-inclusive Sharpe, DSR at 2× cost, the break-even multiple, max and mean participation, the cost-model version, the certified capital, and the CPCV path-distribution quantiles (maximum drawdown, longest losing run, tracking dispersion) from which the performance alerts in [04 — Risk limits and approval workflow](04-risk-and-approval.md) take their thresholds.

**The trial count is checked, not trusted.** The workstation's trial ledger ships inside the artifact as a hashed `trials.parquet`, and the live pipeline refuses a model whose declared trial count differs from the ledger's row count. The live pipeline also refuses any model with no certified row, or a voided one. This is what makes the gates enforceable across the machine boundary rather than a discipline someone remembers to follow. Promotion of a certified model to live is a human act through the server CLI, `agb promote <model_id>`, roughly once a year at the annual refit.

## What paper and live each validate

Alpaca's paper environment has no costs to compare with, so the cost check is split between the stage that can prove each part.

**Paper validates mechanics.** Fills match the official closing price within tolerance. The execution service handles partial fills correctly. The fee arithmetic, computed from the schedule and the fills, is correct.

**Live validates cost.**

- Every live order records a reference price at submission: the NBBO mid at around 15:45 ET.
- Per-fill shortfall is fill price minus reference, signed by side. It is a database view over the fill and its reference, never a stored column.
- Once at least 50 live fills exist, the rolling mean shortfall is compared with the rolling mean modelled cost. If it exceeds the model by more than two standard errors, an alert fires. It notifies and never acts, and feeds the same monitoring layer as the tracking alert.

The shortfall includes ordinary price drift between 15:45 and the close, so it is noisy: the standard error of the mean is around 10 bps over 80 fills. That is enough to catch a model that is wrong by a multiple, which is the dangerous failure, and not enough to fine-tune it, which is not needed.

**Dividends.** Paper does not credit dividends, so any paper-versus-backtest return comparison, including the tracking alert while in paper, must add dividends back or it will show a spurious shortfall of roughly the portfolio's dividend yield.

## UK tax

Tax does not enter certification. It depends on the operator's other income, rate band and allowance usage, which are not properties of the strategy.

The backtest report carries an **after-tax diagnostic**, explicitly labelled approximate: realised gains each UK tax year, net of the £3,000 annual exempt amount, taxed at 24%, computed in USD rather than GBP, and compared with a buy-and-hold SPY position taxed the same way on its eventual disposal. The UK has no short-term/long-term distinction and the strategy realises most of its gains every year, so this is the honest way to see whether the turnover survives tax against doing nothing. It adds no data dependency.

The real CGT ledger — HMRC share matching, GBP conversion at trade-date rates, self-assessment output — is deferred until the build nears live (see the README's deferred list).
