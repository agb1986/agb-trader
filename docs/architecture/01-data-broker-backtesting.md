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

- **Delisting exits.** The engine sells a held name at its last listed print less a delisting return, the forced exit [02 — Classical ranker](02-classical-ranker.md) requires. The lake has no price for what an OTC sale realises, so the return is an assumption: `backtest.DELISTING_RETURN`, −30%, after Shumway (1997), who put performance-related delistings from the NYSE and AMEX at about −30%; Shumway and Warther (1999) found Nasdaq's worse still. It is a backtest option, recorded with the trial, and every run reports the exits and the dollars the return took. Tradeability is read from the signal session alone, as live reads it, so a name whose bars stop before the trade session is still planned and its buy goes unfilled. The scan carries the last price forward and sells the name at the next rebalance that drops it out of the ranking.
- **Trims.** The engine trims a buy the account cannot fund whole, and drops one that rounds to no shares at all. vectorbt's shared cash group does neither.
- **Whole shares.** The engine sizes whole shares at the signal session's close, as the live basket sizes them; vectorbt sizes fractional shares at the fill.

On a synthetic market none of the three ever fires and the two agree exactly, which is what `tests/test_backtest.py` asserts. On the real lake they fire tens of times over a few dozen months, so what is checked there is that the divergence is only those frictions: `tests/test_backtest_live.py` reconciles the two order sets through the engine's own delisting-exit and trim counts and bounds the gap in the scale-free return statistics. Teaching the scan about delistings and trims would make it a second engine and remove the reason it exists.

Because the scan is a filter, what matters is not the size of the gap but whether the scan puts candidate configurations in the engine's order. `tests/test_scan_ranking_live.py` measures that over the saved full-history panel of 344 months: ten configurations varying the signal, the basket size, the rank buffer and the starting capital, each run once through the scan and once through the engine, scored by rank correlation and by top-three agreement.

The ordering holds. Over the seven configurations the scan would run, the rank correlation with the engine is +0.96 on net Sharpe and +0.86 on annual return, and the two sides pick the same top three: `mom_12_1_volscaled`, a 40-name basket with a 60-name buffer, and the live configuration on a tenth of the capital. The test checks membership of the top three rather than their internal order, because the third and fourth places sat 0.054 Sharpe apart while the first three were within 0.053 of each other. An operator screening configurations on the scan and validating the survivors on the engine is therefore screening on something that ranks like the engine.

Three results qualify that, and all three are reasons to validate rather than to trust the scan's numbers:

- **The level is biased high, and not uniformly.** The scan's Sharpe sits above the engine's on every configuration measured, by between 0.005 and 0.050. The bias is a function of the configuration, so the scan's Sharpe is not a figure to quote and the gap on one configuration does not predict the gap on another.
- **The scan refuses configurations the engine runs.** All three non-momentum signals tried (`rev_1m`, `earnings_yield`, `book_to_price`) raise within seconds, at the panel's first rebalances, because they rank a name the lake has no price or liquidity for at the session the scan needs one; the engine runs the same three end to end. A configuration the scan cannot simulate is one it cannot filter, so a refusal sends the candidate straight to the engine rather than discarding it.
- **The two sides do not run quite the same window.** The engine starts after the early months the lake cannot price with the liquidity gate's bars, where the scan runs the whole panel: 342 months against 344 over the saved panel. That asymmetry is part of why the scan's level sits high, and the test bounds it rather than hiding it.

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

The per-share fees, TAF and CAT, count the shares as they traded. The validation run holds split-adjusted shares at adjusted prices, so its equity curve is the adjusted one, and converts each fill's shares at that session's as-traded close (`closeunadj`) before pricing the fees: a name that has split 4:1 since printed a quarter of the shares at four times the price. The liquidity inputs are unaffected, because the spread, σ_daily and ADV_$ read the vendor's split-adjusted close, high, low and volume, whose ratios and products do not depend on the share basis.

Historical Section 31 rates varied year to year; the difference is under 0.3 bps a side and does not justify a dated rate table. The whole fee stack is roughly 0.2 bps per sell at typical position sizes, material only as a correctness check against broker statements.

### Constants are fixed ex ante

`k`, the 60-day windows, the 2 bps minimum and the choice of estimator are fixed now, before any backtest exists. They are a versioned cost model, not parameters. Changing any of them after results have been seen is a trial, logged against the Deflated Sharpe trial budget like any other. **Any change to the cost model invalidates existing certifications**, because a certified model is certified against a specific cost-model version (recorded as a `certification_event` voiding; see [08 — Database schema](08-database-schema.md)).

### One implementation

The cost function is written once in the shared library and used by the vectorbt scan, the NautilusTrader validation run (as its fee and fill model), the certification gates, and live monitoring. A backtest and live monitoring that disagree about what a trade should have cost is a bug class this design does not permit.

## The validation gates and certification

The gates certify **stage 1 of the ranker only** (standing constraint 2). They run on the workstation as part of the research profile.

- **All gates run on returns net of 1× modelled cost:** the Deflated Sharpe Ratio (fail below ~0.9), the Probability of Backtest Overfitting (fail above ~0.5), and the cost-inclusive Sharpe. The Sharpe and both DSR gates read those net returns in excess of SPY, as defined below; the PBO ranks them as total returns.
- **DSR must also pass at 2× modelled cost.** This is the stress requirement, because the largest remaining uncertainty is whether `k = 1` and the half-spread estimate are still too low for the smallest names. A strategy whose certification depends on the cost model being exactly right is not certified. The 2× run is one extra backtest pass with fixed constants and adds no trials.
- **The break-even cost multiple is reported for every run** — the multiple of modelled cost at which net Sharpe reaches zero. A diagnostic, not a gate.
- **The Sharpe > ~2 suspicion check is a gate, and it reads gross returns as well as net.** Its value is the highest of the gross, total-return and gated net Sharpe, and the verdict fails when that exceeds the `suspicious_sharpe` threshold (2.0). An implausible gross Sharpe points at leakage regardless of what costs do to it. Before issue #187 this was an advisory flag that never failed a verdict; the manifest still carries it as `gross_sharpe_flag` beside the `gross_sharpe` gate.
- **The DSR's variance is the variance of Sharpe across the declared trials.** The expected maximum Sharpe under the null scales with the standard deviation of the Sharpe ratios the search produced, so the variance is measured across the candidates' annualised Sharpe ratios (on the excess basis, each candidate's months less the benchmark's mean month), floored by the variance across the model's own paths and by the sampling variance 1/T so that a field of near-identical trials cannot shrink the bar. With fewer than two distinct trials the paths and sampling variance stand alone. The skewness and kurtosis are measured on the de-duplicated monthly series, the paths' mean month, since the CPCV paths reuse the same test months; the moments pooled over every path are reported beside them, and the DSR is the lower of the two statistics. A variance term that comes out zero or negative raises rather than producing a number.
- **A PBO tie counts as overfit.** When the in-sample winner lands exactly at the out-of-sample median, its logit is zero and it is counted as overfit, not as a success.
- **Maximum and mean participation (`order_$ / ADV_$`) are reported for every run**, so a certification's liquidity footprint is visible alongside its returns.
- **Cross-validation is purged and embargoed combinatorial (CPCV).** Labels are forward 1-month returns, so the final month of any training fold overlaps the first month of the adjacent test fold; that overlap is purged and an additional month embargoed after each test block. The embargo is one-sided: only training months whose features were computed inside a test month's window, or in the month after it, are dropped. A training month before a test block needs none, because its label window ends before the test's features exist, and the purge already removes any whose window overlaps. CPCV refits the model roughly 120 times per split-set, which is why research runs on the workstation.
- **A start-date sensitivity check is a gate.** The gated net Sharpe is recomputed from start dates a quarter, a half and three quarters of the way through the paths, and the verdict fails unless every one of them clears the Sharpe bar; the gate's value is the lowest. If results only hold when 1998–2002 is included, that is a red flag rather than a longer backtest. Before issue #187 this was an advisory `start_date_stable` flag; the manifest still carries it beside the `start_date` gate. Certifications made before the change are refused by the server's recompute and have to be certified again.
- **Every configuration tried is a trial, and a trial is a configuration, not a ledger row.** Feature-set variants, hyperparameter searches and cost-model changes all count against the DSR trial budget. The effective sample is roughly 320 monthly cross-sections, not a million rows, so the budget is small.
- **The declared count is the distinct configurations the ledger says were started.** The ledger records a row per run, and two kinds of row are not another configuration searched: a certification writes three rows, its own and the cross-validation and the backtest it nests, which together are one configuration evaluated once; and the same configuration rerun after a crash, or rechecked by `reproduce`, writes a row every time. A row `reproduce` writes also names the trial it reruns, in its `reproduces` column, so the ledger says which rows were checks rather than searches; the mark changes no count, and an older ledger without the column reads and chains as before. So the count drops the rows a run opened inside another, which each row names as its `parent`, and takes distinct configurations rather than rows. A configuration that failed or was interrupted every time it ran still counts: the search spent it, and leaving it out would let a configuration that fell over on a bad result disappear from the budget. A run that merely overlapped another, such as a scan started from a second terminal during a certification, names no parent and counts. Rows from before the `parent` column existed are still nested by their intervals. A configuration's identity includes its training window, the lake partition the panel was built from and the cost-model version, so rebuilding the panel and trying the same model again is a new trial, not the same reading again. Screening scans over raw signals count, because they are searches whose outcome informed what was certified; keeping them in a separate ledger to keep them out of the count would be the same evasion with more files. Every manifest records the count, the rule it was counted under and the ledger's total row count, so the two numbers can always be compared.
- **PBO ranks one column per configuration, and needs at least two.** The candidates are the model's own paths and the ledger's other succeeded backtests over the same panel, window and cost model, one column per distinct configuration: a rerun of a configuration is not another column, and a backtest nested inside a certification is identified by the run at the root of its `parent` links, the certification that ran it, since every nested backtest's own row reads alike; rows from before the `parent` column are still nested by their intervals. Identical series are one candidate. With fewer than two distinct candidates there is no selection to measure, so the PBO is undefined and the gate fails with that cause named, stored as 1.0 because the column cannot hold an undefined value. Every series left out is reported in the manifest with its reason, and a configuration that should have had a column but could not be read (a short series, a month that is not finite) fails the gate rather than quietly shrinking the field.
- **The Sharpe gates read returns in excess of SPY.** A long-only book of US equities earns the market's return before it earns anything of its own, so a Sharpe on total returns clears `sharpe > 0` on equity beta alone. Each month's excess return is the book's net return for the month less SPY's total return (split- and dividend-adjusted close to close) over the same holding window, from the month's entry close to the next month's entry close (the final exit for the last month), with the benchmark's close required on both sessions. The cost-inclusive Sharpe, the DSR and the DSR at 2× cost are computed on that series, and the total-return Sharpe is reported beside them, labelled as such, as a diagnostic. The PBO stays on total returns: it ranks the model's paths against the ledger's other backtests, and those older candidates carry no benchmark, so a mixed field would rank the basis rather than the configurations. The basis is recorded in the manifest as `sharpe_basis` (`total` or `excess_spy`) and in each Sharpe gate. If the lake holds no SPY return for any month of any path, the excess Sharpe is undefined: the three Sharpe gates fail with that cause named and the artifact is still written, so the paper-only waiver has something to waive. Partial coverage fails the same way; no month is dropped and none is filled with zero. A failed gate stores 0.0 for the Sharpe and for each DSR because the columns cannot hold an undefined value. Certifications up to `lgbm-2026-10` predate the basis, carry no `sharpe_basis` and gated total returns; the server now recomputes the basis with the rest of the verdict, so they are refused and have to be certified again.
- **What the count cannot cover.** Choices made without a run leave no row and are not charged for: which features exist and which were kept, the hyperparameters, how the panel and the cost model are built, and how often any of that was adjusted after seeing a result. Counting configurations rather than rows also lowers the declared number, and a lower number is an easier DSR bar, which is a reason to distrust the rule rather than a benefit of it — it is right only to the extent that each row it drops really is one evaluation's mechanics. What keeps the number honest is a budget for the runs a model is allowed, pre-registered before the runs happen, and the row count printed beside the declared count in every manifest and report. The gate's arithmetic cannot do it.

A model that passes becomes a **certification**: a versioned artifact directory plus an immutable `model_certification` row carrying the artifact's sha256 and path, the feature list, the training window, the declared trial count, DSR, PBO and cost-inclusive Sharpe on the recorded basis, DSR at 2× cost, the break-even multiple, max and mean participation, the cost-model version, the certified capital, and the CPCV path-distribution quantiles (maximum drawdown, longest losing run, tracking dispersion) from which the performance alerts in [04 — Risk limits and approval workflow](04-risk-and-approval.md) take their thresholds.

**The trial count is checked, not trusted.** The workstation's trial ledger ships inside the artifact as a hashed `trials.parquet`, and the live pipeline recounts it, always under the `started` rule fixed in the code, and refuses a model whose declared trial count differs from that recount or whose ledger holds a different number of rows than the manifest shipped. Each ledger row carries a digest of the row before it, so a row taken out of the middle, inserted or reordered breaks the chain, and both the certification run and the artifact import refuse a ledger whose chain is broken. Writers take a lock on the ledger and write through a unique temporary file, so two runs appending at once both keep their rows. A manifest that names another rule, or none, is refused with that cause and the model has to be certified again (issue #211). The import and the live pipeline run the same check, `agb.research.artifact.load`, which also recomputes the deflated Sharpe, the PBO, the Sharpe and the verdict from the shipped `paths.json` and ledger and refuses a manifest that differs from them, and the artifact digest the certification row holds covers the whole manifest. A forger who rewrites the returns themselves consistently across `paths.json`, the certification's ledger row and the candidates' rows is not caught by this; the ledger's chain and the row's digest pin those to what the certification wrote, not to what the engine computed. The live pipeline also refuses any model with no certified row, or a voided one. This is what makes the gates enforceable across the machine boundary rather than a discipline someone remembers to follow. Promotion of a certified model to live is a human act through the server CLI, `agb promote <model_id>`, roughly once a year at the annual refit.

**A certification's expensive stages are cached, and the cache is not part of the evidence.** A certification run is dominated by two stages: the CPCV, which refits the model roughly 120 times, and the backtest, which replays every path through the engine. Both are pure functions of the panel, the model configuration and the cost options, so each is keyed by a digest over those inputs and stored under `<research_dir>/cache/`. A rerun that changes nothing reuses both and names the same nested trial ids, so it writes no new ledger rows and does not inflate the declared trial count; a rerun that changes the model misses both, and one that changes only the cost multiple misses only the backtest. The entry is written after the ledger row says the run succeeded, and a hit is served only while the ledger still holds that row, so a cache can never stand in for evidence the ledger does not have. The key always includes the code version, which `make research` sets to the checkout's commit and without which `certify` refuses to run, and a digest of the panel's values, features and labels included, so a panel rebuilt after a feature or label formula changed misses even where its rows and partition digest are unchanged. The record the key was built from is stored beside the payload and compared on read, so a stale or colliding entry is a miss rather than a wrong answer, and a miss is never an error. The `cpcv` command writes the same entry when the build has a version, scores included, so a cross-validation run by hand ahead of a certification of the same configuration over the same panel is not repeated by it. The shipped model's own fit is not cached: every certification fits the artifact it ships. What this buys is resumption — an interrupted run restarts at the first stage it had not finished — and it is a research-side convenience only; the live pipeline reads the artifact and the ledger, never the cache.

## What paper and live each validate

Alpaca's paper environment has no costs to compare with, so the cost check is split between the stage that can prove each part.

**Paper validates mechanics.** Fills match the official closing price within tolerance. The execution service handles partial fills correctly. The fee arithmetic, computed from the schedule and the fills, is correct.

**Live validates cost.**

- Every live order records a reference price at submission: the NBBO mid at around 15:45 ET.
  As built, the quotes for every order in a basket are read in one request before the first order goes out, so a name without a quote sends nothing. The request names its feed: `iex` by default, which a free data plan serves in real time but which is the IEX book rather than the consolidated NBBO; the NBBO is the `sip` feed and needs a paid plan.
- Per-fill shortfall is fill price minus reference, signed by side. It is a database view over the fill and its reference, never a stored column.
- Once at least 50 live fills exist, the rolling mean shortfall is compared with the rolling mean modelled cost. If it exceeds the model by more than two standard errors, an alert fires. It notifies and never acts, and feeds the same monitoring layer as the tracking alert.

The shortfall includes ordinary price drift between 15:45 and the close, so it is noisy: the standard error of the mean is around 10 bps over 80 fills. That is enough to catch a model that is wrong by a multiple, which is the dangerous failure, and not enough to fine-tune it, which is not needed.

**Dividends.** Paper does not credit dividends, so any paper-versus-backtest return comparison, including the tracking alert while in paper, must add dividends back or it will show a spurious shortfall of roughly the portfolio's dividend yield.

## UK tax

Tax does not enter certification. It depends on the operator's other income, rate band and allowance usage, which are not properties of the strategy.

The backtest report carries an **after-tax diagnostic**, explicitly labelled approximate: realised gains each UK tax year, net of the £3,000 annual exempt amount, taxed at 24%, computed in USD rather than GBP, and compared with a buy-and-hold SPY position taxed the same way on its eventual disposal. The UK has no short-term/long-term distinction and the strategy realises most of its gains every year, so this is the honest way to see whether the turnover survives tax against doing nothing. It adds no data dependency.

As built, `agb/research/tax.py` does the arithmetic in `Decimal` and every backtest report carries an `after_tax` entry: the rate, the allowance, a list of the simplifications the figure rests on, and a row each for the strategy and for buy-and-hold. Each disposal's allowable cost comes out of a Section 104 average-cost pool per name, so a part disposal takes its share of the pool and the disposal that empties it takes the rest; a year's losses net against its gains in full and a surplus carries forward to relieve a later year's gains only above the allowance. Positions still open at the end are deemed disposed of at the final session's close with no dealing cost, so the bill covers the same final value the report marks and the comparison with buy-and-hold is like for like: whole shares of SPY bought at the first trade session's close with the same starting cash, the rest left in cash, held to the final exit. SPY is priced from the lake alongside the traded names but kept out of the engine's feed, so it can neither be ranked nor bought and the forced-exit rule's view of where the lake ends does not move with it; a lake of equity prices need not hold an index fund at all, and when it does not the comparison reports nothing and names the ticker it wanted rather than failing the run. The same SPY closes are reported per month beside the strategy's returns, which is the one place certification reads them: the Sharpe gates measure excess over that series, and a certification on a lake without it fails those gates (see the validation gates above). Certification reads none of the after-tax entry: `agb/research/certify.py` and `agb/research/gates.py` never mention the entry, and a test reruns a backtest at a rate that would change any verdict to show every other figure in the report coming out identical.

The real CGT ledger — HMRC share matching, GBP conversion at trade-date rates, self-assessment output — is deferred until the build nears live (see the README's deferred list).
