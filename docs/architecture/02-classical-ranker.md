# 02 — Classical Ranker

**Source:** [#5 Design classical ranker: feature set, target, rebalance mechanics](https://github.com/agb1986/agb-trader/issues/5)
**Parent:** [#1 AGB Trader — Architecture Spec Map](https://github.com/agb1986/agb-trader/issues/1)
**Status:** Settled design.
**Date:** 2026-09-10

The classical ranker is the trade-decision authority. It is a **two-stage cascade**, not a single model.

## Architecture

Stage 1 ranks the full universe using only cheap, locally-computed features. The top **N (default 50, configurable)** become a shortlist. The LLM panel ([03 — LLM panel](03-llm-panel.md)) scores only those N names. Stage 2 blends the panel score into the stage-1 ranking. The top **20** become the portfolio.

The cascade resolves a chicken-and-egg problem: the panel's output is a feature the ranker consumes, but a ranker that scored the whole universe would have to rank without that feature to decide which names are worth scoring. The cascade makes the ordering explicit and caps panel cost at N calls per rebalance regardless of universe size. It also ships a built-in A/B: the same shortlist re-ranked with and without the panel term directly measures whether the LLM contributes anything.

## Universe filter

Applied point-in-time at each rebalance date:

- Market capitalisation > $300M
- 60-day median dollar volume > $1M
- Price > $5
- Common stock only (no ETFs, ADRs, or REITs)

Yields roughly 2,400 names today, and between 1,700 and 2,500 at month-ends since mid-1998, measured against the Sharadar lake. (The three quantitative screens alone pass just over 3,000 names today; the common-stock test removes the ADRs, REITs and Canadian listings among them.) The small/mid-cap band is retained deliberately because that is where the documented cross-sectional edge lives; the cascade means panel cost no longer scales with universe size, so there is no cost argument for restricting to large caps.

## Stage 1 model

**Objective.** LightGBM LambdaRank (pairwise/listwise ranking), not regression on return magnitude. Only relative order is ever consumed downstream, and rank objectives are substantially more robust to return outliers and regime shifts than magnitude regression.

**Label.** Forward 1-month return, ranked cross-sectionally within each month, bucketed into quintile grades 0–4. One query group per rebalance month. The label horizon matches the rebalance cadence so the independence assumptions behind purged cross-validation hold. Raw versus market-excess return is immaterial: subtracting a per-month constant does not change within-month ordering.

**Features (20).** All buildable from Sharadar SEP and SF1, with SF1 gated on `datekey` (SEC filing date) to preserve point-in-time integrity. Every feature is a published factor with a known expected sign, which gives the pipeline a sanity check the model can visibly fail.

*Momentum (4)*
- `mom_12_1` — return from t-12 to t-1, skipping the most recent month (Jegadeesh–Titman)
- `mom_6_1` — same construction, 6-month lookback
- `rev_1m` — prior 1-month return (short-term reversal; expected sign negative)
- `mom_12_1_volscaled` — `mom_12_1` divided by trailing 12-month daily-return volatility

*Value (4)*
- `earnings_yield` — TTM net income / market cap
- `book_to_price`
- `fcf_yield` — TTM free cash flow / enterprise value
- `sales_to_price` — TTM revenue / market cap

*Quality (5)*
- `gross_profitability` — gross profit / total assets (Novy-Marx)
- `roe`
- `accruals` — Sloan accruals, change in net operating assets scaled by total assets (expected sign negative)
- `debt_to_equity`
- `asset_growth` — year-over-year change in total assets (expected sign negative)

*Risk (3)*
- `vol_12m` — trailing 12-month daily-return volatility
- `beta_12m` — trailing 12-month market beta
- `ivol_12m` — residual standard deviation from a market-model regression

*Size and liquidity (3)*
- `log_mktcap`
- `amihud` — mean of |daily return| / daily dollar volume, trailing 60 days
- `turnover` — mean of volume / shares outstanding, trailing 60 days

*Context (1)*
- `sector` — Sharadar sector, passed as a native LightGBM categorical

The feature count is deliberately parsimonious. The effective sample size is approximately 320 monthly cross-sections, not the roughly one million individual rows, and the Deflated Sharpe Ratio gate penalises trial count, so every feature-set variant tried must be logged as a trial against that budget.

**Normalisation.** Cross-sectional percentile rank per month, mapped to [0, 1], computed across the full universe. Chosen over z-scoring because fundamental ratios have severe tails (negative book equity, near-zero denominators) that make standardisation unstable. Chosen over sector-neutral ranking because `sector` is retained as a categorical feature instead, letting the model learn sector conditioning where it exists rather than having neutrality imposed a priori.

**Training.** Expanding window from 1998 (the start of Sharadar SEP coverage, which includes delisted tickers) to one year before the current date. Annual walk-forward refit: the model trained through year *Y* trades year *Y+1* and never sees it.

Rejected: a 10-year rolling window, which would discard roughly 60% of an already small effective sample to hedge against regime change, while the strategy's entire premise is that these factor relationships are stable enough to persist. Rejected: monthly refitting, which makes every live decision come from a different model (so signal decay and model drift become indistinguishable) and multiplies trial count for no gain.

**Cross-validation.** Purged and embargoed folds are mandatory, as described in [01 — Data, broker and backtesting](01-data-broker-backtesting.md).

**Known caveat to check in validation.** The early years are structurally different: decimalisation landed in 2001, Nasdaq had no closing auction until 2004, and pre-2001 small-cap spreads were wide enough that results from that era are unlikely to survive realistic cost modelling. A start-date sensitivity check belongs in the validation suite. If results only hold when 1998–2002 is included, that is a red flag rather than a longer backtest.

**Hyperparameters** (learning rate, leaf count, depth, regularisation) are tuned inside cross-validation folds rather than decided here. Each configuration searched counts as a trial and must be logged accordingly.

## Stage 2 — combining the panel score

```
final_score = stage1_rank_pct + w * llm_score_pct
```

`w` is a single configurable weight, default 0.2, held in the immutable, versioned `panel_config` row the basket references. **Nothing is fitted.** There is no trained stage-2 model.

The reason is contamination, and it is a hard constraint rather than a preference. Training a stage-2 model would require historical panel scores for the top N names in every historical month, and the disqualifying problem is that the LLM already knows what happened. Asked to assess Enron in 2001 or SVB in February 2023, it answers with hindsight baked in. Every historical panel score dated before the model's training cutoff is contaminated, so any backtest of a trained stage-2 measures the model's memory rather than its judgement.

A fixed-form blend cannot overfit because nothing is learned. Stage 1 receives the full backtest and validation-gate treatment; the panel's contribution is validated **forward only**, in paper trading, by running `w = 0` against `w > 0` on identical shortlists (the shadow basket series in [03 — LLM panel](03-llm-panel.md)). Live starts at `w = 0`. If the panel adds nothing after a meaningful sample, `w` stays at zero and the only loss is LLM spend.

A hard threshold on `llm_score_pct` gives veto-only behaviour as a configuration, so no separate veto design is needed.

**Interface contract** the panel must satisfy:

- The panel emits **one scalar per name**. Stage 2 percentile-ranks that scalar across the shortlist into [0, 1], the same normalisation applied to every stage-1 feature. This keeps both terms of the blend on one scale, which is what makes `w` interpretable: `w = 0.2` means the panel can move a name at most one fifth of the ranking range.
- Structured sub-scores (bull strength, bear strength, risk flag) are not consumed by stage 2. They would push role-combination weights into the ranker as new free parameters against the trial budget. The roles argue inside the panel; the panel's job is to finish that argument, not export it. (The panel does emit per-dimension scores and reasons for explainability, but stage 2 reads only the scalar.)
- **Failure is neutral.** A timeout, error, or unparseable response yields `llm_score_pct = 0.5`. A failure must be a no-op on the blend, never an implicit penalty; otherwise an API outage silently reshuffles the portfolio toward whichever names happened to succeed.
- **The panel never sees stage-1 rank.** Told that a name ranked third, it would anchor, and the blend would then double-count the same signal while appearing to be independent confirmation. The panel receives the ticker and its own research inputs only.

## Portfolio construction

**Direction: long only.** Borrow availability is worst precisely in the small-cap band where the edge lives, so a short leg would be systematically unfillable and a backtest assuming free borrow would lie. Shorting US equities also complicates UK CGT treatment for the operator. A long/short extension is recorded as deferred.

**Position count: 20**, sized 5% each. Below roughly 15 positions, single-stock blowups dominate the return series and a broken model becomes indistinguishable from bad luck. Above roughly 30, the portfolio dilutes into ranks the model has no real conviction about. Twenty produces roughly seven sells and seven buys per month at typical turnover, small enough to review by eye in the Telegram approval flow. Alpaca fractional shares remove any capital-scale constraint on this choice.

**Weighting: equal weight.** `vol_12m` is already a stage-1 feature, so inverse-volatility weighting would double-count the same factor and make it impossible to diagnose which stage produced an observed effect. Equal weight has zero free parameters, so it costs nothing from the trial budget, and it is the honest baseline any alternative must beat. Inverse-volatility and rank-proportional weighting remain available later as explicitly logged experiments.

## Rebalance mechanics

These must be concrete because NautilusTrader provides no cross-sectional ranking or rebalancing abstraction; this logic is hand-rolled in the strategy layer, and the live pipeline uses the same implementation.

**Cadence: monthly.** Matches Sharadar SF1's quarterly fundamentals refresh, and keeps turnover and slippage drag low.

**Turnover control: rank buffer.** Buy only from the top 20; hold an existing position until it drops out of the **top 30**. This is standard index-construction practice. The distance between rank 20 and rank 25 is noise, not signal, given an effective sample of roughly 320 cross-sections; selling on it pays real spread and slippage to act on nothing. A 1.5× buffer typically cuts turnover by 30–40% with negligible signal loss. A minimum-holding-period rule was rejected as strictly worse: it is blind to the signal, so it forces continued holding of a name that has genuinely broken down.

**Tie-break.** If buffered incumbents plus new top-20 entrants exceed 20 slots, fill from the top of the current ranking downward and drop the lowest-ranked incumbent. Fully deterministic, no discretion, replayable in backtest.

**Execution timing.** Signal is computed from data through the final trading day of the month; orders fill at the **next session's close** via Alpaca time-in-force `cls`, submitted before 15:45 ET.

Computing a signal from the month-end close and filling at that same close is a lookahead bug and the most common way a monthly backtest lies. The closing auction is chosen over the opening auction because it is the deepest liquidity event of the US trading day, frequently 5–10% of daily volume in a single print, small caps included, and it fills at the official closing price without crossing a spread. The official close is also a single published number, so the backtest models the precise price live trading will receive; MOO and VWAP both require assuming a fill. One session of signal decay is immaterial for a monthly signal that already skips a month in `mom_12_1`.

Accepted cost: MOC is a blind order with no limit protection, so a name that gaps on late news fills at whatever the auction prints. Acceptable at 5% position sizing. Limit orders would introduce unfilled positions, a considerably worse problem for a strategy that assumes a known holding count.

## Between rebalances

No discretionary intra-month action. Positions are held until the next rebalance.

Stop-losses were rejected. `rev_1m` carries a negative expected sign, meaning the strategy explicitly bets that recent losers rebound; a stop-loss sells precisely those names at the local bottom, fighting the model. It is also a free parameter with no supporting prior. Event-driven exits on bankruptcy filings or going-concern warnings were rejected as largely redundant: such news is in the price before the system could act, and the name drops out of the next month's ranking anyway.

Forced-event handling is built, and the same rules serve the backtest and live:

- **Cash acquisition** — the position terminates at the deal price; hold cash until the next rebalance rather than redeploying mid-month, since redeployment is an off-cadence decision with no signal behind it.
- **Stock-for-stock merger** — acquirer shares are received and sold at the next rebalance regardless of the acquirer's rank, because that position was never chosen.
- **Delisting to OTC** — exit at the last listed print. Sharadar SEP carries delisted tickers, which is precisely why it was selected; the backtest models this honestly instead of silently dropping the name.
- **Trading halt spanning a rebalance date** — carry the position, skip its replacement, and run 19 names that month.

The last case implies a requirement that is easy to miss: **position-count logic must tolerate holding fewer than 20 names**, or the strategy fails at the worst possible moment.

As built, an event is a row the operator files through `agb corporate record` before the rebalance runs, carrying the human and the command behind it, and `agb/corporate.py` turns the record into the few rules both paths read: which names cannot be traded this month, which must be sold whatever they rank, the price a terminated position settles at, and the holding the event leaves. A cash acquisition and a merger end the position on the effective date and are charged nothing, since neither was a decision of ours; the acquirer shares land in the book and are sold at the next rebalance. A delisting and a halt leave the holding alone and differ only in what the next rebalance does with it: the delisted name is sold at its last listed print, and the halted one is carried, which drops that month's slot count by one and leaves 19 names to trade. Nothing else moves off cadence. An event nobody filed is not inferred from the book: the rebalance halts instead, because an unexplained change in a holding is exactly what the operator should see.

Live positions are taken from daily broker snapshots rather than summed fills, precisely because corporate actions change holdings without a fill (see [08 — Database schema](08-database-schema.md)).

## Scope of validation

The DSR, PBO, and cost-inclusive Sharpe gates certify **stage 1 only**. The LLM panel rides on top and is unbacktested by construction. A passing backtest is not evidence that the LLM layer works.
