# 04 — Risk Limits and Approval Workflow

**Source:** [#7 Design risk limits and live-approval workflow](https://github.com/agb1986/agb-trader/issues/7), amended by [#11](https://github.com/agb1986/agb-trader/issues/11) (paper-to-live cost criterion, live cost check, capital alert) and [#12](https://github.com/agb1986/agb-trader/issues/12) (pause mid-submission, named halt causes, server-side CLI for upward moves)
**Parent:** [#1 AGB Trader — Architecture Spec Map](https://github.com/agb1986/agb-trader/issues/1)
**Status:** Settled design.
**Date:** 2026-09-10

Risk limits are **malfunction detectors on a pre-trade gate**, and the approval workflow is a **per-basket decision on state the system owns**, surfaced through Telegram and the local UI. This document fixes control *semantics*: which operations exist, what each does, and what state each reads and writes. Command naming and message formatting belong to [05 — Telegram bot](05-telegram-bot.md); rendering and layout to [06 — Local UI](06-local-ui.md); the tables to [08 — Database schema](08-database-schema.md).

## What the limits are for

The limits catch the system misbehaving, not the market misbehaving. Breaching one halts trading and escalates to a human; it never modifies or liquidates positions.

This is a narrower job than the usual retail risk-limit kit, deliberately. Position size is already bounded by construction: the ranker is long-only, 20 equal-weight positions, no leverage, no shorts, so a position cannot exceed roughly 5% of equity without something being broken. There is no sizing decision left for a limit to constrain.

More importantly, a drawdown stop is a second strategy layered on top of the certified one. The validation gates certify a specific strategy: hold 20 names, rebalance monthly, apply the rank buffer. A rule that liquidates the book at some drawdown produces a different return distribution, one that never appeared in any backtest, and it engages at the worst possible moment. Converting a validated strategy into an unvalidated one during a drawdown is a poor trade.

The failure modes worth defending against are engineering failures: duplicate order submission, wrong sizing after a fractional-share rounding error, a stale data snapshot producing a shortlist of delisted names, the panel returning garbage that swings stage 2, an order submitted after the MOC cutoff. Those are detectable, and halting is the right response to every one of them. A 15% drawdown in a small-cap book is an ordinary month.

Rejected: risk overlays (drawdown stops, volatility targeting, de-grossing rules). Rejected: a catastrophic-loss circuit breaker that liquidates automatically. A human-pulled kill-switch is a separate matter, below.

## Where the gate sits

**Pre-trade, in the execution path.** Every order passes a check function before submission. A failed check means the order is never sent, the whole rebalance halts, and an alert fires.

The MOC decision forces this. Orders fill at the closing auction and cannot be pulled back afterwards; there is no intraday window in which to notice a bad order and unwind it. A post-hoc monitor on a monthly strategy reports the problem roughly a day after it became a position, and the next opportunity to correct it is the following month's rebalance. That is not a control.

**A failed check halts the entire rebalance**, rather than skipping the offending order and sending the rest. A partial rebalance is not the strategy: it is 17 of 20 positions with three names' weight sitting in cash, a portfolio nobody chose and nothing validated. A failed sanity check means something upstream is wrong, so the safe state is last month's book unchanged.

Alpaca's account configuration is kept as a backstop rather than the mechanism: `no_shorting` set on the account, cash account, no margin. It costs nothing and covers the case where a bug in the execution service generates an order that the service's own gate has the matching bug about. Two independent rejections on opposite sides of the network boundary.

## What the gate checks

Checks span three levels, because a basket can be perfectly self-consistent and still entirely wrong: 20 names, weights summing to 100%, every order valid, all computed from last month's prices because the data sync silently failed. Order and basket checks pass that without complaint.

**Upstream provenance.**
- The Sharadar snapshot date equals the expected month-end.
- Universe size falls in a plausible band, 1,500 to 3,500 names, so a 200-name universe stops everything. The band brackets the measured history (1,700 to 2,500 at month-ends since mid-1998) with room either side; it is a provenance check on the lake and the filters, not a view on how many names the strategy wants.
- The model artifact hash matches the promoted certification.
- The panel returned scores for the entire shortlist, with dispersion above the gate specified in [03 — LLM panel](03-llm-panel.md).

**Basket invariants.**
- Exactly 20 target positions (or fewer only through the forced-event cases the ranker defines).
- Weights sum to 100% within tolerance.
- No duplicate symbols.
- Projected cash non-negative.
- Turnover below roughly 60% of the book. The 20-buy/30-hold rank buffer should produce far less, so 60% indicates something upstream moved that should not have.

**Order level.**
- Symbol present in the current universe snapshot.
- Alpaca reports the asset tradable, and fractionable where fractional shares are used.
- Side consistent with the current position.
- Per-order notional at or below roughly 8% of equity, the band absorbing intra-month drift around the 5% target.
- Time-in-force is `cls`.
- Submission clock earlier than 15:45 ET, five minutes of pad on Alpaca's approximately 15:50 cutoff.

Every check halts and escalates; none silently skips. The gate result is recorded check by check on the basket, so `/gate` and the UI's gate view show what each measured rather than pass/fail alone. Thresholds are deliberately wide. They are tripwires for "the pipeline is broken", not for "the market is unusual", and a threshold tight enough to fire on legitimate variation trains the operator to ignore the alert.

## Halt semantics

The halt is **sticky**. The system latches into a halted state, refuses all order submission, leaves existing positions untouched, and stays halted until a human explicitly clears it.

Auto-retry is the wrong shape for these checks: none of them fail transiently. A stale snapshot, a 200-name universe, a hash mismatch all remain false until someone fixes something, so retrying only burns the MOC window producing identical failures. Auto-resuming the following month is worse: the failure stays unexplained until a human looks at it, and a month later the same broken pipeline quietly submits a rebalance built on the same bad data, with the alert now four weeks stale and nobody watching.

Two properties make the latch real. It is **persisted outside the process**, as database rows rather than in-memory state, because a container restart that clears the halt turns a crash-loop into an auto-resume, which inverts the intended behaviour: the crash is evidence the halt was correct. And clearing is **explicit and attributed**, a command that names what is being cleared, timestamped and logged, never a passive timeout or a redeploy side effect.

**The latch is a set of named causes.** `halt_event` rows each set or clear one cause:

- `gate_breach` — a pre-trade check failed.
- `execution_lease_expired` — an execution process died mid-submission (see below).
- `pause` — a human pulled the pause lever.
- `liquidation` — a liquidation was executed; the system pauses itself afterwards.

Trading is blocked while *any* cause is open, and the open set is a database view. A second cause arriving while halted neither overwrites nor hides the first. `/resume` clears every open cause but records one clear per cause, attributed, and its reply lists what it cleared, so a pause cleared together with an unexplained gate breach stays visible afterwards.

**Execution re-reads the latch before each individual order**, not only when it claims the job, so a pause at 15:44 stops every order not yet sent.

**The broker latch is observed, not assumed.** Each cycle, execution records the `suspend_trade` value it reads from Alpaca in `broker_state`. A mismatch with the database latch raises an alert in either direction: latched in the database but not suspended at the broker, or the reverse.

**An expired execution lease is never retried automatically.** Pipeline stages write only to the database and are reclaimed and rerun when a lease expires; execution submission has side effects at the broker the database cannot see, so an expired submission lease sets `execution_lease_expired` instead. A human-triggered reconcile looks up what the broker actually holds by deterministic `client_order_id` before anything is resent.

Positions are untouched throughout. Halt stops new orders; it never liquidates.

## Approval unit: the basket

Live mode requires approval of the **whole rebalance basket**, one approve or reject, with drill-down available on any individual name. This refines the map's original "approval per trade" wording, which predates knowing that a rebalance emits roughly 10 to 16 orders at once.

Per-order approval reintroduces exactly the failure the all-or-nothing halt rules out. Approving 12 of 14 orders produces a portfolio nobody validated, and no gate catches it because a human authored it. The all-or-nothing property has to survive contact with the human or it is not a property. It also degrades the review: fourteen near-identical prompts under a hard MOC deadline produce rubber-stamping within two months, and rubber-stamping is worse than autonomy because it manufactures a false record of oversight.

The decision actually being made each month is the portfolio, which is also the only object the backtest ever certified. Individual orders are the arithmetic that moves last month's book to this month's. Approving the arithmetic is not oversight.

Rejected: exception-only approval, auto-approving orders that match the expected pattern. "Unusual" is already what the pre-trade gate defines, and anything it flags already halts; a second, looser definition of unusual adds a code path that fires rarely and is therefore untested when it does. It also undercuts the purpose of live-mode approval, which is building justified trust through repeated exposure to real decisions rather than minimising interruptions.

**Approval names the basket.** Every basket has a typeable reference (`2026-10`); the approve act must carry it, so an approval composed against one basket cannot land on another. The command row stores the reference as typed and the basket it resolved to.

## What the approval message shows

The message is an **anomaly-detection instrument, not a justification document**.

The human is not there to re-derive the model's decision. They cannot do so in the time available, and if they could, the model would be redundant. They are there to catch the two things the model structurally cannot: that something has gone visibly wrong in a way the gate did not anticipate, and exogenous knowledge the pipeline has no access to — a name in the news this morning for something no filing yet reflects, a pending corporate action, a reason unrelated to markets to sit the month out.

Content, in order:

1. **The diff** — names leaving, names entering, names held. The diff is small and is where anything alarming appears; the full holdings list is 20 lines of mostly unchanged.
2. **Portfolio context** — turnover, resulting cash, largest sector share.
3. **Flags, and only flags** — names where the panel's safety score is low while stage 1 ranked them highly, names with any rubric dimension at 0 or 1, names entering with unusually thin liquidity.
4. **Gate status** — an explicit line confirming the checks passed.
5. **The prompt**, plus drill-down commands for any individual name, and the stated consequence of inaction.

Rejected: inlining full stage-1 scores and panel debate transcripts. Volume defeats review; a message containing four debate transcripts is not read, and unread detail is worse than absent detail because it creates the appearance of scrutiny. The six-dimension rubric carries the drill-down: a flag reads "accounting integrity 1/4" with the Judge's reason, which is precisely what a human needs when a name looks wrong.

## Timeout behaviour

If no answer arrives before the cutoff, **the rebalance is skipped**. Live mode only; paper runs unattended.

Auto-approval on timeout would make the approval fiction. If silence executes the trade, the control does nothing on exactly the occasions it was meant to matter: the operator asleep, in a meeting, phone dead. The point of live-mode approval is that a human affirmatively saw it, and a rule treating absence as assent cannot deliver that.

Skipping is affordable because of the rank buffer. Holding names down to rank 30 means the existing book is largely still inside the hold band a month later, so a missed rebalance is closer to a hold decision the strategy already tolerates than to an unmanaged position.

Rejected: automatic roll-forward to the next session's MOC. The backtest assumes fills at the next session's close; drifting to the session after that is an unmodelled timing deviation, and once automatic it can chain across days with no natural stopping point.

Mechanically: reminders at roughly 60 and 20 minutes before the cutoff. At the cutoff the scheduler marks the basket **missed**, not **failed**. It does not set the halt latch, because nothing is broken, and the next month proceeds normally. Afterwards the human may issue an explicit command to run the same basket at the next session's MOC. That is a new basket, `2026-10r1`, with `deviation_of` pointing at the original, which keeps its missed outcome; the deviation is visible later when live performance is reconciled against the backtest.

## The autonomy ladder

Three levels, with movement up by human act and movement down automatic.

- **L0** — live start. Explicit approval every rebalance. Timeout means no trade.
- **L1** — explicit approval required only when the gate raises a flag or turnover is unusual. Otherwise the basket is notified with a countdown and proceeds unless rejected inside the window. The default inverts from opt-in to opt-out; the veto survives. As built, unusual turnover means selling more than 30% of the book, well below the gate's own 60% limit, and the countdown is the fifteen minutes before the 15:45 submission cutoff, so the window closes exactly as execution would otherwise have nothing to send.
- **L2** — notification only, no approval path.

The pre-trade gate and the kill-switch are not part of the ladder. They apply at every level, permanently.

Graduation criteria establish **eligibility**; the human performs the move and it is logged. L0 to L1 wants roughly six consecutive live rebalances with zero gate breaches, no missed-approval events, and **zero human rejections**, with a rejection resetting the counter. L1 to L2 wants a longer run, on the order of a year, with no rejections issued during the opt-out window.

The zero-rejections clause is the honest test. The question the ladder answers is whether the human's review is load-bearing, and the only evidence that it is not is a run of months in which the human looked and changed nothing. A rejection means the control did work, and the case for removing it has not been made.

**Any rejection or gate breach at L1 or L2 drops the system back to L0 automatically**, recorded as a `ladder_event` with actor `system`. Loosening is manual; tightening is not.

**Upward moves happen through a server-side CLI.** Loosening the ladder (`agb level`), promoting paper to live (`agb mode`), and promoting a newly certified model (`agb promote`) are human acts performed on the server, not from Telegram or the UI. The CLI refuses unless an eligibility view says the criteria are met. Eligibility is computed from basket and command history rather than stored counters: consecutive clean rebalances, rejections during the opt-out window, and pause, liquidate and approval each exercised in paper. Criteria that are human judgements are recorded as an attestation on the event. `/level` on Telegram is read-only.

Rejected: time-based automatic transition. The move to autonomy is the most consequential state change in the system, and a timer makes it happen on a Tuesday with nobody watching. Rejected: notional banding. Basket size scales with the account rather than with risk of error; a bug produces the same wrong portfolio at any size, and a threshold merely means it goes unreviewed while the account is small, which is when the pipeline is least proven.

## Kill-switch: two levers

**Pause** sets the `pause` halt cause and, independently, Alpaca's `suspend_trade` account configuration flag, which Alpaca documents as an emergency-stop primitive that blocks new orders broker-side. Two latches failing independently on opposite sides of the network boundary: a bug in the execution service that submits anyway is rejected by the broker. Clearing requires unsetting both.

The pause stops all order submission: scheduled rebalances and any deferred manual rerun. It does **not** stop data ingestion, model inference or panel runs. Those are harmless while paused, and keeping them alive preserves the record of what the system would have done during the pause, which is exactly the evidence needed when deciding whether to resume.

**A pause mid-submission also cancels what was already sent.** A pause that lands partway through submission would otherwise leave exactly the partial rebalance this design rules out, because `suspend_trade` only blocks new orders and does not cancel open ones. When a pause arrives mid-submission, execution cancels that basket's already-sent orders while it is still before 15:50 ET, restoring last month's book. MOC orders can be cancelled freely until 15:50 on both NYSE and Nasdaq. After that, NYSE allows cancellation only for a legitimate error, and Nasdaq allows it only for a legitimate error until 15:55 and never after. The 15:45 submission cutoff leaves a five-minute window. If a cancel does not land, a partial-rebalance alert names what remains live. It notifies and does not act further.

**Liquidate** issues `DELETE /v2/positions?cancel_orders=true`, which cancels open orders and then closes every position. It is two-step with a typed confirmation token, never a single tap, and it pauses first so nothing re-enters behind it. The response is HTTP 207 with per-symbol status, which is parsed and reported back per symbol: partial liquidation is a real outcome (a halted symbol cannot be sold) and must not be reported as success. The `liquidation` cause stays open afterwards, so the remaining position is not acted on by anything.

Having the lever is not the same as wiring it to a threshold. Automatic liquidation stays rejected, but a human deciding to exit is a different act: a compromised account, discovering the pipeline has been trading on corrupt data for weeks, needing the cash, a tax or personal event. Without a one-command exit, that situation gets improvised under stress through the broker's web UI at the worst moment.

Both levers are available in paper as well as live. Paper is where the muscle memory is built, and a kill-switch first used in anger is a kill-switch nobody knows how to use.

## Performance monitoring: alerts that never act

Scoping limits to malfunction detection would otherwise leave the system blind to "the strategy is behaving unlike anything tested". A monitoring layer closes that, and it **notifies without acting**. Every alert states what was not done.

Auto-pausing on drawdown was rejected as a drawdown stop in disguise. Pausing rebalancing still modifies the strategy: it holds a stale book through exactly the period a monthly rebalance exists to navigate, and that modified strategy was never tested. Not liquidating makes it gentler, not validated.

Thresholds are **derived from the backtest's own path distribution, not chosen as round numbers**, and belong to the certification that produced them; each new model brings its own. A 20% drawdown means nothing on its own; a drawdown deeper than anything in several thousand CPCV paths means the live world is outside what validation explored, which is a fact worth a human's attention.

The alerts:

- **Drawdown** deeper than the CPCV path distribution's maximum-drawdown quantile.
- **Consecutive losing months** past the longest losing run in the path distribution.
- **Tracking** — realised returns diverge from the basket's expected return by more than modelled costs can explain, which usually indicates execution is wrong rather than the strategy. While in paper, this comparison must add dividends back, because Alpaca paper does not credit them.
- **Live cost** — once at least 50 live fills exist, the rolling mean shortfall (fill against NBBO mid at submission, signed by side) exceeds the rolling mean modelled cost by more than two standard errors. Detail in [01 — Data, broker and backtesting](01-data-broker-backtesting.md).
- **Certified capital** — the live book's value exceeds the capital the model was certified at. Re-certifying at a higher capital is a deliberate operator act.
- **Partial rebalance** — a mid-submission pause could not cancel every sent order; names what remains live.
- **Broker latch mismatch** — `suspend_trade` at Alpaca disagrees with the database latch.
- **Position snapshot mismatch** — the daily broker snapshot differs from the previous snapshot plus the day's fills (a split will fire this, correctly).
- **Health** — stale heartbeat, overdue expected event, undelivered alert; see [08 — Database schema](08-database-schema.md).

If the operator judges the evidence warrants stopping, they pull the pause: a deliberate, attributed, logged act rather than a threshold firing overnight. The alerts also build the paper trail that the eventual question of whether to keep running the strategy will need.

## Paper to live

Promotion criteria are **operational, explicitly not statistical**.

A monthly rebalance produces twelve observations a year. No feasible paper period constitutes a statistical test of edge; twelve or even twenty-four monthly returns cannot separate a real edge from luck, and the Deflated Sharpe discipline exists precisely because that inference is so easy to fake. The paper period proves the machinery does what it claims, in the real world, on real market data, with real timing. It is not evidence the strategy works.

Criteria:

- At least twelve completed paper rebalances, each either executed cleanly or halted for a reason that was diagnosed and fixed (the diagnosis is a human attestation on the promotion event).
- Realised fills matching the official closing price within tolerance. This is the assumption the entire backtest rests on, and one of the few things paper trading genuinely does validate. As built, every fill within 5 basis points of the session's close, measured over at least twenty fills so a pass means something.
- The fee arithmetic, computed from the schedule and paper fills, is correct, and partial fills are handled correctly. (Alpaca paper simulates neither impact nor slippage nor regulatory fees, so cost itself cannot be validated in paper; it is validated live by the cost alert above.)
- Zero undiagnosed gate breaches. Diagnosed breaches are fine, and are in fact evidence the gate works.
- The panel's pre-flight checks passed, including the dispersion gate.
- Pause, liquidate and the approval flow each deliberately exercised at least once in paper, so the first live use is not the first use. This is a query over the `command` table.

Each criterion is measured from the recorded history by a view, and one nobody has measured reads as unmet rather than passing by default: the three arithmetic criteria stay unmet until `agb paper-check` has recorded a pass, and the pre-flight criterion until the panel pre-flight suite has. A check with too little to measure refuses rather than recording a pass.

Promotion is then a human act through `agb mode`, like every other rung of the ladder, and the CLI refuses unless the eligibility view is satisfied.

The `w=0` versus `w>0` shadow comparison sits alongside this and does not gate it. That comparison needs years to say anything, and waiting on it would mean never going live. **Live starts at `w=0`** with the validated ranker.

## Control surface and channels

**Approval state lives in the system's own database**: the pending basket, its flags, its deadline, its outcome, and who decided it. Telegram and the local UI are views onto that state rather than owners of it. Whichever surface answers first resolves the decision, enforced by the compare-and-swap on `basket_event`; the other reflects the resolution and replies "already approved by X at T".

This matters beyond redundancy. The approval record survives Telegram history being cleared. The audit trail exists in one queryable place for reconciling live performance against the backtest. And the two surfaces render a shared object rather than each implementing approval separately, so the second surface costs a read and a write against state that already exists.

Telegram remains primary. Without a second surface, a Telegram outage at 15:45 ET means a missed month under the timeout rule: safe, but avoidable.

Rejected: email or push fallback. Neither carries state, and approving a live trade by clicking a link in an email is a weak authorisation path for a write action that moves money. Telegram at least has an authenticated bound chat.

## Sources

[Alpaca: account configuration (`suspend_trade`, `no_shorting`)](https://docs.alpaca.markets/reference/patchaccountconfig-1) · [Alpaca: close all positions](https://docs.alpaca.markets/reference/deleteallopenpositions) · [NYSE RM-22-08, Rule 7.35B](https://www.nyse.com/publicdocs/nyse/markets/nyse/rule-interpretations/2022/Update_to_Rule_7.35B_-_Final.pdf) · [Nasdaq on-close cutoff rule change (Federal Register)](https://www.federalregister.gov/documents/2018/10/25/2018-23277/self-regulatory-organizations-the-nasdaq-stock-market-llc-order-approving-proposed-rule-change-as)
