# 08 — Database Schema

**Source:** [#12 Design the operational database schema](https://github.com/agb1986/agb-trader/issues/12)
**Parent:** [#1 AGB Trader — Architecture Spec Map](https://github.com/agb1986/agb-trader/issues/1)
**Status:** Settled design.
**Date:** 2026-09-10

The operational database is **history, not state**. Almost every table is append-only, current state is a view over the latest event, and the two places where a row is updated in place are named exceptions. This follows from two properties set upstream: Postgres is the only channel between services ([07 — Services and deployment](07-services-deployment.md)), and the record must be trustworthy enough to reconcile live performance against the backtest months later ([04 — Risk limits and approval workflow](04-risk-and-approval.md)).

This document fixes which tables exist, what each holds, which are mutable, who may write them, and which invariants the database enforces itself rather than leaving to application code. Concrete DDL, the migration tool, indexes, poll intervals and lease durations are build work.

## Job claims

Scheduler work is a `job` row, unique on `(kind, period)`, so a second monthly rebalance for the same period fails on the constraint instead of producing a duplicate. Pollers claim with `SELECT … FOR UPDATE SKIP LOCKED` and a conditional update that sets `claimed_by` and `lease_until`, renewing the lease while they work. Two pollers cannot hold the same row, and a waiting poller skips rather than blocks.

What happens when a lease expires depends on the stage, because running twice has different consequences:

- **Pipeline stages** (pull, features, rank, panel, gate) write only to the database and are deterministic. An expired lease is simply reclaimed and the stage reruns. A panel rerun costs API calls, which is acceptable.
- **Execution submission** has side effects at the broker that the database cannot see, so it is **never reclaimed automatically**. An expired submission lease sets the halt cause `execution_lease_expired`. Every order carries a deterministic `client_order_id` built from the basket reference and symbol, well inside Alpaca's 128-character limit. The human-triggered reconcile therefore looks up what the broker actually holds (`GET /v2/orders:by_client_order_id`) before anything is resent. The broker adds a second guard: Alpaca rejects a duplicate `client_order_id` on an active order with HTTP 422, and an MOC order stays active until the close, so an accidental resubmission before the close is refused broker-side as well. The reconcile appends what it learns per order and one `reconcile_run` row summarising the walk; it updates nothing, and clearing the cause stays a separate human act — that clear is also what releases the held lease (a `released` job event), so until it happens no later cycle can take the submission.

The lease columns on `job` are **mutable exception 1**. The job's lifecycle (created, claimed, completed, failed, lease expired, reclaimed, released) is append-only `job_event`.

Rejected: a session-scoped advisory lock. It releases when the connection dies, so a crash in the middle of submission is picked up and resubmitted automatically, which is the one outcome that must never happen silently. Rejected: halting on every stale claim, which lets a container restart during feature construction cost the month if nobody is around before the cutoff.

## Baskets

**Content is frozen once the gate has run.** A basket's content is insert-only: the target weights, the order plan, the gate result check by check (what each measured, not pass/fail alone), the stage-1 ranks and panel scores it was built from, the Sharadar snapshot date, and a hash of the lake partition it read. Each basket references the `model_certification` and the `panel_config` it was built with.

**State is a sequence of events.** `basket_event` rows carry `seq`, `from_state`, `to_state`, actor (a human identity or `system`), surface (`telegram`, `ui`, `cli`, `scheduler`, `pipeline`, `execution`), timestamp and reason. `unique(basket_id, seq)` makes every transition a compare-and-swap: a writer inserts `seq = last + 1`, and a concurrent writer hits the constraint. That settles the Telegram-versus-UI race inside the database: the first answer wins, and the loser replies "already approved by X at T". Current state is a view over the latest event. A trigger checks every insert against a `basket_transition` table of permitted `(from, to, role)` triples.

The state set is the one the risk and Telegram documents already use (proposed, halted at the gate, pending, approved, rejected, missed, submitted, filled). The exact enumeration is left to the build.

**References people can type.** Each basket has an internal key and a unique human reference, `2026-10` for that month's rebalance. A deliberate next-session rerun after a missed approval is a new basket, `2026-10r1`, with `deviation_of` pointing at the original, which keeps its missed outcome. Shadow baskets (`kind = 'shadow'`) have no typeable reference.

Rejected: a mutable `state` column plus an audit copy, which gives two records of the same fact with nothing forcing them to agree. Rejected: full event sourcing with projections, a great deal of machinery for about twelve baskets a year.

## Commands and approvals

Every inbound command from Telegram, the UI or the CLI is a `command` row: actor, surface, the raw text as typed, the parsed verb and arguments, and the outcome, accepted or refused with a reason. **Refused commands are logged too**: a bare `/approve`, a basket reference that does not match the pending basket, a wrong liquidation token. A refusal is evidence of what the operator attempted. An approval's `basket_event` references the command that caused it, so the record holds both the basket id as typed and the basket it resolved to.

`/liquidate` writes `liquidation` rows: the token issued, the confirmation, and the broker's per-symbol HTTP 207 result.

## The audit log and immutability

The audit log is a `UNION ALL` view over the domain event tables: `basket_event`, `halt_event`, `order_event`, `ladder_event`, `mode_event`, `promotion_event`, `certification_event`, `liquidation`, `command`, `alert_event` and `job_event`. It cannot disagree with them because it *is* them. `/log` and the UI's decision log both read it.

Append-only is enforced in three layers:

- **Per-service Postgres roles.** `scheduler`, `pipeline`, `execution`, `bot`, `ui` and `cli` each connect as their own role, plus an owner/migration role that no service uses. Service roles have `INSERT` and `SELECT` only on history tables. This carries the credential-line boundaries of the service decomposition into the database.
- **Triggers.** A `BEFORE UPDATE OR DELETE` row trigger on every history table raises for every role, the owner included, so an accidental fix-up in `psql` fails too. A statement-level `BEFORE TRUNCATE` trigger is added as well, because row triggers do not fire on `TRUNCATE`.
- **Role-restricted transitions.** The `basket_transition` table names which role may perform each transition. Only `execution` moves a basket from approved to submitted. Only `bot` or `ui` approve or reject, and only `scheduler` marks an approval missed. A bug in the bot cannot mark a basket submitted.

**Schema evolution never rewrites history.** New columns on history tables must be nullable or carry a default, and they are never backfilled, because a backfill is an `UPDATE` and the trigger refuses it. History is corrected by appending a correcting event, never by editing the original.

Rejected: a single global `audit_log` table written in the same transaction as each domain change; it duplicates the domain rows and relies on every write path remembering to write both. Rejected: a hash chain. It defends against an adversary with database access, which this single-operator box does not have. The realistic threat is bugs, and the grants and triggers address that.

## Halt latch

`halt_event` rows each set or clear one **named cause**: `gate_breach`, `execution_lease_expired`, `pause` or `liquidation`. Trading is blocked while any cause is open, and the open set is a view.

`/resume` clears every open cause but writes **one clear row per cause**, attributed, and its reply lists what it cleared. A pause cleared together with an unexplained gate breach therefore stays visible afterwards.

Execution **re-reads the latch before each individual order**, not only when it claims the job, so a `/pause` at 15:44 stops every order not yet sent. **A pause mid-submission also cancels what was already sent** while it is still before 15:50 ET (the cancellation-window rules are in [04](04-risk-and-approval.md)); if a cancel does not land, a partial-rebalance alert names what remains live.

**The broker latch is observed, not assumed.** Each cycle, execution records the `suspend_trade` value it reads from Alpaca in `broker_state`. A mismatch with the database latch raises an alert, in either direction.

Rejected: a single boolean latch with a free-text reason, where a second cause arriving while halted overwrites or hides the first. Rejected: clearing causes one at a time, which adds friction at the worst moment and changes the locked command set.

## Orders, fills and positions

**Order intent is written before the broker is called.** The `order` row is inserted before the HTTP call. It holds the deterministic `client_order_id`, symbol, side, quantity and time-in-force `cls`. It also holds the modelled cost in basis points with its cost-model version, frozen at basket construction because σ and ADV depend on the snapshot. The NBBO reference mid is captured on it at submission. A crash between the call and the acknowledgement is then recovered by lookup rather than guesswork.

`order_event` is append-only: the broker acknowledgement with Alpaca's order id, status changes, and cancel requests and their results. Each broker fill is its own `fill` row, so a partial fill is several rows, with price, quantity, timestamp and the regulatory fees computed from the schedule.

**Shortfall is a view**, never a stored column: the fill price against the reference mid, signed by side, set beside the modelled cost. It cannot disagree with its inputs. The live cost alert reads this view.

**Positions are daily broker snapshots**, not a sum of fills. Splits, mergers and spin-offs change holdings without any fill, and a fill-derived book drifts silently from the first corporate action. A daily check compares each snapshot against the previous snapshot plus the day's fills and alerts on any mismatch. A split will trigger it, which is correct: it is a real change the operator should see. Like every alert, it notifies and never acts.

`forced_event` is the record of the four forced corporate actions the ranker defines, one append-only row per period, symbol and kind, written by a human with the `command` row that carried it. It holds the holding before the event and the holding it leaves, the deal price or last listed print where there is one, and the acquirer and the shares a merger paid. The mismatch check folds those share deltas into what it expects, so a filed event keeps the alert quiet while an unrecorded corporate action still raises it; the same rows are what let the pre-trade gate accept a basket of fewer than 20 names.

Rejected: mutable order status updated in place from the broker stream, which loses the status history.

## Certified-model registry

`model_certification` is an immutable row holding:

- the artifact's sha256 and path, the feature list, and the training window;
- the declared trial count, which is the distinct configurations its ledger says were searched, not that ledger's row count (see [01 — Data, broker and backtesting](01-data-broker-backtesting.md));
- DSR, PBO and cost-inclusive Sharpe, DSR at 2× modelled cost, the break-even cost multiple, and max and mean participation;
- the cost-model version and the certified capital;
- the CPCV path-distribution quantiles from which the performance alerts take their thresholds: maximum drawdown, longest losing run, and tracking dispersion. Thresholds belong to the certification that produced them, so each new model brings its own.

**The trial count is checked, not trusted.** The workstation's trial ledger ships inside the artifact as a hashed `trials.parquet`, and the live pipeline recounts it, under the rule the manifest says the certification counted by, and refuses a model whose declared trial count differs from that recount. The DSR gate then rests on a count of recorded trials rather than a number someone typed. The row the count is held against keeps only the number; the rule it was counted under lives in the manifest, next to a count of the ledger's rows, and the ledger itself is pinned by the artifact digest the row holds.

`certification_event` records voiding, for example when cost constants change, and live refuses a voided model. It also records a paper-only measurement waiver, at most one per certification: a model that failed its gates, promoted so the stack can be exercised end to end before any model has passed. A waived model trades in paper and nowhere else, and a move to live is refused while it is the live one (`docs/build/deploy.md`). `promotion_event` records which certification is live. **Promotion is a human act through the server-side CLI** (`agb promote <model_id>`), not Telegram or the UI. It happens roughly once a year at the annual refit and is the most consequential state change in the system, so it stays off the phone. `/status` shows which model is live.

Rejected: a declared trial count with no check behind it. Rejected: a `/promote` command, which would put the most consequential act on the surface built for monthly approvals.

## Autonomy ladder, trading mode and panel configuration

`ladder_event` (L0, L1, L2) and `mode_event` (paper, live) are append-only. Upward moves, paper to live included, are human acts through the same CLI (`agb level`, `agb mode`), and the CLI refuses unless an eligibility view says the criteria are met. Criteria that are human judgements, such as a halt having been diagnosed and fixed, are recorded as an attestation on the event rather than computed. Automatic demotion after a rejection or gate breach at L1 or L2 writes a `ladder_event` with actor `system`. `/level` stays read-only.

**Eligibility is a set of views, never a stored counter**: consecutive clean rebalances since the last rejection, breach or missed approval, and rejections issued during the opt-out window. A stored counter can drift from the history it summarises; a view cannot. The paper-to-live requirement that pause, liquidate and approval each be exercised in paper becomes a query over `command`. The three arithmetic criteria are measured from the paper record rather than attested, and each result is appended to `paper_check`, which the paper-to-live view reads for the newest result of each name; a name with no result reads as unmet.

`panel_config` is immutable and versioned. Each row holds the model ID per role, prompt-template hashes, rubric version, scrubber version, shortlist size N and `w`. Changing `w` creates a new row through the CLI, and every basket references the panel configuration it used.

**The shadow portfolio is its own basket series** (`kind = 'shadow'`), holding the `w > 0` counterfactual from [03 — LLM panel](03-llm-panel.md). It carries its own holdings, because under the 20-buy/30-hold buffer the shadow's picks depend on its own previous book rather than the traded one. It is marked at official closes, has no orders, and has no path through the approval transitions.

Rejected: stored counters. Rejected: the shadow as extra columns on the traded basket, which is wrong under the rank buffer.

## Health: heartbeats, expected events and alerts

`heartbeat` is one row per service, upserted every cycle. It records liveness, not history, and is **mutable exception 2**; an append-only row every cycle would dwarf every other table.

**Expected events are what tell a dead system from a quiet month.** The scheduler writes `expected_event` rows ahead of time: the data pull due at T, the rebalance proposal due at T, the approval request due at T, submission due by T. Each is completed by a reference to the row that fulfilled it. An overdue view flags any expected event past its time with no completion. The same view also flags when no rebalance is expected within the next cycle's horizon, which is how a dead scheduler shows up: it simply stops writing future rows. A month without a rebalance is quiet only if nothing was expected. This view drives `/status`, the pre-flight message and the UI banner.

`alert` rows are append-only with a dedupe key `(kind, subject, period)`, so a stale heartbeat produces one alert rather than one per cycle. `alert_event` records delivery (with the Telegram message id) and acknowledgement, so an alert the bot never delivered is visible in the UI.

The bot evaluates the health of every other service and alerts. The UI evaluates everything, including the bot, and shows a banner. The bot's pre-flight message on the session before each rebalance is how a dead bot is noticed with a session to spare.

As built, the performance alerts of [04 — Risk and approval](04-risk-and-approval.md) read four views rather than tables of their own, because each figure is a reading of recorded history and a stored copy could disagree with it. `fill_shortfall` gives each fill's shortfall in basis points, signed by side against the NBBO mid recorded at submission, beside the modelled cost frozen on the order and the trading mode in force when the order was created. `live_cost` aggregates it over live fills alone into a count, the two means, and the standard error of their difference. `equity_month` cuts the `broker_state` equity readings into New York months, opening each at the last reading before it, and reports the month's return both as realised and with recorded dividends added back in paper. `alert_threshold` lifts the promoted certification's `path_quantiles` into one row of thresholds, reading NULL where that certification recorded no such quantile, which is how a check with no validated threshold stays silent.

As built, `dividend` is the one new table: an append-only row per dividend the operator recorded from a broker statement, keyed `(period, symbol, paid_on)` and carrying the command that recorded it. It exists because the paper broker credits no dividends while the backtest assumes them, so a paper month cannot be compared with the backtest without them. Only the operator's surfaces insert; nothing computes it. The certification's `path_quantiles` gained a `monthly_return` distribution in the same change, which is the expectation the tracking alert and `/pnl` read a live month against.

Rejected: staleness from heartbeats alone. It catches a dead process but not a live scheduler that silently never scheduled the rebalance, which is precisely the quiet-month case.

## Panel logs

The structured output lives in Postgres, because `/why` and the UI's panel view query it: per name and per dimension, the score, the reason string, the Analyst's quotes with citations, the model ID per role, and token counts and cost.

The full prompt and response bodies and the source filing texts are **content-addressed zstd files on the `/mnt/data` volume**, referenced by sha256 from the database, the same pattern as the model artifact. They are tens of megabytes a month uncompressed (a rough estimate), which would bloat the nightly `pg_dump` for data nobody queries. The nightly job hash-checks a sample of referenced files, so a missing body is noticed before a replay needs it.

Rejected: everything in Postgres, which grows the dump by hundreds of megabytes a year. Rejected: bodies in the Parquet lake, which mixes vendor data the server re-pulls with records that must never be rewritten.

## Table inventory

| Table | Written by | Mutable? |
|---|---|---|
| `job`, `job_event` | scheduler (create), pipeline / execution (claim, events) | lease columns on `job` only |
| basket content tables, `basket_event`, `basket_transition` | pipeline (content), bot / ui / scheduler / execution (events per transition table) | no |
| `command` | bot, ui, cli | no |
| `liquidation` | bot, ui (token, confirm), execution (result) | no |
| `halt_event`, `broker_state` | pipeline (gate_breach), execution (lease, broker_state), bot / ui (pause, resume, liquidation) | no |
| `order`, `order_event`, `fill`, position snapshots | execution | no |
| `reconcile_run` | execution (the operator's `agb reconcile`) | no |
| `model_certification`, `certification_event`, `promotion_event` | research (certification, via artifact import), cli (promotion, voiding) | no |
| `ladder_event`, `mode_event` | cli (up), scheduler or execution as `system` (down) | no |
| `panel_config` | cli | no |
| panel structured output | pipeline | no |
| panel pre-flight results, spot-check verdicts | cli (all four checks, verdicts), pipeline (recall runs) | no |
| `heartbeat` | every service | upsert |
| `expected_event` | scheduler (create), fulfilling service (completion reference) | completion by append or reference, never rewrite |
| `alert`, `alert_event` | bot, ui, execution, scheduler | no |
| `dividend` | cli, ui (the operator, from a broker statement) | no |

Views: current basket state, open halt causes, audit log, shortfall, eligibility (ladder and mode), overdue expected events, reconcile state (each planned order beside what the broker last said about it), per-fill and live cost shortfall, equity by New York month, and the promoted certification's alert thresholds.

## Conventions

- Money and share quantities are `numeric`, never floating point; fractional shares are quantities too.
- Timestamps are `timestamptz` stored in UTC. Trading dates are `date` on the NYSE calendar in America/New_York.
- Every history row carries an actor wherever an actor exists, with `system` a legitimate value.
- Nothing is deleted. Growth is a handful of rows a month plus daily snapshots and heartbeats updated in place.

## What this does not settle

- Concrete DDL, the migration tool, indexes, poll intervals and lease durations, tuned against measured runs.
- The CLI's full surface beyond the verbs named here (promote, level, mode, panel configuration).
- Backup of the content-addressed panel store on `/mnt/data`, which follows whatever protects the lake.
- The workstation-side tooling that produces the trial ledger. The contract is fixed; the tool is the open experiment-tracking item.

## Sources

[Alpaca: Placing Orders](https://docs.alpaca.markets/us/docs/orders-at-alpaca) · [Alpaca: Get Order by Client Order ID](https://docs.alpaca.markets/reference/getorderbyclientorderid) · [Alpaca: account configuration (`suspend_trade`)](https://docs.alpaca.markets/reference/patchaccountconfig-1) · [Alpaca: common API errors (duplicate `client_order_id`)](https://alpaca.markets/learn/how-to-fix-common-trading-api-errors-at-alpaca) · [NYSE RM-22-08, Rule 7.35B](https://www.nyse.com/publicdocs/nyse/markets/nyse/rule-interpretations/2022/Update_to_Rule_7.35B_-_Final.pdf) · [Nasdaq on-close cutoff rule change (Federal Register)](https://www.federalregister.gov/documents/2018/10/25/2018-23277/self-regulatory-organizations-the-nasdaq-stock-market-llc-order-approving-proposed-rule-change-as)
