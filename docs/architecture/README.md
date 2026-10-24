# agb-trader — Architecture Specification

**Map:** [#1 AGB Trader — Architecture Spec Map](https://github.com/agb1986/agb-trader/issues/1)
**Status:** Settled design, ready for build sessions. Consolidated from the map's resolved decision tickets, with every later amendment folded into the section it changes.
**Date:** 2026-09-10

## What this is

agb-trader is a semi-autonomous, AI-integrated US-equities trading system for a single operator who is a UK tax resident. It replaces the archived [finance-agent](https://github.com/agb1986/finance-agent) as a clean-slate monorepo: no code or infrastructure is ported, only patterns are referenced (the Telegram approach from [torrent-agent](https://github.com/agb1986/torrent-agent), the agent-role structure from [TradingAgents](https://github.com/TauricResearch/TradingAgents)).

This directory is the architecture specification. It is not the code; it is the settled design that build work executes against. Each document below covers one area and is self-contained enough to hand to a build session on its own. The GitHub tickets that produced these decisions remain the decision history, with the reasoning and rejected alternatives in full; where this spec and a ticket disagree, this spec is the later document and is correct, because it folds in amendments that were posted as later comments on earlier tickets.

## The system in one paragraph

Once a month, after the final trading session of the month, a pipeline pulls point-in-time prices and fundamentals from Sharadar, builds twenty published factor features across a universe of roughly 3,000–4,000 US common stocks, and ranks them with a LightGBM LambdaRank model that was trained on the workstation, validated through purged combinatorial cross-validation, and certified against hard Deflated Sharpe, Probability of Backtest Overfitting and cost-inclusive Sharpe gates. The top fifty names go to an LLM panel that reads their SEC filings and scores each for qualitative impairment. A fixed-form blend produces the final ranking, and a rank buffer turns it into a basket: twenty equal-weight long-only positions. A pre-trade gate checks provenance, basket invariants and every order; a failed check halts the whole rebalance and escalates. In paper mode the basket executes unattended. In live mode a human approves the whole basket, by typed basket id, from Telegram or a local UI, and no answer by the cutoff means no trades that month. Orders are market-on-close via Alpaca, submitted before 15:45 ET. Everything the system does is a row in a Postgres database that is the only channel between services, is append-only, and names an actor for every event, with `system` a legitimate actor.

## Standing constraints

These apply across every document and no build session may relax them.

1. **Nothing in the system modifies positions automatically.** Automated controls may halt order flow and escalate to a human. Liquidating or resizing a book on a threshold is an unbacktested strategy overlaid on a certified one, and it engages at the worst moment. Only a human, by deliberate and logged act, changes positions outside a scheduled rebalance.
2. **The validation gates certify the classical ranker only.** The LLM panel's contribution is unbacktested by construction, because historical panel scores are contaminated by the model's hindsight. It is validated forward-only in paper trading. A passing backtest is never evidence that the LLM layer works.
3. **Validation discipline is a hard gate, not an advisory checklist.** DSR, PBO and cost-inclusive Sharpe control promotion between backtest, paper and live. A strategy that fails them cannot be pushed forward without deliberate, logged override, and the live pipeline refuses to load a model that has no certified row.
4. **The operator is a UK tax resident.** US shares are SDRT-exempt; capital gains treatment is the operator's, not the strategy's, and stays out of certification.

## Documents

| Document | Covers |
|---|---|
| [01 — Data, broker and backtesting](01-data-broker-backtesting.md) | Alpaca, Sharadar, NautilusTrader, the Parquet lake, the transaction cost model, the validation gates and certification |
| [02 — Classical ranker](02-classical-ranker.md) | Universe, features, LambdaRank target, training, the stage-2 blend, portfolio construction and rebalance mechanics |
| [03 — LLM panel](03-llm-panel.md) | The impairment question, roles and model tiers, SEC-filing sources, the six-dimension rubric with reason strings, blinding, pre-flight validation, the w=0 versus w>0 comparison |
| [04 — Risk limits and approval workflow](04-risk-and-approval.md) | The pre-trade gate, halt semantics and named causes, per-basket approval, timeout, the autonomy ladder, the kill-switch, performance alerts, paper-to-live criteria |
| [05 — Telegram bot](05-telegram-bot.md) | Fourteen commands, seven unprompted messages, the approval message, `/why`, `/liquidate`, the audit vocabulary |
| [06 — Local UI](06-local-ui.md) | The decision-log shell and its eight named views, the approval affordance, exposure |
| [07 — Services and deployment](07-services-deployment.md) | The seven server services and the research profile, database-only coupling, networks and VPN egress, secrets, resource limits, images and delivery, observability, lake and model hand-off |
| [08 — Database schema](08-database-schema.md) | Job claims, baskets and events, commands, the audit view and immutability, the halt latch, orders and fills, the certification registry, ladder and mode, health, panel logs, conventions |

Read them in order the first time. Documents 02 through 08 each depend on the ones before them.

## Decision history

The tickets, in the order they were resolved. Each holds the full reasoning and the alternatives rejected.

- [#2 Select broker platform](https://github.com/agb1986/agb-trader/issues/2) — Alpaca. Research report: [`docs/research/002-broker-platform-selection.md`](../research/002-broker-platform-selection.md).
- [#3 Select market data vendor](https://github.com/agb1986/agb-trader/issues/3) — Sharadar. Research report: [`docs/research/003-market-data-vendor-selection.md`](../research/003-market-data-vendor-selection.md).
- [#4 Select event-driven backtesting engine](https://github.com/agb1986/agb-trader/issues/4) — NautilusTrader. Research report: [`docs/research/backtesting-engine-selection.md`](../research/backtesting-engine-selection.md).
- [#5 Design classical ranker: feature set, target, rebalance mechanics](https://github.com/agb1986/agb-trader/issues/5)
- [#6 Design LLM panel signal output (TradingAgents-pattern feature generator)](https://github.com/agb1986/agb-trader/issues/6) — amended by #8 (Judge reason strings).
- [#7 Design risk limits and live-approval workflow](https://github.com/agb1986/agb-trader/issues/7) — amended by #11 (paper-to-live cost criterion) and #12 (pause mid-submission, named halt causes, server-side CLI for upward moves).
- [#8 Design Telegram bot command set and UX](https://github.com/agb1986/agb-trader/issues/8) — prototype on [`prototype/telegram-bot-ux`](https://github.com/agb1986/agb-trader/tree/prototype/telegram-bot-ux); amended by #12 (pre-flight message, refused commands logged, `/resume` per-cause clears).
- [#9 Scope local UI dashboards](https://github.com/agb1986/agb-trader/issues/9) — prototype on [`prototype/local-ui`](https://github.com/agb1986/agb-trader/tree/prototype/local-ui).
- [#10 Define Docker Compose service boundaries for the monorepo](https://github.com/agb1986/agb-trader/issues/10)
- [#11 Define transaction cost model for backtest realism](https://github.com/agb1986/agb-trader/issues/11)
- [#12 Design the operational database schema](https://github.com/agb1986/agb-trader/issues/12)

Decisions recorded on the map itself before any ticket was opened: US equities only; paper-first execution through the same broker interface as live; bespoke LLM layer modelled on TradingAgents' role pattern rather than a fork; the classical model is the trade-decision core and the panel feeds it a feature; paper autonomous, live human-approved and loosened over time; single monorepo under Docker Compose; LPAC means least-privilege broker keys (trade-only, no withdrawal); the validation gates are hard-coded.

## Deliberately deferred

In scope but not yet specified. None blocks the build; each becomes a ticket when its precondition arrives.

- **Long/short extension of the ranker.** Deferred by the long-only decision, not ruled out. Revisit only if the long-only strategy clears its gates and survives forward paper testing. Blockers: borrow availability in the small-cap band, and UK CGT treatment of short positions.
- **LLM panel extensions.** A news source alongside SEC filings, and the scrubbed-versus-unscrubbed ablation that measures how much of the panel's output is memory rather than reading. Experiments to run once the panel exists and has cleared pre-flight; each is logged against the DSR trial count.
- **Experiment-tracking tooling.** The contract is fixed: a hashed trial ledger (`trials.parquet`) ships inside the model artifact and live refuses a model whose declared trial count differs from it. Only the workstation tooling that produces the ledger (MLflow versus structured logging) is open. Low priority until the core pipeline exists.
- **UK CGT ledger for self-assessment.** HMRC share matching (same-day, 30-day, section 104 pool), GBP conversion at trade-date FX rates from a source Sharadar does not carry, and the self-assessment output. Ticket once the build nears live; monthly rebalancing can put a sale and a repurchase inside the 30-day window, so the ledger needs the matching rules rather than an average-cost pool.

## Out of scope

- RL and deep-learning strategy tracks (LSTM/transformer price prediction, FinRL). Revisit only as a distinct future effort if the classical track plateaus.
- Non-US markets. Revisit only if the destination is redrawn.
- Porting finance-agent's code or infrastructure.

## What the build must supply that this spec does not

Concrete DDL, the migration tool, indexes, poll intervals and lease durations; per-service memory limits from a measured pipeline run ([the build note](../build/measured-limits.md)); LightGBM hyperparameters (tuned inside cross-validation folds, each configuration a logged trial); the exact basket state enumeration; the CLI surface beyond `agb promote`, `agb level`, `agb mode` and panel configuration; backup of the content-addressed panel store; and the credentials and subscriptions the operator must hold (Sharadar, Alpaca paper account, Telegram bot token, LLM API keys, a ProtonVPN WireGuard key).
