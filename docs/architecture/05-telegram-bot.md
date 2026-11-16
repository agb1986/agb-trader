# 05 — Telegram Bot

**Source:** [#8 Design Telegram bot command set and UX](https://github.com/agb1986/agb-trader/issues/8), amended by [#12](https://github.com/agb1986/agb-trader/issues/12) (pre-flight message, refused commands logged, `/resume` per-cause clears)
**Prototype:** branch [`prototype/telegram-bot-ux`](https://github.com/agb1986/agb-trader/tree/prototype/telegram-bot-ux), file `prototypes/telegram-bot-ux/mock_bot.py`, commit e03daa6. Run with `python3 prototypes/telegram-bot-ux/mock_bot.py`. Throwaway: it talks to no broker and no database, every number in it is invented, and its `sim` command does not exist in the real bot.
**Parent:** [#1 AGB Trader — Architecture Spec Map](https://github.com/agb1986/agb-trader/issues/1)
**Status:** Settled design.
**Date:** 2026-09-10

The bot is the primary control surface and the fast mobile lever for emergencies. It follows torrent-agent's pattern: plain text, two short lines per item, no inline keyboards, no markdown tables, because Telegram renders in a proportional font on a narrow phone screen.

## Telegram is a view, not the owner

Nothing in the bot holds state. The pending basket, the approval decision and its actor, the halt latch, the autonomy level and the eligibility counts all live in the database. The bot reads them and writes `command` rows against them. Two consequences: the local UI inherits this exact vocabulary rather than inventing a second one, and if the bot is down at 15:30 the basket is still pending and still approvable from the other surface.

The bot is the only service holding the Telegram token, and the only one whose traffic leaves through the VPN (see [07 — Services and deployment](07-services-deployment.md)). It is bound to one authenticated chat.

## The command set

Fourteen commands.

```
/status        — what the system is doing right now
/positions     — the 20 holdings
/pnl [period]  — return, versus what the backtest expected
/pending       — re-show the rebalance awaiting your decision
/approve <id>  — approve that rebalance
/reject <id>   — reject it; no trades this month
/basket        — the full 20-name target the approval would buy into
/why <TICKER>  — why this name: stage-1 rank and the panel's rubric
/gate          — the pre-trade checks from the last run, one by one
/level         — autonomy level and progress toward the next
/pause         — stop all order flow now
/resume        — clear the halt and the pause
/liquidate     — close every position at market (two steps)
/log [n]       — recent audit lines
```

The set divides in three. `/status`, `/positions`, `/pnl` and `/log` are read-only monitoring. `/pending`, `/approve`, `/reject`, `/basket`, `/why` and `/gate` are the approval loop and its drill-downs. `/pause`, `/resume` and `/liquidate` are the kill-switch. `/level` sits across the first two, because the autonomy ladder is both a fact about current state and the thing the approval loop is working toward.

**There is no command that changes a position outside the approval loop and `/liquidate`.** That is the standing constraint expressed as an interface: the bot offers no way to trim a name, add one, or override a weight. If the human wants a different book, the mechanism is to reject the rebalance or to liquidate, both recorded as deliberate divergences. There is no `/promote` and no way to move the ladder up: those are server CLI acts ([04 — Risk limits and approval workflow](04-risk-and-approval.md)), and `/level` is read-only.

**Every inbound command is a `command` row, refused ones included**, with the raw text as typed, the parsed verb and arguments, and the outcome. A bare `/approve`, a basket reference that does not match the pending basket, and a wrong liquidation token are all part of the record, because a refusal is evidence of what the operator attempted.

**Each message is handled once, and only while it is fresh.** Every Telegram update the bot handles is a `telegram_update` row keyed on Telegram's `update_id`, written in the same transaction as the update's `command` row and whatever the command did, so either all of it is on record or none of it is; the reply goes out after the commit. A restart asks Telegram from one past the newest `update_id` on record, and an update that comes back anyway finds its row and is skipped, so a crash between handling a batch and confirming it to Telegram cannot run `/pause`, `/resume` or a bare `/liquidate` a second time. A message sent more than five minutes before the bot reads it (`STALE_COMMAND`), or one with no send time, is refused whatever it says: it is recorded as a refused `command` with its age as the reason, logged, and answered with `STALE — /<verb> refused` and an invitation to send it again. Telegram holds an unread message for a day, and a `/resume` typed before an outage must not clear a halt raised after it.

## The bot's own messages

Seven messages the bot sends unprompted:

1. **Approval request** — the message below, when a basket becomes pending.
2. **Reminder at T-60** and
3. **Reminder at T-20** — two lines each, re-stating the consequence (a decision is outstanding, this many minutes remain, no answer means no trades) and pointing at `/pending`. The reminders deliberately do not repeat the basket; repeating a long message twice more trains the reader to skim all three, which is the beginning of the rubber-stamping failure this design exists to avoid.
4. **Fill confirmation** after the close.
5. **Expiry notice** when the cutoff passes with no answer, logged with actor `system`.
6. **Gate-halt alert** when a pre-trade check fails, naming the check and what it measured.
7. **Performance and health alerts** — the notify-never-act family from [04](04-risk-and-approval.md): drawdown, losing run, tracking, live cost, certified capital, partial rebalance, broker latch mismatch, position mismatch, stale heartbeat, overdue expected event, and a bot duty that raised (`cycle_failure`, one per run of failures; the rest of the cycle, delivery included, still runs).
8. **Pre-flight health message** on the session before each rebalance: "rebalance tomorrow, all services healthy", or a list of what is not healthy (overdue expected events, stale heartbeats, open halt causes, a broker `suspend_trade` mismatch). It exists because nothing else detects a dead bot on Telegram. Without it, the first sign is the approval request failing to arrive, which at L0 means a skipped month; with it, the operator finds out with a session left to fix it.

(Counting gate-halt and the alert family as one class each, this is the "seven unprompted messages" of the source tickets.) Like every other bot message, each reads database state and holds none. Every delivery is an `alert_event` carrying the Telegram message id, so an alert the bot never delivered is visible in the UI.

## The approval message

Ordered diff first, then the aggregate numbers, then flags only, then gate status, then the prompt:

```
REBALANCE 2026-10 — approval needed
LIVE · L0 · deadline 15:45 ET (in 58m)

OUT (3)
  x CULP   Culp Inc            rank 47 (was 12)
  x NVEE   NV5 Global          rank 38 (was 9)
  x SMLR   Semler Scientific   rank 33 (was 21)

IN (3)
  + AMRC   Ameresco            rank 4   safety 0.71
  + HCKT   Hackett Group       rank 7   safety 0.83
  + PLPC   Preformed Line      rank 11  safety 0.58

HOLD 17 unchanged

turnover 15.2%  ·  cash after 0.3%  ·  top sector Industrials 30%

FLAGS (1)
  ! PLPC — accounting integrity 1/4, the panel's weakest score
    this month. Ranked 11 by stage 1.  /why PLPC

GATE  12 of 12 passed  /gate

/approve 2026-10   /reject 2026-10   /basket   /why <TICKER>
No answer by 15:45 ET means no trades this month.
```

Three properties are load-bearing.

**The diff leads** because the seventeen unchanged positions are not a decision. Listing twenty names every month buries the three that moved in seventeen that did not, and after two months the reader stops looking. Each sold name carries its old rank next to its new one, because a name falling from 12 to 47 is a different event from one falling from 12 to 21, and the drop is the only thing that explains why it is being sold.

**The flags section lists only what the panel or the gate found unusual.** It is not a summary of the basket's merits. The message is an anomaly-detection instrument: its job is to surface the things a human eye might catch that the gate did not know to check. A month with nothing anomalous shows `FLAGS (none)` and is shorter, which is correct; a quiet month should read as quiet.

**The consequence of inaction is stated every time.** Not "please respond" but the specific outcome: no answer means no trades this month.

At L1 the same message is sent with the countdown and the statement that it proceeds unless rejected; at L2 it is a notification with no prompt.

## `/approve` requires the basket id

Bare `/approve` refuses and re-prints the correct form. The cost is four characters typed; the thing it prevents is an approval composed against one basket landing on another — a reply typed late, or after a halt, or after an expiry and a rerun. Approval is per-basket and all-or-nothing, so the identity of the thing being approved has to be part of the act of approving it. `/reject` takes the same id for symmetry, plus an optional free-text reason that is recorded.

If the other surface answered first, the reply is "already approved by X at T" (the compare-and-swap on `basket_event` decides).

## `/why` and the panel's output contract

The drill-down prints the stage-1 rank, the normalised safety score with its raw total out of 24 and where it sits among this month's fifty shortlisted names, then all six rubric dimensions with their 0–4 scores, then the sources the panel read.

For any dimension scoring 2 or below it prints the Judge's one-sentence reason for that score. Six bare integers explain nothing to someone reading a phone at 15:40; "accounting integrity 1/4" prompts the question it should be answering. The reason strings are part of the panel's output contract in [03 — LLM panel](03-llm-panel.md) precisely so this drill-down can exist; a score without its reason cannot be explained after the fact.

The message ends by stating that the panel scored a name-scrubbed extract and did not know which company it was looking at. That is a fact the reader needs in order to weigh the score correctly, and it is not deducible from the score itself.

## `/gate`

Lists the pre-trade checks from the last run one by one, each with what it actually measured (snapshot date, universe size, model hash, dispersion, turnover, and so on), not pass/fail alone. After a halt, the failing check leads.

## `/pause` and `/resume`

`/pause` sets the `pause` halt cause and Alpaca's `suspend_trade`, and confirms both. If a submission is in flight, it also cancels that basket's already-sent orders and reports what was cancelled and, if anything did not cancel or filled first, what remains live or filled.

`/resume` clears every open halt cause and unsets `suspend_trade`. It records one clear per cause, and its reply lists what it cleared (for example `cleared: pause, gate_breach`), so an unexplained gate breach cleared alongside a pause remains visible. Resume clears the latch and nothing more: a rebalance that halted at 15:31 and is resumed at 15:47 is not automatically retried, because it would submit MOC orders past the cutoff into an unknown state. If the human still wants it and there is time, `/pending` is still there.

## `/liquidate` reports the broker's actual result

Two steps. The first prints what will happen: position count, approximate equity, that open orders are cancelled first, that it cannot be undone, that the system will not re-enter these names until the next scheduled rebalance — and issues a short random token. The second requires that token typed back, within five minutes. A wrong token refuses and does not retry. Token, confirmation and result are `liquidation` rows.

The result message reports what the broker returned, per symbol, including failures. Alpaca's bulk close returns HTTP 207 Multi-Status with a per-symbol status, and partial failure is a normal outcome: a halted symbol cannot be sold. A "liquidated" confirmation that glossed over a remaining position would be a lie told at the worst possible moment, to someone who has just decided to be out of the market. The system pauses itself afterwards (`liquidation` cause open), so the remaining position is not acted on by anything.

## Alerts say what was not done

The performance alert opens by stating that nothing has been done and nothing will be, gives the live figure against the backtest's own CPCV path distribution, says plainly that live is now outside the range validation explored, and then offers `/pause` and `/liquidate` as the human's levers. It never recommends.

As built, a performance alert carries the one header `PERFORMANCE ALERT — nothing has been done and nothing will be.`, then the reading as a two-line item, then the footer `/pause stops the next rebalance and /liquidate closes every position. Both are yours to choose; nothing in this message chooses for you, and no part of the system will act on it.` The reading's own second line ends with `live is outside the range validation explored`. A health alert keeps its own `HEALTH ALERT` header and carries no footer, because a stale heartbeat is a thing to fix rather than a thing to decide about. A test asserts the footer verbatim and that the words "should", "consider", "recommend", "advise" and "suggest" appear nowhere in the message.

This wording is doing real work. An alert that reads as a recommendation converts a monitoring system into an advisory one, and an operator who acts on it at the bottom of a drawdown has overlaid an unbacktested exit rule on a certified strategy by hand, the exact outcome the standing constraint forbids the software from doing automatically. Saying outright that no action was taken and none will be taken removes the ambiguity about whether the system is asking for something.

## The audit log carries an actor, and `system` is one

`/log [n]` reads the audit view, one line per event: timestamp, actor, event, and the fields that identify what it happened to. Attributing every line to the human is wrong: an expiry and a gate halt are things that happened *to* the operator, not by them, and a reconciliation six months later has to be able to tell a rebalance the human declined from one the clock declined for them. The actor field is explicit and `system` is a legitimate value.

## Rejected

- **Inline keyboards with approve/reject buttons.** One tap is what rubber-stamping looks like. Typing `/approve 2026-10` is a deliberate act, and the friction is the point on the one decision per month that moves real money.
- **A per-order approval flow.** Ruled out upstream, and building the message confirmed why: a per-order interface has no place to put turnover, cash and sector concentration, because those are properties of the basket.
- **Showing all twenty names in the approval request.** Tried and discarded. The three that changed disappear into the seventeen that did not.
- **A model-authored summary paragraph in the approval message.** Every field is computed from the basket, the gate result and the panel's structured scores. Model prose drifts, cannot be diffed month to month, and would put a persuasive voice in front of the human at the exact moment they are supposed to be sceptical.
- **Automatic retry of a halted rebalance after `/resume`.** Covered above.
