# 06 — Local UI

**Source:** [#9 Scope local UI dashboards](https://github.com/agb1986/agb-trader/issues/9)
**Prototype:** branch [`prototype/local-ui`](https://github.com/agb1986/agb-trader/tree/prototype/local-ui), file `prototypes/local-ui/index.html`. Three variants switchable via `?variant=` and a floating bottom bar; open the file or `python3 -m http.server -d prototypes/local-ui 8765`. Throwaway; its invented numbers are the same ones the Telegram prototype uses so the two surfaces can be read side by side.
**Parent:** [#1 AGB Trader — Architecture Spec Map](https://github.com/agb1986/agb-trader/issues/1)
**Status:** Settled design.
**Date:** 2026-09-10

The local UI is a read-mostly, server-rendered view onto the system database, with exactly the two write paths Telegram has: approval and the kill-switch. It is the desk surface; Telegram remains the mobile one.

As built, the pages are read on a read-only connection and the writes are their own routes, five of them, enumerated in one place (`ui.WRITES`) and tested as an enumeration so a sixth cannot appear quietly: `/approve` and `/reject` answer a rebalance, and `/pause`, `/resume` and `/liquidate` are the kill-switch, which the Kill switch view carries. Each is a plain HTML form posting to its own route; the server answers a POST to a page with 405, so a page cannot become a write path by accident. Both write paths call the same modules the bot calls — `agb.approval` and `agb.killswitch` — rather than reimplementing them, which is what makes the claim that the two surfaces behave identically something other than a promise.

## The shell: a decision log

Three variants were prototyped, each committing to a different answer to *what is this UI for*: **A, Console** (the UI is where you operate the system; the pending basket is the page), **B, Ledger** (the UI is where you decide whether to keep trusting the system; persistent nav over separate dashboards landing on validation and promotion), and **C, Timeline** (the UI is the archive of every decision the system made and who made it).

**The answer is C as the shell with B's dashboards as named views inside it. A is rejected as the landing page.**

The home view is the decision log: one reverse-chronological row per rebalance cycle, each carrying its state (approved, rejected, expired, halted, pending), the actor who put it in that state, the realised return of the month that followed, and the turnover. Expanding a row shows the basket diff as it stood at decision time, the gate result, and the panel's flags for that cycle. This is the spine because it is the only view that answers the question that actually matters after the fact — *what did this system do, and who decided it* — and because it degrades gracefully: a month where nothing happened is one quiet line, not an empty dashboard. The log reads the same audit view `/log` reads.

A health banner sits above the log, driven by the overdue-expected-event view and stale heartbeats. The UI evaluates the health of every service, the bot included, and is the one place an undelivered alert is visible. A dead scheduler shows up as a stale "next cycle" line, an explicit warning rather than an absence, because with a monthly rebalance a rebalance that never happens looks exactly like a quiet month.

## The nine views beside the log

1. **Validation** — DSR, PBO and cost-inclusive Sharpe against their gate thresholds, with DSR at 2× cost, the break-even cost multiple, the declared trial count and the CPCV path count shown next to each, plus the path distribution with the live drawdown marked on it. Carries the standing caveat in the view itself: these gates certify the classical ranker only.
2. **Promotion** — the backtest / paper / live ladder with the date, actor and evidence for each transition (the `promotion_event`, `mode_event` and `certification_event` history), which model certification is live, and the L0 / L1 / L2 autonomy ladder with progress toward the next level from the eligibility views.
3. **Performance** — live return, drawdown and worst month, each shown *against* what the backtest expected rather than alone, with an explicit note that a handful of months of live returns cannot distinguish a working strategy from a lucky one. Paper comparisons add dividends back.
4. **Rebalance** — the pending basket, or the most recent one if none is pending: the diff first, then turnover, cash and top sector, then flags.
5. **Positions** — the twenty holdings from the latest broker snapshot, with weight, return, months held, panel safety score and current stage-1 rank.
6. **Panel** — the six-dimension rubric for a name, each dimension showing its 0–4 score alongside the Judge's reason string, with the filings it read listed underneath, and the note that the panel scored a name-scrubbed extract.
7. **Gate** — the pre-trade checks with what each one actually measured, not just pass or fail.
8. **Alerts** — the notify-never-act log, each entry stating what was not done, with delivery and acknowledgement state.
9. **Kill switch** — every open halt cause with who opened it and what a resume would clear, and whether a liquidation token is outstanding and until when, never the token itself.

## Decisions the prototype settled

**The UI can approve, but it cannot be a one-click approval.** Approval state lives in the database with Telegram and the UI as views onto it, so a UI that could only watch would be lying about where the state lives. But the central adversary of the approval design is rubber-stamping, and a button next to the basket is exactly the affordance that makes rubber-stamping effortless. The UI's approve action therefore requires typing the basket id, the same deliberate friction Telegram demands, and writes the same `command` row and `basket_event` the bot writes (surface `ui`). As built, the reference box arrives empty and nothing on any page is prefilled, since a field that arrives already correct is the button this surface is not allowed to have; a missing, wrong or late reference is refused and recorded as a refused `command` row, and when both surfaces answer at once the compare-and-set on the basket's own sequence settles it, so the loser is told who answered and when and leaves one refused row behind. This is why A is rejected as the landing page: a surface whose home screen is a basket and a green button trains the habit the whole approval design exists to prevent.

**The UI inherits Telegram's vocabulary.** A sold name carries its old rank next to its new one. Bought names carry the panel safety score. Flags list anomalies only, so a quiet month reads as quiet. Every surface that shows a pending basket also states that no answer means no trades. Every logged event names an actor, and `system` is an actor, so an expiry is visibly distinguishable from a rejection. Two surfaces onto one database should not require learning two dialects.

**Evidence views exist to detect malfunction, not to judge the edge.** Every performance number is rendered against its backtest expectation, and the alert thresholds shown are the ones derived from the CPCV path distribution. A UI that displays a live return on its own invites the operator to draw a conclusion the sample size cannot support, and the conclusion they would draw at the bottom of a drawdown is precisely the unbacktested exit rule the standing constraint forbids the software from applying.

**The log is the primary source, not a tab.** Making the decision history the shell rather than a view is the difference between a system that can be audited and one that merely can be watched. It also gives the UI an obvious answer to what it shows when nothing is pending, which is eleven months of the year.

## Exposure and authentication

Published on `127.0.0.1:8080` only and viewed from the workstation over `ssh -L`. The UI has two write paths and no login of its own, so SSH is the authentication. Publishing it on the LAN would let any device on the home network approve a live basket, reducing the typed-basket-id friction to the only thing between a guest device and a real trade. A real auth surface is worse: a single shared password is weak actor attribution for an audit log whose entire purpose is recording who decided what. The tunnel's friction lands only on deliberate desk work; Telegram remains the fast mobile lever.

The UI service holds no broker credentials and no Telegram token, has no outbound network at all, and connects to Postgres as its own `ui` role with `INSERT` and `SELECT` only. As built, a write is attributed to actor `ui:operator`, which is as much as a surface without a login can honestly claim, and the tunnel is the only credential — so a form on another site could post here with nothing of ours. Comparing `Origin` with the request's own `Host` does not stop that: a page that rebinds its own name to the loopback address sends a matching pair. Every request must therefore be addressed to one of the names in `AGB_UI_ALLOWED_HOSTS` (by default `127.0.0.1:8080` and `localhost:8080`, the two the tunnel is opened as) or it is a 421, and every POST must declare an `Origin` from the same list or it is refused before the form is read. Those refusals are logged and not recorded: they name no operator, and the audit log records what operators did. Every response sends `X-Frame-Options: DENY`, a `Content-Security-Policy` with `frame-ancestors 'none'`, `X-Content-Type-Options: nosniff` and `Referrer-Policy: same-origin`, so no other site can frame a page carrying one-click writes or receive its address as a `Referer`. The referrer policy is not `no-referrer`: under it a browser sends `Origin: null` on the page's own form posts, and the write check refuses `null` because a sandboxed frame on another site sends it too, so every form would fail. It is server-rendered Python inside the `app` image; there is no JavaScript build and no Node toolchain.

## Rejected

- **A, Console, as the landing page**, for the rubber-stamping reason above. Its content survives; its information hierarchy does not.
- **B, Ledger, as the shell.** Cleanest separation of concerns and the runner-up, but it demotes the decision history to one tab among the rest and makes *who decided this* a question you have to navigate to answer.
- **A live-updating ticker or streaming position view.** The strategy rebalances monthly and fills at the close. A UI that updates every second would display noise at a resolution the strategy does not act on, and would invite watching.
- **Charts of the equity curve as the landing view.** An equity curve is the most persuasive and least informative thing this system could show, and the direct route to the discretionary intervention the standing constraint rules out.
- **Any write path other than approval and the kill-switch.** The UI does not resize positions, edit the universe, override the ranker, move the ladder, or promote a model. As built, every page says so in its footer, and the test that enumerates the write routes is how that sentence stays true.
