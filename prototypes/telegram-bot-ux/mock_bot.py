#!/usr/bin/env python3
"""PROTOTYPE — throwaway. Mock Telegram bot for agb-trader, issue #8.

Not production code. No Telegram, no broker, no database — every number below
is invented. The question this answers is whether the command set and the
live-approval flow feel right when you drive them in sequence, which is not
something a static mock-up of one message can tell you.

Run it:

    python prototypes/telegram-bot-ux/mock_bot.py

Type commands at the `you >` prompt exactly as you would in Telegram. The
`sim` command drives the world forward (a rebalance arriving, the clock
running out, a gate failing), so you can walk the paths that only exist in
time. `sim` is a prototype affordance and would not exist in the real bot.

Messages are printed the way Telegram would render them: plain text, no
inline keyboards, no markdown tables. Telegram uses a proportional font on a
narrow phone screen, so column alignment is wasted effort — the layout here
is two short lines per item, the same shape torrent-agent settled on.
"""

import random
import sys
import textwrap
from datetime import datetime, timedelta, timezone

# --------------------------------------------------------------------------
# Invented world state. In the real bot every one of these fields is read from
# the system database, which owns approval state; Telegram is only a view.
# --------------------------------------------------------------------------

WHO = "agb1986"

STATE = {
    "mode": "live",              # paper | live
    "level": 0,                  # autonomy ladder L0 / L1 / L2
    "halted": False,
    "halt_reason": None,
    "paused": False,
    "equity": 48_210.44,
    "cash_pct": 0.4,
    "positions": 20,
    "clean_run": 3,              # consecutive rebalances the human did not reject
    "clean_needed": 6,
    "pending": None,             # the basket awaiting a decision
    "liquidate_token": None,
    "last_gate": None,
    "log": [],
}

BASKET_ID = "2026-10"

# One rebalance: three out, three in, seventeen held. Ranks are stage-1 ranks
# out of the ~3.2k universe; safety is the LLM panel's normalised [0,1] score.
SELLS = [
    ("CULP", "Culp Inc", 47, 12),
    ("NVEE", "NV5 Global", 38, 9),
    ("SMLR", "Semler Scientific", 33, 21),
]
BUYS = [
    ("AMRC", "Ameresco", 4, 0.71),
    ("HCKT", "Hackett Group", 7, 0.83),
    ("PLPC", "Preformed Line", 11, 0.58),
]
HOLDS = [
    "ACLS", "AMPH", "ARIS", "ATEN", "AVAV", "BCC", "CALM", "CEIX", "CRVL",
    "DAKT", "EPAC", "GRBK", "IESC", "JBSS", "MLI", "PRDO", "TGLS",
]

# The panel's six-dimension rubric for one name, plus the Judge's one-line
# reason per dimension. This is the explainability payload from issue #6.
RUBRIC = {
    "PLPC": {
        "total": 14,
        "shortlist_pct": 22,
        "dims": [
            ("going concern / liquidity", 4,
             "Net cash, no revolver draw, current ratio 3.1x."),
            ("accounting integrity", 1,
             "Auditor change 8-K 2026-08-14. New ICFR risk factor added "
             "versus the FY2025 10-K."),
            ("litigation / regulatory", 3,
             "One ordinary-course contract dispute, immaterial to earnings."),
            ("business-model impairment", 2,
             "Two consecutive quarters of order-book decline disclosed in MD&A."),
            ("governance / management", 2,
             "CFO departed 2026-06, interim in seat, no permanent hire named."),
            ("disclosure delta", 2,
             "Item 1A expanded on supply concentration versus prior year."),
        ],
        "sources": [
            "10-K FY2025 (filed 2026-03-02)",
            "10-Q Q2 2026 (filed 2026-08-07)",
            "8-K (filed 2026-08-14)",
        ],
    },
}

GATE_CHECKS = [
    ("price data freshness", "SEP snapshot 2026-09-30, 0 days stale"),
    ("fundamentals point-in-time", "SF1 datekey <= signal date on 3,214 rows"),
    ("universe size", "3,214 names, within 2,500-5,000"),
    ("panel coverage", "50 of 50 shortlisted names scored"),
    ("panel dispersion", "sd 0.19, above 0.05 floor"),
    ("basket size", "20 names"),
    ("basket weights", "equal, max deviation 0.02%"),
    ("no leverage", "gross 99.6% of equity"),
    ("no shorts", "0 short orders"),
    ("sector concentration", "Industrials 30%, below 40% cap"),
    ("order notional", "max $2,410, below $5,000 cap"),
    ("duplicate orders", "none against today's submitted set"),
]


# --------------------------------------------------------------------------
# Rendering helpers
# --------------------------------------------------------------------------

def bot(text: str) -> None:
    """Print a bot message the way Telegram would show it."""
    print()
    for line in textwrap.dedent(text).strip("\n").split("\n"):
        print("  " + line)
    print()


def log(event: str, who: str = WHO, **fields) -> None:
    """One audit line per action. In the real bot this is a JSON line on disk,
    appended and flushed, so a kill -9 still leaves every decision recorded.

    Every line carries an actor, and "system" is a real actor — an expiry or a
    gate halt happened to the human, not by them, and the audit has to be able
    to tell those apart later.
    """
    STATE["log"].append({
        "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "who": who,
        "event": event,
        **fields,
    })


def state_line() -> str:
    bits = [STATE["mode"].upper(), f"L{STATE['level']}"]
    if STATE["halted"]:
        bits.append("HALTED")
    if STATE["paused"]:
        bits.append("PAUSED")
    return " · ".join(bits)


def deadline_text() -> str:
    p = STATE["pending"]
    if not p:
        return ""
    mins = p["minutes_left"]
    return f"deadline 15:45 ET (in {mins}m)"


# --------------------------------------------------------------------------
# Bot messages
# --------------------------------------------------------------------------

USAGE = """
Commands:
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
"""


def msg_approval() -> str:
    out = "\n".join(
        f"  x {t:<6} {n:<24} rank {r} (was {w})" for t, n, r, w in SELLS)
    inn = "\n".join(
        f"  + {t:<6} {n:<24} rank {r}   safety {s:.2f}" for t, n, r, s in BUYS)
    return f"""
REBALANCE {BASKET_ID} — approval needed
{state_line()} · {deadline_text()}

OUT (3)
{out}

IN (3)
{inn}

HOLD 17 unchanged

turnover 15.2%  ·  cash after 0.3%  ·  top sector Industrials 30%

FLAGS (1)
  ! PLPC — accounting integrity 1/4, the panel's weakest score
    this month. Ranked 11 by stage 1.  /why PLPC

GATE  12 of 12 passed  /gate

/approve {BASKET_ID}   /reject {BASKET_ID}   /basket   /why <TICKER>
No answer by 15:45 ET means no trades this month.
"""


def msg_reminder(mins: int) -> str:
    return f"""
Rebalance {BASKET_ID} still needs a decision. {mins} minutes to the
market-on-close cutoff. No answer means no trades this month and the
book stays as it is.

/pending   /approve {BASKET_ID}   /reject {BASKET_ID}
"""


def msg_why(ticker: str) -> str:
    r = RUBRIC.get(ticker)
    if not r:
        return f"No panel record for {ticker}. Only shortlisted names are scored."
    name = next((n for t, n, _, _ in BUYS if t == ticker), ticker)
    rank = next((rk for t, _, rk, _ in BUYS if t == ticker), "?")
    safety = next((s for t, _, _, s in BUYS if t == ticker), 0)
    dims = []
    for label, score, reason in r["dims"]:
        mark = "  !" if score <= 1 else "   "
        dims.append(f"{mark} {label:<30} {score}/4")
        if score <= 2:
            for line in textwrap.wrap(reason, 58):
                dims.append(f"      {line}")
    srcs = "\n".join(f"  {s}" for s in r["sources"])
    return f"""
{ticker} — {name}

Stage 1  rank {rank} of 3,214
Panel    safety {safety:.2f}  ({r['total']}/24, weakest {r['shortlist_pct']}%
         of this month's 50 shortlisted names)

{chr(10).join(dims)}

Read from:
{srcs}

The panel scored a name-scrubbed extract; it did not know this was
{ticker}.
"""


def msg_gate() -> str:
    lines = []
    for label, detail in GATE_CHECKS:
        lines.append(f"  ok  {label}")
        lines.append(f"        {detail}")
    return f"""
GATE — rebalance {BASKET_ID}, run 15:31 ET

{chr(10).join(lines)}

12 of 12 passed. A single failure halts the whole rebalance; no
partial basket is ever submitted.
"""


def msg_gate_failed() -> str:
    return f"""
HALTED — rebalance {BASKET_ID} was not submitted.

  FAIL  universe size
          214 names, expected 2,500-5,000
  FAIL  price data freshness
          SEP snapshot 2026-08-31, 30 days stale

Nothing was sent to the broker. Your 20 positions are untouched and
will not be changed by anything automatic.

This halt is sticky. It survives a restart and clears only when you
clear it.

/gate for the full list   /resume when the data is fixed
"""


def msg_status() -> str:
    p = STATE["pending"]
    pending = (f"awaiting your decision on rebalance {BASKET_ID}, "
               f"{p['minutes_left']}m left" if p else "nothing awaiting you")
    flow = "stopped" if (STATE["halted"] or STATE["paused"]) else "active"
    reason = f"\nHalted: {STATE['halt_reason']}" if STATE["halted"] else ""
    return f"""
agb-trader — {state_line()}

Order flow: {flow}{reason}
Equity ${STATE['equity']:,.0f} · {STATE['positions']} positions · cash {STATE['cash_pct']}%
Pending: {pending}

Next rebalance: signal 2026-10-30, fills at the close 2026-10-31.
Level L{STATE['level']}, {STATE['clean_run']} of {STATE['clean_needed']} toward L{STATE['level'] + 1}.  /level
"""


def msg_positions() -> str:
    rows = []
    shown = [t for t, _, _, _ in BUYS] + HOLDS
    for i, t in enumerate(shown[:15]):
        pnl = random.uniform(-14, 22)
        rows.append(f"  {t:<6} {5.0:.1f}%   {pnl:+.1f}% since entry")
        rows.append(f"        held {random.randint(1, 5)} months")
    more = len(shown) - 15
    tail = f"\n...and {more} more." if more > 0 else ""
    return f"""
20 positions · equal weight · ${STATE['equity']:,.0f}

{chr(10).join(rows)}{tail}

/basket for this month's target
"""


def msg_pnl() -> str:
    return """
Live since 2026-01-02 (9 months)

  return        +11.4%
  benchmark     +7.9%  (SPY, same window)
  max drawdown  -12.1%
  worst month   -6.3%  (2026-04)

Against the backtest's CPCV path distribution over the same length of
window, this run sits at the 61st percentile of paths. Inside the
tested range.

Nine monthly observations cannot tell edge from luck. This is an
operational readout, not evidence the strategy works.
"""


def msg_level() -> str:
    return f"""
Autonomy L{STATE['level']} of 2

  L0  every rebalance needs your approval        <- you are here
  L1  approval only when the gate raises a flag
  L2  notify only; approval on flags above a size threshold

{STATE['clean_run']} of {STATE['clean_needed']} consecutive rebalances approved, none rejected.
A rejection resets the count, because a rejection is evidence the
review was doing work.

Moving up is a manual act by you. Dropping back is automatic on any
halt, any rejection, or any gate failure.
"""


def msg_pause() -> str:
    return f"""
PAUSED by {WHO}.

  halt latch          set, persisted, survives restart
  Alpaca suspend_trade ON

No orders will be submitted by anything. Your {STATE['positions']} positions are
untouched — pausing does not sell anything.

Data ingestion, model training and panel runs carry on, so nothing
has to be caught up when you resume.

/resume to clear both latches.
"""


def msg_liquidate_prompt(token: str) -> str:
    return f"""
LIQUIDATE — this closes all {STATE['positions']} positions at market, about
${STATE['equity']:,.0f}, and cancels every open order first.

This cannot be undone. The system will not re-enter these names
until the next scheduled rebalance, and it will be paused after.

To confirm, within 5 minutes, send:

  /liquidate {token}
"""


def msg_liquidate_done() -> str:
    return """
LIQUIDATED 19 of 20.

  closed   19 positions
  failed   CULP — 422 from Alpaca, symbol halted, still held

  cancelled 0 open orders

The system is now paused. One position remains. Broker returned a
per-symbol result and this is that result, not a summary of it.

/status   /positions
"""


def msg_drawdown() -> str:
    return """
DRAWDOWN ALERT — nothing has been done and nothing will be.

Live drawdown -19.4% from the peak of 2026-06-30.
Deepest drawdown in 4,000 CPCV backtest paths: -18.1%.

Live is now outside the range validation explored. That is the whole
content of this alert: the world is somewhere the backtest never
went. It is not a recommendation.

No position will be changed automatically. If you want to stop,
/pause stops order flow and /liquidate closes the book.

/pnl   /positions
"""


def msg_filled() -> str:
    return f"""
REBALANCE {BASKET_ID} filled at the close.

  sold    CULP, NVEE, SMLR
  bought  AMRC, HCKT, PLPC

  6 of 6 orders filled at the official close
  slippage vs official close: 0.0 bp
  commission: $0.00

Approved by {WHO} at 15:38 ET. 20 positions, cash 0.3%.

/positions
"""


def msg_missed() -> str:
    return f"""
Rebalance {BASKET_ID} expired without a decision. No orders were sent.

The book is unchanged: the 3 names the model wanted out are still
held, and the 3 it wanted in were not bought. This is a deliberate
divergence from the strategy, recorded as such, and it will show up
in the reconciliation against the backtest.

Next scheduled rebalance: 2026-10-30.
"""


# --------------------------------------------------------------------------
# Command handling
# --------------------------------------------------------------------------

def cmd_approve(arg: str) -> None:
    p = STATE["pending"]
    if not p:
        bot("Nothing is awaiting approval.")
        return
    if arg.strip() != BASKET_ID:
        bot(f"""
        Approve which basket? Send the id shown in the request:

          /approve {BASKET_ID}

        The id is required so that an approval typed late cannot land
        on a basket you never read.
        """)
        return
    STATE["pending"] = None
    STATE["clean_run"] += 1
    log("approved", basket=BASKET_ID)
    bot(f"""
    Approved {BASKET_ID} by {WHO}. 6 orders queued as market-on-close.

    You will get a fill confirmation after the close. Until then
    /pause still stops them.
    """)


def cmd_reject(arg: str) -> None:
    p = STATE["pending"]
    if not p:
        bot("Nothing is awaiting approval.")
        return
    STATE["pending"] = None
    STATE["clean_run"] = 0
    log("rejected", basket=BASKET_ID, reason=arg or None)
    bot(f"""
    Rejected {BASKET_ID} by {WHO}. No orders were sent. The book is
    unchanged and this month is a deliberate divergence from the
    strategy.

    Autonomy progress reset to 0 of {STATE['clean_needed']}.

    Reason recorded: {arg or '(none given)'}
    """)


def cmd_pause() -> None:
    STATE["paused"] = True
    log("paused")
    bot(msg_pause())


def cmd_resume() -> None:
    if not (STATE["paused"] or STATE["halted"]):
        bot("Nothing to clear. Order flow is already active.")
        return
    STATE["paused"] = False
    STATE["halted"] = False
    STATE["halt_reason"] = None
    log("resumed")
    bot(f"""
    Cleared by {WHO}. Halt latch off, Alpaca suspend_trade off.
    Order flow active.

    The rebalance that was halted is not retried automatically. If it
    is still before the cutoff, /pending will show it.
    """)


def cmd_liquidate(arg: str) -> None:
    if arg.strip():
        if arg.strip() == STATE["liquidate_token"]:
            STATE["liquidate_token"] = None
            STATE["positions"] = 1
            STATE["paused"] = True
            log("liquidated", closed=19, failed=["CULP"])
            bot(msg_liquidate_done())
        else:
            bot("Token does not match. Send /liquidate again for a new one.")
        return
    token = "CONFIRM-" + "".join(
        random.choice("ABCDEFGHJKLMNPQRSTUVWXYZ23456789") for _ in range(4))
    STATE["liquidate_token"] = token
    log("liquidate_requested")
    bot(msg_liquidate_prompt(token))


def cmd_sim(arg: str) -> None:
    """Prototype-only. Drives the world so time-dependent paths are walkable."""
    a = arg.strip()
    if a == "rebalance":
        STATE["pending"] = {"minutes_left": 58}
        bot(msg_approval())
    elif a in ("t-60", "t60"):
        STATE["pending"] = {"minutes_left": 60}
        bot(msg_reminder(60))
    elif a in ("t-20", "t20"):
        STATE["pending"] = {"minutes_left": 20}
        bot(msg_reminder(20))
    elif a == "timeout":
        STATE["pending"] = None
        STATE["clean_run"] = 0
        log("expired", who="system", basket=BASKET_ID)
        bot(msg_missed())
    elif a == "fill":
        bot(msg_filled())
    elif a == "gatefail":
        STATE["halted"] = True
        STATE["halt_reason"] = "gate failed on universe size and data staleness"
        STATE["pending"] = None
        log("halted", who="system",
            checks=["universe size", "price data freshness"])
        bot(msg_gate_failed())
    elif a == "drawdown":
        bot(msg_drawdown())
    else:
        bot("""
        sim rebalance   the monthly approval request arrives
        sim t-60        the first reminder
        sim t-20        the last reminder
        sim timeout     the cutoff passes with no answer
        sim fill        fills land after the close
        sim gatefail    the pre-trade gate fails and halts
        sim drawdown    a performance alert fires
        """)


def cmd_log(arg: str) -> None:
    n = int(arg) if arg.strip().isdigit() else 10
    entries = STATE["log"][-n:]
    if not entries:
        bot("Nothing logged yet this session.")
        return
    lines = []
    for e in entries:
        extra = " ".join(f"{k}={v}" for k, v in e.items()
                         if k not in ("ts", "who", "event"))
        lines.append(f"  {e['ts']}  {e['event']}")
        lines.append(f"    by {e['who']} {extra}".rstrip())
    bot("\n".join(lines))


DISPATCH = {
    "/help": lambda a: bot(USAGE),
    "/start": lambda a: bot(USAGE),
    "/status": lambda a: bot(msg_status()),
    "/positions": lambda a: bot(msg_positions()),
    "/pnl": lambda a: bot(msg_pnl()),
    "/pending": lambda a: bot(msg_approval() if STATE["pending"]
                              else "Nothing awaiting your decision."),
    "/basket": lambda a: bot(msg_positions()),
    "/why": lambda a: bot(msg_why(a.strip().upper() or "PLPC")),
    "/gate": lambda a: bot(msg_gate()),
    "/level": lambda a: bot(msg_level()),
    "/approve": cmd_approve,
    "/reject": cmd_reject,
    "/pause": lambda a: cmd_pause(),
    "/resume": lambda a: cmd_resume(),
    "/liquidate": cmd_liquidate,
    "/log": cmd_log,
    "sim": cmd_sim,
}


def main() -> None:
    print(__doc__.split("\n\n")[0])
    print("PROTOTYPE. Every number is invented. `sim` for scenarios, "
          "/help for commands, ctrl-d to quit.")
    bot(msg_status())
    while True:
        try:
            raw = input("you > ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            return
        if not raw:
            continue
        command, _, argument = raw.partition(" ")
        command = command.split("@", 1)[0].lower()
        handler = DISPATCH.get(command)
        if not handler:
            bot(f"Unknown command {command}. /help")
            continue
        handler(argument)
        # Rule 5: surface the state after every action.
        print(f"      [{state_line()} · {STATE['positions']} pos"
              f"{' · pending ' + BASKET_ID if STATE['pending'] else ''}]",
              file=sys.stderr)


if __name__ == "__main__":
    main()
