# The 2026-11 turnover halt

**Basket:** live 2026-11, session 2026-11-02, proposed 2026-10-30, `halted` since 2026-10-30 22:31:46Z
**Gate run 2:** 200 checks, one failure — `basket / turnover / 95.61% / < 60%`
**Decision deadline:** 2026-11-30 23:00 UTC, when the 2026-12 rebalance claims its job
**Date:** 2026-11-05. Read-only analysis; nothing in it was executed.

The 2026-11 basket is halted on a single check and will not trade. The question this answers is
what the check is actually measuring, what turnover limit a rebuild from the current book would
need, and what that rebuild would cost. The decision — clear the halt and raise the limit, or
leave it and skip 2026-12 as well — is the operator's.

## What failed

One check out of 200. Every provenance check, every other basket check, and all 180 order
checks passed. The failure is `turnover`, measured at 95.61% against a `< 60%` threshold.

`gate._turnover` computes `sold / value`, where `sold` is the notional of the plan's sell
orders and `value` is the book at the plan's reference prices. For basket 2 that is
$83,254.93 of sells against a book worth $87,079.30, which is 95.61%. The missing 4.39% is
TWST: $3,824.37 of it, the one name the plan keeps.

## Why it is 95.61%

The driver is a legitimate change of ranker, not a broken pipeline. Basket 1 was selected by
`placeholder-momentum-252-21`; basket 2 by `lgbm-2026-10`. Of the twenty October names, exactly
one survives into the November top twenty:

| October name | November rank |
| --- | --- |
| TWST | 19 — held, the only name kept |
| TXG | 62 |
| LITE | 109 |
| MU | 207 |
| RVMD | 226 |
| OMER | 263 |
| … | the remaining thirteen rank 1217 to 2368 |
| OPTX | not in the November ranking at all |

A near-total turn of the book is the correct output of a model change. It would have read
~96% whatever had happened at the broker.

## The book the check measured is not the book

`basket.book()` reads `agb.basket_target` for the last **filled** live basket. Those are the
twenty names basket 1 was *proposed* to hold. What the account holds is three:

| symbol | shares | value, 2026-11-04 snapshot | value at the plan's 2026-10-30 marks |
| --- | --- | --- | --- |
| LITE | 5 | $3,164.65 | $4,203.50 |
| MU | 4 | $3,189.96 | $3,538.96 |
| OPTX | 606 | $2,763.36 | $3,811.74 |
| | | **$9,117.97** | **$11,554.20** |

Of basket 1's twenty market-on-close orders, three filled and **seventeen expired**
(`agb.order_state`). The seventeen names were never bought, so sixteen of the nineteen sells in
the November plan are sells of shares that do not exist — $71,700.73 of the $83,254.93. The
per-order `a sell closes one it holds` check passed on all nineteen because it reads the same
`book()`, so the phantom holdings are consistent with themselves.

Measured against the real book, the plan sells all three holdings. Turnover is exactly
**100.00%**, and the comparison is a strict `ratio < limits.turnover`, so **no limit value up to
and including 1.00 admits a full liquidation**. A limit of 1.00 does not pass a 100% turn.

## The deadlock

`book()` keys off the last *filled* live basket. Basket 2 is halted, so it never fills, so
basket 1 remains the reference indefinitely. Every future month is measured against the same
twenty phantom targets and turns over ~95% of them, because the ranker that chose them has
been replaced. 2026-12 and 2027-01 fail the same check for the same reason, each after a full
rebalance including another stage-2 panel run and its Anthropic spend.

`/resume` clears the kill-switch latch but does not resubmit: *"A halted rebalance is not
resubmitted"* (`killswitch.py:119`, `execution.py:850`). Clearing the latch alone changes
nothing about this basket.

## What the rebuild costs

Modelled at cost model version 1, from the basket's own stored `modelled_cost_bps` and
`fees_usd`:

| | orders | notional | wavg bps | modelled | fees | total |
| --- | --- | --- | --- | --- | --- | --- |
| sells | 19 | $83,254.93 | 22.34 | $185.98 | $2.71 | $188.70 |
| buys | 19 | $93,798.01 | 18.59 | $174.41 | — | $174.41 |
| **whole plan** | **38** | **$177,052.94** | **20.51** | **$360.39** | **$2.71** | **$363.11** |

The honest number is smaller, because sixteen of the sells cannot execute. Selling only what
is really held:

| | notional | wavg bps | modelled | fees | total |
| --- | --- | --- | --- | --- | --- |
| LITE, MU, OPTX | $11,554.20 | 11.54 | $13.34 | $0.39 | **$13.73** |

The other $71,700.73 of sells carries $172.64 of modelled cost that will never be paid, because
those orders will be rejected or expire the way basket 1's did.

So the real cost of rebuilding is about **$13.73 to liquidate** and **$174.41 to buy the
twenty November names** — roughly **$188 on $94,449 of equity, about 20 bps**. OPTX alone
accounts for $11.31 of the $13.34: its impact term is 27.15 bps on a $3,811.74 position, which
is what a 606-share line in a thin name costs to exit.

## Capital

A side finding worth flagging: sizing used `capital_usd = 100000` while equity is
**$94,449.35** with **$85,433.03** in cash (broker state, 2026-11-05 10:09Z). The plan's buys
come to $93,798.01. Cash plus the $11,554.20 the real sells raise is about $96,987, so the
buys fit — but with roughly $3,200 of headroom, not the $6,200 the $100,000 figure implies.
The `cash` gate check passed against the $100,000 assumption, not against real equity.

Fixed, issue #125: the rebalance now sizes each basket on the account's equity as execution
last read it (`agb.broker_state_now`), and defers the job when there is no reading fresh
enough to size against. `AGB_CAPITAL_USD` remains as an override for a run that has to be
sized by hand.

Where the 100000 came from: `env/pipeline.env.example` set `AGB_CAPITAL_USD=100000`, and the
server's `env/pipeline.env` was filled in from it, so the override was set on every rebalance
the pipeline has ever run. The example now carries the variable commented out, as the override
it is. The code change alone does not reach the server: the line has to come out of
`env/pipeline.env` on the deploy machine, or every basket is still sized on 100000.

    ssh casaos.local "sed -i '/^AGB_CAPITAL_USD=/d' /mnt/data/AppData/agb-trader/env/pipeline.env"
    ssh casaos.local "/mnt/data/AppData/agb-trader/agb-stack up -d pipeline"

## The options

**(a) Raise the turnover limit above 1.00.** `gate.Limits.turnover` is a hard-coded dataclass
default (`gate.py:76`), so this is a code change and a pull request, not configuration. It
would clear this halt and every future one. It also retires the check: a limit above 100%
cannot fail, which removes the thing that catches a genuinely broken pipeline proposing to sell
a book it should be holding.

**(b) Fix `book()` to read observed positions.** This is correct regardless of what is decided
about the halt — the gate is currently reasoning about a book that does not exist, and the
per-order sell check inherits the same fiction. It is necessary but not sufficient: three of
three holdings sold is still 100%, which still fails any limit at or below 1.00.

**(c) Liquidate to an empty book.** `_turnover` returns a pass on an empty book — "an empty
book has nothing to turn over". But liquidating at the broker does not touch
`agb.basket_target`, so with `book()` as it stands the gate would still see twenty phantom
names. This only works on top of (b).

**(d) Exempt a changed promoted model from the turnover check.** This fits what the check says
it is for — catching a broken pipeline, not a bad month. A basket whose ranker differs from the
last filled basket's has a declared reason to turn the book over, and the check could record
that rather than fail. It is the narrowest change that keeps the check meaningful for the case
it was written for.

My reading: (b) is owed in any case and should land on its own merits. On top of it, (d) is the
change that clears this halt without blinding the gate — (a) buys the same outcome by deleting
the check, and (c) only works once (b) is in. Whether to clear the halt and let 2026-12 trade,
or to leave it and skip again, is the call to make before 2026-11-30 23:00 UTC.

## Verified against

- `agb.basket_state`, `agb.basket_target`, `agb.basket_rank`, `agb.basket_order` for baskets 1
  and 2, and `agb.gate_run` / `agb.gate_check` for gate runs 1 and 2, on casaos.local.
- `agb.order_state` for basket 1's fills: 3 filled, 17 expired.
- `agb.position` / `agb.position_snapshot` snapshot 31, 2026-11-04 21:05:12Z.
- `agb.broker_state`, 2026-11-05 10:09:10Z.
- `src/agb/gate.py:76` and `src/agb/gate.py:361-378`; `src/agb/basket.py:158-167` and
  `src/agb/basket.py:285-288`.

## Decision

Taken 2026-11-05: option (d), on top of (b), which landed separately as PR #118.

The gate's turnover check now admits up to a full rotation where the model behind the book is not
the model that ranked the basket, and records the change that admitted it. The reasoning is the
one the module docstring already states: these limits are malfunction detectors, not risk
management. The rank buffer holds the overlap up within one ranking; across two rankings it says
nothing, so a basket ranked by a newly promoted model has a declared reason to turn the book over,
and a flat 60% would halt every month that promotes one.

How it is built:

- The model behind the book is the ranker and artifact digest of the last live basket whose orders
  actually filled (`gate._HELD`, joining `agb.broker_order` to `agb.fill`). Deliberately not the
  last basket in `filled`: that state only says the session is over, and a basket whose every
  order expired unfilled reaches it having bought nothing, so its ranker never touched the book.
  `basket.book()` cannot supply this, because after (b) it reads the broker's position snapshot,
  which carries no provenance.
- The change test is fail closed (`gate._model_changed`). No basket has ever filled, the same
  ranker with the same digest, or the same ranker with either digest missing: all read as
  unchanged, and the flat 60% stands. A different ranker name, or the same name with two differing
  digests on the record, is a change.
- The exemption is bounded at 100%. A plan that claims to sell more than the book is worth is a
  malfunction under any model, which is the case the check was written for.
- The exemption text goes in the check's `threshold`, not its `measured`. `measured` stays a bare
  percentage, because `approval._fraction` and `readout.turnover_text` parse it as one; putting
  prose there would silence the separate 30% `UNUSUAL_TURNOVER` ask to the human on exactly the
  months that rotate hardest. A test pins that the L1 ask survives the exemption.

What this does not do: it does not un-halt the 2026-11 basket. The gate runs once per basket and
`run()` returns the verdict it recorded, and `/resume` clears the `gate_breach` latch without
resubmitting. The change takes effect at the 2026-12 rebalance.

Rejected: exempting a basket whose targets are disjoint from the book. That reads the same as (d)
in this instance but lets a churny model rotate the book fully every month, which is the thing the
check exists to stop.
