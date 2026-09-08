# Broker Platform Selection — Research Report

**Issue:** [#2 Select broker platform (paper + live, LPAC-scoped API keys)](https://github.com/agb1986/agb-trader/issues/2)
**Parent:** [#1 AGB Trader — Architecture Spec Map](https://github.com/agb1986/agb-trader/issues/1)
**Status:** Research only. No decision has been made — this document is input to that decision.
**Date:** 2026-09-07

## Why this matters

agb-trader is designed to start paper-only and move to live trading later **through the same
order-placement interface**, without a redesign. That constraint, plus the project's
least-privilege-access-control (LPAC) requirement — an API credential that can place/cancel
orders and read positions/account state, but structurally **cannot** withdraw or transfer funds —
rules a lot of otherwise-popular brokers in or out before price and features even enter the
picture. This report evaluates the candidates named in the issue (Alpaca, Interactive Brokers,
Tradier) plus two others a 2026 retail algo trader would realistically consider (TradeStation,
Charles Schwab Trader API), against:

- Paper trading with real feature/endpoint parity to live
- API documentation quality and SDK/community maturity
- Whether a credential can be scoped to trade-only, with no withdrawal/transfer capability (hard requirement — flagged clearly per candidate)
- US equities coverage and order types
- Pricing, commissions, account minimums
- Operational fit for a single-operator, semi-autonomous, Dockerized system (no vendor desktop app requirement, ideally)

## Candidates evaluated

| Broker | Paper trading parity | LPAC (no-withdrawal key) | Docs/SDK quality | Commissions | Account minimum | Desktop app / gateway required? |
|---|---|---|---|---|---|---|
| **Alpaca** | Excellent — same REST/WS surface, only base URL / key pair differs | Effectively yes, by construction (see below) — needs final confirmation | Excellent, modern, active community/tooling | $0 equities | $0 | No — pure REST/WebSocket |
| **Tradier** | Good — sandbox mirrors production endpoints (delayed data) | Yes — explicit `read`/`write`/`trade`/`market`/`stream` OAuth scopes, `trade` excludes `write` | Good, clear docs, smaller ecosystem | $0/trade (or $10/mo Pro flat) | Not found / apparently none | No — pure REST/OAuth |
| **Interactive Brokers (IBKR)** | Good, once linked to a funded live Pro account | Unclear/no self-service equivalent — read-only mode exists but is the opposite of what's needed; OAuth scopes exist for institutional partners, not a simple retail toggle | Extensive but dense; large ecosystem | ~$0.005/share (IBKR Lite is commission-free but has **no API access**) | $0 (IBKR Pro, no balance minimum) | **Yes** — TWS or Client Portal Gateway process must run and be periodically re-authenticated (weekly login + manual 2FA approval; cannot be fully automated) |
| **TradeStation** | Good — SIM environment documented against LIVE | Not found in public docs | Decent docs | Commission-free tiers available | **$10,000 minimum to get an API key at all**, plus manual Business Development review for key issuance | No — pure REST |
| **Charles Schwab Trader API** | **No** — sandbox is synthetic auth/data-shape testing only, not a real simulated order/fill environment | Not evaluated further (disqualified on paper-trading parity) | Good docs, but young post-TDA-migration | $0 equities | $0 | No — pure REST/OAuth |

Detailed notes per candidate below.

---

### 1. Alpaca

**Paper trading:** Free for all users, no minimum, and uses the identical Trading API
surface as live — same endpoints, same order/account/position semantics — with the only
difference being which base URL and API key pair (paper vs. live) you point at. This is the
cleanest match in the field for "same interface, no redesign to go live."

**API/docs quality:** Modern REST + WebSocket streaming API, official Python and JS/TS SDKs,
OpenAPI-style reference docs, and by far the largest mindshare among retail/indie algo traders in
2026 — extensive community tooling, tutorials, and even an official MCP server. Supports market,
limit, stop, and more complex order types; fractional shares down to $1 on 2,000+ names.

**LPAC:** This needed the most digging, because Alpaca actually has two distinct products and it's
easy to conflate them:

- The **Broker API** (for firms building a brokerage-style app for many end-customers) has a
  genuinely granular custom access-control system: each key can be set to "Read only," "Full
  access," or "Custom" per category (Accounts, **Funding**, Admin, Crypto, Rebalancing, Trading,
  Journaling, Data, Reporting, SSE), and setting **Funding → No Access** explicitly blocks
  withdrawal/ACH-transfer endpoints. This is not the product a single-operator retail trader would
  use for their own account — it requires a broker-dealer-style business relationship with Alpaca.
- The **Trading API** (the one a single retail account holder actually uses) has a simpler model:
  a paper key pair and a live key pair, each with essentially full account access. However,
  documentation and support pages consistently describe deposits/withdrawals for a standard
  self-directed brokerage account as being handled through the **web dashboard only** — the
  Trading API surface does not appear to expose a transfer/withdrawal endpoint for this account
  type at all (that capability lives in the Broker API and ACH/Plaid-partner flows). Alpaca's own
  OAuth2 scopes for the Trading API are `account:write`, `trading`, and `data` — no funding scope
  exists at all in that list, reinforcing that withdrawal is simply not part of this API's surface
  for individual accounts.
  - **Net assessment:** for a standard individual account, an ordinary Trading API key likely
    cannot move money out, not because you toggled a "no withdrawal" switch, but because the API
    it authenticates against doesn't do withdrawals in the first place. This satisfies the spirit
    of LPAC by default. This should be **explicitly reconfirmed with Alpaca support/current docs**
    before finalizing the decision, since no single doc page states "the Trading API cannot ever
    withdraw funds" in so many words — it's an inference from the absence of a funding
    endpoint/scope in that product, not an affirmative documented guarantee.

**Pricing / minimums:** $0 commission on self-directed cash brokerage equities/options trades
(live and paper). $0 account minimum. Real-time market data has free (limited) and paid
(unlimited) tiers.

**Rate limits:** 200 requests/minute per account on standard access — should be ample for a
monthly cross-sectional rebalance strategy; higher limits available on request.

**Docker/self-hosting fit:** Best-in-class. Pure cloud REST/WebSocket API — no desktop
application, no local gateway process, no session babysitting. Trivially containerized and
well-suited to a semi-autonomous, headless system.

---

### 2. Tradier

**Paper trading:** Tradier's sandbox environment (`sandbox.tradier.com/v1/...`) mirrors the
production API (`api.tradier.com/v1/...`) endpoint-for-endpoint, with paper money. The one caveat:
sandbox market data is delayed rather than real-time — likely immaterial for a monthly
cross-sectional rebalance strategy (as opposed to intraday/low-latency execution), but worth
naming.

**API/docs quality:** Clean, well-organized REST API with solid documentation and an active
developer program. Smaller SDK/community ecosystem than Alpaca's, but mature and API-first (no
legacy desktop-platform baggage).

**LPAC:** The strongest, most explicitly self-documented scoping model of any candidate reviewed.
Tradier's OAuth access tokens carry named scopes:

| Scope | Grants |
|---|---|
| `read` | Read access to account information, positions, and market data |
| `write` | Write access to account data — **explicitly does not include placing or updating trades** |
| `trade` | Permission to place and manage trades |
| `market` | Market data access (non-streaming) |
| `stream` | Real-time streaming data |

An application can request `trade` + `read` + `market` and deliberately omit `write`, giving a
credential that can trade and read the account but not modify general account settings. No scope
covering funds transfer/withdrawal appears in this list at all for the standard Brokerage API —
withdrawal-style endpoints were only found under Tradier's separate **Advisor API** (for RIAs
managing client/virtual accounts), which is a different product/use case than a single retail
operator trading their own account. As with Alpaca, this suggests withdrawal simply isn't reachable
through the standard trade-scoped OAuth flow — but the explicit, documented scope list here is a
more auditable mechanism than Alpaca's "the endpoint doesn't exist" argument, and is worth weighing
in the final decision.

**Pricing / minimums:** Standard account: $0/order equities, $0.35/contract options. Optional $10/
month Pro package: flat-fee, commission-free equities and options (with narrow per-contract
exceptions for certain index options). No account minimum found in current documentation.

**Docker/self-hosting fit:** Same class as Alpaca — pure REST/OAuth, no desktop app or gateway
process required.

---

### 3. Interactive Brokers (IBKR)

**Paper trading:** IBKR provides a paper/demo account associated with a funded live account, and
it mirrors live trading behavior reasonably well once set up. However, note the account-type
constraint below.

**API/docs quality:** The broadest asset-class and market coverage in the field (global equities,
options, futures, FX, bonds), extensive documentation, and a large ecosystem of community
libraries. But the documentation is dense and split across multiple, historically inconsistent
API generations: the legacy socket-based **TWS API**, and the newer REST/WebSocket **Client
Portal Web API**. This is a meaningfully steeper learning curve than Alpaca's or Tradier's single,
modern REST surface.

**LPAC:** No clear, documented, self-service way to mint a "trade but never withdraw" retail
credential was found. IBKR does offer a "read-only access" mode, but that's the inverse of what
this project needs (it blocks trading entirely rather than blocking withdrawals while allowing
trading). The newer Web API does support OAuth 2.0 with scopes referenced in developer materials
(e.g., `account.read`, `trading.write`), which suggests genuine granular scoping is architecturally
possible — but this appears targeted at institutional/qualified OAuth partner integrations, not a
simple self-service flow for an individual retail client using the standard Client Portal Gateway
login. Individual clients typically authenticate the gateway with username/password (+2FA) rather
than issuing a scoped API key. Withdrawals themselves are also gated by IBKR's own account-level
verification steps regardless of API path, which reduces (but doesn't eliminate, and doesn't
formally scope away) the risk.

**Account type / cost:** API access requires an **IBKR Pro** account — **IBKR Lite does not
support API access at all**. IBKR Pro has no account minimum, but commissions are per-share
(~$0.005/share, roughly $1 minimum per trade) rather than commission-free, with narrow exceptions.

**Docker/self-hosting fit:** This is IBKR's biggest operational drawback for this project. Both
TWS and the Client Portal Gateway are local processes that must be running to reach the API —
community Docker images exist (e.g. `gnzsnz/ib-gateway-docker`) so it's containerizable, but the
gateway session still requires a **full manual login roughly once a week**, and **two-factor
authentication approval on a physical device that cannot be automated away**. For a system
explicitly framed as "semi-autonomous, single-operator," this recurring manual-intervention
requirement is a real fit problem, independent of API quality.

---

### 4. TradeStation

**Paper trading:** A documented "SIM" (simulator) environment exists alongside "LIVE," with
parallel semantics — reasonable parity on paper.

**LPAC:** No public documentation found describing granular, no-withdrawal API key scoping.

**Disqualifying factor for this project:** API key issuance requires a **funded account with at
least $10,000**, and keys are issued only after a **manual Business Development review/onboarding**
process — not instant self-service. Both the minimum and the manual-approval friction are a poor
fit for "reasonable for a retail-scale single-operator system... not institutional minimums,"
which is an explicit requirement in this ticket. Ruled out primarily on this basis rather than API
quality.

---

### 5. Charles Schwab Trader API (successor to the retired TD Ameritrade API)

**Disqualifying factor:** Schwab's Trader API connects only to **live** accounts. Its "sandbox" is
a synthetic environment for testing authentication/token flow and data shapes — it is **not** a
functioning simulated order/fill environment. There is no real paper-trading mode with
endpoint/semantics parity to live. This breaks the ticket's explicit "paper trading with feature
parity to live" requirement outright, so it was not evaluated further on LPAC, pricing, or other
axes.

---

## Recommendation (research input, not a decision)

**Alpaca** is the strongest overall fit for agb-trader's stated constraints:

1. **Best paper/live parity** of any candidate — identical API surface, differing only by base
   URL and key pair, which most directly satisfies "same order-placement interface, no redesign
   to go live."
2. **Best documentation and ecosystem** for a single developer building a Dockerized, Python-
   heavy system in 2026 — modern REST/WebSocket API, official SDKs, and the largest retail-algo
   community of any candidate reviewed.
3. **No desktop app or gateway process, ever** — a pure cloud API that Dockerizes trivially and
   requires zero recurring manual authentication, unlike IBKR's weekly-login/2FA burden. This
   matters specifically because the system is meant to run semi-autonomously with a single
   operator.
4. **LPAC is effectively satisfied by construction** for a standard individual account — the
   Trading API a retail user actually authenticates against does not appear to expose any
   withdrawal/transfer capability at all, and its OAuth2 scopes (`account:write`, `trading`,
   `data`) contain no funding scope. This should still be explicitly reconfirmed with Alpaca
   (support ticket or a hands-on test against a funded account) before finalizing, since it's an
   inference from the absence of an endpoint/scope rather than an affirmatively documented
   guarantee.
5. **$0 commissions, $0 account minimum**, and a 200 req/min rate limit that comfortably covers a
   monthly-rebalance cross-sectional strategy.

**Tradier is a strong, very close second** and arguably has the more auditable LPAC story: its
OAuth scopes (`read`, `write`, `trade`, `market`, `stream`) are explicit, named, and documented,
with `write` explicitly called out as excluding trade placement — meaning `trade`+`read` is a
concretely documented "no general account write" credential in a way Alpaca's "no such endpoint
exists" argument isn't. If the human decision-maker weighs a formally documented scope model more
heavily than Alpaca's ecosystem/tooling advantage, Tradier is the natural alternative and should be
the fallback choice.

**Interactive Brokers** should stay on the radar for later — once account size and asset-class
needs (options, futures, better margin) outgrow Alpaca/Tradier, IBKR's breadth and margin/short-
locate quality become genuinely attractive. But its lack of a clear self-service no-withdrawal
credential and its mandatory weekly manual-login/2FA requirement make it a poor fit for a
semi-autonomous system starting from scratch in 2026.

**TradeStation** and the **Schwab Trader API** are not recommended: TradeStation on account-minimum
and manual-onboarding grounds, Schwab on the hard "no real paper trading" disqualifier.

This report does not close out issue #2 — the final broker decision (and hands-on verification of
the LPAC claims above against an actual funded/paper account before wiring in real credentials) is
left to a future decision-making session, per the ticket's instructions.

## Sources

- https://docs.alpaca.markets/docs/trading-api
- https://docs.alpaca.markets/us/docs/paper-trading
- https://docs.alpaca.markets/us/docs/credential-management.md
- https://docs.alpaca.markets/us/docs/using-oauth2-and-trading-api.md
- https://docs.alpaca.markets/us/docs/funding-accounts
- https://docs.alpaca.markets/docs/ach-funding
- https://alpaca.markets/support/deposits-and-withdraws-faqs
- https://alpaca.markets/algotrading
- https://alpaca.markets/support/usage-limit-api-calls
- https://docs.tradier.com/docs/authentication
- https://docs.tradier.com/docs/getting-started
- https://docs.tradier.com/docs/trading
- https://documentation.tradier.com/brokerage-api/overview/faq
- https://trade.tradier.com/developer-api/
- https://www.interactivebrokers.com/campus/ibkr-api-page/web-api-trading/
- https://www.interactivebrokers.com/docs/web-api/authentication/sessions
- https://www.ibkrguides.com/orgportal/usersettings/readonlyaccess.htm
- https://www.interactivebrokers.com/en/trading/why-ibkr-lite.php
- https://www.interactivebrokers.com/en/accounts/required-minimums.php
- https://www.interactivebrokers.com/campus/trading-lessons/launching-and-authenticating-the-gateway/
- https://github.com/gnzsnz/ib-gateway-docker
- https://github.com/IbcAlpha/IBC/issues/118
- https://api.tradestation.com/docs/fundamentals/sim-vs-live/
- https://developer.tradestation.com/trading-api/
- https://www.tradestation.com/insights/getting-started-with-a-trading-platform-api/
- https://medium.com/@carstensavage/the-unofficial-guide-to-charles-schwabs-trader-apis-14c1f5bc1d57
- https://usethinkscript.com/threads/schwab-api-paper-trading-for-automated-trading-systems.22197/
- https://grokipedia.com/page/Schwab_Trader_API
