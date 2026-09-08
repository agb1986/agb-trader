# Market Data Vendor Selection — Research Report

**Issue:** [#3 Select market data vendor (survivorship-bias-free, point-in-time US data)](https://github.com/agb1986/agb-trader/issues/3)
**Parent:** [#1 AGB Trader — Architecture Spec Map](https://github.com/agb1986/agb-trader/issues/1)
**Status:** Research only. No decision has been made — this document is input to that decision.
**Date:** 2026-09-07

## Why this matters

The grounding research report (`compass_artifact_wf-30a6f2d2`) identified survivorship-bias-free,
point-in-time (PIT) data as one of the highest-leverage investments for backtesting validity in a
cross-sectional equity ranking strategy. Two failure modes are in scope:

- **Survivorship bias**: dropping delisted/bankrupt/acquired tickers from historical data inflates
  backtested returns and hides tail risk, because failures are removed from the sample by
  construction.
- **Look-ahead bias (via non-PIT fundamentals)**: even a dataset that keeps delisted tickers can
  still leak information from the future if it stores only the *current, restated* value of a
  financial metric instead of the value that was actually known/public as of each historical date.
  Restatements, and the lag between a fiscal period ending and the filing that discloses it
  (often 6–8 weeks for a 10-Q), are the usual leak paths.

A usable vendor needs to solve **both** problems, for **both** prices and fundamentals, at a
retail budget, in a format a single operator can pipe into local Parquet + DuckDB.

## Candidates evaluated

| Vendor | PIT fundamentals? | Survivorship-bias-free prices? | US equities breadth | Retail pricing | Delivery |
|---|---|---|---|---|---|
| **Sharadar** (sharadar.com, formerly Nasdaq Data Link/Quandl) | Yes | Yes | Broad (~16,000+ companies incl. delisted) | Yes ($9–$69/mo) | Bulk CSV + REST API |
| **Norgate Data** | Partial (price/fundamentals bundled, not truly as-reported PIT) | Yes (their core specialty) | Broad, back to 1950s | Yes (~$52–60/mo equiv.) | Proprietary local DB, Windows-only, Python API |
| **Polygon.io** | No | No (weak) | Broad but ticker-centric, sparse delisted metadata | Yes | Clean REST/WebSocket API |
| **EODHD** | Partial (delisted included; fundamentals not clearly as-reported/datekey PIT) | Yes (11,000+ delisted US names) | Broad | Yes ($20–$100/mo) | REST API |
| **Tiingo** | No (fundamentals is a thin add-on) | Unclear/likely incomplete | US + China, ~49k securities | Yes ($0–$30/mo) | REST API |
| **Financial Modeling Prep (FMP)** | No (identity mapping only, not as-reported financials) | Partial (delisted symbol list, not full historical financials for them) | Broad, global | Yes | REST API |

Detailed notes per vendor below.

---

### 1. Sharadar (sharadar.com / formerly Nasdaq Data Link "Quandl")

**What it is:** An independent data firm (founded 2013) that extracts, standardizes, and
organizes US financial data from SEC filings. Historically distributed exclusively through
Nasdaq Data Link (Quandl); as of 2026 it also sells subscriptions directly via sharadar.com,
which appears to be the more current/actively-marketed channel and is meaningfully cheaper than
legacy Nasdaq Data Link list pricing.

**PIT / survivorship-bias-free design (the key differentiator):**
- **SEP** (equity prices): covers 21,000+ active *and delisted* tickers back to 1998, so a
  backtest naturally includes companies that went bankrupt, were acquired, or delisted —
  no survivorship bias by construction.
- **SF1** (Core US Fundamentals): covers 16,000+ companies (again, including delisted) with
  fundamentals history to 1990. Critically, every record carries both:
  - the **reporting period** (fiscal quarter/year the numbers describe), and
  - a **`datekey`** — the actual SEC filing date.
  - It also exposes both an **As-Reported (AR)** dimension (the number as originally filed,
    frozen) and a **Most-Recent-Reported (MR)** dimension (post-restatement). This is exactly
    the mechanism needed to answer "what did the market actually know on date X" — query AR
    data as-of `datekey <= X` and you get a strategy-safe, look-ahead-free fundamentals feed.
    This is the single most important feature distinguishing Sharadar from most competitors
    above, which store only a current/restated snapshot.

**Coverage:** Broad cross-sectional universe (thousands of tickers, not just S&P 500) — suitable
for a cross-sectional ranking strategy across small/mid/large caps.

**Pricing (sharadar.com direct, as of this research):** Sold à la carte across four plans
(Fundamentals, Prices, Investors, Bundle) × three history depths (5yr / 10yr / Full History):
- Fundamentals: $19–$39/mo
- Prices (SEP): $9–$39/mo
- Investors (insider + institutional holdings): $19–$39/mo
- **Bundle (all of the above): $29/mo (5yr) / $49/mo (10yr) / $69/mo (Full History, ~1990s–present)**

This is squarely retail-scale — a single operator can get the full bundle with full history for
under $70/mo, or start on 10-year history at $49/mo. (Nasdaq Data Link's own listed pricing for
the same tables has historically run higher; sharadar.com direct is the cheaper current path and
should be treated as the primary channel to evaluate.)

**Delivery:** Bulk CSV/JSON downloads and a REST API with a query builder; also documented
integrations with Excel, Google Sheets, PostgreSQL, and SQLite. Bulk CSV drops directly into a
Parquet + DuckDB pipeline (`duckdb.sql("SELECT * FROM read_csv('sf1.csv')").write_parquet(...)`)
with no intermediate service needed — this is the best fit of any candidate for a self-hosted,
scriptable local pipeline.

**Live/ongoing feed:** Sharadar itself is EOD-only (no intraday/real-time). For a live trading
feed post-backtest, this needs a complementary source (e.g., a broker API such as Alpaca or
Interactive Brokers, which agb-trader will likely need anyway for execution) — this is expected
and consistent with the "clear complementary source" requirement rather than a gap unique to
Sharadar.

**Verdict:** Strongest fit against every stated requirement.

---

### 2. Norgate Data

**What it is:** A long-standing, well-regarded provider built specifically around
survivorship-bias-free backtesting for US, Australian, and Canadian equities, plus futures/FX.
US equities history extends back to 1950.

**PIT / survivorship-bias-free design:**
- Prices: this is Norgate's core specialty — delisted stocks, historically-accurate index
  constituents (so you can reconstruct "what was in the Russell 2000 on date X"), and
  survivorship-bias-free data are all first-class, long-proven features. Strong.
- Fundamentals: available bundled into Gold/Platinum/Diamond tiers, including EPS and other
  metrics per reporting period. However, nothing in Norgate's public documentation describes an
  as-reported-vs-restated split or a filing-date (`datekey`-equivalent) field the way Sharadar's
  SF1 does. Norgate's fundamentals are best understood as point-in-time for *index membership and
  price/volume-derived metrics*, but not demonstrably PIT/look-ahead-safe for *financial statement
  line items* the way this project's requirements call for. This is a meaningful gap if the
  cross-sectional ranking strategy leans on fundamentals (e.g., value or quality factors) rather
  than purely price/volume-derived signals.

**Pricing:** Subscription-only, 6- or 12-month terms (no monthly billing, no auto-renew). US
equities "Platinum" package: **USD 346.50 / 6 months or USD 630 / 12 months** (~$52–58/mo
equivalent). Retail-viable but higher than Sharadar's bundle, and less flexible (can't start/stop
monthly, no month-to-month trial-and-drop).

**Delivery — the key operational drawback:** Data lives in a **proprietary local database format,
Windows-only**, updated by the "Norgate Data Updater" (NDU) desktop application, which must be
running for any programmatic access. There is a `norgatedata` Python package (depends on pandas/
numpy/requests), but it talks to the local NDU process, not a REST API or a bulk file drop — so it
fundamentally requires either a Windows machine or Windows Subsystem for Linux (WSL2) running NDU
in the background. There is no native Linux/macOS path and no direct bulk export to
Parquet/CSV that isn't mediated by the Windows-only NDU service. For a Linux-based local Parquet +
DuckDB pipeline, this adds real friction and a dependency the other candidates don't have (either
a dedicated Windows box/VM, or WSL2 running an always-on background service).

**Live/ongoing feed:** Explicitly does **not** provide live, delayed, or intraday/tick data —
EOD only, same gap as Sharadar, same mitigation (pair with a broker feed).

**Verdict:** Excellent, proven survivorship-bias-free *price* history and index-reconstruction
data; weaker, less-documented PIT guarantees on fundamentals; meaningfully worse operational fit
for a Linux/Parquet/DuckDB stack due to the Windows-only NDU dependency.

---

### 3. Polygon.io

Point-in-time-by-ticker design avoids one narrow class of bias (ticker reuse), but their own
documentation and independent reviews indicate delisted-ticker data is "spotty at best" — often
missing company name, trading dates, or even a reliable delisted timestamp. Not recommended as a
primary source for survivorship-bias-free backtesting; better suited as a live/intraday
complement than as the PIT fundamentals+prices backbone.

### 4. EOD Historical Data (EODHD)

Genuinely includes delisted-company EOD prices (11,000+ delisted US names since ~2000) and
historical index-constituent snapshots (S&P 500 membership changes since 2012), which is a real
survivorship-bias mitigation for prices. However, nothing found indicates their Fundamentals Data
Feed carries an as-reported/filing-date PIT structure — it appears to be current/restated
snapshots, same gap as most non-specialist vendors. Cheaper entry point ($20–$100/mo tiers) and a
clean REST API. Worth keeping as a secondary/cross-check source, or a live-feed candidate, but not
a PIT-fundamentals substitute for Sharadar.

### 5. Tiingo

Broad US coverage (~49k securities) and a clean, cheap API ($0–$30/mo) plus an IEX real-time feed
(useful for the "live feed" requirement). But fundamentals are a thin add-on, not the product's
focus, and there's no clear evidence of survivorship-bias-free or PIT fundamentals design. Best
positioned as a candidate for the live/ongoing price feed complement, not the historical
PIT backbone.

### 6. Financial Modeling Prep (FMP)

Offers a free "Delisted Companies" endpoint that maps which symbols delisted and when — useful as
a reference/identity table — but this is metadata, not full historical financials or prices for
those delisted names, and there's no as-reported/restatement-aware fundamentals structure. Not
sufficient on its own for survivorship-bias-free backtesting; could supplement another vendor's
delisted-ticker identification.

---

## Recommendation (research input, not a decision)

**Sharadar (via sharadar.com direct subscription, Bundle plan)** is the strongest fit against all
five stated requirements simultaneously:

1. Both prices (SEP) and fundamentals (SF1) are genuinely survivorship-bias-free (delisted tickers
   retained) *and* point-in-time (as-reported values keyed to actual SEC filing date via
   `datekey`, with an explicit as-reported vs. restated split) — this combination is not clearly
   matched by any other candidate reviewed.
2. Broad cross-sectional coverage (thousands of tickers, not index-only) fits a ranking strategy.
3. Retail pricing: $29–$69/mo for the full bundle, no long lock-in mentioned, cheaper than
   Norgate's equivalent tier.
4. Delivery (bulk CSV + REST API) is Linux-friendly and drops directly into a Parquet + DuckDB
   pipeline with no OS-specific dependency.
5. Live feed is a known, expected gap (EOD only) — needs a complementary broker/live-data source,
   which the project needs regardless of data vendor for execution.

**Norgate Data** is the strongest runner-up, particularly if a Windows/WSL2 box is acceptable in
the stack and the strategy leans more on price/volume signals than fundamentals — its
survivorship-bias-free price and historical-index-constituent data are best-in-class and
longer-proven than Sharadar's. It should be kept as a fallback or a cross-validation source for
price data specifically.

This report does not close out issue #3 — the final vendor decision (and any trial-account
verification of the claims above, since several details were sourced from vendor marketing pages
and secondary write-ups rather than hands-on testing) is left to the maintainer.

## Sources

- https://sharadar.com/
- https://sharadar.com/subscribe
- https://sharadar.com/docs/fundamentals
- https://data.nasdaq.com/databases/SEP
- https://www.quantrocket.com/sharadar/
- https://resources.quandl.com/a/res-hub/Sharadar_Datasheet_final.pdf
- https://tradevodata.com/blog/lookahead-bias-fundamental-backtests
- https://norgatedata.com/
- https://norgatedata.com/stockmarketpackages.php
- https://norgatedata.com/fundamental-field-definitions.php
- https://alvarezquanttrading.com/blog/norgate-data-review/
- https://pypi.org/project/norgatedata/
- https://www.thetradler.com/how-to-access-norgatedata-library-from-ubuntu
- https://concretumgroup.com/how-to-construct-a-survivorship-bias-free-database-in-norgate-using-python/
- https://polygon.io/knowledge-base/article/what-does-polygon-do-with-delisted-tickers
- https://stonkscapital.substack.com/p/massive-problems-part-1
- https://eodhd.com/pricing
- https://eodhd.com/financial-apis-blog/sp-500-historical-constituents-data
- https://eodhd.com/financial-apis/delisted-stock-companies-data-2
- https://www.tiingo.com/pricing
- https://site.financialmodelingprep.com/how-to/how-to-handle-delisted-companies-and-historical-symbols-with-a-free-api
- https://site.financialmodelingprep.com/developer/docs/survivorship-bias-api
