# Event-Driven Backtesting Engine Selection

Research for GitHub issue #4 (child of #1, "AGB Trader — Architecture Spec Map").
Status: research only — no decision recorded here. This is input for the
architecture decision, not the decision itself.

## Context

The project's grounding research report recommends a two-stage backtesting
workflow:

1. **vectorbt** — fast, vectorized scanning/screening of candidate ideas
   across large universes and parameter grids.
2. **An event-driven engine** — rigorous re-validation of survivors with
   realistic order execution, slippage, partial fills, look-ahead-bias
   controls, walk-forward analysis, and support for Combinatorial Purged
   Cross-Validation (CPCV)-style path reconstruction.

This document evaluates the three named candidates for stage 2 —
**Zipline-reloaded**, **NautilusTrader**, and **QuantConnect LEAN**
(self-hosted, open-source engine, not the cloud platform) — plus other
credible alternatives, against agb-trader's actual constraints:

- Dockerized services, Docker Compose monorepo.
- Classical ML **cross-sectional ranking** strategy (LightGBM/XGBoost style
  — rank a universe of equities each period, not a single-asset or pairs
  strategy).
- Python-heavy stack (pandas, LightGBM, PyTorch-adjacent tooling).
- Eventual path to live paper/live execution via a broker API (broker not
  yet chosen — separate ticket).

## Candidates evaluated

### Zipline-reloaded (stefan-jansen/zipline-reloaded)

- **Maintenance status**: This is the community fork of the original
  Quantopian `zipline`, which itself has been dead upstream since Quantopian
  shut down in late 2020. Zipline-reloaded is maintained by Stefan Jansen
  (author of *Machine Learning for Algorithmic Trading*), currently around
  1.9k stars / 331 forks, 6,600+ commits. Recent releases (3.0 for pandas
  2.0+, 3.05 for NumPy 2.0 compat) show it is kept alive and dependency-
  compatible, but development reads as **maintenance mode**: keeping the
  library installable and current, not adding major new features. Multiple
  2026 sources describe it as "mature but slowing" / community-maintained
  rather than actively evolving.
- **Cross-sectional support**: This is Zipline's strongest point. The
  **Pipeline API** was purpose-built for exactly this project's use case —
  define a dynamic universe, compute cross-sectional factors on it in a
  vectorized way every day, rank, and select (e.g., top/bottom decile).
  Multiple 2026 comparative sources single out zipline-reloaded's Pipeline
  API as the best-in-class abstraction for daily-bar cross-sectional equity
  factor research with dynamic universe selection.
- **Execution realism**: Medium. It has a commission/slippage model
  (`slippage.VolumeShareSlippage`, fixed/per-share/per-dollar commission
  models) sufficient for daily-bar equity work, but it is **not designed for
  realistic intraday execution simulation** — no order book, no
  latency/partial-fill modelling comparable to NautilusTrader or LEAN.
- **Python-native**: Yes, fully.
- **Dockerizing**: Straightforward in principle (pure Python + a bundle data
  store), but the dependency stack (older pandas/numpy pins historically,
  C-extension bundle ingestion) has a reputation for being fiddly to pin
  exactly; no official Docker image is maintained by the project.
  Community images cover this, but you own the Dockerfile.
- **Live-trading bridge**: None built-in. Live trading requires third-party
  add-ons (historically "zipline-live", not well maintained today). This is
  fine per this ticket's framing (broker adapter will be a separate custom
  service) but there is no native live path to fall back on.
- **Community/docs**: Decent, mostly sustained by Stefan Jansen's book
  audience; GitHub discussions are active but the core team is effectively
  one maintainer.

### NautilusTrader (nautechsystems/nautilus_trader)

- **Maintenance status**: Actively maintained and growing. ~22.8k stars,
  137 contributors. The project targets roughly bi-weekly releases; 2026
  releases occurred in April, August (final 1.x release, marking the
  legacy Cython v1 core's sunset), and September. It is mid-transition to a
  v2 Rust + PyO3 runtime (release-candidate stage as of this research).
  This is the most actively engineered of the three by release cadence and
  contributor count.
- **Cross-sectional support**: Capable but not purpose-built the way
  Zipline's Pipeline is. NautilusTrader is strategy-and-instrument centric
  (a `Strategy` subscribes to and manages many instruments); a
  cross-sectional ranking strategy is straightforward to implement — score
  the whole subscribed universe each bar/day inside the strategy's own
  Python logic and route target-weight orders — but there is no built-in
  "define universe, compute factor cross-sectionally, rank" abstraction to
  lean on. This means more of the cross-sectional/rebalancing logic has to
  be hand-rolled in the strategy layer (portfolio construction, target
  weights, rebalance cadence) compared to Zipline or LEAN's framework.
- **Execution realism**: Highest of the three, by design. Nanosecond-
  resolution, event-driven simulation with a configurable `FillModel`
  (`prob_fill_on_limit`, `prob_fill_on_stop`, `prob_slippage`), a
  `MakerTakerFeeModel` for realistic commission/fee simulation, and
  order-book/tick-level matching engines. The project's stated design goal
  is explicitly to close the "implementation gap" between backtest and
  live — the same execution semantics and time model apply in both modes.
  An open GitHub issue (#2194, "Enhanced order-fill simulation") shows
  active work to push fill-simulation realism further (liquidity
  consumption, partial limit fills).
- **Python-native**: Python is the control/strategy-authoring layer; the
  core engine is Rust (Cython historically, migrating to Rust+PyO3). This
  is not "pure Python" but the authoring surface for strategies is pure
  Python and installs as a normal `pip`-installable wheel — no separate
  runtime/VM to bridge to (unlike LEAN's C#).
- **Dockerizing**: Good fit. Official Docker images are published
  (`ghcr.io/nautechsystems/...`), including a headless JupyterLab image for
  research use, and the repo ships an example `docker-compose.yml` (with
  Postgres) under `.docker/` for bootstrapping a working environment —
  explicitly flagged as a dev-environment starting point, not a hardened
  production compose file, so the production Compose service would need to
  be built out rather than reused as-is. Running it headless as a batch
  backtest job or a resident live-trading service is a well-trodden path
  for this project.
- **Live-trading bridge**: Native, and this is a real differentiator.
  NautilusTrader ships modular execution/data adapters for many venues
  (crypto exchanges, Interactive Brokers, etc.) and is architected so the
  *same* strategy code runs unchanged in backtest and live — it's a
  production trading system, not just a backtester with a live bolt-on.
  Since this project has an eventual live/paper execution goal, this
  reduces future migration risk even though the ticket frames the broker
  adapter as a separate custom service.
- **Community/docs**: Active and improving — structured docs (Getting
  Started / Concepts / How-To / Tutorials / Integrations / Developer
  Guide), active GitHub Discussions and issue triage. Steeper learning
  curve than Zipline given the Rust core and lower-level execution/venue
  concepts exposed in configuration.

### QuantConnect LEAN (open-source engine, self-hosted)

- **Maintenance status**: Actively maintained, backed by QuantConnect
  the company. ~17,900–19,900 stars (varies by mirror/measurement), 180–229
  contributors, multiple commits per week. Caveat: contribution
  concentration is high — a small number of contributors/one organization
  account for the majority of commits, i.e., this is a commercially-backed
  open-source project rather than a broad community-governed one. That's a
  reasonable trade for maintenance velocity but worth naming.
- **Cross-sectional support**: Excellent, via the built-in **Algorithm
  Framework**: Universe Selection → Alpha (emits `Insight` objects, which
  can rank/score across the universe) → Portfolio Construction (turns
  insights into target weights) → Execution → Risk. This pipeline is
  explicitly designed for exactly the "rank N assets, take a cross-
  sectional position" pattern (example: `SykesShortMicroCapAlpha` ranks
  intraday % movers). This is comparable in fitness to Zipline's Pipeline
  API for this project's LightGBM ranking use case, arguably more complete
  since it also gives you the portfolio-construction and execution stages
  as pluggable, swappable modules.
- **Execution realism**: Very high. Robust portfolio accounting (margin,
  leverage, buying-power checks) across asset classes, configurable
  slippage/fee/fill models, and the same engine underlies QuantConnect's
  live and cloud offering — i.e., battle-tested against real broker
  behavior, not just a backtest-only approximation.
- **Python-native**: Partial. The LEAN engine core is **C#**; Python
  algorithms run via a Python.Net bridge into the C# runtime. This works
  well in practice (QuantConnect's own users write almost entirely in
  Python) but it is not a pure-Python stack — expect a heavier, more
  complex local toolchain (.NET runtime + Python interop) than
  NautilusTrader or Zipline, and occasional friction points (debugging
  across the interop boundary, dependency management spanning both
  ecosystems).
- **Dockerizing**: Good, and arguably the most turnkey here — `pip install
  lean` (the LEAN CLI) plus the official `quantconnect/lean` Docker image
  is designed specifically for consistent, container-based local/self-
  hosted runs, and is documented for that purpose (unlike NautilusTrader's
  explicitly dev-only compose example).
- **Live-trading bridge**: Native and mature — LEAN has first-class,
  actively maintained broker integrations (Interactive Brokers, Tradier,
  Alpaca, etc.) used in production by QuantConnect's live/cloud platform,
  so self-hosted live trading is a supported, well-worn path, not a
  theoretical one.
- **Data/licensing consideration**: LEAN is designed around QuantConnect's
  data ecosystem. Self-hosting is fully possible and open-source, but
  wiring in your own Parquet/DuckDB-backed data (rather than QuantConnect's
  data formats/subscriptions) requires writing a custom `IDataProvider` /
  data feed adapter in C#, which is more integration work than Zipline's
  bundle-ingestion or NautilusTrader's Python-native custom data adapters.
- **Community/docs**: Very strong documentation (QuantConnect maintains
  extensive docs, an active forum, and inline framework examples), but
  much of the best documentation and support pathway assumes usage of
  the QuantConnect cloud platform even when running the OSS engine
  locally.

## Other alternatives considered

- **backtesting.py**: Single-asset/simple vectorized-ish loop, minimal
  execution modelling, best for teaching or quick single-strategy sketches.
  Not viable for a multi-asset cross-sectional ranking strategy.
- **bt**: Clean portfolio allocation/rebalancing engine (weight-based),
  good for ETF-basket/rotation strategies, but optimizes for portfolio
  construction rather than execution timing — explicitly not designed for
  realistic intraday/order-level execution simulation. Not a substitute for
  a true event-driven validation stage.
- **Backtrader**: Widely used historically, but multiple independent 2026
  sources describe it as a legacy framework with no significant recent
  commits/releases and slower runtime — an at-risk choice for a project
  starting fresh in 2026. Not recommended.
- **vectorbt (PRO) for the validation stage too**: vectorbt PRO has added
  probabilistic-fill features and "Signal Contexts" that partially bridge
  vectorized and event-driven semantics, but by its own vectorized nature
  it still assumes cleaner fills than a true event-driven order-matching
  engine, and it's a commercial product for the PRO tier. It remains the
  right tool for stage 1 (screening), not a substitute for stage 2.
- **cvxportfolio**: Portfolio-optimization-focused backtester (good for
  the "how do I turn ranks into weights under constraints" problem), not a
  general event-driven execution simulator. Worth keeping in mind for the
  portfolio-construction layer regardless of which execution engine is
  chosen, but out of scope as a candidate for this ticket.

## Comparison summary

| Criterion | Zipline-reloaded | NautilusTrader | QuantConnect LEAN (OSS) |
|---|---|---|---|
| Actively maintained (2026) | Maintenance mode, single lead maintainer | Yes — active, ~bi-weekly releases | Yes — active, commercially backed |
| Cross-sectional/multi-asset fit | Excellent (Pipeline API, purpose-built) | Capable, more hand-rolled | Excellent (Alpha/Portfolio Construction framework) |
| Execution realism | Medium (daily-bar oriented) | Highest (tick/order-book, probabilistic fills) | Very high (production-parity accounting) |
| Python-native | Fully | Python authoring layer, Rust core | Python via interop with C# core |
| Dockerizing / headless | Workable, no official image | Good — official images, dev-mode compose example | Good — CLI + official Docker image, turnkey |
| Native live-trading bridge | None (third-party only) | Yes — production-parity, same code path | Yes — mature broker integrations |
| Community/docs | Decent, one-maintainer risk | Active, steeper learning curve | Very strong docs, cloud-platform-centric |
| Data integration (Parquet/DuckDB) | Custom bundle ingestion (Python) | Native Python custom data adapters | Requires C# `IDataProvider` adapter |

## Recommendation (research input, not a decision)

**NautilusTrader** is the strongest fit for agb-trader's stage-2 validation
engine, on balance of this project's specific constraints:

1. It has the **highest execution realism** of the three, which is the
   entire point of stage 2 (the report's stated goal is realistic order
   execution, slippage, and fill modelling that vectorbt cannot provide).
2. It is **Python-native at the authoring layer** and installs as a normal
   Python package — no separate language runtime to bridge to, unlike
   LEAN's C# core. This matters for a team whose stack is otherwise
   pandas/LightGBM/PyTorch-adjacent.
3. It **Dockerizes cleanly** with official images and is designed to run
   headless as a service — a natural fit for a Docker Compose monorepo.
4. It has a **native live/paper trading path** with the same strategy code
   used in backtest and live. Given this project's stated eventual goal of
   live/paper execution, this meaningfully reduces the risk of a second
   migration later, even though the current plan is a separate custom
   broker-adapter service — NautilusTrader's adapters could subsume or
   simplify that work if the chosen broker has (or gets) a Nautilus
   adapter.
5. It is the **most actively engineered** of the three by release cadence,
   contributor count, and issue-triage activity, which matters for a
   multi-year architecture bet made in 2026.

The main cost is that NautilusTrader's cross-sectional ranking support is
less turnkey than Zipline's Pipeline API or LEAN's Alpha/Portfolio-
Construction framework — the "rank the universe, take top/bottom decile"
logic will need to be hand-rolled in the strategy layer rather than
leaning on a built-in abstraction. This is a one-time implementation cost,
not a structural limitation, and is mitigated by the fact that vectorbt
(stage 1) is already doing the heavy cross-sectional research/ranking
iteration — stage 2 mainly needs to replay a settled ranking/rebalancing
rule with realistic execution, not re-derive the ranking logic itself.

**QuantConnect LEAN** is a credible, well-documented runner-up, and would
be the better choice if the team ends up wanting the built-in
Alpha/Portfolio-Construction framework more than execution-engine
flexibility, or if a chosen broker has first-class QuantConnect/LEAN
integration. Its C#-core/Python-interop nature and heavier local toolchain
are the main friction points against an otherwise-pure-Python stack.

**Zipline-reloaded** is not recommended as the stage-2 validation engine
given its medium execution realism and lack of a live-trading path — but
its Pipeline API remains worth keeping in mind as a reference design (or
even a light dependency) for stage-1 cross-sectional research ergonomics,
separate from this ticket's stage-2 decision.

This is research input only. The actual engine selection should be
finalized as a formal architecture decision on issue #4 / map issue #1,
weighing this report against team preference and any constraints from the
broker-selection ticket.

## Sources

- https://github.com/stefan-jansen/zipline-reloaded
- https://github.com/stefan-jansen/zipline-reloaded/releases
- https://github.com/stefan-jansen/zipline-reloaded/issues
- https://nautilustrader.io/docs/latest/
- https://nautilustrader.io/docs/latest/concepts/backtesting/
- https://nautilustrader.io/docs/nightly/concepts/execution/
- https://nautilustrader.io/docs/nightly/getting_started/backtest_high_level/
- https://nautilustrader.io/docs/nightly/developer_guide/environment_setup/
- https://github.com/nautechsystems/nautilus_trader
- https://github.com/nautechsystems/nautilus_trader/releases
- https://github.com/nautechsystems/nautilus_trader/issues/2194
- https://github.com/nautechsystems/nautilus_trader/blob/develop/RELEASES.md
- https://www.quantconnect.com/docs/v2/lean-engine/getting-started
- https://www.quantconnect.com/docs/v2/writing-algorithms/algorithm-framework/overview
- https://www.quantconnect.com/docs/v2/writing-algorithms/algorithm-framework/alpha/key-concepts
- https://www.quantconnect.com/docs/v2/writing-algorithms/universes/equity/legacy-fundamental-universes
- https://github.com/QuantConnect/Lean
- https://github.com/QuantConnect/Lean/graphs/contributors
- https://www.star-history.com/quantconnect/lean/
- https://hub.docker.com/r/quantconnect/lean
- https://python.financial/ ("The Python Backtesting Landscape (2026): Choosing the Right Framework")
- https://blog.quantinsti.com/cross-validation-embargo-purging-combinatorial/
- https://skfolio.org/generated/skfolio.model_selection.CombinatorialPurgedCV.html
- https://github.com/eslazarev/purged-cross-validation
