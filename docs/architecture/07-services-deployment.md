# 07 — Services and Deployment

**Source:** [#10 Define Docker Compose service boundaries for the monorepo](https://github.com/agb1986/agb-trader/issues/10), with the per-service database roles from [#12](https://github.com/agb1986/agb-trader/issues/12)
**Parent:** [#1 AGB Trader — Architecture Spec Map](https://github.com/agb1986/agb-trader/issues/1)
**Status:** Settled design.
**Date:** 2026-09-10, revised 2026-09-15 when T03 added the `backup` service

The stack is split across two machines, decomposed into eight services on the server plus a research profile on the workstation, coupled only through Postgres, and separated by credentials and egress rather than by tidiness.

## Where it runs

The home server is a Celeron N3450: four cores at 1.10 GHz, 7.8 GB RAM with 3.6 GB available, swap already fully consumed, seventeen containers already running (the household media stack among them). Storage is abundant (1.7 TB free on `/media/local`, 17 TB free on `/mnt/data`; the root is a 28 GB eMMC) but CPU and RAM are not. The workstation is a Core Ultra 7 165H with 22 threads, 62 GB RAM and 1.5 TB free.

That asymmetry decides the split. The always-on half — scheduling, the monthly pipeline, order execution, the Telegram bot, the UI, the database — runs on the server, because it must be up when the workstation is not. The research half — purged combinatorial cross-validation, LightGBM training, NautilusTrader backtests, vectorbt scans, and the DSR/PBO/Sharpe validation gates — runs on the workstation as an on-demand profile.

The specific argument is CPCV. It refits the model roughly 120 times per split-set, and the DSR discipline counts every declared trial, so the trial count is expected to grow. An overnight run on the workstation becomes a fortnight on the server, contending with Jellyfin for the same 3.6 GB. The realistic failure mode is not a slow run; it is that the validation discipline gets skipped because running it hurts. Moving research off the server keeps the gates cheap enough to actually apply.

## The eight services

- **`db`** — stock Postgres. Holds jobs, baskets, approval decisions with their actors, the audit history, alerts, expected events, ladder and mode state, and the certified-model registry ([08 — Database schema](08-database-schema.md)). Market data does not live here; it stays Parquet on disk, read with DuckDB.
- **`scheduler`** — owns the calendar. Fires the monthly rebalance and the daily data pull by writing `job` rows, writes `expected_event` rows ahead of time, marks approvals missed at the cutoff, and schedules the nightly backup job, which `backup` runs. Needs an explicit timezone and a real NYSE market calendar, not naive cron syntax: the rebalance is defined against trading sessions and the MOC cutoff, not wall-clock days.
- **`pipeline`** — the Sharadar pull, feature construction, ranker inference, the LLM panel, and the pre-trade gate. Ends by writing a proposed basket (and the shadow basket). Holds the Sharadar and Anthropic credentials.
- **`execution`** — the only service with Alpaca credentials. Polls for approved-and-unsubmitted baskets, submits MOC orders, reconciles fills, takes daily position snapshots, records `broker_state`, and owns the broker-side halt levers (`suspend_trade`, `DELETE /v2/positions`).
- **`bot`** — the only service with the Telegram token ([05 — Telegram bot](05-telegram-bot.md)). Evaluates the health of every other service and sends the alerts.
- **`ui`** — the local UI ([06 — Local UI](06-local-ui.md)). Read-mostly, server-rendered, no broker credentials, no outbound network at all.
- **`vpn`** — agb-trader's own gluetun, with its own WireGuard key.
- **`backup`** — claims the scheduler's nightly backup job and writes a `pg_dump` of the database to a dated file on `/mnt/data`. It connects as its own `backup` role, which reads every table through Postgres's `pg_read_all_data` and writes nothing but its heartbeat and its job's events. It is a service of its own so that no role which writes trading state also holds read access to everything, and so that only this container needs the Postgres client tools.

Plus a **`research`** service behind a profile that only ever runs on the workstation, and an **`agb` CLI** run on the server (inside the `app` image) as its own `cli` database role, for the upward moves: `agb promote`, `agb level`, `agb mode`, and panel configuration.

The boundaries are drawn on credential and egress lines, not on module tidiness. A container cannot leak a credential it was never given, so `execution` is the only place Alpaca keys exist, and `bot` is the only place the Telegram token exists. `ui` holds neither, which is possible because approval state lives in the database with both surfaces as views onto it. Each service connects to Postgres as its own role with `INSERT` and `SELECT` only on history tables; an owner/migration role that no service uses holds the rest.

## Coupling: the database is the only channel

No service calls another. The scheduler writes a job row; the pipeline polls and claims it; the pipeline writes a proposed basket; the bot and the UI write the approval decision; execution polls for approved-and-unsubmitted baskets. Every hand-off is a row, and claims use `FOR UPDATE SKIP LOCKED` leases so two pollers cannot run the same job.

This is a deliberate revision of the more obvious design, where the scheduler launches one-shot containers. The only way to keep the pipeline a `restart: "no"` one-shot job — the shape finance-agent used — is to give the scheduler `/var/run/docker.sock`, which is host root on a box that also runs the household media stack and a container talking to a broker. That trade is not worth making, so the pipeline becomes a long-lived poller instead. Every service in the stack is therefore a long-lived poller, and `restart: unless-stopped` applies throughout.

The database-only rule also means there is no internal HTTP surface to secure, no service discovery, and no partial-failure story between services: a service that is down simply stops claiming rows, and the rows wait.

## Networks

Two networks:

- **`core`** — `db`, `scheduler`, `pipeline`, `execution`, `bot`, `ui`, `backup`.
- **`egress`** — `vpn`, `bot`.

`bot` is the only service on both. This makes the most important constraint structural rather than conventional: **`execution` must never route broker traffic through the VPN.** Submitting Alpaca orders from a foreign VPN exit invites account flags. With two networks, a stray `HTTPS_PROXY` in the wrong env file causes a connection failure rather than silently tunnelling live orders, because `execution` has no route to the proxy at all. torrent-agent already carries the scar of a proxy variable reaching calls that should not have used it; there, `NO_PROXY=127.0.0.1,localhost` is load-bearing.

## The bot's egress

The ISP blocks `api.telegram.org`. On this host the only route to it is through a VPN tunnel.

The stack runs **its own gluetun** rather than borrowing torrent-agent's. The existing gluetun publishes its HTTP proxy on `127.0.0.1:8888` only (deliberately, since an open proxy on the LAN would relay anyone's traffic out through the VPN account), so no container can reach it via the host gateway, and the alternatives were to join a foreign Compose project's network or to widen that binding. Both create a cross-project dependency in which tearing down the torrent stack breaks trading, and the failure surfaces as a Compose error rather than a Telegram one.

A second tunnel costs nothing on the existing Proton plan (ten simultaneous connections, roughly three in use). Additional WireGuard keys are free to generate, and each container needs its own: reusing one key across two live tunnels does not work, and separate keys mean revoking one does not take down the other. agb-trader's tunnel needs **no** port forwarding, since its egress is outbound-only, so `VPN_PORT_FORWARDING` and `PORT_FORWARD_ONLY` are both off — a wider server choice and less to go wrong than torrent-agent's. The real cost is roughly 150–250 MB of RAM on a box with no headroom, which the memory limits account for.

The bot attaches to the tunnel by **proxy environment variables**, not by sharing its network namespace. torrent-agent locks Deluge inside the tunnel with `network_mode: service:gluetun` so it has no interface except the VPN, but the bot cannot do that: sharing the VPN namespace would sever it from `core`, and the database is its only channel. So `vpn` sits on `egress` publishing nothing, and the bot runs with `HTTPS_PROXY=http://vpn:8888` and `NO_PROXY=db,localhost,127.0.0.1` — Telegram out through the tunnel, Postgres direct.

The known weakness is that this is fail-open in one direction: a library that ignores proxy environment variables would bypass the VPN silently. Accepted, because the alternative (splitting the bot into a namespace-locked relay plus a bridge-side logic process) adds a ninth service, a second holder of the Telegram token, and a private protocol between the halves. If the tunnel drops, the proxy refuses the connection and the bot fails loudly, which is the common case, and the pre-flight health message exists to make a dead bot visible a session early.

## Secrets

One env file per service, following torrent-agent's split rather than finance-agent's single root `.env`:

```
env/db.env          POSTGRES_PASSWORD, per-service role passwords
env/pipeline.env    Sharadar, Anthropic
env/execution.env   Alpaca key, secret, endpoint
env/bot.env         Telegram token, HTTPS_PROXY, NO_PROXY
env/vpn.env         WIREGUARD_PRIVATE_KEY
env/research.env    workstation only
```

The file list *is* the credential inventory. Under one shared `.env`, the UI, which needs no credentials and has no outbound network, would still hold the Alpaca keys and the Telegram token in its environment, which is exactly the boundary this decomposition exists to draw. Each file ships with a `.example` template; none is committed.

## Resource limits

Every service declares an explicit memory limit, sized to its job: `pipeline` largest, `ui` and `scheduler` small.

On a box with 3.6 GB available and swap fully consumed, the OOM killer is otherwise the only backstop, and it chooses by score; it does not know that `execution` holding an unsubmitted basket matters more than a transcode. With per-service limits, a runaway monthly feature build dies inside its own cgroup instead of putting the whole host under pressure. The limits double as the written statement of what this stack is allowed to cost on a shared machine. The known cost is that a limit set too tight kills a legitimate run with exit code 137 and no application-level explanation, so **the pipeline's limit is set from a measured run, not guessed**. Concrete numbers are build work.

## Postgres durability

Data on a named volume under the existing docker root on `/media/local`. The scheduler writes a nightly backup job with an expected event, and `backup` runs it: a `pg_dump` to a dated file in the deploy directory on `/mnt/data`, keeping the newest fourteen. The backup is an ordinary scheduled job, so a dump that fails or never runs leaves its expected event unfulfilled and shows in the overdue view like any other missed work. `agb-stack restore` loads a dump into a scratch database and compares it with the live one ([Deploying to the server](../build/deploy.md)). The nightly hash check of a sample of the content-addressed panel bodies on `/mnt/data` joins this job once the panel store exists.

The database is small — baskets, decisions, audit rows, daily snapshots and heartbeats, not market data or panel bodies — so a plain-text dump is cheap, restores with one documented command, and survives a Postgres major-version upgrade in a way a raw volume does not. The exposure is up to a day of history, which for a monthly rebalance is almost always nothing. Continuous archiving was rejected as real Postgres operations (archive commands, replication slots that fill a disk when the workstation is off, a restore nobody rehearses) for a database that gains a handful of rows a month.

## Images

One multi-stage Dockerfile with three targets:

- **`base`** — shared feature definitions, the cost function, and data access.
- **`app`** — what the server runs, as six commands (`scheduler`, `pipeline`, `execution`, `bot`, `ui`, `backup`) plus the `agb` CLI, alongside stock Postgres and stock gluetun.
- **`research`** — workstation only, adding NautilusTrader, vectorbt and the training stack.

Sharing `base` is what keeps the backtest and the live path from drifting: features and costs are defined once and both halves import the same definition. Splitting `app` from `research` keeps hundreds of megabytes of training dependencies off a 28 GB eMMC root that the server never imports. The UI is server-rendered Python within `app` rather than a separate JavaScript build, so the stack has no Node toolchain.

Rejected: running research natively on the workstation with `uv`. Faster to iterate, but it makes the environment that certifies a model unreproducible from the repo alone, which sits badly with a certification that records the exact training window, feature list and trial count a model passed on.

## Image delivery

Built on the workstation and shipped over SSH: `docker build`, then `docker save | ssh … docker load`, tagged with the commit. `make deploy` and `make rollback` run [scripts/deploy](../../scripts/deploy/); [Deploying to the server](../build/deploy.md) is the runbook.

Building on the server means a long build on four 1.1 GHz cores while seventeen containers compete for 3.6 GB; the build itself would be the largest memory spike the box ever sees. A registry would give immutable digests and real rollback, but it puts a credential on the server and sends the image to a third party, for a one-person stack on one LAN. Rollback is handled by keeping the previous tag on the server.

## Observability

Self-contained: **the database is the health surface.**

Every service upserts a `heartbeat` row each cycle, and the scheduler writes `expected_event` rows ahead of time (the data pull due at T, the proposal due at T, the approval request due at T, submission due by T). An overdue view flags any expected event past its time with no completion, and flags when no rebalance is expected within the next cycle's horizon, which is how a dead scheduler shows up: it stops writing future rows. A month without a rebalance is quiet only if nothing was expected. This view drives `/status`, the pre-flight message and the UI banner. The bot evaluates the health of every other service; the UI evaluates everything, the bot included.

No exporters, no log shipping, no sidecars. Shipping logs to the host's existing victorialogs would reintroduce exactly the cross-project dependency rejected for the VPN, and would flow logs from the service holding broker credentials into a shared store. A Prometheus-and-Grafana path would add an HTTP surface to every service that otherwise needs none, and would create a second place where system state is displayed, competing with the UI whose entire purpose is being that place. Container logs stay container logs.

## The lake and the model hand-off

Two machines hold state, so ownership is explicit.

**The server owns the Parquet lake** and performs the only vendor pull, bind-mounted from `/mnt/data`. The workstation `rsync`s the lake down over the LAN before a research run, so the backtest reads exactly the bytes live reads. Independent pulls on both machines would diverge on a Sharadar restatement or a failed fetch with nothing comparing them, producing a backtest that cannot be reproduced against live behaviour: a correctness property, not a convenience.

**The model travels back** as a versioned artifact directory (containing the hashed `trials.parquet` ledger) plus an immutable `model_certification` row. The live pipeline refuses to load a model that has no certified row, a voided one, or one whose declared trial count differs from the ledger. Promotion to live is `agb promote <model_id>` on the server. This is what makes the validation gates enforceable across the machine boundary rather than a discipline someone remembers to follow.

**The panel store** — prompt and response bodies and source filing texts — is content-addressed zstd on `/mnt/data`, referenced by sha256 from Postgres. Its backup follows whatever protects the lake, which is left to the build.

## What this does not settle

Concrete memory limits per service await a measured pipeline run. Poll intervals and lease durations are tuned against measured runs alongside the schema build.
