# Operator setup outside the repo

This is everything the operator has to do by hand, outside development: accounts, credentials, server preparation, and the attended steps during the build. It expands the `PRE` tasks in the [Orca build plan](orca-build-plan.md#operator-prerequisites-and-live-checks).

No credential is needed to build code. Workers build against fakes, fixtures and the committed `env/*.example` files. A credential only gates the **merge** of the ticket whose live check needs it, so each item below has a "needed by" wave. Nothing is needed before T01.

## Handling secrets

- Until the `env/` folder exists (T01 creates it with `.example` templates), keep every key in a password manager and nowhere else.
- Never paste a secret into chat, GitHub, Orca or a commit. Only `env/*.example` files are committed; real env files are git-ignored.
- Put each secret only in the env file the spec assigns it to, on the machine where it is used, and make that file readable only by you (`chmod 600`).
- Each service reads only its own env file. The separation is a safety property. For example, `execution` must never see a proxy variable, so the Alpaca key lives in `env/execution.env` and nowhere else.

Env file layout, from [07 — Services and deployment](../architecture/07-services-deployment.md):


| File                | Holds                                                 | Machine                                            |
| ------------------- | ----------------------------------------------------- | -------------------------------------------------- |
| `env/db.env`        | `POSTGRES_PASSWORD`, per-service role passwords       | server                                             |
| `env/pipeline.env`  | Sharadar key, Anthropic key                           | server (and the workstation for early live checks) |
| `env/execution.env` | Alpaca key, secret, endpoint                          | server (and the workstation for early live checks) |
| `env/bot.env`       | Telegram token and chat id, `HTTPS_PROXY`, `NO_PROXY` | server                                             |
| `env/vpn.env`       | `WIREGUARD_PRIVATE_KEY`                               | server                                             |
| `env/research.env`  | research settings                                     | workstation only                                   |


The exact variable names are fixed by the `.example` files once the tickets that introduce them merge. The coordinator says which file and variable each merge needs.

## Checklist by when it is needed


| Item                          | Orca task       | Gates                     | Needed by |
| ----------------------------- | --------------- | ------------------------- | --------- |
| Server access and storage     | `PRE server`    | MERGE T03                 | wave 3    |
| Sharadar subscription         | `PRE sharadar`  | MERGE T04                 | wave 3    |
| Telegram bot                  | `PRE telegram`  | MERGE T11                 | wave 3    |
| ProtonVPN WireGuard key       | `PRE vpn`       | MERGE T11                 | wave 3    |
| Anthropic API key             | `PRE anthropic` | MERGE T23                 | wave 6    |
| SEC EDGAR contact string      | none            | T23's retrieval           | wave 6    |
| Alpaca paper account          | `PRE alpaca`    | MERGE T09                 | wave 7    |
| Database passwords            | none            | first server deploy (T03) | wave 3    |


Waves are a planning view, not a schedule; see [Waves](orca-build-plan.md#waves). Doing everything in the first three rows early means the build never stalls on a key.

### 1. Server access and storage

Needed for T03 (server delivery). Images are built on the workstation and shipped with `docker save | ssh … docker load`, so the workstation has to reach the server non-interactively.

- [x] Key-based SSH from the workstation to the server works without a password prompt.
- [x] Your server user can run `docker` and `docker compose` without `sudo`.
- [x] A directory for agb-trader exists on `/mnt/data` and is writable by that user. It will hold the Parquet lake, the nightly `pg_dump` backups and the content-addressed panel store. T03 fixes the exact layout inside it.
- [ ] Note the free RAM (`free -h`) before the T03 live check. The server had 3.6 GB available with swap fully used at design time, and agb-trader adds about seven containers, including a second gluetun at 150–250 MB.

### 2. Sharadar subscription

Needed for T04 (Sharadar lake).

- [x] Subscribe directly at [sharadar.com](https://sharadar.com/subscribe) to the **Bundle** plan at **Full History**. T04 needs **SEP** (daily prices), **SF1** (fundamentals) and **TICKERS** from 1998, with delisted tickers included; without the delisted tickers the backtest has survivorship bias. The 5-year and 10-year tiers are too short. SEP is in the Prices plan and SF1 in the Fundamentals plan, and buying both separately at Full History costs more than the Bundle. When this was written the Bundle was $69 a month or $499 a year, so pay monthly until the backfill has been checked, then switch to annual.
- [x] Check that the licence covers personal, non-professional use, and note the renewal date.
- [x] Copy the API key from your sharadar.com account profile into the password manager. It goes in `env/pipeline.env`. It is a sharadar.com key, not a Nasdaq Data Link key, and sharing it can get access revoked.

The direct API is `https://api.sharadar.com/v1.0/data/<table>`, with the key passed as `api_key`. It accepts the Nasdaq Data Link table names (`sep`, `sf1`, `tickers`) as well as its own (`stocks`, `fundamentals`, `tickers`). Two things differ from the Nasdaq Data Link version: SF1's SEC filing date is called `date`, not `datekey`, and the `table` column in TICKERS says `stocks` rather than `SEP`. A query returns at most 10,000 rows, so the backfill uses the bulk download instead: adding `years=full` to a table request redirects to a zipped CSV of the whole table. The public key `test-api-key` returns AAPL only, which is enough to exercise the pull without a subscription.

After T03 and T04 have both merged, run the **full backfill from 1998** onto the server's `/mnt/data`. It must finish before the T17 live check (wave 6), which syncs the lake from the server. T04's own live check only pulls a small date range.

### 3. Telegram bot

Needed for T11 (Telegram bot).

- [x] In Telegram, message **@BotFather**, send `/newbot`, and choose a name and a username. Save the token it returns.
- [x] Open the bot's chat (`t.me/<bot username>`) and press **Start**. Then fetch `https://api.telegram.org/bot<token>/getUpdates` with `curl` rather than a browser, so the token stays out of your browser history. The chat id is the number at `result[].message.chat.id`, which appears in the response as `"chat":{"id":…}`; for a private chat it equals your own user id. Save it next to the token. If `result` is empty, send the bot a fresh message and fetch again, because Telegram keeps updates for only about 24 hours.
- [x] Optional: in @BotFather, turn off `/setjoingroups` so the bot cannot be added to groups.

Both go in `env/bot.env` on the server. The bot's Telegram traffic leaves through agb-trader's own VPN tunnel, so T11's live check also needs the next item.

### 4. ProtonVPN WireGuard key

Needed for T11. This is a **new** key for agb-trader, separate from torrent-agent's. Two live tunnels cannot share one key, and separate keys mean revoking one does not take the other down. The existing Proton plan allows ten connections, and roughly three are in use.

- [x] In your Proton account, go to the WireGuard configuration downloads and create a new configuration. Name it `agb-trader`.
- [x] Turn **off** NAT-PMP (port forwarding). agb-trader's egress is outbound only.
- [x] Copy the `PrivateKey` value from the generated configuration. Proton shows it only once, so put it straight into the password manager.

It goes in `env/vpn.env` on the server as `WIREGUARD_PRIVATE_KEY`. The rest of the configuration file is not needed.

### 5. Anthropic API key

Needed for T23 (panel retrieval).

- [x] In the Anthropic Console, create a workspace for agb-trader and an API key inside it.
- [x] Set a monthly spend limit on that workspace. The panel runs Haiku 4.5 as the Analyst, Sonnet 5 as the Prosecutor and Defender, and Opus 5 as the Judge. The design estimate is about 1M Analyst input tokens plus about 450k for the other three roles per month; see [03 — LLM panel](../architecture/03-llm-panel.md).

The key goes in `env/pipeline.env`.

### 6. SEC EDGAR contact string

Needed for T23. This is not a credential. The SEC's fair-access policy requires every automated request to declare a User-Agent with a name and a contact email, and limits clients to ten requests a second.

- [x] Decide the name and email to declare, for example `agb-trader Your Name you@example.com`. T23 defines the setting that holds it.

### 7. Alpaca paper account

Needed for T09 (paper market-on-close orders).

- [x] Open an Alpaca account and switch to the paper trading dashboard.
- [x] Set the paper balance to the capital you expect to trade live, and keep it below **$100,000**. That is the capital the model is certified at (twice the largest book expected in the first year), and the certified-capital alert fires when the book's value crosses it, so a paper balance at or above $100k would alert almost at once. See [01 — Data, broker and backtesting](../architecture/01-data-broker-backtesting.md).
- [x] Generate a paper API key and secret. The endpoint is `https://paper-api.alpaca.markets`.

All three go in `env/execution.env`. This traffic never goes through the VPN; submitting orders from a foreign VPN exit invites account flags.

The T09 live check needs a trading day. The market-on-close order has to be submitted before 15:45 ET, which is 20:45 in the UK for most of the year and 19:45 during the few weeks when US and UK daylight saving are out of step.

### 8. Database passwords

Needed for the first server deploy in T03.

- [ ] Once T01 has merged, read `env/db.env.example` to see which roles need passwords.
- [ ] Generate a strong random password for each, for example with `openssl rand -base64 32`, and store them in the password manager before writing them into `env/db.env` on the server.

## During the build

- **Approve merges.** The coordinator merges nothing without your explicit approval.
- **Attend the server live checks** for T03, T11 and T17. The coordinator runs them while you watch.
- **Run the full Sharadar backfill** once T03 and T04 have merged (see item 2).
- **Be available around a US close** for the T09 live check (see item 7).
- **Confirm each PRE task** to the coordinator once its item is done, so it can mark the task completed in Orca. Say that it is done; never send the secret itself.

## Housekeeping

- Archive the old [finance-agent](https://github.com/agb1986/finance-agent) repository once you are happy nothing more is needed from it. agb-trader ports no code from it.

