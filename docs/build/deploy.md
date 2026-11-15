# Deploying to the server

The server runs the stack from images built on the workstation. `make deploy` builds the
image for the current commit, ships it to the server over SSH with no registry, and restarts
the server's stack on it. The server keeps the release before, and `make rollback` returns to
it. The database is dumped every night into the deploy directory.

The scripts are in [scripts/deploy](../../scripts/deploy/). The design and its reasons are in
[07 — Services and deployment](../architecture/07-services-deployment.md).

## The deploy directory

Everything on the server lives in one directory, `/mnt/data/AppData/agb-trader` by default:

| Path               | Holds                                                                  |
| ------------------ | ---------------------------------------------------------------------- |
| `env/`             | The real env files. They exist only here, and nothing copies them off. |
| `backups/`         | The nightly dumps, `agb-YYYY-MM-DD.sql.gz`.                            |
| `lake/`            | The Parquet lake: the vendor's data in `sep/`, `sf1/` and `tickers/`.  |
| `models/`          | Certified model artifacts, one `<model id>/` each. See below.          |
| `releases/<tag>/`  | `compose.yaml` and `agb-stack` as of commit `<tag>`.                   |
| `current`          | A link to the release that is running.                                 |
| `previous`         | A link to the release it replaced.                                     |
| `agb-stack`        | A link to `current/agb-stack`, the command for everything below.       |

A tag is the commit's 12-character short hash, and the image is `agb-trader-app:<tag>`. The
server keeps only the current and previous releases and their images. Each deploy removes the
rest. The database lives on the `agb-trader_pgdata` Docker volume, not in this directory.

Run `agb-stack` from the deploy directory as `./agb-stack`, or by its full path.

## Before the first deploy

- Key-based SSH from the workstation to the server works without a prompt, and the server
  user can run `docker compose` without `sudo`. See
  [Operator setup](operator-setup.md#1-server-access-and-storage).
- `env/db.env` exists in the deploy directory and sets every variable that
  `env/db.env.example` sets, including `BACKUP_DB_PASSWORD`. Generate each password on the
  server, for example with `openssl rand -hex 24`, and keep a copy in the password manager.
  Make the file readable only by you (`chmod 600`).
- `env/pipeline.env` exists in the deploy directory and sets every variable that
  `env/pipeline.env.example` sets. `SHARADAR_API_KEY` is the vendor's API key, and this file
  is the only place it is kept. `ANTHROPIC_API_KEY` may stay empty until a service uses it.
  Make the file readable only by you (`chmod 600`).
- `env/bot.env` sets every variable `env/bot.env.example` does: the bot's token and the id of
  the one chat it answers (see
  [Operator setup](operator-setup.md#3-telegram-bot)). Keep `HTTPS_PROXY` and `NO_PROXY` as
  the example has them, so Telegram traffic goes through the VPN and database traffic does
  not. No other service reads this file.
- `env/vpn.env` sets every variable `env/vpn.env.example` does, with the WireGuard private
  key generated for this stack alone (see
  [Operator setup](operator-setup.md#4-protonvpn-wireguard-key)).

The other env files join the list as the services that read them are deployed.

`vpn` and `bot` are in the `telegram` Compose profile. `agb-stack` enables it on the server,
so `./agb-stack logs bot` and every other command see them. `make up` on the workstation
leaves them out, because only one process may poll a bot token. To check the bot's route to
Telegram without sending a message, run `./agb-stack exec bot python -m agb.telegram`.

## Deploying

```sh
make deploy
```

That runs `scripts/deploy/deploy.sh`, which:

1. Stops if the working tree has uncommitted changes, since the image is built from the
   commit and not from the working tree.
2. Checks that the deploy directory exists and that each server env file sets every variable
   its example does. The variable names are read on the server, so no value leaves it. A
   missing file or variable stops the deploy before anything is built.
3. Builds `agb-trader-app:<tag>` from `git archive HEAD`, for `linux/amd64`, with the tag as
   `AGB_VERSION`. Each service reports that version with its heartbeat.
4. Ships the image with `docker save | gzip | ssh … docker load`, unless the server already
   has it.
5. Installs the release: the commit's `compose.yaml` and `agb-stack` go into
   `releases/<tag>/`.
6. Runs `agb-stack activate <tag>` on the server. That moves `previous` to the release that
   was running and `current` to the new one, then runs `docker compose up --wait`. `migrate`
   applies any new migrations before the services start. `--wait` returns only once every
   service reports healthy, and each app service is healthy only while its own heartbeat is
   younger than five minutes, so a service that crash-loops or never beats fails the deploy
   instead of passing it. Once the stack is up, it prunes old releases and images.
7. Prints `agb-stack status`: each service's last heartbeat and the version it runs. It exits
   1, and so fails the deploy, if any service has never beaten or last beat more than five
   minutes ago.

`AGB_DEPLOY_HOST` (default `casaos.local`) and `AGB_DEPLOY_DIR` (default
`/mnt/data/AppData/agb-trader`) point it elsewhere:

```sh
AGB_DEPLOY_HOST=other.local make deploy
```

Deploying the commit that is already running restarts nothing that has not changed.

If the stack does not come up, the deploy stops with an error. `current` already points at the
new release, so `make rollback` returns to the one that was running. A rollback ends with the
same `status`, so it too fails while a service is stale.

## Rolling back

```sh
make rollback
```

This runs `agb-stack rollback` on the server, which activates the `previous` release. The
release it leaves becomes `previous`, so running `make rollback` again returns to it. To go back
further, deploy an older commit: check it out and run `make deploy`.

A rollback does not undo migrations. It does not need to: migrations only ever add, so the
older image runs against the newer schema, and `migrate` ignores ledger entries it has no file
for. See [migrations/README.md](../../migrations/README.md).

## Looking at the stack

On the server, in the deploy directory:

```sh
./agb-stack status                 # each service's last heartbeat and its age; exits 1 if one is stale
./agb-stack tags                   # the current and previous release
./agb-stack ps                     # the containers
./agb-stack logs --follow backup   # any other command goes to docker compose
./agb-stack agb <command>          # run an agb CLI command
```

`status` names on stderr each service that has never beaten or last beat longer ago than
`AGB_STALE_AFTER_SECONDS` (default 300), and exits 1 if there is one. On the server every
service is expected, the bot included. `AGB_EXPECTED_SERVICES`, a list of service names, narrows
that: `make status` on the workstation sets it to every service but the bot unless
`COMPOSE_PROFILES` includes `telegram`.

`./agb-stack ps` shows each container's health. A long-running app service runs
`python -m agb.heartbeat` every 30 seconds, which fails once the service's own heartbeat is
stale; three failures in a row mark it unhealthy. The UI's check also sends its own server a
`HEAD http://127.0.0.1:8080/`, because that server runs in a thread beside the heartbeat loop
and could stop answering while the loop still beats. The request is addressed to the first name
in `AGB_UI_ALLOWED_HOSTS` when that is set, so replacing the list does not fail the check. Docker does not restart an unhealthy
container, so an unhealthy service is a signal to read its logs, not something that heals itself.

## The local UI

The `ui` service publishes on the server's loopback address only, so it is reached from the
workstation over an SSH tunnel:

```sh
ssh -N -L 8080:127.0.0.1:8080 casaos.local   # then open http://127.0.0.1:8080
```

SSH is the authentication: there is no login page, and nothing outside the server can open the
port. The home page is the decision log, one row per rebalance with the health banner above it;
`Rebalance`, `Positions`, `Gate` and `Alerts` are the views behind it. A page is read on a
connection that is read-only in Postgres; approving or rejecting a basket and the kill switch are
forms that post to the UI's write routes, the same answers Telegram gives.

Set `AGB_UI_PORT` in the deploy directory's `.env` if 8080 is taken on the server; the container
still listens on 8080, so the tunnel's right-hand side does not change.

Open the UI as `http://127.0.0.1:8080` or `http://localhost:8080`, so keep the tunnel's left-hand
side on local port 8080. The UI answers only requests addressed to those two names and refuses
any other `Host` with a 421. This is what stops a page on another site that rebinds its own name
to the loopback address. A write must also send an `Origin` naming one of them, so a POST from
curl needs `-H 'Origin: http://127.0.0.1:8080'`. The `AGB_UI_ALLOWED_HOSTS` variable
(comma-separated `host:port`), set in the deploy directory's `.env`, replaces the list.

## Backups

The scheduler plans a backup job for every calendar day. The job becomes claimable at 03:00
New York time, and its expected event is due two hours later. The `backup` service claims the
job, runs `pg_dump` as the `backup` role, and writes the dump to
`backups/agb-YYYY-MM-DD.sql.gz`, named for the night it covers. The dump is written as
`.partial` and renamed only once `pg_dump` has exited cleanly and written its closing line.
After a good dump, the service keeps the newest fourteen and deletes the rest.
`AGB_BACKUP_KEEP` in the service's environment changes that number.

A dump that fails is recorded as a failed job. The partial file is deleted, and nothing is
pruned. The job's expected event stays unfulfilled, so from 05:00 New York time the night
shows in the overdue view, as a night whose backup never ran does. To check:

```sh
ls -l backups/
./agb-stack exec -T db psql -U postgres -d agb -c "SELECT * FROM agb.overdue"
./agb-stack exec -T db psql -U postgres -d agb -c \
  "SELECT period, state, claimed_by FROM agb.job_state WHERE kind = 'backup' ORDER BY period DESC LIMIT 5"
```

The failure's message is in `./agb-stack logs backup` and in the job's `failed` event in
`agb.job_event`.

## The Parquet lake

The `pipeline` service lands the vendor's SEP, SF1 and TICKERS tables in `lake/`, one Parquet
file per partition:

```text
lake/sep/date=YYYY-MM-DD/part.parquet          prices for one session
lake/sf1/lastupdated=YYYY-MM-DD/part.parquet   fundamentals rows the vendor changed that day
lake/tickers/snapshot=YYYY-MM-DD/part.parquet  the tickers table as it stood that session
```

The scheduler plans a `data_pull` job for every session. It becomes claimable two hours after
the close, and its expected event is due at the next open. The service pulls the session's
prices, the fundamentals the vendor changed in the week to it, and the tickers table,
delisted tickers included. It records each partition's date and SHA-256 in
`agb.lake_snapshot`. If the vendor has not published the session yet, the service tries
again every 15 minutes. A pull that fails is recorded as a failed job. A pull that fails, or
never runs, shows in the overdue view from the next open. Re-running a pull for a session that
already landed writes the same bytes, so it leaves each file and its hash as they were.

The first time, land the history from 1998 on. It downloads the vendor's full exports, so
it takes a while, and it is safe to run again:

```sh
./agb-stack run --rm pipeline python -m agb.services.pipeline backfill
```

To query the lake with DuckDB, and to see the latest snapshots:

```sh
./agb-stack exec pipeline python -m agb.lake "SELECT max(date), count(*) FROM sep"
./agb-stack exec -T db psql -U postgres -d agb -c \
  "SELECT snapshot_date, partition, sha256 FROM agb.lake_snapshot ORDER BY id DESC LIMIT 5"
./agb-stack exec -T db psql -U postgres -d agb -c \
  "SELECT period, state FROM agb.job_state WHERE kind = 'data_pull' ORDER BY period DESC LIMIT 5"
```

The views are `sep`, `sf1`, which shows the latest version of each fundamentals row, and
`tickers`, which shows the newest snapshot.

## Promoting a model

Each rebalance ranks with the promoted model, and nothing else. Until a model is promoted,
and whenever the promoted one is voided or its artifact is missing or altered, the rebalance
job fails and no basket is proposed. `./agb-stack status` says which model is live.

A model is certified on the workstation by the research profile, which writes its artifact
directory under `research/certifications/<model id>/`. Getting it live takes three steps, which
`make promote MODEL=<model id>` runs in order over SSH, skipping each one that is already done;
`make promote MODEL=<model id> CMD=check` reports where a model stands without changing
anything, including the waiver it is live under, if any. Neither `make agb` nor
`make research` can do these writes: the `cli` service is pointed at the workstation's own
database, where no certification was ever imported, and the `research` service has no database
at all. The steps by hand are:

1. Copy the artifact directory, unchanged, to `models/<model id>/` in the deploy directory:

   ```sh
   rsync -a research/certifications/<model id> <server>:/mnt/data/AppData/agb-trader/models/
   ```

2. Record its certification, on the server:

   ```sh
   ./agb-stack import <model id>
   ```

   This runs on the server because the database is reachable only from the stack's own
   network, and in its own container because only the `research` role may insert into
   `agb.model_certification`. It verifies the artifact before it writes: a missing, altered
   or unlisted file, a shipped ledger that does not recount to the trial count the
   certification declared, and a model that is already certified are each refused, and
   nothing is written. The row's `artifact_path` records where the model was certified, from
   the manifest, not the path the import read.

3. Promote it on the server:

   ```sh
   ./agb-stack agb promote <model id>
   ```

`agb promote` refuses a model with no certification, one that failed its gates (unless it is
waived for paper, below), a voided one, one certified under an earlier cost model, and one
whose artifact is missing from `models/`, altered since certification, or ships a trial ledger
that does not recount to the trial count the certification declared. The recount uses the rule
the manifest says the certification counted by, so a model certified before the rule changed is
still checked against the number it declared. It also refuses the model
that is already live. Each refusal says why and is recorded in `agb.command`, as is each
promotion, which also writes an `agb.promotion_event`. The next rebalance loads the new model
and checks its artifact again.

To take a certification out of service for good:

```sh
./agb-stack agb void <model id> --reason "why"
```

A voided model cannot be promoted. If it is the live one, rebalances fail until another is
promoted. A release that changes the cost model voids every certification made under the old
one: `agb-stack activate` runs `agb void-stale` once the stack is up, and `agb promote` runs it
too. Each voiding is an `agb.certification_event`. A certification made under a cost model the
running release does not know yet, by research code ahead of the deploy, is refused but not
voided, so it can be promoted once that release is live; for the same reason a rollback voids
nothing.

### Promoting a failed certification to measure in paper

The sizing run (`docs/build/measured-limits.md`) needs a real rebalance to measure, and a
rebalance with nothing promoted fails before it proposes a basket. While no model has passed
its gates, a paper-only measurement waiver promotes one that failed, for that purpose alone:

```sh
make promote MODEL=<model id> CMD=waive REASON="measuring the live limits"
```

That is the three steps above with the waiver carried through to the last one, so a failed
certification is shipped and imported exactly as a passing one is. By hand, on the server:

```sh
./agb-stack agb promote <model id> --waive-for-paper --reason "measuring the live limits"
```

The waiver is refused unless the trading mode is paper, without a reason, on a model that
passed (it needs no waiver), and on a voided or stale certification. Everything else about a
promotion still holds: the artifact is verified as it always is. It is recorded as an
`agb.certification_event`, at most one per certification, naming the command that asked for
it.

A waived model trades in paper and nowhere else. `agb mode live` is refused while it is the
live model, and says so; promoting a model that passed its gates, or voiding the waived
certification, lifts that refusal. `/status` shows the model as live under a paper waiver, and
each rebalance that ranks with it logs a warning naming the waiver.

## Restoring a backup

### Checking that a dump restores

```sh
./agb-stack restore backups/agb-2026-09-16.sql.gz
```

This loads the dump into a scratch database, `agb_restore`, in the running `db` container. It
prints the row count of every table beside the live database's, then drops the scratch
database. A table can differ where rows were written after the dump, most often
`agb.job_event` and `agb.heartbeat`. A second argument names the scratch database, and
`--keep` leaves it in place for a closer look:

```sh
./agb-stack restore backups/agb-2026-09-16.sql.gz agb_scratch --keep
./agb-stack exec -T db psql -U postgres -d agb_scratch
./agb-stack exec -T db dropdb -U postgres agb_scratch
```

`restore` refuses to load into `agb`.

### Replacing the live database

This is for when the live database is lost or damaged. It replaces the live database with
the dump, and everything written since the dump is lost. Do the steps in this order, in the
deploy directory:

1. Stop the stack. This keeps the database volume.

   ```sh
   ./agb-stack down
   ```

2. Start only the database, and run `migrate` once. On a new, empty volume, this creates the
   roles that the dump's grants name.

   ```sh
   ./agb-stack up --detach --wait db
   ./agb-stack run --rm migrate
   ```

3. Replace the `agb` database with an empty one:

   ```sh
   ./agb-stack exec -T db dropdb -U postgres --force agb
   ./agb-stack exec -T db createdb -U postgres agb
   ```

4. Load the newest good dump:

   ```sh
   gunzip -c backups/agb-2026-09-16.sql.gz |
     ./agb-stack exec -T db psql -U postgres -d agb -v ON_ERROR_STOP=1 --quiet >/dev/null
   ```

   Errors still reach the terminal, and the first one stops the load.

5. Start everything:

   ```sh
   ./agb-stack up --detach --wait
   ./agb-stack status
   ```

   `migrate` runs first and gives the database back to its owner, with its grants. The
   migration ledger came with the dump, so `migrate` applies only migrations newer than the
   dump.

## Moving from the hand-made checkout

The first T01 and T02 live checks ran the stack from a plain clone of the repository in the
deploy directory, with the image `agb-trader-app:dev`. Before the first `make deploy`:

1. Stop that stack from the clone with `docker compose down`. This keeps the database
   volume, which the deployed stack reuses.
2. Delete everything in the deploy directory except `env/`.
3. Add `BACKUP_DB_PASSWORD` to `env/db.env`.

The first deploy's prune then removes `agb-trader-app:dev`.
