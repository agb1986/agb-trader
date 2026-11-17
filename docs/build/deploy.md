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
| `backups/`         | The nightly dumps, `agb-YYYY-MM-DD.sql.gz`, and the panel mirror.      |
| `lake/`            | The Parquet lake: the vendor's data in `sep/`, `sf1/` and `tickers/`.  |
| `panel/`           | The panel store: the compressed prompts and answers behind the panel.  |
| `models/`          | Certified model artifacts, one `<model id>/` each. See below.          |
| `releases/<tag>/`  | `compose.yaml` and `agb-stack` as of commit `<tag>`.                   |
| `current`          | A link to the release that is running.                                 |
| `previous`         | A link to the release it replaced.                                     |
| `healthy`          | A link to the last release that came up healthy. Pruning keeps it.     |
| `agb-stack`        | A link to `current/agb-stack`, the command for everything below.       |

A tag is the commit's 12-character short hash, and the image is `agb-trader-app:<tag>`. The
server keeps only the current, previous and last healthy releases and their images. Each
successful deploy removes the rest; a failed one removes nothing. The database lives on the `agb-trader_pgdata` Docker volume, not in this directory.

Run `agb-stack` from the deploy directory as `./agb-stack`, or by its full path.

## Before the first deploy

- Key-based SSH from the workstation to the server works without a prompt, and the server
  user can run `docker compose` without `sudo`. The server's Compose must be v2.20 or newer
  (`docker compose version` on the server shows it): `compose.yaml` relies on the top-level
  `name`, on `depends_on` conditions, and on `up --wait --wait-timeout`, which `agb-stack`
  passes. The Docker Engine on the server is 20.10, so the file uses only fields that engine
  knows. See
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

- The `agb-trader_pgdata` volume, if it already exists, is owned by uid 70. `db` runs as the
  image's `postgres` user, not as root, so it cannot take ownership of a volume that root
  owns. The image's own entrypoint gave the volume to uid 70 when it first initialised it, so
  a volume made by this stack already is. To check, on the server:
  `docker run --rm -v agb-trader_pgdata:/data --entrypoint stat postgres:17-alpine -c %u /data`
  prints `70`.

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

1. Stops if the working tree has uncommitted or untracked files, naming them, since the image
   is built from the commit and not from the working tree. `--allow-dirty` (or
   `AGB_DEPLOY_ALLOW_DIRTY=1 make deploy`) deploys the commit anyway and lists what it leaves
   out.
2. Checks that the deploy directory exists and that each server env file sets every variable
   its example does. The variable names are read on the server, so no value leaves it. A
   missing file or variable stops the deploy before anything is built.
3. Builds `agb-trader-app:<tag>` from `git archive HEAD`, for `linux/amd64`, with the tag as
   `AGB_VERSION`. Each service reports that version with its heartbeat. Compose takes the same
   value from `AGB_IMAGE_TAG`, which `agb-stack` sets from the release it activates.
   The workstation's research image takes its version from `AGB_VERSION` instead, which `make
   research` sets to the checkout's commit, with `-dirty` when the tree has changes. Both
   default to `dev` when unset, and `certify` refuses to run under `dev`, so a research image
   built by hand without `AGB_VERSION` cannot certify a model.
4. Ships the image with `docker save | gzip | ssh … docker load`, unless the server already
   has that tag with the same image as the one just built. The images are compared by their
   layer digests (`docker image inspect --format '{{json .RootFS.Layers}}'`), not by `.Id`:
   `.Id` is a different digest of the same image under the containerd image store on the
   workstation's Docker 29 than on the server's Docker 20.10. A tag the server holds with other
   layers is shipped again, and after every load the server's layers for the tag are compared
   with the built ones: a mismatch stops the deploy before anything is activated.
5. Installs the release: the commit's `compose.yaml` and `agb-stack` go into
   `releases/<tag>/`.
6. Runs `agb-stack activate <tag>` on the server. That starts the new release with
   `docker compose up --wait`. `migrate` applies any new migrations before the services start,
   and refuses one that is older than the newest already applied, naming both versions. If that
   migration is safe to run after the newer one, run the activation again with the flag, on the
   server in the deploy directory: `AGB_MIGRATE_ALLOW_OUT_OF_ORDER=1 ./agb-stack activate <tag>`
   (see [migrations/README.md](../../migrations/README.md)). `--wait` returns only once every
   service reports healthy, and each app service is healthy only while its own heartbeat is
   younger than five minutes, so a service that crash-loops or never beats fails the deploy
   instead of passing it. Then it runs `agb void-stale`, which must succeed too. Only once both
   have passed does `current` move to the new release, `previous` to the one that was running,
   and `healthy` to the new release. Last, it prunes old releases and images.
7. Prints `agb-stack status`: each service's last heartbeat and the version it runs. It exits
   1, and so fails the deploy, if any service has never beaten or last beat more than five
   minutes ago.

`AGB_DEPLOY_HOST` (default `casaos.local`) and `AGB_DEPLOY_DIR` (default
`/mnt/data/AppData/agb-trader`) point it elsewhere:

```sh
AGB_DEPLOY_HOST=other.local make deploy
```

Deploying the commit that is already running restarts nothing that has not changed.

If the new release does not come up healthy, or `agb void-stale` fails, `agb-stack activate`
brings the release that was running back up, leaves `current`, `previous` and `healthy` as they
were, prunes nothing, and exits non-zero, so the deploy fails and the stack is on the release it
was on before. The failed release stays in `releases/` until a later successful deploy prunes it.
The exceptions: on the very first deploy there is nothing to return to, so the stack is stopped
and `current` is not set; and if the release that was running will not come back up either, the
error says so and the stack is down. Either way, fix the cause and run `make deploy` again, or
`./agb-stack activate <tag>` for a release already on the server. A rollback ends with the same
`status`, so it too fails while a service is stale.

## Pinned images

`compose.yaml` pins `db` and `vpn` by digest, and the Dockerfile pins its `python` and `uv`
bases by digest, so every deploy of a commit runs the same images. The server pulls the
Postgres and gluetun digests itself the first time a release names them. To take a newer
image, pull its tag on the workstation, read the digest, and put it in place of the old one
in a commit of its own:

```sh
docker pull postgres:17-alpine
docker image inspect --format '{{index .RepoDigests 0}}' postgres:17-alpine
```

The same two commands, with `qmcgaw/gluetun:v3`, `python:3.12-slim` or
`ghcr.io/astral-sh/uv:0.10.10`, give the others. `tests/test_compose.py` fails on an image
that is not pinned.

## Rolling back

```sh
make rollback
```

This runs `agb-stack rollback` on the server, which activates the `previous` release under the
rules above: if it does not come up, the stack returns to the release it was on. The release it
leaves becomes `previous`, so running `make rollback` again returns to it. To go back
further, deploy an older commit: check it out and run `make deploy`.

A rollback does not undo migrations. It does not need to: migrations only ever widen: they add tables
and nullable or defaulted columns, and they loosen a `CHECK` constraint by dropping it and
adding it back with a longer list (`migrations/20261014000000_execution_recovery.sql`), never
by narrowing it. Every row an older image wrote stays valid, so the older image runs against
the newer schema, and `migrate` ignores ledger entries it has no file
for. Those entries still count as applied: a later deploy whose new migration is older than
one the rolled-back release had applied is refused as out of order, and
`AGB_MIGRATE_ALLOW_OUT_OF_ORDER=1 ./agb-stack activate <tag>` lets it through. See
[migrations/README.md](../../migrations/README.md).

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

The same job then backs up the panel store. The store keeps each prompt and answer the panel
sent or received as a zstd body named by the SHA-256 of its text, under `panel/`. The service
reads it through a read-only mount and copies each body the mirror, `backups/panel/`, does not
yet hold. It checks each body twice: before copying it, and after, from the copy it wrote. A
body is renamed into place only once it has read back sound. The mirror only grows. A body
already in the mirror is never overwritten, and a body lost from the live store stays in the
mirror. A live body that does not decompress or match its hash is not copied. The service logs
it and goes on, and `panel_audit` already raises an alert for it. Pruning ignores
`backups/panel/`.

A dump that fails, or a panel store the service cannot read or a mirror it cannot write, is
recorded as a failed job. Both parts still run, and the failure names each part that failed.
The partial dump is deleted, and nothing is pruned. The job's expected event stays
unfulfilled, so from 05:00 New York time the night shows in the overdue view, as a night whose
backup never ran does. To check:

```sh
ls -l backups/
./agb-stack exec -T db psql -U postgres -d agb -c "SELECT * FROM agb.overdue"
./agb-stack exec -T db psql -U postgres -d agb -c \
  "SELECT period, state, claimed_by FROM agb.job_state WHERE kind = 'backup' ORDER BY period DESC LIMIT 5"
```

The failure's message is in `./agb-stack logs backup` and in the job's `failed` event in
`agb.job_event`.

### What the backups hold, and who can read them

The dumps hold every table in the database: orders, fills, positions, the audit log, and the
panel's database rows. The mirror holds the panel's prompts and answers, and the filing
sections the panel read. Neither holds an API key, a password or a bot token: those live
only in the env files in `env/`, which are never copied into `backups/`, nor anywhere else.
The database roles' passwords are not in a dump either, since `pg_dump` dumps one database
and not its cluster's roles.

A dump is created with mode 0600, whatever the umask, and the panel bodies are 0600 too. The
service sets `backups/` and `backups/panel/` to 0700. After each good night it also sets any
older dump that is not 0600 to 0600, and `backups/` back to 0700, so dumps written before this
change are tightened on the first night after it. A failure to tighten is logged as a warning
and does not fail the night. Only the host user the stack runs as can read the backups.

The backups are not encrypted by the stack. They sit on the same data disk as the live
database and the panel store, so they protect against a lost or damaged table, file or volume,
not against losing the disk. Encryption at rest is the host's concern: encrypt the data disk or
its filesystem. A copy off the host is also left to the operator.

### The lake is not backed up

The backup service does not copy `lake/`. The lake holds only the vendor's data, so the
pipeline can pull it again:

```sh
./agb-stack run --rm pipeline python -m agb.services.pipeline backfill
```

A pull lands the vendor's data as it stands now, though. Where the vendor has revised a
partition since it landed, the new partition's SHA-256 differs from the one recorded in
`agb.lake_snapshot` and in the certification manifests that name it, and a certification
checked against the lake stops matching. The workstation's copy from `make sync-lake` keeps
the partitions as they landed, because that sync only pulls and never deletes. To restore the
lake with its recorded hashes, copy that copy back from the workstation:

```sh
rsync -a lake/ casaos.local:/mnt/data/AppData/agb-trader/lake/
```

Sessions that landed after the workstation last synced are not in its copy, and only a
new pull brings them back, with the caveat above. The panel store is backed up, unlike
the lake, because the panel's answers were paid for and cannot be pulled again.

## The Parquet lake

The `pipeline` service lands the vendor's SEP, SF1 and TICKERS tables in `lake/`, one Parquet
file per partition:

```text
lake/sep/date=YYYY-MM-DD/part.parquet          prices for one session
lake/sf1/lastupdated=YYYY-MM-DD/part.parquet   fundamentals rows the vendor changed that day
lake/tickers/snapshot=YYYY-MM-DD/part.parquet  the tickers table as it stood that session
```

The scheduler plans a `data_pull` job for every session. It becomes claimable two hours after
the close, and its expected event is due at the next open. The service pulls the prices for
every session since the last one that landed, so a session missed to a vendor error is picked
up by the next pull. The vendor adjusts prices backwards, so a split or dividend rescales a
ticker's earlier sessions. Each pull also fetches the last session that landed, and a ticker
the vendor now prices differently on it has its whole history fetched again and rewritten
across the sessions the lake holds, which changes those partitions' hashes. It also pulls
the fundamentals the vendor changed since a week before the last pull, and the tickers table,
delisted tickers included. It records each partition's date
and SHA-256 in `agb.lake_snapshot`. If the vendor has not published a session yet, or has
published fewer than 90% of the rows the session before it had, the service lands the
complete sessions before it and tries again every 15 minutes, without an alert of its own.
A request that meets a vendor error is tried up to three times with backoff, honouring
`Retry-After`; an error that outlasts the tries fails the job and raises a
`pipeline_job_failed` alert. A pull that fails, or never completes, shows in the overdue
view from the next open. A gap of more than 45 days is left to the backfill below. Re-running
a pull for a session that already landed writes the same bytes, so it leaves each file and
its hash as they were.

The first time, land the history from 1998 on. It downloads the vendor's full exports, so
it takes a while, and it is safe to run again:

```sh
./agb-stack run --rm pipeline python -m agb.services.pipeline backfill
```

To query the lake with DuckDB, and to see the latest snapshots:

```sh
./agb-stack exec pipeline python -m agb.lake "SELECT max(date), count(*) FROM sep"
./agb-stack exec pipeline python -m agb.lake --check
./agb-stack exec -T db psql -U postgres -d agb -c \
  "SELECT snapshot_date, partition, sha256 FROM agb.lake_snapshot ORDER BY id DESC LIMIT 5"
./agb-stack exec -T db psql -U postgres -d agb -c \
  "SELECT period, state FROM agb.job_state WHERE kind = 'data_pull' ORDER BY period DESC LIMIT 5"
```

`--check` lists each ticker whose stored adjusted prices sit on two bases, an action the lake
missed, and exits 1 if there is one. Running the backfill again rewrites the history on the
vendor's current basis.

The views are `sep`, `sf1`, which shows the latest version of each fundamentals row, and
`tickers`, which shows the newest snapshot. `sf1_as_of('2020-06-30')` shows the fundamentals
as they were known on that day, which is what research and replays read.

## Promoting a model

Each rebalance ranks with the promoted model, and nothing else. Until a model is promoted,
and whenever the promoted one is voided or its artifact is missing or altered, the rebalance
job fails and no basket is proposed. `./agb-stack status` says which model is live.

A model is certified on the workstation by the research profile, which writes its artifact
directory under `research/certifications/<model id>/`. Getting it live takes three steps, which
`make promote MODEL=<model id>` runs in order over SSH, skipping each one that is already done;
`make promote MODEL=<model id> CMD=check` reports where a model stands without changing
anything, including the waiver it is live under, if any. `make promote` stops before it ships
anything when the artifact's manifest says the certification failed its gates, as `certify`
itself exits 1 after writing a failed one; the paper waiver below is the only way past that.
Neither `make agb` nor `make research` can do these writes: the `cli` service is pointed at the
workstation's own database, where no certification was ever imported, and the `research`
service has no database at all. The steps by hand are:

1. Copy the artifact directory, unchanged, to `models/<model id>/` in the deploy directory.
   `make promote` extracts it into a temporary directory beside the destination and renames
   it into place, so an interrupted transfer leaves nothing under the model's name and a rerun
   ships the whole artifact. It treats a copy already there as shipped only after checking, on
   the server, the digest of every file the manifest lists and of the manifest itself, and that
   nothing else is in the directory; a copy that does not match is refused, not completed or
   replaced. By hand, copy to a temporary name and rename:

   ```sh
   rsync -a -- research/certifications/<model id>/ <server>:/mnt/data/AppData/agb-trader/models/.incoming.<model id>/
   ssh <server> mv -T -- /mnt/data/AppData/agb-trader/models/.incoming.<model id> /mnt/data/AppData/agb-trader/models/<model id>
   ```

   A model id starts with a letter or digit and holds only letters, digits, dot, dash and
   underscore; `make promote` and `certify` refuse any other, a leading dash included.

2. Record its certification, on the server:

   ```sh
   ./agb-stack import <model id>
   ```

   This runs on the server because the database is reachable only from the stack's own
   network, and in its own container because only the `research` role may insert into
   `agb.model_certification`. It verifies the artifact before it writes: a missing, altered
   or unlisted file, a manifest altered since certification, a trial count by any rule but
   `started`, a shipped ledger that does not recount to the trial count the certification
   declared, a verdict that the gates do not give again when they are rerun over the shipped
   `paths.json` and `trials.parquet`, and a model that is already certified are each
   refused, and nothing is written. The row's `artifact_path` records where the model was certified, from
   the manifest, not the path the import read.

3. Promote it on the server:

   ```sh
   ./agb-stack agb promote <model id>
   ```

`agb promote` refuses a model with no certification, one that failed its gates (unless it is
waived for paper, below), a voided one, one certified under an earlier cost model, and one
whose artifact is missing from `models/` or fails the same check the import runs, which is
the same code. It also refuses the model that is already live.

The check recomputes the deflated Sharpe, the PBO, the Sharpe, every gate and the verdict from
the paths and the ledger the artifact ships, and refuses the artifact if any of them differs
from the manifest by more than a relative 1e-9 (an absolute 1e-12 near zero), naming each field
that differs. The trial count is always counted as `started`, the rule in the code; the
manifest's own field only has to agree with it. The digest covers the whole manifest, so no
field the server reads can be edited without changing the digest the certification row holds.
A model certified before issue #211 (every model up to and including `lgbm-2026-10`) names no
rule and has a digest over its files only, so it is refused with that cause and has to be
certified again under the current code, imported and promoted; see
[Checking a promoted model after issue #211](#checking-a-promoted-model-after-issue-211). Each refusal says why and is recorded in `agb.command`, as is each
promotion, which also writes an `agb.promotion_event`. The next rebalance loads the new model
and checks its artifact again.

To take a certification out of service for good:

```sh
./agb-stack agb void <model id> --reason "why"
```

A voided model cannot be promoted. If it is the live one, rebalances fail until another is
promoted. A release that changes the cost model voids every certification made under the old
one: `agb-stack activate` runs `agb void-stale` once the stack is up and fails the deploy,
returning to the release that was running, if it cannot; `agb promote` runs it too. Each voiding is an `agb.certification_event`. A certification made under a cost model the
running release does not know yet, by research code ahead of the deploy, is refused but not
voided, so it can be promoted once that release is live; for the same reason a rollback voids
nothing.

### Checking a promoted model after issue #211

A release with issue #211 runs the import's check every time the pipeline loads the promoted
model, so before deploying it, check the model that is live. Checking writes nothing:

```sh
./agb-stack import lgbm-2026-10
```

The import verifies the artifact before it looks for an existing certification, so for a model
that is already certified this either prints the check's refusal or, once the check passes,
`model 'lgbm-2026-10' is already certified`, which is the pass. `lgbm-2026-10` (artifact
`3c7b4e50f2f2`) was certified before the fix and is refused before anything is recomputed:

```text
manifest.json names no trial count rule: the model was certified before the rule was fixed at 'started' (issue #211); re-certify the model under the current code and import the new artifact
```

Once that release is deployed, the rebalance job fails on that model as it does on a missing
artifact, so a model certified under the current code has to be imported and promoted with
the deploy, before the next rebalance. Its copy under `research/certifications/` also shows
why it cannot be recomputed as it stands: its ledger counts 13 trials by `rows` and 2 by
`started`; its paths ship no SPY return, so its deflated Sharpe and its Sharpe are undefined
on the excess basis the gates now use; and its overfitting matrix has one candidate, so its
PBO is undefined. Its total-return Sharpe, 0.5741, does recompute exactly.

For a model certified under the current code, a difference between the manifest and the gates
run again names each field, with both values:

```text
the verdict recomputed from paths.json and trials.parquet differs from manifest.json in 1 field(s): dsr: manifest 0.9312, recomputed 0.9212
```

A manifest edited after certification without its digest being recomputed reads
`manifest.json has been altered since certification`, and one whose digest was recomputed
too reads `has digest <new>; the certification's is <old>` when it is promoted or loaded,
since the certification row holds the digest it was imported with. Either is a refusal to
investigate, not to work around: certify the model again.

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

### Restoring panel bodies

The panel store's bodies sit beside its database rows, and a row whose body is missing or
damaged fails `panel_audit`. To bring the bodies back from the mirror, run the restore in the
`pipeline` service, which writes the panel store as its owner, with the mirror mounted
read-only:

```sh
./agb-stack run --rm --no-deps --volume "$PWD/backups/panel:/panel-backup:ro" pipeline \
  python -m agb.services.backup restore-panel /panel-backup /panel
```

The restore copies each body the store lacks, and replaces each body in the store that does
not decompress or match its hash. It checks every body as the nightly backup does, and leaves
the store's sound bodies alone. It deletes nothing. It logs how many bodies it restored, and
exits with status 1 if the mirror holds a bad body, naming it. After a database restore, run
it as well, so that the bodies match the restored rows; bodies the restored rows no longer
name do no harm.

## Moving from the hand-made checkout

The first T01 and T02 live checks ran the stack from a plain clone of the repository in the
deploy directory, with the image `agb-trader-app:dev`. Before the first `make deploy`:

1. Stop that stack from the clone with `docker compose down`. This keeps the database
   volume, which the deployed stack reuses.
2. Delete everything in the deploy directory except `env/`.
3. Add `BACKUP_DB_PASSWORD` to `env/db.env`.

The first deploy's prune then removes `agb-trader-app:dev`.
