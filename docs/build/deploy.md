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
   applies any new migrations before the services start. Once the stack is up, it prunes old
   releases and images.
7. Prints `agb-stack status`: each service's last heartbeat and the version it runs.

`AGB_DEPLOY_HOST` (default `casaos.local`) and `AGB_DEPLOY_DIR` (default
`/mnt/data/AppData/agb-trader`) point it elsewhere:

```sh
AGB_DEPLOY_HOST=other.local make deploy
```

Deploying the commit that is already running restarts nothing that has not changed.

If the stack does not come up, the deploy stops with an error. `current` already points at the
new release, so `make rollback` returns to the one that was running.

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
./agb-stack status                 # each service's last heartbeat and its age
./agb-stack tags                   # the current and previous release
./agb-stack ps                     # the containers
./agb-stack logs --follow backup   # any other command goes to docker compose
./agb-stack agb <command>          # run an agb CLI command
```

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
