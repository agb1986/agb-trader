# agb-trader

A semi-autonomous, AI-integrated US equities trader. The design lives in
[docs/architecture](docs/architecture/); the build plan in [docs/build](docs/build/).

## Quickstart

Needs Docker with Compose v2, GNU Make and [uv](https://docs.astral.sh/uv/).

```sh
make env      # copy each env/*.env.example to env/*.env; set real passwords in env/db.env
make up       # build the image, create the roles, migrate, start the services
make status   # each service's last heartbeat and its age
make down     # stop the stack; the database volume is kept
```

The real `env/*.env` files are git-ignored and never committed. `make ENV_DIR=/some/dir up`
reads them from elsewhere. The stack's backup service writes a dump of the database into
`backups/` each night, also git-ignored; `make BACKUP_DIR=/some/dir up` puts them elsewhere.

## Deploying to the server

```sh
make deploy     # build this commit's image, ship it to the server over SSH, restart the stack there
make rollback   # restart the server's stack on the release before
```

[docs/build/deploy.md](docs/build/deploy.md) covers the server's layout, the nightly backups,
and restoring from one. The server's memory is tight, so each service's limit comes from a
measured run rather than a guess:

```sh
make measure                  # sample each container's peak memory until interrupted
make agb CMD=runtimes         # how long each job kind has actually taken
make sizing CMD=check         # drive the server's sizing run: check, start, watch, stop
make panel-check CMD=check    # drive the panel store's corruption check on the server
```

[docs/build/measured-limits.md](docs/build/measured-limits.md) is the procedure, the headroom
rule between a peak and a limit, and the nightly panel-store hash check.

## Development

```sh
uv sync         # install every dependency group
make test       # the whole suite, including a full stack bring-up
make test-fast  # everything except the stack bring-up
make lint
```

The tests start their own throwaway Postgres container on a random local port and never
touch the stack's database, so several worktrees can run them at once.

Schema changes go in [migrations/](migrations/); read its README first.
