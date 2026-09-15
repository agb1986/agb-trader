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
reads them from elsewhere.

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
