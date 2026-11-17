# agb-trader

A semi-autonomous, AI-integrated US equities trader. The design lives in
[docs/architecture](docs/architecture/); the build plan in [docs/build](docs/build/).

## Quickstart

Needs Docker with Compose v2.20 or newer, GNU Make and [uv](https://docs.astral.sh/uv/).

```sh
make env      # copy each env/*.env.example to env/*.env; set real passwords in env/db.env
make up       # build the image, create the roles, migrate, start the services
make status   # each service's last heartbeat and its age; fails if one is stale or missing
make down     # stop the stack; the database volume is kept
```

The real `env/*.env` files are git-ignored and never committed. `make ENV_DIR=/some/dir up`
reads them from elsewhere. The stack's backup service writes a dump of the database into
`backups/` each night, and copies the panel store's new bodies into `backups/panel/`. It is
also git-ignored; `make BACKUP_DIR=/some/dir up` puts it elsewhere.

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
uv sync --frozen  # install every dependency group, exactly as uv.lock pins them
make test       # the whole suite, including a full stack bring-up, without the live tests
make test-fast  # everything except the stack bring-up and the live tests
make test-live  # only the live tests, each skipped unless its own variable is set
make lint       # ruff check and format, and shellcheck over scripts/deploy (install shellcheck locally)
make types      # mypy: strict on four modules, permissive elsewhere
make cover      # the fast tests with coverage; fails below the floor in pyproject.toml
```

The tests start their own throwaway Postgres container on a random local port and never
touch the stack's database, so several worktrees can run them at once. Each container carries
the label `agb.test=1` and the pid of the run that started it (`agb.test.pid`); at the start of
a session the suite removes labelled containers whose run is gone, and nothing else, so a
killed run does not leave one behind and `agb-trader-db-1` and `agb-trader-ui-1` are never
touched. The stack test brings up its own Compose project with the UI published on a free local port (`AGB_UI_PORT`), so it runs
beside a stack that holds `127.0.0.1:8080`; it asserts that every service beats, that `agb
status` lists them all and that the UI answers on its landing page, and it removes its project
when it fails or is interrupted.

[docs/build/type-checking.md](docs/build/type-checking.md) holds the permissive module list
the type check lets through, where the coverage floor lives and how the per-test timeout works.

GitHub Actions ([.github/workflows/ci.yml](.github/workflows/ci.yml)) runs five checks on
every pull request, whatever branch it targets, and on every push to `main`: `lint` (what
`make lint` runs), `types` (`make types`), `fast-tests` (what `make test-fast` runs),
`coverage` (`make cover`, the fast tests again with the coverage floor) and `image-build`
(`docker build --target app .`). CI sets no `AGB_LIVE_*` variable and holds no secret, so the
live tests never run there; the stack test is not run there either. Dependabot opens one grouped
pull request a week for the `uv` lockfile and one for the pinned actions.

Tests marked `live` talk to real services, so no default run collects them: `make test`,
`make test-fast` and a plain `uv run --frozen pytest` all deselect them, whatever is exported in the
shell. `make test-live` (or `uv run --frozen pytest -m live`) collects them, and each module still runs
only when its own variable is set to `1`, with the credentials it names in its docstring read
from the shell environment:

| Variable | What it touches |
| --- | --- |
| `AGB_LIVE_LIQUIDATE` | Alpaca paper: **closes every position in the paper account** |
| `AGB_LIVE_ALPACA` | Alpaca paper: reads the account, sets and unsets `suspend_trade`, submits nothing |
| `AGB_LIVE_TELEGRAM` | Telegram: sends a few messages to the real chat through the real proxy |
| `AGB_LIVE_SHARADAR` | Sharadar: pulls the latest session into a temporary directory |
| `AGB_LIVE_EDGAR` | SEC EDGAR: a few dozen requests for real filings |
| `AGB_LIVE_ANTHROPIC` | Claude API: paid calls for the Analyst and the debate |
| `AGB_LIVE_LAKE` | The real lake under `AGB_LAKE_DIR`, read only |
| `AGB_LIVE_RSYNC` | The server, over the operator's SSH: a dry-run sync of its lake, copies nothing |

Name one module to run only it, e.g. `AGB_LIVE_TELEGRAM=1 uv run --frozen pytest -m live
tests/test_telegram_live.py`.

Schema changes go in [migrations/](migrations/); read its README first.

## Licence

The repository carries no `LICENSE` file, so all rights are reserved by default and nothing here
is offered for reuse. That is deliberate and not an oversight: the choice of licence, if any,
belongs to the owner.
