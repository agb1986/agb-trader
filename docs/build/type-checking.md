# Type checking, coverage and the test timeout

Three guards for the test run, added for #192 (finding T-02 of
[the 2026-11-12 review](../review-2026-11-12.md)). Each has one place where its setting lives.

## Types: `make types`

`make types` runs `mypy` over `src/agb`, configured under `[tool.mypy]` in `pyproject.toml`.

- **Strict** (every `strict` flag): `agb.jobs`, `agb.execution`, `agb.gate` and `agb.approval`,
  the modules that claim work, move a basket and send orders. They are held to it by a
  per-module override in `pyproject.toml`; `strict` is a global-only switch in mypy, so the
  override lists the flags it stands for.
- **Permissive**: every other module is checked at mypy's default level (unannotated
  functions are allowed and not looked into).
- **Not yet passing**: the modules in the last `[[tool.mypy.overrides]]` block of
  `pyproject.toml` (`ignore_errors = true`) still have type errors, 97 of them on 2026-11-16,
  and their errors are not reported. That block is the permissive list. It only shrinks: fix a
  module, delete its line, run `make types`. Fixing them is not part of #192.

A statement that always returns a row (`INSERT ... RETURNING`, an aggregate, a view over a
one-row table) is read with `agb.rows.one`, which raises when no row comes back, so the
strict modules need no `fetchone()[0]` on an optional.

Errors per module in the permissive list, 2026-11-16:

| Module | Errors |
| --- | --- |
| `agb.cli` | 13 |
| `agb.research.certify` | 11 |
| `agb.services.execution` | 8 |
| `agb.research.backtest` | 7 |
| `agb.ui` | 6 |
| `agb.research.reproduce` | 5 |
| `agb.services.bot`, `agb.research.trials`, `agb.readout`, `agb.health` | 4 each |
| `agb.services.backup`, `agb.heartbeat`, `agb.basket` | 3 each |
| `agb.services.ui`, `agb.research.cpcv`, `agb.research.artifact`, `agb.research.__main__`, `agb.registry`, `agb.lake`, `agb.features` | 2 each |
| `agb.universe`, `agb.services.pipeline`, `agb.schedule`, `agb.recovery`, `agb.preflight`, `agb.panel_audit`, `agb.panel`, `agb.migrate` | 1 each |

To count a module again, delete its line from the block and run `uv run --frozen mypy`.

## Coverage: `make cover`

`make cover` runs the fast tests (`-m "not stack and not live"`) with `pytest-cov` over the
`agb` package, prints the figure per module and fails when the total is under `fail_under`
in `[tool.coverage.report]` in `pyproject.toml`. That is the one place the floor is recorded.
It sits a little below the figure measured when it was set; raise it when coverage rises, and
never lower it to make a change pass.

## Timeout: `pytest-timeout`

`timeout` under `[tool.pytest.ini_options]` in `pyproject.toml` is the default per-test limit
in seconds. A test that runs past it fails with `Failed: Timeout (>Ns)` and the run goes on,
instead of hanging. `tests/test_timeout.py` runs a test that sleeps past a one-second limit.
A test that legitimately needs longer takes `@pytest.mark.timeout(seconds)` and says why.
The default is above the slowest test in `make test-fast` and `make test` (the stack bring-up
included, which builds an image), so no existing test trips it.

CI runs `make types` and `make cover` as the `types` and `coverage` jobs of
`.github/workflows/ci.yml`, beside `lint`, `fast-tests` and `image-build`.
