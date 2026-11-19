# Orca build plan

How the 32 build tickets (issues #14–#45) are worked in parallel with Orca, and how the coordinator decides which ticket a worker picks up next.

The tickets and their blocking edges live on GitHub: every ticket is an issue labelled `ready-for-agent`, and native `blocked_by` links carry the dependencies. This plan adds no new dependencies between tickets. It mirrors the GitHub graph into an Orca run, and adds merge and operator steps so that "ready" in Orca means the same thing as "unblocked" on GitHub.

`scripts/orca/plan.py` implemented the plan. All 32 tickets have merged, so the script has been removed (it is in the git history, last at `ae0541e`), and the commands below, and the `plan.py` references through the rest of this document, describe how the build phase was run:

| Command | What it does |
|---|---|
| `seed [--run RUN] [--apply]` | Creates the Orca tasks. It is a dry run unless `--apply` is given, and it is idempotent by task title, so it can be re-run safely. |
| `next --run RUN [--slots N]` | Tells the coordinator what to start next and what is waiting to merge. |
| `frontier` | Answers the same question from GitHub alone, as a cross-check. |

On this host the Orca CLI is `orca-ide`. Bare `orca` is the GNOME screen reader.

## Roles

- **Operator.** The human. Provides credentials, watches server-side checks, and approves every merge.
- **Coordinator.** One Claude session bound to the Orca run. It:
  - picks tickets and starts workers,
  - reviews PRs,
  - runs merges once the operator approves.

  It is the only session that writes to `main`, and only by merging a PR.
- **Workers.** One Claude session per ticket, each in its own worktree created from `origin/main`. A worker opens exactly one PR and stops. Workers never:
  - merge,
  - touch the server,
  - hold real secrets,
  - dispatch further workers (Orca limits nesting to one level).

## Why a ticket is split into several tasks

Orca marks a task complete as soon as its worker sends a valid `worker_done`. If each ticket were a single task, its dependents would become ready while its PR was still unreviewed and unmerged. Workers start from `origin/main`, so they would build on code that is not there yet.

Each ticket is therefore two tasks, and some tickets also wait on operator prerequisites:

| Task title | Kind | Completed by |
|---|---|---|
| `BUILD Tnn #N` | Implementation. Dispatched to a worker. | The worker's `worker_done`, sent once the PR is open and the suite passes. |
| `MERGE Tnn #N` | Merge gate. Never dispatched. | The coordinator, with `task-update`, after the PR is merged. |
| `PRE <name>` | Operator prerequisite. Never dispatched. | The coordinator, with `task-update`, once the operator confirms it is done. |
| `FIX Tnn rK` | Review fixes. Created only when a review asks for changes. | The same worker, reusing its terminal and worktree. |

Dependencies are encoded like this:

- **BUILD Tnn** depends on the MERGE task of every ticket that blocks Tnn on GitHub. It becomes ready only when all of its blockers are on `main`.
- **MERGE Tnn** depends on:
  - BUILD Tnn,
  - any PRE task its live check needs,
  - the extra MERGE edges listed below.

A BUILD task never depends on a PRE task. Workers build and test against fakes, fixtures and the committed `*.example` env files, so a missing credential never stops code from being written. Credentials only gate the merge, which is where the live check runs. This keeps the maximum amount of work parallel while keys are still being set up.

## Operator prerequisites and live checks

Six tickets have acceptance criteria that fakes cannot prove. For each one, the worker adds an opt-in live check, which is skipped by default. The worker describes how to run it under **Operator live check** in the PR. The coordinator runs the check before merging. The operator's step-by-step checklist for each prerequisite is in [Operator setup outside the repo](operator-setup.md).

| Prerequisite | What the operator provides | Gates |
|---|---|---|
| `PRE server` | SSH from the workstation to the server; `/mnt/data` writable | MERGE T03 |
| `PRE sharadar` | Sharadar key (SEP, SF1, TICKERS) in `env/pipeline.env` | MERGE T04 |
| `PRE alpaca` | Alpaca paper key, secret and endpoint in `env/execution.env` | MERGE T09 |
| `PRE telegram` | Telegram bot token and chat id in `env/bot.env` on the server | MERGE T11 |
| `PRE vpn` | ProtonVPN WireGuard private key in `env/vpn.env` on the server | MERGE T11 |
| `PRE anthropic` | Anthropic API key in `env/pipeline.env` | MERGE T23 |

Rules for secrets:

- Secrets are placed in the env files by the operator.
- They are never pasted into Orca, GitHub or chat.
- They never enter a worker's worktree.

The extra MERGE edges:

- **MERGE T11 waits for MERGE T03.** The bot's live check runs on the server, so it needs the delivery path from T03.
- **MERGE T17 waits for MERGE T03.** The research profile's live check rsyncs the lake from the server.

Live checks that touch the server (T03, T11 and T17) are **attended**: the coordinator runs them with the operator watching. `next` flags these tickets.

Two live checks need particular timing:

- **T04, full backfill.** T04's live check proves the pull with a small date range from the workstation. The full backfill from 1998 onto the server's `/mnt/data` is an operator step once T03 and T04 have both merged. It must finish before the T17 live check, because T17 reads the lake from the server.
- **T09, trading-day close.** The paper MOC has to be submitted before 15:45 ET and fill at the close. 15:45 ET is 20:45 in the UK for most of the year and 19:45 during the few weeks when US and UK daylight saving are out of step. MERGE T09 therefore waits for the next trading session, not for the operator.

## How the coordinator picks the next ticket

The rule is:

1. **Candidates** are the BUILD tasks in `task-list --ready`. MERGE and PRE tasks are never dispatched.
2. **Rank the candidates:**
   1. by critical-path height, highest first;
   2. then, among equal heights, prefer a lane that no active worker is in;
   3. then by ticket number.
3. **Start workers** until the number of active BUILD workers reaches the cap: 3 at first, and 4 once the harness has proved parallel-safe.

Height is the number of tickets on the longest chain from a ticket to the end of the build, the ticket included. Starting the highest ticket first keeps the critical path moving (T01 → T02 → T04 → T06 → T07 → T08 → T09 → T12 → T13, T25 or T27 → leaf). Lane spreading keeps two workers out of the same service at once, which is where merge conflicts come from.

`plan.py next --run RUN` applies this rule and prints the `worker-start` command for each free slot. It also prints:

- the merge queue, which is the ready MERGE tasks, highest first;
- any open operator prerequisites.

## Worker models

Each BUILD worker starts on a Claude model and effort level chosen for its ticket. The choice is `WORKERS` in `plan.py`, and `next` puts it into each `worker-start` command it prints. The rule is how costly a subtle mistake in the ticket would be, not how much code it needs.

| Model | Effort | Tickets | Why |
|---|---|---|---|
| `fable` | xhigh | T09, T10, T17, T18, T20 | A subtle mistake costs money or silently corrupts research. These are idempotent broker calls and the halt re-read (T09), recovery from each crash point (T10), purged and embargoed cross-validation and the trial ledger (T17), the backtest engine and its parity with the fast path (T18), and the deflated Sharpe and overfitting statistics (T20). |
| `fable` | high | T07 | The proposed basket's event-sourced state and rebalance rules are reused by execution, the backtest and the panel. |
| `opus` | xhigh | T06, T08, T13, T16, T19, T21, T31 | Each has one hard invariant: point-in-time membership with delisted names (T06), the halt latch (T08), the approval race against the cutoff (T13), feature leakage (T16), corporate events matching between backtest and live (T19), promotion gates (T21) and the autonomy ladder (T31). |
| `opus` | high | T01–T05, T11, T12, T14, T15, T23, T24, T25, T28 | Ordinary service and integration work. T01, T02, T03 and T05 were built this way. |
| `sonnet` | high | T22, T26, T27, T29, T30, T32 | Small, well-specified tickets or read-only views. |

The model names are Claude Code aliases, so each one always means the newest model of that tier. To change a ticket's model, edit `WORKERS` before its worker starts. A worker keeps the model it started with.

## Waves

Waves are a planning view, not a schedule. A ticket is actually ready the moment its last blocker merges, so the coordinator works the live frontier and never waits for a whole wave to finish.

| Wave | Tickets (issue) |
|---|---|
| 1 | T01 (#14) |
| 2 | T02 (#15), T05 (#18) |
| 3 | T03 (#16), T04 (#17), T11 (#24) |
| 4 | T06 (#19) |
| 5 | T07 (#20), T16 (#29) |
| 6 | T08 (#21), T17 (#30), T23 (#36) |
| 7 | T09 (#22), T18 (#31), T24 (#37) |
| 8 | T10 (#23), T12 (#25), T14 (#27), T19 (#32), T20 (#33), T22 (#35), T26 (#39) |
| 9 | T13 (#26), T15 (#28), T21 (#34), T25 (#38), T27 (#40) |
| 10 | T28 (#41), T29 (#42), T30 (#43), T31 (#44), T32 (#45) |

Waves 1 to 7 rarely offer more than three tickets at once, so three workers are enough until wave 8. The cap of 4 only pays off from wave 8 onward.

## Heights and lanes

| Ticket | Issue | Height | Lane | Ticket | Issue | Height | Lane |
|---|---|---|---|---|---|---|---|
| T01 | #14 | 10 | infra | T17 | #30 | 5 | research |
| T02 | #15 | 9 | infra | T18 | #31 | 4 | research |
| T03 | #16 | 2 | infra | T19 | #32 | 1 | pipeline |
| T04 | #17 | 8 | data | T20 | #33 | 3 | research |
| T05 | #18 | 7 | core | T21 | #34 | 2 | pipeline |
| T06 | #19 | 7 | data | T22 | #35 | 1 | research |
| T07 | #20 | 6 | pipeline | T23 | #36 | 4 | panel |
| T08 | #21 | 5 | execution | T24 | #37 | 3 | panel |
| T09 | #22 | 4 | execution | T25 | #38 | 2 | panel |
| T10 | #23 | 1 | execution | T26 | #39 | 1 | panel |
| T11 | #24 | 4 | bot | T27 | #40 | 2 | ui |
| T12 | #25 | 3 | bot | T28 | #41 | 1 | ui |
| T13 | #26 | 2 | bot | T29 | #42 | 1 | ui |
| T14 | #27 | 3 | execution | T30 | #43 | 1 | bot |
| T15 | #28 | 2 | execution | T31 | #44 | 1 | pipeline |
| T16 | #29 | 6 | data | T32 | #45 | 1 | infra |

`plan.py` computes the heights from the dependency table, and that table is checked against GitHub's `blocked_by` links on every `seed` and `frontier` run.

## Setup, once

1. Merge T01 before starting Orca. It is built solo because it creates the repo layout, the Compose stack and the test harness that every later worker relies on.
2. Once T01 has merged, set the Orca repo setup script so that each new worktree runs `uv sync --frozen`. Workers need nothing else; they never receive real secrets.
3. Create the run from the coordinator's terminal, which binds that terminal to it:

   ```sh
   orca-ide orchestration run-create --objective "Build agb-trader tickets #14-#45" --json
   ```

   Record the run id where the coordinator will find it again after a restart or compaction.

4. Seed the run. The first command is a dry run; `--apply` creates the tasks.

   ```sh
   python3 scripts/orca/plan.py seed --run <run_id>
   python3 scripts/orca/plan.py seed --run <run_id> --apply
   ```

   Tickets already closed on GitHub, such as T01, are created with both BUILD and MERGE marked completed, so their dependents are ready straight away.

5. Complete each PRE task as the operator confirms it:

   ```sh
   orca-ide orchestration task-update --id <pre task> --status completed --run <run_id>
   ```

## The coordinator loop

1. Run `plan.py next --run <run_id>`.
2. For each free slot:
   1. Assign the ticket's GitHub issue to the operator. This is the claim, so a person browsing GitHub sees the ticket is taken.
   2. Run the printed `worker-start` command. It carries the ticket's model and effort from [Worker models](#worker-models).
3. Wait for worker messages, piping only stdout because keepalives go to stderr:

   ```sh
   orca-ide orchestration check --wait --types worker_done,escalation,question --timeout-ms 900000 --json
   ```

   Acknowledge each message with `check --ack <delivery_id>`. Answer questions with `reply --id <msg> --body ...`.

4. On each `worker_done`, review the PR against the ticket's acceptance criteria and the conflict rules below.
   - **Changes needed:**
     1. Create `FIX Tnn rK`, with the BUILD task as parent and the review findings as the spec.
     2. Start it on the same terminal and worktree:

        ```sh
        orca-ide orchestration worker-start --task <fix> --terminal <handle> --worktree <path>
        ```

        `--model` and `--effort` cannot be combined with `--terminal`, so the fix runs on the model the BUILD worker started with.

     3. Review again when its `worker_done` arrives.
   - **PR is good:** release the worker with `worker-release --dispatch <id>`.
5. Work the merge queue one PR at a time, highest first. For each:
   1. If `main` has moved, rebase the branch on it and rerun the suite.
   2. Run the operator live check, if the PR has one.
   3. Get the operator's explicit approval.
   4. Merge:

      ```sh
      gh pr merge <pr> --merge --delete-branch
      ```

      The issue closes through "Closes #N".
   5. Mark the MERGE task complete:

      ```sh
      orca-ide orchestration task-update --id <merge task> --status completed --result '{"pr": <pr>, "merge_sha": "<sha>"}' --run <run_id>
      ```

   6. Remove the ticket's worktree:

      ```sh
      orca-ide worktree rm --worktree path:<worktree>
      ```

      Neither `worker-release` nor `--delete-branch` removes it. Remove it only after the merge, because a FIX task reuses it until then. Orca also deletes the local branch once it can prove the branch is merged.

6. Go back to step 1. Every merge can unblock new BUILD tasks.

A task that fails three times in a row is circuit-broken to `failed` by Orca. The coordinator reads its reports with `worker-show` and `worker-read`, then either:

- fixes the spec and resets the task to `ready`, or
- takes the question to the operator.

### Recovering the coordinator

After a restart or compaction, Orca and GitHub hold all the state; the coordinator's memory holds none of it. To recover:

```sh
orca-ide orchestration run-use --id <run_id>
python3 scripts/orca/plan.py next --run <run_id>
orca-ide orchestration worker-list --json
orca-ide orchestration inbox --json
python3 scripts/orca/plan.py frontier
```

These answer four questions:

- `next` shows what is ready and what is waiting to merge.
- `worker-list` shows who is still working.
- `inbox` shows messages that are still unread.
- `frontier` confirms that GitHub agrees with Orca.

A ready BUILD task in Orca should match an unclaimed frontier ticket on GitHub. If the two disagree, a merge was not recorded with `task-update`.

## Conflict rules for workers

Parallel branches collide in a few predictable places. Every worker follows these rules, and the BUILD spec points them here.

- **Migrations.** Add new migrations only, with a timestamp prefix so that two branches never claim the same number. Never edit a migration that is already on `main`. Migrations stay additive: new columns are nullable or have a default.
- **Dependencies.** Add packages with `uv add`. When rebasing, take `main`'s `uv.lock` and run `uv lock` again; never hand-merge the lockfile.
- **Compose file.** Change only the services the ticket owns. Keep each service's block contiguous so that parallel edits to different services merge cleanly.
- **Tests.**
  - Tests must run while other worktrees run theirs.
  - Never bind a fixed host port.
  - Never share a Compose project name: derive it from the worktree.
  - Every test database is throwaway.
- **Scope.** Build only the ticket's scope. Anything found out of scope goes into the PR description for the coordinator, not into the code.

## The worker's contract

`plan.py seed` writes this into every BUILD task's spec, filling in the ticket's number, title, issue and lane:

1. Read the issue with `gh issue view`, then the spec documents it links under `docs/architecture/`, then this plan.
2. Build only this ticket, in the worktree Orca created from `origin/main`. Never commit to or push `main`.
3. Meet every acceptance criterion that can be met without real credentials, the server or a live market session. For the rest, write an opt-in live check and describe it in the PR.
4. Never use SSH or contact the server. Never read, create or commit real env files.
5. Follow the conflict rules above.
6. Run the full suite with the repo's single test command.
7. Open a PR titled `Tnn: <title>`:
   - The body starts with `Closes #N`.
   - It maps the work to each acceptance criterion.
   - It has an **Operator live check** section, which says "None" when there is none.

   Do not merge. Do not edit any issue, and never touch issue #1.
8. If blocked on a decision, ask the coordinator rather than guessing.
9. Send `worker_done` once, with the PR URL on the first line of the body. The outcome is `succeeded` only if the PR is open and the suite passes.
