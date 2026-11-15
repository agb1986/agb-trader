# Measured memory limits, run times, and the panel-store check

**Source:** [#45 Measured memory limits and panel-store integrity check](https://github.com/agb1986/agb-trader/issues/45)
**Design:** [07 — Services and deployment](../architecture/07-services-deployment.md)
**Date:** 2026-10-24, limits set from the server sizing run 2026-11-05

Three of the numbers this stack runs on were left to the build because they cannot be
reasoned out: how much memory each service really needs, how long each job really takes, and
whether the panel bodies on disk are still the bodies their hashes say they are. This is the
procedure that produces the first two from a real run and the job that keeps answering the
third every night.

The server is a Celeron N3450 with about 3.6 GB available and swap already fully consumed,
shared with a seventeen-container household stack. A memory limit set too high is not a
limit; set too low it kills a legitimate rebalance with exit code 137 and no application-level
explanation. So the limits in `compose.yaml` are to be moved only by a measurement, and this
file records both the measurement and the arithmetic between it and the limit.

## What is measured here and what the operator still owes

| Criterion                                          | State                                                                     |
| -------------------------------------------------- | ------------------------------------------------------------------------- |
| (a) Peak memory per service during a full rebalance | Done: 68455 readings on the server across the 2026-11 rebalance, below.    |
| (b) Every limit derived from its measurement        | Limits set from (a); **the second rebalance under them is owed.**         |
| (c) Poll intervals and leases from measured times   | `agb runtimes` built; current values below are the unmeasured defaults.   |
| (d) Nightly hash check of a sample of panel bodies  | Done in code and tested; the corruption check on the server is scripted.  |

Nothing below invents a number for (a) or (b). Two tables follow and only one of them is a
measurement of this stack under load: the server sizing run of 2026-11-05, which is where every
limit in `compose.yaml` now comes from. The other is a workstation baseline taken while two
services were idle, kept because it is the demonstration that the tool reads docker correctly;
it is **not** the sizing run and no limit may be changed from it.

## The sampler

`src/agb/measure.py` samples `docker stats` and reports, per compose service, the highest
memory it saw the container holding, the limit in force, and the limit that peak argues for.

On the workstation, against this checkout's stack:

```
make measure                 # until interrupted, table to standard output
make measure OUT=peaks.md    # and to a file
```

It depends on nothing outside the standard library, so it also runs on the server, which has
Docker and a system Python but no project environment and no checkout:

```
ssh casaos.local 'python3 - --out peaks.md' < src/agb/measure.py
```

That system Python is 3.9, where the rest of this project is 3.12, so `src/agb/measure.py` is
held to 3.9: it postpones its annotations and uses nothing newer. `tests/test_measure.py` runs
it under a 3.9 interpreter to keep that true, and skips if one is not installed, which
`uv python install 3.9` fixes.

It samples every 5 seconds by default, stops on the first `Ctrl-C` (or `SIGTERM`), and writes
its table then. `--interval`, `--project` and `--samples` are there for a shorter leash.

Only containers labelled with this compose project are read, so the household stack's
seventeen containers are left out of both the table and the arithmetic.

### From a peak to a limit

A measured peak is one run's high-water mark, not the worst run's. The limit adds headroom on
top of it rather than pretending otherwise:

- **half again** as much as the peak (`HEADROOM = 1.5`),
- but **never less than 64 MiB more** than the peak (`FLOOR`), which is what keeps a service
  measured at almost nothing from getting a limit of almost nothing,
- rounded **up to the next 32 MiB** (`STEP`), so the limits stay readable,
- written in the `m`/`g` form `mem_limit` takes.

So a peak of 50 MiB argues for `128m`, 300 MiB for `480m`, and 900 MiB for `1376m`.

### The caveat that matters

Sampling is the honest part and the weak part. A spike shorter than the sampling interval can
fall between two readings, so **every peak here is a lower bound on the true peak.** Two
consequences, both deliberate:

- The headroom rule above is not a safety margin against a bad guess; it is the margin
  against the spike the sampler missed and against the run heavier than the one measured.
- An exit code 137 after these limits are applied is evidence about the measurement, not just
  about the limit. The response is to re-measure with a shorter `--interval` during the
  failing stage, not to raise the limit until it stops happening.

### Workstation baseline, 2026-10-24 — not the sizing run

Taken with only `db` and `ui` up and idle on the workstation, three readings two seconds
apart. It shows the tool reading docker correctly and nothing else: an idle `ui` is not a `ui`
serving the evidence views, and the workstation is not the server.

| service | container | samples | peak | current limit | limit it argues for |
| --- | --- | --: | --: | --: | --: |
| db | agb-trader-db-1 | 3 | 50m | 512m | 128m |
| ui | agb-trader-ui-1 | 3 | 155m | 256m | 256m |

`db`'s 512m stood until a rebalance had been measured against it, because an idle Postgres with
no lake query running says nothing about Postgres under the pipeline's reads. The sizing run
below measured it and the limit moved.

## The sizing run the operator owes

This is the one step nothing in the repo can do on its own, because it needs the real server,
real vendor data, a real panel run and the Anthropic spend that goes with it. What *is* in the
repo is everything around it: `scripts/deploy/sizing.sh` checks the stack can produce a
measurement, starts and stops the sampler on the server, and brings the table back.

### When

A rebalance is the measurement, so the run happens when the next one does rather than when it
is convenient. `check` says when that is, from the same planner the scheduler uses:

```
make sizing CMD=check
```

It refuses the run early if it would measure the wrong thing: nothing deployed, the pipeline
not running, no promoted model (the job fails before it proposes a basket; while no model has
passed its gates, promote one under the paper-only measurement waiver,
`docs/build/deploy.md`), or no `panel_config` (the rebalance is stage-1 only and the panel, the
heaviest stage, never runs). It also takes two readings through the server's own Docker, which is a different engine from
the workstation's, so a format this cannot parse is found days before the run and not during
it.

Forcing a rebalance earlier is possible — insert the job as the scheduler would — but `(kind,
period)` is unique, so it needs a period no rebalance has used, and that leaves a basket under
a reference that means nothing and has to be voided. Waiting for the real one is cheaper.

### The run

```
make sizing CMD=start     # before the rebalance becomes claimable
make sizing CMD=watch     # as often as you like while it runs
make sizing CMD=stop      # once the pipeline logs the proposed basket
```

`start` copies `src/agb/measure.py` to the server and runs it detached in its own session, so
a dropped SSH connection does not take the measurement with it. The sampler rewrites its table
after every reading, so `watch` shows the peaks as they stand, alongside the pipeline's last
ten lines and any container that has exited 137 during the run — the one thing that invalidates
the readings, and worth seeing while there is still time to re-measure with a shorter
`--interval`.

`docker stats --no-stream` itself takes a second or two per call on a Celeron, so the real
cadence is a little longer than `AGB_SIZING_INTERVAL` asks for.

`stop` sends one SIGTERM, which is how the sampler is asked to write its table and exit, and
copies the table to `peaks.md` on the workstation.

### Turning the table into limits

1. For each service, set `mem_limit` in `compose.yaml` to the "limit it argues for" column,
   except where that is *above* what the box can afford — in which case the finding is that
   the box cannot afford that service as written, and it goes in an issue rather than into a
   limit that will be hit.
2. Check the total: the sum of the limits of the services that run at once has to leave the
   household stack its share of 3.6 GB. The pipeline and the research profile never run on the
   server at the same time, and `migrate`, `cli`, `reconcile` and `certification-import` are
   one-shot.
3. Paste the table into this file under a dated heading, so the limit above and the reading it
   came from are read together.
4. Deploy, and run a second full rebalance under the new limits with no container exiting 137.
   That second run is criterion (b); the first run alone is not, and since a rebalance is
   monthly, (b) lands a month after (a) unless a forced run is accepted.

### Server sizing run, 2026-11-05 — the reading the limits come from

68455 readings on casaos.local, from 2026-10-30 to 2026-11-05 09:08 UTC, through the server's
own Docker. The window covers the 2026-11 rebalance that became claimable at 2026-10-30 22:00
UTC, including its stage-2 panel run: the EDGAR fetches and the model calls, which is the
heaviest thing this stack does and the reason the run had to wait for a real rebalance. No
container exited 137 during the window, and `pipeline` finished the panel with `RestartCount 0`
and `OOMKilled false`, so the readings stand.

| service | container | samples | peak | limit then | limit it argues for | limit now |
| --- | --- | --: | --: | --: | --: | --: |
| backup | agb-trader-backup-1 | 68455 | 81m | 256m | 160m | 160m |
| bot | agb-trader-bot-1 | 68455 | 105m | 256m | 192m | 192m |
| db | agb-trader-db-1 | 68455 | 95m | 512m | 160m | 160m |
| execution | agb-trader-execution-1 | 68455 | 105m | 256m | 192m | 192m |
| pipeline | agb-trader-pipeline-1 | 68455 | 1441m | 2g | 2176m | 2176m |
| scheduler | agb-trader-scheduler-1 | 68455 | 104m | 256m | 192m | 192m |
| ui | agb-trader-ui-1 | 68455 | 100m | 256m | 192m | 192m |
| vpn | agb-trader-vpn-1 | 68455 | 243m | 256m | 384m | 384m |

Every limit is the column it argues for, so step 1 above took no judgement. Three of them are
worth reading twice:

- **`vpn` was the one at risk.** 243m against a 256m limit, drifting up over the week
  (222m, 223m, 243m) as gluetun's tunnel stayed open. It is the only service that was anywhere
  near its limit, and the only one whose limit went up for safety rather than down for room.
- **`pipeline` rose from 99m to 1441m during the 28-minute panel run** and stayed inside its 2g
  with about 600m to spare — but 1441m argues for 2176m, which is *above* the old limit. The
  limit went up: a peak this close to a cap on the heaviest stage is the case the headroom rule
  exists for.
- **`db` dropped from 512m to 160m.** 95m is Postgres under the gate's 200 checks and the panel
  writes, not an idle Postgres, so the reading is a real one. It is still one rebalance, and
  one whose execution filled only 3 of 20 orders, so a month with a full basket to reconcile is
  the run that tests it.

The one-shots (`migrate`, `cli`, `reconcile`, `certification-import`) are not in the table
because they exit long before a sampling interval means anything for them. They stay at 256m.
`research` stays at 40g: it runs on the workstation only, never on the server.

The total, which is step 2: the eight long-running limits sum to **3648m**, down from 4096m.
That is more than the box has, deliberately — `mem_limit` is a cap, not a reservation, and what
the stack actually holds at once is the peak column, **2274m**, of which `pipeline`'s 1441m
happens for under half an hour a month. The sum is the worst case the kernel would allow, not
the working set, and it is now 448m lower than it was.

Criterion (b) is the next rebalance under these limits with nothing exiting 137. The 2026-11
rebalance is halted at the turnover check, so that is 2026-12 unless the halt is resolved and a
run is forced.

`tests/test_compose.py` enforces that every service still declares a limit, so a limit cannot
be deleted in passing, only changed.

## Run times, poll intervals and leases

`agb runtimes` reports how long each job kind has actually taken, from the claim that started
it to the completion that ended it, reading the event log:

```
make agb CMD=runtimes
```

```
# the shape of the output; these are not readings from the server
KIND               RUNS   FASTEST    MEDIAN   SLOWEST
backup               42       31s     1m 2s     4m 9s
panel_audit           9        4s       11s    1m 36s
rebalance             3    12m 4s    25m 1s     1h 8m
```

A job that is still running has no length yet and is left out; so is a job that failed, since
how long work took to fail is not how long the work takes. A reclaimed job is measured from
the reclaim, so the half hour a dead holder's lease spent expiring is not counted as work.
The median is one of the readings rather than an average of them, so every number printed is a
run that really happened.

What the numbers are for:

| Setting                                                   | Now         | What it has to clear                                          |
| --------------------------------------------------------- | ----------- | ------------------------------------------------------------- |
| `AGB_HEARTBEAT_SECONDS` (every service's poll and beat)    | 30s         | Short against the shortest deadline the stack owes.           |
| `health.STALE_AFTER` (when a silent service is flagged)    | 5m          | Several beats, so one slow cycle is not an alert.             |
| `pipeline.LEASE`, `backup.LEASE`                           | 1h          | Longer than the **slowest** run of the longest job it claims. |
| `pipeline.RETRY` (how long a failed job waits)             | 15m         | Long enough that a vendor outage is not hammered.             |

The rule for a lease is the slowest column, not the median: a lease that expires mid-run hands
the job to a second worker while the first is still holding it. Once a rebalance has run on the
server a few times, read `agb runtimes` and raise `pipeline.LEASE` above its slowest rebalance
with room to spare, or lower it if the slowest is far under the hour.

These four are still the defaults chosen before anything was measured. They are listed here so
that the first real reading has somewhere to go.

## The nightly panel-store check

Criterion (d), and the one part of this that is finished.

The panel store is content-addressed zstd on `/mnt/data`: a filing's document and text, each
model request and response, and each scrubbed extract live as `<sha[:2]>/<sha>.zst`, and
Postgres holds only the hash. Nothing in the database can tell that a file has gone missing or
that its bytes have rotted — the hash in the row is what the file was, not what it is.

So the scheduler plans a `panel_audit` job every night at **04:00 New York**, an hour after the
backup, with a two-hour grace period. The pipeline claims it, draws a random sample of up to
100,000 of the hashes (the whole store, at its present size) the panel tables reference, reads each body back out of the store, and records
what it found in `agb.panel_audit`: how many bodies are referenced, how many were checked, how
many read back sound, and the hashes of any that were missing or corrupt.

A body is **missing** when there is no file. It is **corrupt** when there is a file and it does
not hash to its name, including when the file does not decompress at all — `PanelStore.get`
turns a `ZstdError` into the same `StoreError` a hash mismatch raises, so broken compression is
reported as corruption and not as a crash.

The audit reads the whole store each night, up to a cap (`panel_audit.SAMPLE`, 100,000 bodies),
so each night's result is a statement about the store and not about the bodies drawn. The cap
is there because the store grows without bound and the box is a Celeron. Past it, each night
draws its own random sample, so the store is covered over time rather than at a point in time.
A cited body is counted once however many rows cite it.

The cap is sized from a reading. On 2026-11-08 the job was run on the server with
`AGB_PANEL_AUDIT_SAMPLE=776`, the whole store: it was claimed at 10:44:12Z and the check
finished at 10:44:14Z, so **776 bodies read back in 2 seconds**, about 390 bodies a second. The
earlier sample of 200 was a guess against the two-hour grace period, and that reading put it
four orders of magnitude under it. At the measured rate 100,000 bodies is about four minutes,
under 4% of the grace, so the cap stays honest until the store is well over a hundred times its
present size. When `agb runtimes` shows `panel_audit` approaching a fifth of the grace, lower
the cap with `AGB_PANEL_AUDIT_SAMPLE`, which is the escape hatch for a machine where the
measurement no longer holds.

The job **completes** even when it finds an unsound body: the finding is the store's, not the
job's, and a failed job would be reclaimed and re-run to find the same thing again. What raises
the alarm is `health.panel_bodies`, which reads the most recent row and returns a
`panel_body_unsound` finding when anything in it was missing or corrupt. The bot sends that as
a health alert naming the counts and the first unsound hash, once, and the UI shows it.

### Verifying it on the server

Because this is the one criterion with a deliberate-corruption test, it can be checked for real
without a rebalance — though not before one, since the store has bodies only after a panel run.
`scripts/deploy/panel-check.sh` drives it:

```
make panel-check CMD=check      # the store has cited bodies, and the sample will read the broken one
make panel-check CMD=corrupt    # back one body up on the server, then overwrite it
make panel-check CMD=run        # insert the audit job now and wait for what it finds
make panel-check CMD=restore    # put the body back, byte for byte
make panel-check CMD=status     # whether a body is broken, and the last three audits
```

`corrupt` copies the body to `panel-check/<sha>.zst.keep` beside the store, checks the copy
hashes the same as the original, records what it broke, and only then overwrites the body with
bytes that are not a zstd frame. It refuses to break a second body while one is still broken,
so there is exactly one thing to put back. `restore` copies it back and refuses to finish
unless the restored file hashes to what the marker recorded, which is the difference between
putting a body back and losing it. The store is the only copy of a panel body: the database
holds hashes, not bytes.

`run` inserts the `panel_audit` job as the scheduler would, for tomorrow's period by default,
and waits up to five minutes for the pipeline to claim it, read the sample, and record the row.
Then it prints the row and the bot's lines naming the store. It takes a period explicitly —
`make panel-check CMD="run 2026-10-26"` — because `(kind, period)` is unique, so a period that
already has an audit job inserts nothing instead of auditing twice.

### Making the check certain

While the store holds fewer bodies than the cap, one audit reads all of them, so a single
corrupted body is found on the first night and the drill needs no change on the server. Past the
cap a corrupted body is read on any one night with probability cap/bodies, which is right for
the nightly job and no use as a test.

So the cap is settable on the deploy machine, with `AGB_PANEL_AUDIT_SAMPLE` in
`env/pipeline.env`. `check` and `corrupt` read the store's size and the cap in force, and
`corrupt` refuses to break anything when the cap does not cover the store, printing the two
commands that fix it:

```
ssh casaos.local "echo AGB_PANEL_AUDIT_SAMPLE=<bodies> >> /mnt/data/AppData/agb-trader/env/pipeline.env"
ssh casaos.local "/mnt/data/AppData/agb-trader/agb-stack up -d pipeline"
```

Remove the line when the check is done: the nightly job has a two-hour grace period and
shares one slow disk with the household stack, and a cap sized for a test is not sized for
every night. The variable is deliberately absent from `env/pipeline.env.example`, since
`deploy.sh` requires the server to set every name the example does and this one is a temporary
override, not a setting the stack needs.
