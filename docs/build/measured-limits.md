# Measured memory limits, run times, and the panel-store check

**Source:** [#45 Measured memory limits and panel-store integrity check](https://github.com/agb1986/agb-trader/issues/45)
**Design:** [07 — Services and deployment](../architecture/07-services-deployment.md)
**Date:** 2026-10-24

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
| (a) Peak memory per service during a full rebalance | Tool built and tested; **the sizing run on the server is owed.**          |
| (b) Every limit derived from its measurement        | Arithmetic fixed and tested; limits move when (a) lands.                  |
| (c) Poll intervals and leases from measured times   | `agb runtimes` built; current values below are the unmeasured defaults.   |
| (d) Nightly hash check of a sample of panel bodies  | Done: scheduled job, recorded result, alert on an unsound body.           |

Nothing below invents a number for (a) or (b). The table further down is a workstation
baseline taken while two services were idle, which is useful only as a demonstration that the
tool reads docker correctly; it is **not** the sizing run and no limit may be changed from it.

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

`db`'s 512m stays until a rebalance has been measured against it: an idle Postgres with no
lake query running says nothing about Postgres under the pipeline's reads.

## The sizing run the operator owes

This is the one step nothing in the repo can do, because it needs the real server, real
vendor data, a real panel run and the Anthropic spend that goes with it. Run it once, paste
the table into this file under a dated heading, and only then move the limits.

1. On the server, check the stack is up and that a rebalance is due:
   ```
   ssh casaos.local
   cd /mnt/data/AppData/agb-trader
   ./agb-stack status
   ./agb-stack exec -T db psql -U postgres -d agb -c "SELECT * FROM agb.job_state ORDER BY id DESC LIMIT 10"
   ```
2. From the workstation, start the sampler against the server and leave it running:
   ```
   ssh casaos.local 'python3 - --interval 5 --out /mnt/data/AppData/agb-trader/peaks.md' \
       < src/agb/measure.py
   ```
3. Let the full rebalance run — the data pull, the feature build, the panel with its model
   calls, the gate, the basket. The panel is the heaviest stage and the one worth not missing.
4. Interrupt the sampler once the basket exists. Collect the table:
   ```
   scp casaos.local:/mnt/data/AppData/agb-trader/peaks.md .
   ```
5. For each service, set `mem_limit` in `compose.yaml` to the "limit it argues for" column,
   except where that is *above* what the box can afford — in which case the finding is that
   the box cannot afford that service as written, and it goes in an issue rather than into a
   limit that will be hit.
6. Check the total: the sum of the limits of the services that run at once has to leave the
   household stack its share of 3.6 GB. The pipeline and the research profile never run on the
   server at the same time, and `migrate`, `cli`, `reconcile` and `certification-import` are
   one-shot.
7. Deploy, run a second full rebalance under the new limits, and confirm it completes with no
   container exiting 137. That second run is criterion (b); the first run alone is not.

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
200 of the hashes the panel tables reference, reads each body back out of the store, and records
what it found in `agb.panel_audit`: how many bodies are referenced, how many were checked, how
many read back sound, and the hashes of any that were missing or corrupt.

A body is **missing** when there is no file. It is **corrupt** when there is a file and it does
not hash to its name, including when the file does not decompress at all — `PanelStore.get`
turns a `ZstdError` into the same `StoreError` a hash mismatch raises, so broken compression is
reported as corruption and not as a crash.

The sample exists because the store grows without bound and the box is a Celeron: reading every
body every night would eventually be the largest thing the server does. Each night draws its own
sample, so the store is covered over time rather than at a point in time, and a cited body is
counted once however many rows cite it.

The job **completes** even when it finds an unsound body: the finding is the store's, not the
job's, and a failed job would be reclaimed and re-run to find the same thing again. What raises
the alarm is `health.panel_bodies`, which reads the most recent row and returns a
`panel_body_unsound` finding when anything in it was missing or corrupt. The bot sends that as
a health alert naming the counts and the first unsound hash, once, and the UI shows it.

### Verifying it on the server

Because this is the one criterion with a deliberate-corruption test, it can be checked for real
without a rebalance:

1. Pick a body the database cites — the store has bodies only after a panel run — and copy
   it somewhere safe:
   ```
   cd /mnt/data/AppData/agb-trader
   ./agb-stack exec -T db psql -U postgres -d agb -tAc \
       "SELECT document_sha256 FROM agb.panel_filing LIMIT 1"
   cp panel/<xx>/<sha>.zst ~/<sha>.zst.keep
   ```
2. Overwrite the body with something that is not its own content:
   ```
   printf 'not a zstd frame' > panel/<xx>/<sha>.zst
   ```
3. Either wait for 04:00, or insert tomorrow's job now, as the scheduler would, and let the
   pipeline's next poll claim it:
   ```
   ./agb-stack exec -T db psql -U postgres -d agb -c \
       "WITH j AS (INSERT INTO agb.job (kind, period, session_date, not_before, created_by)
                   VALUES ('panel_audit', '2026-10-25', '2026-10-25', now(), 'scheduler')
                   RETURNING id)
        INSERT INTO agb.job_event (job_id, event, actor)
        SELECT id, 'created', 'scheduler' FROM j"
   ```
   Pick a period no `panel_audit` job has yet: `(kind, period)` is unique, so re-running a
   period inserts nothing rather than auditing twice.
4. Read what it found, and expect the Telegram alert naming the counts and the first unsound
   hash:
   ```
   ./agb-stack exec -T db psql -U postgres -d agb -c \
       "SELECT period, bodies, checked, sound, missing, corrupt FROM agb.panel_audit
        ORDER BY id DESC LIMIT 1"
   ./agb-stack logs --tail 50 bot
   ```
5. Put the body back from the copy, and confirm the next audit's row is sound again:
   ```
   cp ~/<sha>.zst.keep panel/<xx>/<sha>.zst
   ```

With a sample of 200, a single corrupted body in a large store is found with probability
200/bodies on any one night, so a store of a few thousand bodies surfaces it within days rather
than on the first night. For a test that has to be deterministic, corrupt a body and run the
job with a sample larger than the store.
