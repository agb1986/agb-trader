# Migrations

Plain SQL files, applied in order by `python -m agb.migrate` (the one-shot `migrate` service
in Compose, which runs on every `make up`). Each file runs in its own transaction as the owner
role `agb_owner`, and is recorded in `agb.schema_migration` with its SHA-256.

## The rule

**Schema changes only add columns that are nullable or carry a default, and never backfill.**

History is append-only: every history table refuses `UPDATE`, `DELETE` and `TRUNCATE` for
every role, the owner included. A backfill is an `UPDATE`, so the trigger refuses it anyway.
History is corrected by appending a correcting event, never by editing the original row.

The same rule keeps an older image working against a newer schema, which is what a rollback
does.

## Conventions

- **File names** are `YYYYMMDDHHMMSS_slug.sql`, with a UTC timestamp and a lowercase slug. The
  timestamp prefix keeps parallel branches from colliding.
- **A migration is never edited once it is on main.** `migrate` refuses to run if an applied
  file's checksum has changed. Fix a mistake with a new migration.
- **Schema-qualify everything** as `agb.<name>`.
- **Every new history table** calls `SELECT agb.make_append_only('agb.<table>');`, which
  attaches the `BEFORE UPDATE OR DELETE` row trigger and the `BEFORE TRUNCATE` statement
  trigger. Never drop or disable those triggers.
- **Grants are explicit and minimal.** Service roles get `SELECT` and, where they write,
  `INSERT` on history tables, and never `UPDATE`, `DELETE` or `TRUNCATE`. The roles are
  `scheduler`, `pipeline`, `execution`, `bot`, `ui`, `backup` and `cli`. `backup` already
  reads every table through `pg_read_all_data`, so a new table needs no grant for it.
- **Row-level security never hides a row from `backup`.** The nightly dump runs as `backup`
  with `--enable-row-security`, and a policy that hides rows from it silently drops them from
  every backup. A table with row-level security keeps a permissive `SELECT` policy that is
  `USING (true)` for `backup`, as `agb.heartbeat` does. `tests/test_backup.py` checks this.
- **The mutable exceptions** are the ones spec 08 names: the `heartbeat` upsert and the job
  lease columns. `tests/test_append_only.py` checks every table in the `agb` schema against
  this list, so a new exception has to be declared there. On `agb.job`, workers hold
  `UPDATE` on `claimed_by` and `lease_until` only, and the `lease_only` trigger refuses any
  other change for every role, the owner included. `DELETE` and `TRUNCATE` are refused as on
  any history table.
- **Types.**
  - Money and share quantities are `numeric`, never floating point.
  - Timestamps are `timestamptz`, and every role's session runs in UTC.
  - Trading dates are `date` on the NYSE calendar.
  - Every history row carries an actor column of type `agb.actor`: a service, the operator's
    identity, or `system`.
- **Nothing is ever deleted.**
