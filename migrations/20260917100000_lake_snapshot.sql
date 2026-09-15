-- The Parquet lake's snapshots. Each Sharadar pull records the date it covers and the SHA-256 of
-- every lake partition it wrote, one row per partition (src/agb/lake.py has the layout). The
-- lake's files live on the server's data disk; this table is how the database knows what landed.
--
-- A partition is recorded once per distinct hash: landing the same bytes again adds nothing,
-- and a partition the vendor revised gets a new row next to the old one. `job_id` is the
-- `data_pull` job that landed it, or NULL for the one-off backfill.

CREATE TABLE agb.lake_snapshot (
    id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    snapshot_date date NOT NULL,
    dataset       text NOT NULL CHECK (dataset IN ('sep', 'sf1', 'tickers')),
    partition     text NOT NULL CHECK (partition ~ '^[a-z0-9]+/[a-z]+=\d{4}-\d{2}-\d{2}$'),
    sha256        text NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
    row_count     bigint NOT NULL CHECK (row_count >= 0),
    job_id        bigint REFERENCES agb.job (id),
    actor         agb.actor NOT NULL,
    recorded_at   timestamptz NOT NULL DEFAULT now(),
    UNIQUE (dataset, partition, sha256),
    CHECK (partition LIKE dataset || '/%')
);

SELECT agb.make_append_only('agb.lake_snapshot');

GRANT SELECT, INSERT ON agb.lake_snapshot TO pipeline;
GRANT SELECT ON agb.lake_snapshot TO scheduler, execution, bot, ui, cli;
