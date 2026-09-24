-- The certified-model registry (docs/architecture/08): one immutable row per certification
-- artifact the research profile imports. The gates certify stage 1 of the ranker only, the
-- classical LightGBM ranker, and never the LLM panel or the blend; `certifies` says so on
-- every row. A run that failed its gates is recorded too, with `passed` false, so a failed
-- certification is evidence rather than something that quietly never happened. Promotion,
-- voiding and the live pipeline's refusals are `certification_event` and `promotion_event`,
-- which belong to the server CLI and are not created here.

-- The research profile's one server-side write: the artifact import runs as `research`.
GRANT USAGE ON SCHEMA agb TO research;

-- Every metric is what the gates measured on the CPCV paths, net of 1x modelled cost unless
-- the column says otherwise. `declared_trials` is the trial ledger's row count at
-- certification; the ledger ships inside the artifact as trials.parquet, whose sha256 is
-- `ledger_sha256`, and the import refuses an artifact whose ledger has a different count.
-- `artifact_sha256` covers every file in the directory except the manifest that lists them.
-- `gates` holds each gate's measured value, threshold and verdict; `path_quantiles` the
-- CPCV path distribution of maximum drawdown, longest losing run and tracking dispersion,
-- from which the performance alerts in docs/architecture/04 take their thresholds.
CREATE TABLE agb.model_certification (
    id                   bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    model_id             text NOT NULL UNIQUE CHECK (model_id ~ '^[A-Za-z0-9][A-Za-z0-9._-]*$'),
    certifies            text NOT NULL CHECK (certifies = 'stage 1 (classical ranker) only'),
    passed               boolean NOT NULL,
    artifact_sha256      text NOT NULL CHECK (artifact_sha256 ~ '^[0-9a-f]{64}$'),
    artifact_path        text NOT NULL CHECK (artifact_path <> ''),
    ledger_sha256        text NOT NULL CHECK (ledger_sha256 ~ '^[0-9a-f]{64}$'),
    features             jsonb NOT NULL CHECK (jsonb_typeof(features) = 'array'),
    train_start          date NOT NULL,
    train_end            date NOT NULL CHECK (train_end >= train_start),
    declared_trials      integer NOT NULL CHECK (declared_trials > 0),
    dsr                  double precision NOT NULL CHECK (dsr BETWEEN 0 AND 1),
    pbo                  double precision NOT NULL CHECK (pbo BETWEEN 0 AND 1),
    sharpe_net           double precision NOT NULL,
    dsr_2x               double precision NOT NULL CHECK (dsr_2x BETWEEN 0 AND 1),
    break_even_multiple  double precision CHECK (break_even_multiple >= 0),
    sharpe_gross         double precision NOT NULL,
    gross_sharpe_flag    boolean NOT NULL,
    participation_max    double precision NOT NULL CHECK (participation_max >= 0),
    participation_mean   double precision NOT NULL CHECK (participation_mean >= 0),
    cost_model_version   text NOT NULL CHECK (cost_model_version <> ''),
    certified_capital    numeric NOT NULL CHECK (certified_capital > 0),
    path_quantiles       jsonb NOT NULL CHECK (jsonb_typeof(path_quantiles) = 'object'),
    gates                jsonb NOT NULL CHECK (jsonb_typeof(gates) = 'object'),
    certified_at         timestamptz NOT NULL,
    actor                agb.actor NOT NULL,
    at                   timestamptz NOT NULL DEFAULT now()
);
SELECT agb.make_append_only('agb.model_certification');

GRANT SELECT ON agb.model_certification
    TO scheduler, pipeline, execution, bot, ui, cli, research;
GRANT INSERT ON agb.model_certification TO research;

-- Every login role can ask whether the system is overdue (tests/test_overdue.py). The view
-- calls agb.overdue_at as its caller, so the tables it reads need the grant too.
GRANT SELECT ON agb.expected_event, agb.expected_event_completion, agb.overdue TO research;
