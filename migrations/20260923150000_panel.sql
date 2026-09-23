-- The LLM panel's configuration and its evidence-gathering half: what it retrieved from EDGAR
-- for each shortlisted name, every model call it made, and the Analyst's verbatim quotes.
--
-- The full bodies (filing documents and texts, prompts, responses and the scrubbed extract)
-- live in the content-addressed panel store on the data disk, and these tables refer to them
-- by SHA-256 (docs/architecture/08, "Panel logs"). Nothing here is ever updated.

-- One immutable, versioned panel configuration per row: the model per role, the hashes of the
-- prompt templates keyed by role, the rubric and scrubber versions, the shortlist size N and
-- the blend weight w. Only the CLI creates rows; changing anything is a new row, and every
-- panel run and basket references the row it was built with.
CREATE TABLE agb.panel_config (
    id                bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    analyst_model     text NOT NULL CHECK (analyst_model <> ''),
    prosecutor_model  text NOT NULL CHECK (prosecutor_model <> ''),
    defender_model    text NOT NULL CHECK (defender_model <> ''),
    judge_model       text NOT NULL CHECK (judge_model <> ''),
    prompt_hashes     jsonb NOT NULL CHECK (jsonb_typeof(prompt_hashes) = 'object'),
    rubric_version    text NOT NULL CHECK (rubric_version <> ''),
    scrubber_version  text NOT NULL CHECK (scrubber_version <> ''),
    shortlist_n       integer NOT NULL CHECK (shortlist_n > 0),
    w                 numeric NOT NULL CHECK (w >= 0),
    created_at        timestamptz NOT NULL DEFAULT now(),
    actor             agb.actor NOT NULL
);
SELECT agb.make_append_only('agb.panel_config');

-- One panel run over a shortlist, as of the date nothing filed later may be read.
CREATE TABLE agb.panel_run (
    id               bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    panel_config_id  bigint NOT NULL REFERENCES agb.panel_config,
    job_id           bigint REFERENCES agb.job,
    as_of            date NOT NULL,
    started_at       timestamptz NOT NULL DEFAULT now(),
    actor            agb.actor NOT NULL
);
SELECT agb.make_append_only('agb.panel_run');

-- What EDGAR gave for each name. `outcome` is 'retrieved', 'no_filings' (nothing to read) or
-- 'edgar_failure' (the system failed, not the company; the blend gives the name the neutral
-- 0.5). A delinquent filer is flagged 'nt_10k' or 'late_10k' and its filings are still read;
-- a recent IPO read from its registration statement has `ipo` set. `aliases` are the names and
-- tickers the scrubber removed.
CREATE TABLE agb.panel_retrieval (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id      bigint NOT NULL REFERENCES agb.panel_run,
    symbol      text NOT NULL CHECK (symbol <> ''),
    cik         text CHECK (cik ~ '^\d{10}$'),
    company     text,
    outcome     text NOT NULL CHECK (outcome IN ('retrieved', 'no_filings', 'edgar_failure')),
    delinquent  text CHECK (delinquent IN ('nt_10k', 'late_10k')),
    ipo         boolean NOT NULL DEFAULT false,
    detail      text,
    aliases     text[] NOT NULL DEFAULT '{}',
    UNIQUE (run_id, symbol),
    CHECK ((outcome = 'retrieved') = (detail IS NULL))
);
SELECT agb.make_append_only('agb.panel_retrieval');

-- Each piece of a filing the Analyst read, as the document id it was cited by ('D1', ...).
-- `document_sha256` is the filing's primary document as EDGAR served it, `text_sha256` the
-- section's plain text; `sent_chars` is how much of that text fitted in the request.
CREATE TABLE agb.panel_filing (
    id               bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    retrieval_id     bigint NOT NULL REFERENCES agb.panel_retrieval,
    document         text NOT NULL CHECK (document ~ '^D\d+$'),
    form             text NOT NULL CHECK (form <> ''),
    accession        text NOT NULL CHECK (accession <> ''),
    filed            date NOT NULL,
    report_date      date,
    section          text NOT NULL CHECK (section <> ''),
    document_sha256  text NOT NULL CHECK (document_sha256 ~ '^[0-9a-f]{64}$'),
    text_sha256      text NOT NULL CHECK (text_sha256 ~ '^[0-9a-f]{64}$'),
    chars            integer NOT NULL CHECK (chars >= 0),
    sent_chars       integer NOT NULL CHECK (sent_chars >= 0 AND sent_chars <= chars),
    UNIQUE (retrieval_id, document)
);
SELECT agb.make_append_only('agb.panel_filing');

-- Every model call, with the model asked for and the model that answered. The request body's
-- SHA-256 is the cache key: a call whose request matches an earlier one is answered from the
-- store, and is logged here with `cached` set and the earlier call's response.
CREATE TABLE agb.panel_model_call (
    id                     bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id                 bigint NOT NULL REFERENCES agb.panel_run,
    retrieval_id           bigint NOT NULL REFERENCES agb.panel_retrieval,
    role                   text NOT NULL
                           CHECK (role IN ('analyst', 'prosecutor', 'defender', 'judge')),
    model                  text NOT NULL CHECK (model <> ''),
    response_model         text NOT NULL CHECK (response_model <> ''),
    request_sha256         text NOT NULL CHECK (request_sha256 ~ '^[0-9a-f]{64}$'),
    response_sha256        text NOT NULL CHECK (response_sha256 ~ '^[0-9a-f]{64}$'),
    cached                 boolean NOT NULL,
    input_tokens           integer NOT NULL CHECK (input_tokens >= 0),
    output_tokens          integer NOT NULL CHECK (output_tokens >= 0),
    cache_read_tokens      integer NOT NULL CHECK (cache_read_tokens >= 0),
    cache_creation_tokens  integer NOT NULL CHECK (cache_creation_tokens >= 0),
    stop_reason            text,
    request_id             text,
    at                     timestamptz NOT NULL DEFAULT now()
);
SELECT agb.make_append_only('agb.panel_model_call');
CREATE INDEX panel_model_call_request ON agb.panel_model_call (request_sha256, id);

-- The Analyst's result for a name it read: 'extracted', with the scrubbed extract the debate
-- roles read, or 'analyst_failure' with the reason.
CREATE TABLE agb.panel_extract (
    id                bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    retrieval_id      bigint NOT NULL UNIQUE REFERENCES agb.panel_retrieval,
    model_call_id     bigint REFERENCES agb.panel_model_call,
    outcome           text NOT NULL CHECK (outcome IN ('extracted', 'analyst_failure')),
    detail            text,
    scrubber_version  text NOT NULL CHECK (scrubber_version <> ''),
    extract_sha256    text CHECK (extract_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK ((outcome = 'extracted') = (extract_sha256 IS NOT NULL)),
    CHECK ((outcome = 'extracted') = (detail IS NULL))
);
SELECT agb.make_append_only('agb.panel_extract');

-- Every quote the Analyst returned, verified or not. A verified quote was found verbatim in
-- the document it cites and carries its scrubbed text; a rejected one says why and goes no
-- further.
CREATE TABLE agb.panel_quote (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    extract_id  bigint NOT NULL REFERENCES agb.panel_extract,
    filing_id   bigint REFERENCES agb.panel_filing,
    dimension   smallint CHECK (dimension BETWEEN 1 AND 6),
    document    text NOT NULL,
    section     text NOT NULL,
    quote       text NOT NULL,
    verified    boolean NOT NULL,
    rejection   text,
    scrubbed    text,
    CHECK (verified = (rejection IS NULL)),
    CHECK (verified = (scrubbed IS NOT NULL)),
    CHECK (NOT verified OR (filing_id IS NOT NULL AND dimension IS NOT NULL))
);
SELECT agb.make_append_only('agb.panel_quote');

GRANT SELECT ON agb.panel_config, agb.panel_run, agb.panel_retrieval, agb.panel_filing,
    agb.panel_model_call, agb.panel_extract, agb.panel_quote
    TO scheduler, pipeline, execution, bot, ui, cli;
GRANT INSERT ON agb.panel_config TO cli;
GRANT INSERT ON agb.panel_run, agb.panel_retrieval, agb.panel_filing, agb.panel_model_call,
    agb.panel_extract, agb.panel_quote TO pipeline;
