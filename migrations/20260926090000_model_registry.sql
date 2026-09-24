-- The certified-model registry's server side (docs/architecture/08): which certification is
-- live, which have been voided, and which one each basket was built with.
--
-- The research profile imports certifications (agb.model_certification); the operator
-- promotes one with `agb promote <model id>`, roughly once a year, and the pipeline ranks
-- with the promoted model alone. Every promotion and every voiding is a CLI command, so each
-- names the agb.command row that asked for it, and a refused `agb promote` is a refused
-- command with its reason. A certification is voided, never deleted: when the cost model it
-- was certified under is superseded, `agb void-stale` (which `agb promote` also runs first)
-- voids it, and a voided certification can never be promoted again.

-- Voiding a certification. At most one per certification; the cost-model version is the one
-- in force when it was voided, which for a stale certification is the version that
-- superseded its own.
CREATE TABLE agb.certification_event (
    id                  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    certification_id    bigint NOT NULL REFERENCES agb.model_certification,
    event               text NOT NULL CHECK (event IN ('voided')),
    reason              text NOT NULL CHECK (reason <> ''),
    cost_model_version  text NOT NULL CHECK (cost_model_version <> ''),
    command_id          bigint NOT NULL REFERENCES agb.command,
    actor               agb.actor NOT NULL,
    at                  timestamptz NOT NULL DEFAULT now()
);
SELECT agb.make_append_only('agb.certification_event');
CREATE UNIQUE INDEX certification_event_voided_once ON agb.certification_event (certification_id)
    WHERE event = 'voided';

-- Promoting a certification to live. The latest row is the live model; promoting an earlier
-- certification again is a new row.
CREATE TABLE agb.promotion_event (
    id                bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    certification_id  bigint NOT NULL REFERENCES agb.model_certification,
    command_id        bigint NOT NULL UNIQUE REFERENCES agb.command,
    actor             agb.actor NOT NULL,
    at                timestamptz NOT NULL DEFAULT now()
);
SELECT agb.make_append_only('agb.promotion_event');

-- The live model: the latest promotion and its certification, with whether that
-- certification has since been voided. No row means nothing has been promoted. A voided live
-- model stays here, voided, until another is promoted: the pipeline and the gate refuse it
-- rather than fall back to anything.
CREATE VIEW agb.promoted_model AS
SELECT p.id AS promotion_id, c.id AS certification_id, c.model_id, c.artifact_sha256,
       c.artifact_path, c.declared_trials, c.passed, c.cost_model_version, c.features,
       p.actor AS promoted_by, p.at AS promoted_at,
       v.id IS NOT NULL AS voided, v.at AS voided_at, v.reason AS voided_reason
FROM (SELECT * FROM agb.promotion_event ORDER BY id DESC LIMIT 1) p
JOIN agb.model_certification c ON c.id = p.certification_id
LEFT JOIN LATERAL (
    SELECT e.id, e.at, e.reason FROM agb.certification_event e
    WHERE e.certification_id = c.id AND e.event = 'voided'
) v ON true;

-- Each basket names the certification it was ranked with and the artifact digest the
-- pipeline verified before ranking, which the gate compares with the promoted
-- certification's. Baskets proposed before the registry have neither.
ALTER TABLE agb.basket
    ADD COLUMN model_certification_id bigint REFERENCES agb.model_certification,
    ADD COLUMN model_sha256 text CHECK (model_sha256 ~ '^[0-9a-f]{64}$'),
    ADD CHECK ((model_certification_id IS NULL) = (model_sha256 IS NULL));

GRANT SELECT ON agb.certification_event, agb.promotion_event, agb.promoted_model
    TO scheduler, pipeline, execution, bot, ui, cli;
GRANT INSERT ON agb.certification_event, agb.promotion_event TO cli;

-- The audit log gains promotions and voidings, two branches more.
CREATE OR REPLACE VIEW agb.audit_log AS
SELECT e.at, 'basket_event'::text AS source, e.id AS row_id, e.actor::text AS actor,
       'basket ' || e.to_state AS event,
       b.kind || ' basket ' || coalesce(b.reference, b.id::text) AS subject,
       e.reason AS detail
FROM agb.basket_event e
JOIN agb.basket b ON b.id = e.basket_id
UNION ALL
SELECT h.at, 'halt_event', h.id, h.actor,
       'halt ' || h.action || ' ' || h.cause,
       CASE WHEN b.id IS NULL THEN NULL
            ELSE b.kind || ' basket ' || coalesce(b.reference, b.id::text) END,
       h.reason
FROM agb.halt_event h
LEFT JOIN agb.basket b ON b.id = h.basket_id
UNION ALL
SELECT m.at, 'mode_event', m.id, m.actor, 'mode ' || m.mode, NULL, m.reason
FROM agb.mode_event m
UNION ALL
SELECT e.at, 'order_event', e.id, e.actor,
       'order ' || e.kind || ' ' || e.status,
       o.side || ' ' || o.qty || ' ' || o.symbol || ' (' || o.client_order_id || ')',
       e.detail
FROM agb.order_event e
JOIN agb.broker_order o ON o.id = e.order_id
UNION ALL
SELECT c.at, 'command', c.id, c.actor,
       'command ' || c.outcome,
       c.surface || ' ' || left(c.raw_text, 200),
       c.reason
FROM agb.command c
UNION ALL
SELECT e.at, 'alert_event', e.id, e.actor,
       'alert ' || e.event,
       a.kind || ' ' || a.subject || ' ' || a.period,
       e.detail
FROM agb.alert_event e
JOIN agb.alert a ON a.id = e.alert_id
UNION ALL
SELECT e.at, 'job_event', e.id, e.actor,
       'job ' || e.event,
       j.kind || ' ' || j.period,
       e.detail
FROM agb.job_event e
JOIN agb.job j ON j.id = e.job_id
UNION ALL
SELECT p.at, 'paper_approval_event', p.id, p.actor,
       CASE WHEN p.required THEN 'paper approval required' ELSE 'paper approval unattended' END,
       NULL, p.reason
FROM agb.paper_approval_event p
UNION ALL
SELECT p.at, 'promotion_event', p.id, p.actor,
       'model promoted',
       'model ' || c.model_id,
       NULL
FROM agb.promotion_event p
JOIN agb.model_certification c ON c.id = p.certification_id
UNION ALL
SELECT e.at, 'certification_event', e.id, e.actor,
       'model ' || e.event,
       'model ' || c.model_id,
       e.reason
FROM agb.certification_event e
JOIN agb.model_certification c ON c.id = e.certification_id;

GRANT SELECT ON agb.audit_log TO scheduler, pipeline, execution, bot, ui, cli;
