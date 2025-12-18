-- ============================================================
-- OpsIQ: Decision Evidence (Operational Risk) v1
-- ============================================================
CREATE TABLE IF NOT EXISTS gold_decision_evidence_operational_risk_v1 (
    tenant_id           STRING    NOT NULL,
    subject_type        STRING    NOT NULL,
    subject_id          STRING    NOT NULL,
    primitive_name      STRING    NOT NULL,
    primitive_version   STRING    NOT NULL,
    canonical_version   STRING    NOT NULL,
    config_version      STRING    NOT NULL,

    as_of_ts            TIMESTAMP NOT NULL,

    evidence_id         STRING    NOT NULL,  -- UUID from runtime
    evidence_json       STRING    NOT NULL,  -- JSON object (structured evidence)

    computed_at         TIMESTAMP NOT NULL,
    correlation_id      STRING    NOT NULL,
    adapter_mode        STRING    NOT NULL,

    created_at          TIMESTAMP NOT NULL,
    updated_at          TIMESTAMP NOT NULL
) USING DELTA
PARTITIONED BY (tenant_id)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true'
);
