-- ============================================================
-- OpsIQ: Runtime Run Registry v1
-- One row per run attempt (correlation_id)
-- ============================================================
CREATE TABLE IF NOT EXISTS gold_runtime_run_registry_v1 (
    tenant_id           STRING    NOT NULL,
    primitive_name      STRING    NOT NULL,
    primitive_version   STRING    NOT NULL,
    canonical_version   STRING    NOT NULL,
    config_version      STRING    NOT NULL,

    as_of_ts            TIMESTAMP NOT NULL,

    correlation_id      STRING    NOT NULL,  -- primary identifier for the run
    adapter_mode        STRING    NOT NULL,  -- "local" | "databricks"
    status              STRING    NOT NULL,  -- STARTED | SUCCESS | FAILED

    started_at          TIMESTAMP NOT NULL,
    completed_at        TIMESTAMP NULL,
    duration_ms         BIGINT    NULL,

    input_count         BIGINT    NULL,
    decision_count      BIGINT    NULL,
    at_risk_count       BIGINT    NULL,
    not_at_risk_count   BIGINT    NULL,
    unknown_count       BIGINT    NULL,

    decision_table      STRING    NULL,      -- fully qualified name or table id
    evidence_table      STRING    NULL,

    error_type          STRING    NULL,
    error_message       STRING    NULL,
    error_stack         STRING    NULL,

    updated_at          TIMESTAMP NOT NULL
) USING DELTA
PARTITIONED BY (tenant_id)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true'
);

