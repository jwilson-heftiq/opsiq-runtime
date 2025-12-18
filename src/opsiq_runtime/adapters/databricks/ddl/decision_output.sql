-- ============================================================
-- OpsIQ: Decision Outputs (Operational Risk) v1
-- ============================================================
CREATE TABLE IF NOT EXISTS gold_decision_output_operational_risk_v1 (
    tenant_id           STRING                  NOT NULL,
    subject_type        STRING                  NOT NULL,  -- e.g. "shopper"
    subject_id          STRING                  NOT NULL,  -- e.g. account id
    primitive_name      STRING                  NOT NULL,  -- "operational_risk"
    primitive_version   STRING                  NOT NULL,  -- "1.0.0"
    canonical_version   STRING                  NOT NULL,  -- "v1"
    config_version      STRING                  NOT NULL,  -- e.g. "cfg_v1"

    -- The as-of time that the decision represents (usually the runtime run time)
    as_of_ts            TIMESTAMP               NOT NULL,

    decision_state      STRING                  NOT NULL,  -- AT_RISK | NOT_AT_RISK | UNKNOWN
    confidence          STRING                  NOT NULL,  -- HIGH | MEDIUM | LOW

    -- JSON payloads to prevent schema churn
    drivers_json        STRING                  NOT NULL,  -- JSON array
    metrics_json        STRING                  NOT NULL,  -- JSON object
    evidence_refs_json  STRING                  NOT NULL,  -- JSON array (ids/refs)

    computed_at         TIMESTAMP               NOT NULL,  -- when computed
    valid_until         TIMESTAMP               NULL,      -- optional TTL/validity window

    correlation_id      STRING                  NOT NULL,  -- run correlation id
    adapter_mode        STRING                  NOT NULL,  -- "local" | "databricks" etc.

    -- housekeeping
    created_at          TIMESTAMP               NOT NULL,
    updated_at          TIMESTAMP               NOT NULL
) USING DELTA
PARTITIONED BY (tenant_id)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true'
);
