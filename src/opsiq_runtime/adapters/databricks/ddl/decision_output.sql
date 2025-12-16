-- DDL for gold_decision_output_operational_risk_v1 table
-- Run this in Databricks SQL to create the table

CREATE TABLE IF NOT EXISTS gold_decision_output_operational_risk_v1 (
    tenant_id STRING NOT NULL,
    subject_type STRING NOT NULL,
    subject_id STRING NOT NULL,
    primitive_name STRING NOT NULL,
    primitive_version STRING NOT NULL,
    canonical_version STRING NOT NULL,
    config_version STRING NOT NULL,
    as_of_ts TIMESTAMP NOT NULL,
    decision_state STRING NOT NULL,
    confidence DOUBLE,
    drivers_json STRING NOT NULL,
    metrics_json STRING NOT NULL,
    evidence_refs_json STRING NOT NULL,
    computed_at TIMESTAMP NOT NULL,
    valid_until TIMESTAMP,
    correlation_id STRING
) USING DELTA
PARTITIONED BY (tenant_id)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- Create unique constraint on natural key
ALTER TABLE gold_decision_output_operational_risk_v1
ADD CONSTRAINT pk_decision_output
PRIMARY KEY (tenant_id, subject_type, subject_id, primitive_name, primitive_version, as_of_ts);

