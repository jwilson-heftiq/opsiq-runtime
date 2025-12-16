-- MERGE INTO template for evidence outputs
-- Parameters: :table_name, plus row values
-- This template will be used programmatically to build MERGE statements
MERGE INTO :table_name AS target
USING (
    SELECT
        :tenant_id AS tenant_id,
        :subject_type AS subject_type,
        :subject_id AS subject_id,
        :primitive_name AS primitive_name,
        :primitive_version AS primitive_version,
        :canonical_version AS canonical_version,
        :config_version AS config_version,
        :as_of_ts AS as_of_ts,
        :evidence_id AS evidence_id,
        :evidence_json AS evidence_json,
        :computed_at AS computed_at,
        :correlation_id AS correlation_id
) AS source
ON target.tenant_id = source.tenant_id
    AND target.subject_type = source.subject_type
    AND target.subject_id = source.subject_id
    AND target.primitive_name = source.primitive_name
    AND target.primitive_version = source.primitive_version
    AND target.as_of_ts = source.as_of_ts
    AND target.evidence_id = source.evidence_id
WHEN MATCHED THEN
    UPDATE SET
        canonical_version = source.canonical_version,
        config_version = source.config_version,
        evidence_json = source.evidence_json,
        computed_at = source.computed_at,
        correlation_id = source.correlation_id
WHEN NOT MATCHED THEN
    INSERT (
        tenant_id,
        subject_type,
        subject_id,
        primitive_name,
        primitive_version,
        canonical_version,
        config_version,
        as_of_ts,
        evidence_id,
        evidence_json,
        computed_at,
        correlation_id
    ) VALUES (
        source.tenant_id,
        source.subject_type,
        source.subject_id,
        source.primitive_name,
        source.primitive_version,
        source.canonical_version,
        source.config_version,
        source.as_of_ts,
        source.evidence_id,
        source.evidence_json,
        source.computed_at,
        source.correlation_id
    )

