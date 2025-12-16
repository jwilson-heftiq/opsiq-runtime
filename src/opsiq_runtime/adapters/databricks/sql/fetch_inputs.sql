-- Fetch operational risk inputs from canonical table
-- Parameters: :tenant_id, :table_name, :as_of_ts (optional)
-- Note: canonical_version is not in source table, default to 'v1' in code
SELECT
    tenant_id,
    subject_type,
    subject_id,
    as_of_ts,
    last_trip_ts,
    days_since_last_trip,
    config_version
FROM :table_name
WHERE tenant_id = :tenant_id
ORDER BY subject_id, as_of_ts DESC

