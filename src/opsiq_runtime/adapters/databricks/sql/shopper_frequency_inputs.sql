-- Fetch shopper frequency trend inputs from canonical table
-- Parameters: :tenant_id, :table_name, :as_of_ts (optional)
SELECT
    tenant_id,
    subject_type,
    subject_id,
    as_of_ts,
    last_trip_ts,
    prev_trip_ts,
    recent_gap_days,
    baseline_avg_gap_days,
    baseline_trip_count,
    baseline_window_days,
    config_version
FROM :table_name
WHERE tenant_id = :tenant_id
ORDER BY subject_id, as_of_ts DESC

