-- Fetch latest order_fulfillment_risk decisions per order for customer rollup
-- Parameters: :tenant_id, :as_of_ts, :table_name
-- Note: Aggregation by customer_id is done in Python after fetching
WITH ranked_decisions AS (
    SELECT
        tenant_id,
        subject_type,
        subject_id,
        primitive_name,
        decision_state,
        metrics_json,
        evidence_refs_json,
        as_of_ts,
        config_version,
        ROW_NUMBER() OVER (
            PARTITION BY subject_id
            ORDER BY 
                CASE WHEN as_of_ts = :as_of_ts THEN 0 ELSE 1 END,
                as_of_ts DESC
        ) as rn
    FROM :table_name
    WHERE tenant_id = :tenant_id
        AND subject_type = 'order'
        AND primitive_name = 'order_fulfillment_risk'
        AND (:as_of_ts IS NULL OR as_of_ts <= :as_of_ts)
)
SELECT
    tenant_id,
    subject_id,
    decision_state,
    metrics_json,
    evidence_refs_json,
    as_of_ts,
    config_version
FROM ranked_decisions
WHERE rn = 1
ORDER BY subject_id

