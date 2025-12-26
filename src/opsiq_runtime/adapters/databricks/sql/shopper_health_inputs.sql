-- Fetch shopper health classification inputs by reading and pivoting decision outputs
-- from operational_risk and shopper_frequency_trend primitives
-- Parameters: :tenant_id, :as_of_ts (optional)
-- 
-- Strategy:
-- 1. Filter by tenant_id, subject_type='shopper', and primitive_name IN ('operational_risk','shopper_frequency_trend')
-- 2. If :as_of_ts provided, prefer rows with as_of_ts = :as_of_ts, otherwise get latest per subject+primitive
-- 3. Pivot using conditional aggregation to get both risk_state and trend_state per shopper

WITH ranked_decisions AS (
    SELECT
        tenant_id,
        subject_type,
        subject_id,
        primitive_name,
        decision_state,
        evidence_refs_json,
        as_of_ts,
        config_version,
        ROW_NUMBER() OVER (
            PARTITION BY subject_id, primitive_name
            ORDER BY 
                CASE WHEN as_of_ts = :as_of_ts THEN 0 ELSE 1 END,
                as_of_ts DESC
        ) as rn
    FROM gold_decision_output_v1
    WHERE tenant_id = :tenant_id
        AND subject_type = 'shopper'
        AND primitive_name IN ('operational_risk', 'shopper_frequency_trend')
        AND (:as_of_ts IS NULL OR as_of_ts <= :as_of_ts)
),
latest_decisions AS (
    SELECT
        tenant_id,
        subject_type,
        subject_id,
        primitive_name,
        decision_state,
        evidence_refs_json,
        as_of_ts,
        config_version
    FROM ranked_decisions
    WHERE rn = 1
)
SELECT
    tenant_id,
    subject_type,
    subject_id,
    MAX(CASE WHEN primitive_name = 'operational_risk' THEN decision_state END) as risk_state,
    MAX(CASE WHEN primitive_name = 'operational_risk' THEN evidence_refs_json END) as risk_evidence_refs_json,
    MAX(CASE WHEN primitive_name = 'operational_risk' THEN as_of_ts END) as risk_source_as_of_ts,
    MAX(CASE WHEN primitive_name = 'shopper_frequency_trend' THEN decision_state END) as trend_state,
    MAX(CASE WHEN primitive_name = 'shopper_frequency_trend' THEN evidence_refs_json END) as trend_evidence_refs_json,
    MAX(CASE WHEN primitive_name = 'shopper_frequency_trend' THEN as_of_ts END) as trend_source_as_of_ts,
    COALESCE(
        MAX(CASE WHEN primitive_name = 'operational_risk' THEN as_of_ts END),
        MAX(CASE WHEN primitive_name = 'shopper_frequency_trend' THEN as_of_ts END)
    ) as as_of_ts,
    COALESCE(
        MAX(CASE WHEN primitive_name = 'operational_risk' THEN config_version END),
        MAX(CASE WHEN primitive_name = 'shopper_frequency_trend' THEN config_version END)
    ) as config_version
FROM latest_decisions
GROUP BY tenant_id, subject_type, subject_id
ORDER BY subject_id

