"""SQL query builders for pack readiness metrics."""

from __future__ import annotations

from typing import Any


def get_canonical_freshness_query(table_name: str) -> tuple[str, list[Any]]:
    """
    Build SQL query to get the latest as_of_ts for a canonical input table.

    Args:
        table_name: Fully qualified table name (with catalog/schema/prefix)

    Returns:
        Tuple of (SQL query string, parameter list)
    """
    sql = f"""
    SELECT MAX(as_of_ts) as last_as_of_ts
    FROM {table_name}
    WHERE tenant_id = ?
    """
    return sql, []


def get_decision_health_query(
    decision_table_name: str, primitive_names: list[str], hours_window: int = 24
) -> tuple[str, list[Any]]:
    """
    Build SQL query to get decision health metrics for primitives.

    Args:
        decision_table_name: Fully qualified decision table name
        primitive_names: List of primitive names to query
        hours_window: Number of hours to look back (default 24)

    Returns:
        Tuple of (SQL query string, parameter list)
    """
    if not primitive_names:
        # Return empty result query
        sql = f"""
        SELECT 
            CAST(NULL AS STRING) as primitive_name,
            CAST(0 AS BIGINT) as total_decisions,
            CAST(0 AS BIGINT) as at_risk_count,
            CAST(0 AS BIGINT) as not_at_risk_count,
            CAST(0 AS BIGINT) as unknown_count,
            CAST(NULL AS TIMESTAMP) as last_computed_at
        WHERE 1 = 0
        """
        return sql, []

    # Build IN clause with placeholders
    placeholders = ",".join(["?" for _ in primitive_names])

    sql = f"""
    SELECT 
        primitive_name,
        COUNT(*) as total_decisions,
        SUM(CASE WHEN decision_state = 'AT_RISK' THEN 1 ELSE 0 END) as at_risk_count,
        SUM(CASE WHEN decision_state = 'NOT_AT_RISK' THEN 1 ELSE 0 END) as not_at_risk_count,
        SUM(CASE WHEN decision_state = 'UNKNOWN' THEN 1 ELSE 0 END) as unknown_count,
        MAX(computed_at) as last_computed_at
    FROM {decision_table_name}
    WHERE tenant_id = ?
        AND primitive_name IN ({placeholders})
        AND computed_at >= CURRENT_TIMESTAMP - INTERVAL {hours_window} HOURS
    GROUP BY primitive_name
    """
    return sql, primitive_names


def get_rollup_integrity_query(
    decision_table_name: str, primitive_name: str, hours_window: int = 24
) -> tuple[str, list[Any]]:
    """
    Build SQL query to check rollup integrity for manufacturing pack.

    Checks for presence of required JSON fields in metrics_json:
    - ordernum in order_line_fulfillment_risk decisions
    - customer_id in order_fulfillment_risk decisions
    - at_risk_order_subject_ids array in customer_order_impact_risk decisions

    Args:
        decision_table_name: Fully qualified decision table name
        primitive_name: Primitive name to check
        hours_window: Number of hours to look back (default 24)

    Returns:
        Tuple of (SQL query string, parameter list)
    """
    if primitive_name == "order_line_fulfillment_risk":
        sql = f"""
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN get_json_object(metrics_json, '$.ordernum') IS NOT NULL 
                AND get_json_object(metrics_json, '$.ordernum') != '' THEN 1 ELSE 0 END) as has_ordernum
        FROM {decision_table_name}
        WHERE tenant_id = ?
            AND primitive_name = ?
            AND computed_at >= CURRENT_TIMESTAMP - INTERVAL {hours_window} HOURS
        """
        return sql, [primitive_name]
    elif primitive_name == "order_fulfillment_risk":
        sql = f"""
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN get_json_object(metrics_json, '$.customer_id') IS NOT NULL 
                AND get_json_object(metrics_json, '$.customer_id') != '' THEN 1 ELSE 0 END) as has_customer_id
        FROM {decision_table_name}
        WHERE tenant_id = ?
            AND primitive_name = ?
            AND computed_at >= CURRENT_TIMESTAMP - INTERVAL {hours_window} HOURS
        """
        return sql, [primitive_name]
    elif primitive_name == "customer_order_impact_risk":
        # Check if at_risk_order_subject_ids exists and is a non-empty array
        # get_json_object returns a STRING representation of the JSON value
        # A non-empty array will be a string like '["order1","order2"]' (length > 2)
        # An empty array will be '[]' (length = 2)
        # With sparse emission (Model A), all NEW decisions should have non-empty arrays
        # Note: This query includes ALL data in the time window, including old data
        # created before sparse emission, which may have empty arrays
        sql = f"""
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN get_json_object(metrics_json, '$.at_risk_order_subject_ids') IS NOT NULL 
                AND TRIM(get_json_object(metrics_json, '$.at_risk_order_subject_ids')) != ''
                AND TRIM(get_json_object(metrics_json, '$.at_risk_order_subject_ids')) != 'null'
                AND TRIM(get_json_object(metrics_json, '$.at_risk_order_subject_ids')) != '[]'
                AND LENGTH(TRIM(get_json_object(metrics_json, '$.at_risk_order_subject_ids'))) > 2 THEN 1 ELSE 0 END) as has_impacted_order_ids
        FROM {decision_table_name}
        WHERE tenant_id = ?
            AND primitive_name = ?
            AND subject_type = 'customer'
            AND computed_at >= CURRENT_TIMESTAMP - INTERVAL {hours_window} HOURS
        """
        return sql, [primitive_name]
    else:
        # Return empty result for non-manufacturing primitives
        sql = f"""
        SELECT 
            CAST(0 AS BIGINT) as total,
            CAST(0 AS BIGINT) as has_field
        WHERE 1 = 0
        """
        return sql, []

