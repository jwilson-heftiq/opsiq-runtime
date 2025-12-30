-- Fetch order line fulfillment inputs from canonical table
-- Parameters: :tenant_id, :table_name
SELECT
    tenant_id,
    subject_type,
    subject_id,
    as_of_ts,
    need_by_date,
    open_quantity,
    projected_available_quantity,
    order_status,
    is_on_hold,
    release_shortage_qty,
    plant_shortage_qty,
    projected_onhand_qty_eod,
    supply_qty,
    demand_qty,
    partnum,
    customer_id,
    plant,
    warehouse,
    config_version,
    canonical_version
FROM :table_name
WHERE tenant_id = :tenant_id
ORDER BY subject_id, as_of_ts DESC

