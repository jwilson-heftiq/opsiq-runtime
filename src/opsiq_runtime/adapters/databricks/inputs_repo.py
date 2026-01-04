from __future__ import annotations

import json
import logging
from datetime import date, datetime
from typing import Iterable

from opsiq_runtime.adapters.databricks.client import DatabricksSqlClient
from opsiq_runtime.application.run_context import RunContext
from opsiq_runtime.domain.common.ids import CorrelationId
from opsiq_runtime.domain.primitives.operational_risk.model import OperationalRiskInput
from opsiq_runtime.domain.primitives.order_line_fulfillment_risk.model import OrderLineFulfillmentInput
from opsiq_runtime.domain.primitives.order_fulfillment_risk.model import OrderRiskInput, SourceLineRef
from opsiq_runtime.domain.primitives.customer_order_impact_risk.model import CustomerImpactInput, SourceOrderRef
from opsiq_runtime.domain.primitives.shopper_frequency_trend.model import ShopperFrequencyInput
from opsiq_runtime.domain.primitives.shopper_health_classification.model import ShopperHealthInput
from opsiq_runtime.ports.inputs_repository import InputsRepository
from opsiq_runtime.settings import Settings, get_settings

logger = logging.getLogger(__name__)


class DatabricksInputsRepository(InputsRepository):
    """Inputs repository that reads from Databricks tables."""

    def __init__(
        self,
        client: DatabricksSqlClient,
        settings: Settings | None = None,
        input_table_name: str | None = None,
    ) -> None:
        self.client = client
        self.settings = settings or get_settings()
        self.input_table_name = input_table_name or f"{self.settings.databricks_table_prefix}gold_canonical_shopper_recency_input_v1"

    def _build_table_name(self) -> str:
        """Build fully qualified table name with catalog and schema if specified."""
        parts = []
        if self.settings.databricks_catalog:
            parts.append(self.settings.databricks_catalog)
        if self.settings.databricks_schema:
            parts.append(self.settings.databricks_schema)
        parts.append(self.input_table_name)
        return ".".join(parts)

    def _parse_timestamp(self, value: str | datetime | None) -> datetime | None:
        """Parse timestamp value to datetime."""
        if value is None:
            return None
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            # Try ISO format first
            try:
                return datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                # Try other common formats
                for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"]:
                    try:
                        return datetime.strptime(value, fmt)
                    except ValueError:
                        continue
                logger.warning(f"Could not parse timestamp: {value}")
                return None
        return None

    def _parse_date(self, value: str | date | datetime | None) -> date | None:
        """Parse date value to date."""
        if value is None:
            return None
        if isinstance(value, date):
            return value
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, str):
            # Try ISO format first
            try:
                return date.fromisoformat(value)
            except ValueError:
                # Try datetime format and extract date
                try:
                    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
                    return dt.date()
                except ValueError:
                    # Try other common date formats
                    for fmt in ["%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y"]:
                        try:
                            return datetime.strptime(value, fmt).date()
                        except ValueError:
                            continue
                    logger.warning(f"Could not parse date: {value}")
                    return None
        return None

    def _compute_days_since_last_trip(self, last_trip_ts: datetime | None, as_of_ts: datetime) -> int | None:
        """Compute days since last trip if last_trip_ts is provided."""
        if last_trip_ts is None:
            return None
        delta = as_of_ts.date() - last_trip_ts.date()
        return delta.days

    def fetch_operational_risk_inputs(self, ctx: RunContext) -> Iterable[OperationalRiskInput]:
        """
        Fetch operational risk inputs from Databricks table.

        Filters by tenant_id (required) and optionally by as_of_ts window.
        Returns latest row per subject_id if multiple exist.
        """
        table_name = self._build_table_name()
        tenant_id = str(ctx.tenant_id)

        # Build SQL query
        # Note: Table name must be string substitution (can't be parameterized)
        # Use ? placeholder for tenant_id parameter
        sql = f"""
        SELECT
            tenant_id,
            subject_type,
            subject_id,
            as_of_ts,
            last_trip_ts,
            days_since_last_trip,
            config_version
        FROM {table_name}
        WHERE tenant_id = ?
        ORDER BY subject_id, as_of_ts DESC
        """

        logger.info(
            f"Fetching inputs from {table_name} for tenant {tenant_id}",
            extra={"correlation_id": ctx.correlation_id.value} if ctx.correlation_id else {},
        )

        try:
            # databricks-sql-connector uses positional parameters with ?
            rows = self.client.query(sql, params=[tenant_id])

            # Group by subject_id and take latest as_of_ts (already sorted DESC)
            seen_subjects = set()
            inputs = []

            for row in rows:
                subject_id = str(row.get("subject_id", ""))
                if subject_id in seen_subjects:
                    continue  # Skip duplicates, taking first (latest as_of_ts)
                seen_subjects.add(subject_id)

                as_of_ts = self._parse_timestamp(row.get("as_of_ts"))
                if as_of_ts is None:
                    logger.warning(f"Skipping row with null as_of_ts for subject {subject_id}")
                    continue

                last_trip_ts = self._parse_timestamp(row.get("last_trip_ts"))
                days_since_last_trip = row.get("days_since_last_trip")
                if days_since_last_trip is not None:
                    try:
                        days_since_last_trip = int(days_since_last_trip)
                    except (ValueError, TypeError):
                        days_since_last_trip = None

                # Compute days_since_last_trip if null but last_trip_ts exists
                if days_since_last_trip is None and last_trip_ts is not None:
                    days_since_last_trip = self._compute_days_since_last_trip(last_trip_ts, as_of_ts)

                config_version = str(row.get("config_version", ""))
                # canonical_version not in source table, use config_version as fallback
                canonical_version = config_version
                subject_type = str(row.get("subject_type", "shopper"))

                input_obj = OperationalRiskInput.new(
                    tenant_id=str(row.get("tenant_id", tenant_id)),
                    subject_id=subject_id,
                    as_of_ts=as_of_ts,
                    last_trip_ts=last_trip_ts,
                    config_version=config_version,
                    canonical_version=canonical_version,
                    subject_type=subject_type,
                    days_since_last_trip=days_since_last_trip,
                )
                inputs.append(input_obj)

            logger.info(
                f"Fetched {len(inputs)} input rows for tenant {tenant_id}",
                extra={"correlation_id": ctx.correlation_id.value} if ctx.correlation_id else {},
            )
            return inputs

        except Exception as e:
            logger.error(
                f"Error fetching inputs from {table_name}: {e}",
                extra={"correlation_id": ctx.correlation_id.value} if ctx.correlation_id else {},
            )
            raise

    def fetch_shopper_frequency_inputs(self, ctx: RunContext) -> Iterable[ShopperFrequencyInput]:
        """
        Fetch shopper frequency trend inputs from Databricks table.

        Filters by tenant_id (required) and optionally by as_of_ts window.
        Returns latest row per subject_id if multiple exist.
        """
        table_name = self._build_table_name_for_primitive("gold_canonical_shopper_frequency_input_v1")
        tenant_id = str(ctx.tenant_id)

        # Build SQL query
        # Note: Table name must be string substitution (can't be parameterized)
        # Use ? placeholder for tenant_id parameter
        sql = f"""
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
        FROM {table_name}
        WHERE tenant_id = ?
        ORDER BY subject_id, as_of_ts DESC
        """

        logger.info(
            f"Fetching shopper frequency inputs from {table_name} for tenant {tenant_id}",
            extra={"correlation_id": ctx.correlation_id.value} if ctx.correlation_id else {},
        )

        try:
            # databricks-sql-connector uses positional parameters with ?
            rows = self.client.query(sql, params=[tenant_id])

            # Group by subject_id and take latest as_of_ts (already sorted DESC)
            seen_subjects = set()
            inputs = []

            for row in rows:
                subject_id = str(row.get("subject_id", ""))
                if subject_id in seen_subjects:
                    continue  # Skip duplicates, taking first (latest as_of_ts)
                seen_subjects.add(subject_id)

                as_of_ts = self._parse_timestamp(row.get("as_of_ts"))
                if as_of_ts is None:
                    logger.warning(f"Skipping row with null as_of_ts for subject {subject_id}")
                    continue

                last_trip_ts = self._parse_timestamp(row.get("last_trip_ts"))
                prev_trip_ts = self._parse_timestamp(row.get("prev_trip_ts"))
                
                recent_gap_days = row.get("recent_gap_days")
                if recent_gap_days is not None:
                    try:
                        recent_gap_days = float(recent_gap_days)
                    except (ValueError, TypeError):
                        recent_gap_days = None

                baseline_avg_gap_days = row.get("baseline_avg_gap_days")
                if baseline_avg_gap_days is not None:
                    try:
                        baseline_avg_gap_days = float(baseline_avg_gap_days)
                    except (ValueError, TypeError):
                        baseline_avg_gap_days = None

                baseline_trip_count = row.get("baseline_trip_count")
                if baseline_trip_count is not None:
                    try:
                        baseline_trip_count = int(baseline_trip_count)
                    except (ValueError, TypeError):
                        baseline_trip_count = None

                baseline_window_days = row.get("baseline_window_days")
                if baseline_window_days is not None:
                    try:
                        baseline_window_days = int(baseline_window_days)
                    except (ValueError, TypeError):
                        baseline_window_days = None

                config_version = str(row.get("config_version", ""))
                canonical_version = "v1"
                subject_type = str(row.get("subject_type", "shopper"))

                input_obj = ShopperFrequencyInput.new(
                    tenant_id=str(row.get("tenant_id", tenant_id)),
                    subject_id=subject_id,
                    as_of_ts=as_of_ts,
                    last_trip_ts=last_trip_ts,
                    prev_trip_ts=prev_trip_ts,
                    config_version=config_version,
                    canonical_version=canonical_version,
                    subject_type=subject_type,
                    recent_gap_days=recent_gap_days,
                    baseline_avg_gap_days=baseline_avg_gap_days,
                    baseline_trip_count=baseline_trip_count,
                    baseline_window_days=baseline_window_days,
                )
                inputs.append(input_obj)

            logger.info(
                f"Fetched {len(inputs)} shopper frequency input rows for tenant {tenant_id}",
                extra={"correlation_id": ctx.correlation_id.value} if ctx.correlation_id else {},
            )
            return inputs

        except Exception as e:
            logger.error(
                f"Error fetching shopper frequency inputs from {table_name}: {e}",
                extra={"correlation_id": ctx.correlation_id.value} if ctx.correlation_id else {},
            )
            raise

    def fetch_shopper_health_inputs(self, ctx: RunContext) -> Iterable[ShopperHealthInput]:
        """
        Fetch shopper health classification inputs by reading decision outputs from
        operational_risk and shopper_frequency_trend primitives.

        Reads from gold_decision_output_v1, pivots results to get both risk_state and
        trend_state per shopper. Handles missing primitives by setting state to UNKNOWN.
        """
        table_name = self._build_table_name_for_primitive("gold_decision_output_v1")
        tenant_id = str(ctx.tenant_id)
        as_of_ts = ctx.as_of_ts

        # Build SQL query with pivot logic
        # If as_of_ts provided, prefer rows with that timestamp, otherwise get latest per subject+primitive
        sql = f"""
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
                        CASE WHEN as_of_ts = ? THEN 0 ELSE 1 END,
                        as_of_ts DESC
                ) as rn
            FROM {table_name}
            WHERE tenant_id = ?
                AND subject_type = 'shopper'
                AND primitive_name IN ('operational_risk', 'shopper_frequency_trend')
                AND (? IS NULL OR as_of_ts <= ?)
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
        """

        logger.info(
            f"Fetching shopper health inputs from {table_name} for tenant {tenant_id}",
            extra={"correlation_id": ctx.correlation_id.value} if ctx.correlation_id else {},
        )

        try:
            # databricks-sql-connector uses positional parameters with ?
            # Parameters: as_of_ts (for ordering), tenant_id, as_of_ts (for filter), as_of_ts (for filter)
            as_of_ts_str = as_of_ts.isoformat() if as_of_ts else None
            rows = self.client.query(sql, params=[as_of_ts_str, tenant_id, as_of_ts_str, as_of_ts_str])

            inputs = []

            for row in rows:
                subject_id = str(row.get("subject_id", ""))
                if not subject_id:
                    logger.warning("Skipping row with empty subject_id")
                    continue

                as_of_ts_parsed = self._parse_timestamp(row.get("as_of_ts"))
                if as_of_ts_parsed is None:
                    logger.warning(f"Skipping row with null as_of_ts for subject {subject_id}")
                    continue

                # Parse risk state and evidence refs
                risk_state = row.get("risk_state")
                risk_evidence_refs_json = row.get("risk_evidence_refs_json")
                risk_evidence_refs = []
                if risk_evidence_refs_json:
                    try:
                        risk_evidence_refs = json.loads(risk_evidence_refs_json)
                        if not isinstance(risk_evidence_refs, list):
                            risk_evidence_refs = []
                    except (json.JSONDecodeError, TypeError):
                        logger.warning(f"Failed to parse risk_evidence_refs_json for subject {subject_id}")
                        risk_evidence_refs = []

                risk_source_as_of_ts = self._parse_timestamp(row.get("risk_source_as_of_ts"))

                # Parse trend state and evidence refs
                trend_state = row.get("trend_state")
                trend_evidence_refs_json = row.get("trend_evidence_refs_json")
                trend_evidence_refs = []
                if trend_evidence_refs_json:
                    try:
                        trend_evidence_refs = json.loads(trend_evidence_refs_json)
                        if not isinstance(trend_evidence_refs, list):
                            trend_evidence_refs = []
                    except (json.JSONDecodeError, TypeError):
                        logger.warning(f"Failed to parse trend_evidence_refs_json for subject {subject_id}")
                        trend_evidence_refs = []

                trend_source_as_of_ts = self._parse_timestamp(row.get("trend_source_as_of_ts"))

                # Handle missing primitives: set to UNKNOWN if NULL
                if risk_state is None:
                    risk_state = "UNKNOWN"
                    risk_evidence_refs = []
                if trend_state is None:
                    trend_state = "UNKNOWN"
                    trend_evidence_refs = []

                config_version = str(row.get("config_version", ""))
                canonical_version = "v1"
                subject_type = str(row.get("subject_type", "shopper"))

                input_obj = ShopperHealthInput.new(
                    tenant_id=str(row.get("tenant_id", tenant_id)),
                    subject_id=subject_id,
                    as_of_ts=as_of_ts_parsed,
                    config_version=config_version,
                    canonical_version=canonical_version,
                    subject_type=subject_type,
                    risk_state=risk_state,
                    trend_state=trend_state,
                    risk_evidence_refs=risk_evidence_refs,
                    trend_evidence_refs=trend_evidence_refs,
                    risk_source_as_of_ts=risk_source_as_of_ts,
                    trend_source_as_of_ts=trend_source_as_of_ts,
                )
                inputs.append(input_obj)

            logger.info(
                f"Fetched {len(inputs)} shopper health input rows for tenant {tenant_id}",
                extra={"correlation_id": ctx.correlation_id.value} if ctx.correlation_id else {},
            )
            return inputs

        except Exception as e:
            logger.error(
                f"Error fetching shopper health inputs from {table_name}: {e}",
                extra={"correlation_id": ctx.correlation_id.value} if ctx.correlation_id else {},
            )
            raise

    def fetch_order_line_fulfillment_inputs(self, ctx: RunContext) -> Iterable[OrderLineFulfillmentInput]:
        """
        Fetch order line fulfillment inputs from Databricks table.

        Filters by tenant_id (required).
        Returns latest row per subject_id if multiple exist.
        
        Note: Table location is determined by DATABRICKS_CATALOG and DATABRICKS_SCHEMA
        environment variables. For this primitive, set:
        - DATABRICKS_CATALOG=opsiq_dev
        - DATABRICKS_SCHEMA=gold
        """
        table_name = self._build_table_name_for_primitive("gold_canonical_order_line_fulfillment_input_v1")
        tenant_id = str(ctx.tenant_id)

        # Build SQL query
        # Note: Table name must be string substitution (can't be parameterized)
        # Use ? placeholder for tenant_id parameter
        sql = f"""
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
            ordernum,
            orderline,
            orderrelnum,
            plant,
            warehouse,
            config_version,
            canonical_version
        FROM {table_name}
        WHERE tenant_id = ?
        ORDER BY subject_id, as_of_ts DESC
        """

        logger.info(
            f"Fetching order line fulfillment inputs from {table_name} for tenant {tenant_id}",
            extra={"correlation_id": ctx.correlation_id.value} if ctx.correlation_id else {},
        )

        try:
            # databricks-sql-connector uses positional parameters with ?
            rows = self.client.query(sql, params=[tenant_id])

            # Group by subject_id and take latest as_of_ts (already sorted DESC)
            seen_subjects = set()
            inputs = []

            for row in rows:
                subject_id = str(row.get("subject_id", ""))
                if subject_id in seen_subjects:
                    continue  # Skip duplicates, taking first (latest as_of_ts)
                seen_subjects.add(subject_id)

                as_of_ts = self._parse_timestamp(row.get("as_of_ts"))
                if as_of_ts is None:
                    logger.warning(f"Skipping row with null as_of_ts for subject {subject_id}")
                    continue

                need_by_date = self._parse_date(row.get("need_by_date"))

                # Parse numeric fields
                def parse_float(value):
                    if value is None:
                        return None
                    try:
                        return float(value)
                    except (ValueError, TypeError):
                        return None

                open_quantity = parse_float(row.get("open_quantity"))
                projected_available_quantity = parse_float(row.get("projected_available_quantity"))
                release_shortage_qty = parse_float(row.get("release_shortage_qty"))
                plant_shortage_qty = parse_float(row.get("plant_shortage_qty"))
                projected_onhand_qty_eod = parse_float(row.get("projected_onhand_qty_eod"))
                supply_qty = parse_float(row.get("supply_qty"))
                demand_qty = parse_float(row.get("demand_qty"))

                # Parse boolean
                is_on_hold = row.get("is_on_hold")
                if is_on_hold is not None:
                    if isinstance(is_on_hold, bool):
                        pass  # Already boolean
                    elif isinstance(is_on_hold, str):
                        is_on_hold = is_on_hold.lower() in ("true", "1", "yes", "t")
                    elif isinstance(is_on_hold, (int, float)):
                        is_on_hold = bool(is_on_hold)
                    else:
                        is_on_hold = None

                # Parse string fields
                order_status = str(row.get("order_status")) if row.get("order_status") is not None else None
                partnum = str(row.get("partnum")) if row.get("partnum") is not None else None
                customer_id = str(row.get("customer_id")) if row.get("customer_id") is not None else None
                # Parse ordernum (can be string or int)
                ordernum_raw = row.get("ordernum")
                ordernum = None
                if ordernum_raw is not None:
                    if isinstance(ordernum_raw, (int, float)):
                        # If it's a number, keep as int if it's a whole number, otherwise convert to string
                        if isinstance(ordernum_raw, float) and ordernum_raw.is_integer():
                            ordernum = int(ordernum_raw)
                        elif isinstance(ordernum_raw, int):
                            ordernum = ordernum_raw
                        else:
                            ordernum = str(ordernum_raw)
                    else:
                        ordernum = str(ordernum_raw)
                
                # Parse orderline (should be int)
                orderline_raw = row.get("orderline")
                orderline = None
                if orderline_raw is not None:
                    try:
                        orderline = int(orderline_raw)
                    except (ValueError, TypeError):
                        # If it's a float, try to convert to int if it's a whole number
                        try:
                            if isinstance(orderline_raw, float) and orderline_raw.is_integer():
                                orderline = int(orderline_raw)
                        except (ValueError, TypeError):
                            orderline = None
                
                # Parse orderrelnum (should be int)
                orderrelnum_raw = row.get("orderrelnum")
                orderrelnum = None
                if orderrelnum_raw is not None:
                    try:
                        orderrelnum = int(orderrelnum_raw)
                    except (ValueError, TypeError):
                        # If it's a float, try to convert to int if it's a whole number
                        try:
                            if isinstance(orderrelnum_raw, float) and orderrelnum_raw.is_integer():
                                orderrelnum = int(orderrelnum_raw)
                        except (ValueError, TypeError):
                            orderrelnum = None
                
                plant = str(row.get("plant")) if row.get("plant") is not None else None
                warehouse = str(row.get("warehouse")) if row.get("warehouse") is not None else None

                config_version = str(row.get("config_version", ""))
                canonical_version = str(row.get("canonical_version", "v1"))
                subject_type = str(row.get("subject_type", "order_line"))

                input_obj = OrderLineFulfillmentInput.new(
                    tenant_id=str(row.get("tenant_id", tenant_id)),
                    subject_id=subject_id,
                    as_of_ts=as_of_ts,
                    config_version=config_version,
                    canonical_version=canonical_version,
                    subject_type=subject_type,
                    need_by_date=need_by_date,
                    open_quantity=open_quantity,
                    projected_available_quantity=projected_available_quantity,
                    order_status=order_status,
                    is_on_hold=is_on_hold,
                    release_shortage_qty=release_shortage_qty,
                    plant_shortage_qty=plant_shortage_qty,
                    projected_onhand_qty_eod=projected_onhand_qty_eod,
                    supply_qty=supply_qty,
                    demand_qty=demand_qty,
                    partnum=partnum,
                    customer_id=customer_id,
                    ordernum=ordernum,
                    orderline=orderline,
                    orderrelnum=orderrelnum,
                    plant=plant,
                    warehouse=warehouse,
                )
                inputs.append(input_obj)

            logger.info(
                f"Fetched {len(inputs)} order line fulfillment input rows for tenant {tenant_id}",
                extra={"correlation_id": ctx.correlation_id.value} if ctx.correlation_id else {},
            )
            return inputs

        except Exception as e:
            logger.error(
                f"Error fetching order line fulfillment inputs from {table_name}: {e}",
                extra={"correlation_id": ctx.correlation_id.value} if ctx.correlation_id else {},
            )
            raise

    def _build_table_name_for_primitive(self, table_suffix: str) -> str:
        """Build fully qualified table name with catalog and schema if specified."""
        table_name = f"{self.settings.databricks_table_prefix}{table_suffix}"
        parts = []
        if self.settings.databricks_catalog:
            parts.append(self.settings.databricks_catalog)
        if self.settings.databricks_schema:
            parts.append(self.settings.databricks_schema)
        parts.append(table_name)
        return ".".join(parts)

    def fetch_order_risk_inputs(self, ctx: RunContext) -> Iterable[OrderRiskInput]:
        """
        Fetch order risk inputs by aggregating order_line_fulfillment_risk decisions.
        
        Reads from gold_decision_output_v1, groups by ordernum from metrics_json,
        and aggregates counts per order.
        """
        table_name = self._build_table_name_for_primitive("gold_decision_output_v1")
        tenant_id = str(ctx.tenant_id)
        as_of_ts = ctx.as_of_ts
        
        # Build SQL query to fetch latest order_line decisions per line
        sql = f"""
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
                        CASE WHEN as_of_ts = ? THEN 0 ELSE 1 END,
                        as_of_ts DESC
                ) as rn
            FROM {table_name}
            WHERE tenant_id = ?
                AND subject_type = 'order_line'
                AND primitive_name = 'order_line_fulfillment_risk'
                AND (? IS NULL OR as_of_ts <= ?)
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
        """
        
        logger.info(
            f"Fetching order risk inputs from {table_name} for tenant {tenant_id}",
            extra={"correlation_id": ctx.correlation_id.value} if ctx.correlation_id else {},
        )
        
        try:
            as_of_ts_str = as_of_ts.isoformat() if as_of_ts else None
            rows = self.client.query(sql, params=[as_of_ts_str, tenant_id, as_of_ts_str, as_of_ts_str])
            
            # Group by ordernum (from metrics_json)
            orders_dict: dict[str, dict] = {}
            
            for row in rows:
                subject_id = str(row.get("subject_id", ""))
                if not subject_id:
                    continue
                
                decision_state = str(row.get("decision_state", ""))
                metrics_json_str = row.get("metrics_json")
                evidence_refs_json_str = row.get("evidence_refs_json")
                as_of_ts_parsed = self._parse_timestamp(row.get("as_of_ts"))
                config_version = str(row.get("config_version", ""))
                
                if not metrics_json_str:
                    logger.warning(f"Skipping row with null metrics_json for subject {subject_id}")
                    continue
                
                # Parse metrics_json to extract ordernum
                try:
                    metrics = json.loads(metrics_json_str)
                except (json.JSONDecodeError, TypeError):
                    logger.warning(f"Failed to parse metrics_json for subject {subject_id}")
                    continue
                
                ordernum = metrics.get("ordernum")
                if ordernum is None:
                    logger.warning(f"Skipping row with null ordernum in metrics_json for subject {subject_id}")
                    continue
                
                # Normalize ordernum to string for grouping
                ordernum_key = str(ordernum)
                
                # Parse evidence_refs_json
                evidence_refs = []
                if evidence_refs_json_str:
                    try:
                        evidence_refs = json.loads(evidence_refs_json_str)
                        if not isinstance(evidence_refs, list):
                            evidence_refs = []
                    except (json.JSONDecodeError, TypeError):
                        logger.warning(f"Failed to parse evidence_refs_json for subject {subject_id}")
                        evidence_refs = []
                
                # Extract customer_id from metrics
                customer_id = metrics.get("customer_id")
                customer_id_str = str(customer_id) if customer_id is not None else None
                
                # Initialize order dict if needed
                if ordernum_key not in orders_dict:
                    orders_dict[ordernum_key] = {
                        "tenant_id": str(row.get("tenant_id", tenant_id)),
                        "ordernum": ordernum_key,
                        "as_of_ts": as_of_ts_parsed,
                        "config_version": config_version,
                        "customer_ids": [],  # Track all customer_ids for conflict resolution
                        "lines": [],
                    }
                else:
                    # Update as_of_ts to latest
                    if as_of_ts_parsed and (orders_dict[ordernum_key]["as_of_ts"] is None or 
                                           as_of_ts_parsed > orders_dict[ordernum_key]["as_of_ts"]):
                        orders_dict[ordernum_key]["as_of_ts"] = as_of_ts_parsed
                
                # Track customer_id if present
                if customer_id_str:
                    orders_dict[ordernum_key]["customer_ids"].append(customer_id_str)
                
                # Add line info
                orders_dict[ordernum_key]["lines"].append({
                    "line_subject_id": subject_id,
                    "decision_state": decision_state,
                    "evidence_refs": evidence_refs,
                })
            
            # Build OrderRiskInput objects
            inputs = []
            for ordernum_key, order_data in orders_dict.items():
                if order_data["as_of_ts"] is None:
                    logger.warning(f"Skipping order {ordernum_key} with null as_of_ts")
                    continue
                
                lines = order_data["lines"]
                total = len(lines)
                at_risk = sum(1 for line in lines if line["decision_state"] == "AT_RISK")
                unknown = sum(1 for line in lines if line["decision_state"] == "UNKNOWN")
                not_at_risk = sum(1 for line in lines if line["decision_state"] == "NOT_AT_RISK")
                
                # Resolve customer_id conflicts: pick most frequent non-null value
                customer_id = None
                customer_ids = order_data.get("customer_ids", [])
                if customer_ids:
                    from collections import Counter
                    customer_id_counts = Counter(customer_ids)
                    # Get most frequent customer_id
                    most_common = customer_id_counts.most_common(1)[0]
                    customer_id = most_common[0]
                    # Log warning if there are conflicting customer_ids
                    if len(customer_id_counts) > 1:
                        logger.warning(
                            f"Order {ordernum_key} has conflicting customer_ids: {dict(customer_id_counts)}. "
                            f"Using most frequent: {customer_id}",
                            extra={"correlation_id": ctx.correlation_id.value} if ctx.correlation_id else {},
                        )
                
                # Build at_risk_line_subject_ids (cap at 50)
                at_risk_line_subject_ids = [line["line_subject_id"] for line in lines if line["decision_state"] == "AT_RISK"][:50]
                
                # Build source_line_refs (cap at 50)
                source_line_refs = [
                    SourceLineRef(
                        line_subject_id=line["line_subject_id"],
                        decision_state=line["decision_state"],
                        evidence_refs=line["evidence_refs"],
                    )
                    for line in lines[:50]
                ]
                
                input_obj = OrderRiskInput.new(
                    tenant_id=order_data["tenant_id"],
                    subject_id=ordernum_key,
                    as_of_ts=order_data["as_of_ts"],
                    config_version=order_data["config_version"],
                    canonical_version="v1",
                    subject_type="order",
                    customer_id=customer_id,
                    order_line_count_total=total,
                    order_line_count_at_risk=at_risk,
                    order_line_count_unknown=unknown,
                    order_line_count_not_at_risk=not_at_risk,
                    at_risk_line_subject_ids=at_risk_line_subject_ids,
                    source_line_refs=source_line_refs,
                )
                inputs.append(input_obj)
            
            logger.info(
                f"Fetched {len(inputs)} order risk input rows for tenant {tenant_id}",
                extra={"correlation_id": ctx.correlation_id.value} if ctx.correlation_id else {},
            )
            return inputs
            
        except Exception as e:
            logger.error(
                f"Error fetching order risk inputs from {table_name}: {e}",
                extra={"correlation_id": ctx.correlation_id.value} if ctx.correlation_id else {},
            )
            raise

    def fetch_customer_impact_inputs(self, ctx: RunContext) -> Iterable[CustomerImpactInput]:
        """
        Fetch customer impact inputs by aggregating order_fulfillment_risk decisions.
        
        Reads from gold_decision_output_v1, groups by customer_id from metrics_json,
        and aggregates counts per customer.
        """
        table_name = self._build_table_name_for_primitive("gold_decision_output_v1")
        tenant_id = str(ctx.tenant_id)
        as_of_ts = ctx.as_of_ts
        
        # Build SQL query to fetch latest order decisions per order
        sql = f"""
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
                        CASE WHEN as_of_ts = ? THEN 0 ELSE 1 END,
                        as_of_ts DESC
                ) as rn
            FROM {table_name}
            WHERE tenant_id = ?
                AND subject_type = 'order'
                AND primitive_name = 'order_fulfillment_risk'
                AND (? IS NULL OR as_of_ts <= ?)
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
        """
        
        logger.info(
            f"Fetching customer impact inputs from {table_name} for tenant {tenant_id}",
            extra={"correlation_id": ctx.correlation_id.value} if ctx.correlation_id else {},
        )
        
        try:
            as_of_ts_str = as_of_ts.isoformat() if as_of_ts else None
            rows = self.client.query(sql, params=[as_of_ts_str, tenant_id, as_of_ts_str, as_of_ts_str])
            
            # Group by customer_id (from metrics_json)
            customers_dict: dict[str, dict] = {}
            
            for row in rows:
                subject_id = str(row.get("subject_id", ""))
                if not subject_id:
                    continue
                
                decision_state = str(row.get("decision_state", ""))
                metrics_json_str = row.get("metrics_json")
                evidence_refs_json_str = row.get("evidence_refs_json")
                as_of_ts_parsed = self._parse_timestamp(row.get("as_of_ts"))
                config_version = str(row.get("config_version", ""))
                
                if not metrics_json_str:
                    logger.warning(f"Skipping row with null metrics_json for subject {subject_id}")
                    continue
                
                # Parse metrics_json to extract customer_id
                try:
                    metrics = json.loads(metrics_json_str)
                except (json.JSONDecodeError, TypeError):
                    logger.warning(f"Failed to parse metrics_json for subject {subject_id}")
                    continue
                
                customer_id = metrics.get("customer_id")
                if customer_id is None:
                    logger.warning(f"Skipping row with null customer_id in metrics_json for subject {subject_id}")
                    continue
                
                customer_id_key = str(customer_id)
                
                # Parse evidence_refs_json
                evidence_refs = []
                if evidence_refs_json_str:
                    try:
                        evidence_refs = json.loads(evidence_refs_json_str)
                        if not isinstance(evidence_refs, list):
                            evidence_refs = []
                    except (json.JSONDecodeError, TypeError):
                        logger.warning(f"Failed to parse evidence_refs_json for subject {subject_id}")
                        evidence_refs = []
                
                # Initialize customer dict if needed
                if customer_id_key not in customers_dict:
                    customers_dict[customer_id_key] = {
                        "tenant_id": str(row.get("tenant_id", tenant_id)),
                        "customer_id": customer_id_key,
                        "as_of_ts": as_of_ts_parsed,
                        "config_version": config_version,
                        "orders": [],
                    }
                else:
                    # Update as_of_ts to latest
                    if as_of_ts_parsed and (customers_dict[customer_id_key]["as_of_ts"] is None or 
                                           as_of_ts_parsed > customers_dict[customer_id_key]["as_of_ts"]):
                        customers_dict[customer_id_key]["as_of_ts"] = as_of_ts_parsed
                
                # Add order info (subject_id is the ordernum)
                customers_dict[customer_id_key]["orders"].append({
                    "order_subject_id": subject_id,
                    "decision_state": decision_state,
                    "evidence_refs": evidence_refs,
                })
            
            # Build CustomerImpactInput objects
            inputs = []
            for customer_id_key, customer_data in customers_dict.items():
                if customer_data["as_of_ts"] is None:
                    logger.warning(f"Skipping customer {customer_id_key} with null as_of_ts")
                    continue
                
                orders = customer_data["orders"]
                total = len(orders)
                at_risk = sum(1 for order in orders if order["decision_state"] == "AT_RISK")
                unknown = sum(1 for order in orders if order["decision_state"] == "UNKNOWN")
                
                # Build at_risk_order_subject_ids (cap at 100)
                at_risk_order_subject_ids = [order["order_subject_id"] for order in orders if order["decision_state"] == "AT_RISK"][:100]
                
                # Build source_order_refs (cap at 100)
                source_order_refs = [
                    SourceOrderRef(
                        order_subject_id=order["order_subject_id"],
                        decision_state=order["decision_state"],
                        evidence_refs=order["evidence_refs"],
                    )
                    for order in orders[:100]
                ]
                
                input_obj = CustomerImpactInput.new(
                    tenant_id=customer_data["tenant_id"],
                    subject_id=customer_id_key,
                    as_of_ts=customer_data["as_of_ts"],
                    config_version=customer_data["config_version"],
                    canonical_version="v1",
                    subject_type="customer",
                    order_count_total=total,
                    order_count_at_risk=at_risk,
                    order_count_unknown=unknown,
                    at_risk_order_subject_ids=at_risk_order_subject_ids,
                    source_order_refs=source_order_refs,
                )
                inputs.append(input_obj)
            
            logger.info(
                f"Fetched {len(inputs)} customer impact input rows for tenant {tenant_id}",
                extra={"correlation_id": ctx.correlation_id.value} if ctx.correlation_id else {},
            )
            return inputs
            
        except Exception as e:
            logger.error(
                f"Error fetching customer impact inputs from {table_name}: {e}",
                extra={"correlation_id": ctx.correlation_id.value} if ctx.correlation_id else {},
            )
            raise
