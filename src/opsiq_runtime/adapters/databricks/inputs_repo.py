from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Iterable

from opsiq_runtime.adapters.databricks.client import DatabricksSqlClient
from opsiq_runtime.application.run_context import RunContext
from opsiq_runtime.domain.common.ids import CorrelationId
from opsiq_runtime.domain.primitives.operational_risk.model import OperationalRiskInput
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
