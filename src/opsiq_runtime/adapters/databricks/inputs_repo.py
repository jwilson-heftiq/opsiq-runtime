from __future__ import annotations

import logging
from datetime import datetime
from typing import Iterable

from opsiq_runtime.adapters.databricks.client import DatabricksSqlClient
from opsiq_runtime.application.run_context import RunContext
from opsiq_runtime.domain.common.ids import CorrelationId
from opsiq_runtime.domain.primitives.operational_risk.model import OperationalRiskInput
from opsiq_runtime.domain.primitives.shopper_frequency_trend.model import ShopperFrequencyInput
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
