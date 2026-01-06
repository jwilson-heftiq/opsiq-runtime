"""Pack readiness service API."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from opsiq_runtime.adapters.databricks.client import DatabricksSqlClient
from opsiq_runtime.app.api.services.pack_readiness.calculator import PackReadinessCalculator
from opsiq_runtime.app.api.services.pack_readiness.databricks_queries import (
    get_canonical_freshness_query,
    get_decision_health_query,
    get_rollup_integrity_query,
)
from opsiq_runtime.app.api.services.pack_readiness.models import (
    CanonicalFreshnessResult,
    DecisionHealthResult,
    PackReadinessResponse,
    RollupIntegrityResult,
)
from opsiq_runtime.settings import Settings

logger = logging.getLogger(__name__)


class PackReadinessService:
    """Service for calculating pack readiness metrics."""

    def __init__(
        self,
        settings: Settings,
        db_client: DatabricksSqlClient | None = None,
        calculator: PackReadinessCalculator | None = None,
    ) -> None:
        """
        Initialize the pack readiness service.

        Args:
            settings: Application settings
            db_client: Optional Databricks client (if None, will return WARN status)
            calculator: Optional calculator instance (creates default if None)
        """
        self.settings = settings
        self.db_client = db_client
        self.calculator = calculator or PackReadinessCalculator()

    def _build_table_name(self, table_name: str) -> str:
        """Build fully qualified table name with catalog and schema if specified."""
        parts = []
        if self.settings.databricks_catalog:
            parts.append(self.settings.databricks_catalog)
        if self.settings.databricks_schema:
            parts.append(self.settings.databricks_schema)
        parts.append(table_name)
        return ".".join(parts)

    def _get_canonical_freshness(
        self, tenant_id: str, canonical_table: str
    ) -> CanonicalFreshnessResult:
        """
        Get canonical freshness for a single table.

        Args:
            tenant_id: Tenant ID
            canonical_table: Canonical table name (without prefix)

        Returns:
            CanonicalFreshnessResult
        """
        if not self.db_client:
            return CanonicalFreshnessResult(
                table=canonical_table,
                last_as_of_ts=None,
                hours_since_last_update=None,
                status="WARN",
            )

        try:
            table_name = self._build_table_name(f"{self.settings.databricks_table_prefix}{canonical_table}")
            sql, params = get_canonical_freshness_query(table_name)
            logger.info(f"Executing canonical freshness query for {canonical_table}: {sql} with params: tenant_id={tenant_id}")
            results = self.db_client.query(sql, [tenant_id] + params)
            logger.info(f"Query returned {len(results)} row(s) for {canonical_table}")

            last_as_of_ts = None
            if results and results[0].get("last_as_of_ts"):
                ts_value = results[0]["last_as_of_ts"]
                logger.info(f"Raw timestamp value from Databricks for {canonical_table}: {ts_value} (type: {type(ts_value)})")
                if isinstance(ts_value, str):
                    # Try ISO format first
                    try:
                        last_as_of_ts = datetime.fromisoformat(ts_value.replace("Z", "+00:00"))
                    except ValueError:
                        # Try other common formats
                        for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"]:
                            try:
                                last_as_of_ts = datetime.strptime(ts_value, fmt)
                                # Make timezone-aware
                                if last_as_of_ts.tzinfo is None:
                                    last_as_of_ts = last_as_of_ts.replace(tzinfo=timezone.utc)
                                break
                            except ValueError:
                                continue
                        if last_as_of_ts is None:
                            logger.warning(f"Could not parse timestamp: {ts_value}")
                elif isinstance(ts_value, datetime):
                    last_as_of_ts = ts_value
                    # Ensure timezone-aware
                    if last_as_of_ts.tzinfo is None:
                        last_as_of_ts = last_as_of_ts.replace(tzinfo=timezone.utc)
                else:
                    logger.warning(f"Unexpected timestamp type: {type(ts_value)}, value: {ts_value}")
                
                # Validate datetime is reasonable (not too far in future/past)
                if last_as_of_ts:
                    now = datetime.now(timezone.utc)
                    years_diff = abs((now - last_as_of_ts).days / 365.25)
                    if years_diff > 10:  # More than 10 years difference is suspicious
                        logger.warning(
                            f"Suspicious timestamp from Databricks for {canonical_table}: parsed={last_as_of_ts}, "
                            f"raw={ts_value}, {years_diff:.1f} years from now (current: {now})"
                        )
                    else:
                        logger.info(
                            f"Parsed timestamp for {canonical_table}: {last_as_of_ts} "
                            f"(raw: {ts_value}, {years_diff:.2f} years from now)"
                        )

            return self.calculator.calculate_canonical_freshness(canonical_table, last_as_of_ts)
        except Exception as e:
            logger.warning(f"Error querying canonical freshness for {canonical_table}: {e}")
            return CanonicalFreshnessResult(
                table=canonical_table,
                last_as_of_ts=None,
                hours_since_last_update=None,
                status="WARN",
            )

    def _get_decision_health(
        self, tenant_id: str, primitive_names: list[str]
    ) -> list[DecisionHealthResult]:
        """
        Get decision health for primitives.

        Args:
            tenant_id: Tenant ID
            primitive_names: List of primitive names to check

        Returns:
            List of DecisionHealthResult
        """
        if not self.db_client:
            return [
                DecisionHealthResult(
                    primitive_name=name,
                    total_decisions=0,
                    state_counts={},
                    unknown_rate=0.0,
                    last_computed_at=None,
                    status="WARN",
                )
                for name in primitive_names
            ]

        try:
            decision_table = self._build_table_name(
                f"{self.settings.databricks_table_prefix}gold_decision_output_v1"
            )
            sql, params = get_decision_health_query(decision_table, primitive_names)
            results = self.db_client.query(sql, [tenant_id] + params)

            health_results = []
            for row in results:
                primitive_name = row.get("primitive_name")
                if not primitive_name:
                    continue

                total = row.get("total_decisions", 0) or 0
                at_risk = row.get("at_risk_count", 0) or 0
                not_at_risk = row.get("not_at_risk_count", 0) or 0
                unknown = row.get("unknown_count", 0) or 0

                state_counts = {
                    "AT_RISK": at_risk,
                    "NOT_AT_RISK": not_at_risk,
                    "UNKNOWN": unknown,
                }

                last_computed_at = None
                if row.get("last_computed_at"):
                    ts_value = row["last_computed_at"]
                    if isinstance(ts_value, str):
                        # Try ISO format first
                        try:
                            last_computed_at = datetime.fromisoformat(ts_value.replace("Z", "+00:00"))
                        except ValueError:
                            # Try other common formats
                            for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"]:
                                try:
                                    last_computed_at = datetime.strptime(ts_value, fmt)
                                    # Make timezone-aware
                                    if last_computed_at.tzinfo is None:
                                        last_computed_at = last_computed_at.replace(tzinfo=timezone.utc)
                                    break
                                except ValueError:
                                    continue
                            if last_computed_at is None:
                                logger.warning(f"Could not parse timestamp: {ts_value}")
                    elif isinstance(ts_value, datetime):
                        last_computed_at = ts_value
                        # Ensure timezone-aware
                        if last_computed_at.tzinfo is None:
                            last_computed_at = last_computed_at.replace(tzinfo=timezone.utc)
                    else:
                        logger.warning(f"Unexpected timestamp type: {type(ts_value)}, value: {ts_value}")

                health_result = self.calculator.calculate_decision_health(
                    primitive_name, total, state_counts, last_computed_at
                )
                health_results.append(health_result)

            # Ensure all primitives are represented (even if no decisions)
            found_names = {r.primitive_name for r in health_results}
            for name in primitive_names:
                if name not in found_names:
                    health_results.append(
                        DecisionHealthResult(
                            primitive_name=name,
                            total_decisions=0,
                            state_counts={},
                            unknown_rate=0.0,
                            last_computed_at=None,
                            status="FAIL",
                        )
                    )

            return health_results
        except Exception as e:
            logger.warning(f"Error querying decision health: {e}")
            return [
                DecisionHealthResult(
                    primitive_name=name,
                    total_decisions=0,
                    state_counts={},
                    unknown_rate=0.0,
                    last_computed_at=None,
                    status="WARN",
                )
                for name in primitive_names
            ]

    def _get_rollup_integrity(
        self, tenant_id: str, pack_id: str, primitive_names: list[str]
    ) -> list[RollupIntegrityResult]:
        """
        Get rollup integrity checks (manufacturing pack only).

        Args:
            tenant_id: Tenant ID
            pack_id: Pack ID (only "order_fulfillment_risk" has integrity checks)
            primitive_names: List of primitive names in the pack

        Returns:
            List of RollupIntegrityResult
        """
        # Only manufacturing pack has rollup integrity checks
        if pack_id != "order_fulfillment_risk":
            return []

        if not self.db_client:
            return []

        integrity_results = []

        try:
            decision_table = self._build_table_name(
                f"{self.settings.databricks_table_prefix}gold_decision_output_v1"
            )

            # Check order_line_fulfillment_risk for ordernum
            if "order_line_fulfillment_risk" in primitive_names:
                sql, params = get_rollup_integrity_query(
                    decision_table, "order_line_fulfillment_risk"
                )
                results = self.db_client.query(sql, [tenant_id] + params)
                if results:
                    row = results[0]
                    total = row.get("total", 0) or 0
                    has_ordernum = row.get("has_ordernum", 0) or 0
                    integrity_results.append(
                        self.calculator.calculate_rollup_integrity(
                            "order_line_has_ordernum", total, has_ordernum
                        )
                    )

            # Check order_fulfillment_risk for customer_id
            if "order_fulfillment_risk" in primitive_names:
                sql, params = get_rollup_integrity_query(decision_table, "order_fulfillment_risk")
                results = self.db_client.query(sql, [tenant_id] + params)
                if results:
                    row = results[0]
                    total = row.get("total", 0) or 0
                    has_customer_id = row.get("has_customer_id", 0) or 0
                    integrity_results.append(
                        self.calculator.calculate_rollup_integrity(
                            "order_has_customer_id", total, has_customer_id
                        )
                    )

            # Check customer_order_impact_risk for at_risk_order_subject_ids
            if "customer_order_impact_risk" in primitive_names:
                sql, params = get_rollup_integrity_query(
                    decision_table, "customer_order_impact_risk"
                )
                results = self.db_client.query(sql, [tenant_id] + params)
                if results:
                    row = results[0]
                    total = row.get("total", 0) or 0
                    has_impacted_order_ids = row.get("has_impacted_order_ids", 0) or 0
                    integrity_results.append(
                        self.calculator.calculate_rollup_integrity(
                            "customer_has_impacted_orders", total, has_impacted_order_ids, zero_total_status="WARN"
                        )
                    )
        except Exception as e:
            logger.warning(f"Error querying rollup integrity: {e}")
            # Return empty list on error (integrity checks are optional)

        return integrity_results

    def calculate_pack_readiness(
        self, tenant_id: str, pack_id: str, pack_version: str, pack_definition: dict[str, Any]
    ) -> PackReadinessResponse:
        """
        Calculate complete readiness for a pack.

        Args:
            tenant_id: Tenant ID
            pack_id: Pack ID
            pack_version: Pack version
            pack_definition: Pack definition dictionary

        Returns:
            PackReadinessResponse with all metrics
        """
        # Extract canonical inputs from primitives
        canonical_inputs = set()
        primitive_names = []
        for primitive in pack_definition.get("primitives", []):
            primitive_name = primitive.get("primitive_name")
            if primitive_name:
                primitive_names.append(primitive_name)
            depends_on = primitive.get("depends_on", {})
            for canonical_input in depends_on.get("canonical_inputs", []):
                canonical_inputs.add(canonical_input)

        # Calculate canonical freshness
        canonical_freshness = [
            self._get_canonical_freshness(tenant_id, table) for table in canonical_inputs
        ]

        # Calculate decision health
        decision_health = self._get_decision_health(tenant_id, primitive_names)

        # Calculate rollup integrity (manufacturing pack only)
        rollup_integrity = self._get_rollup_integrity(tenant_id, pack_id, primitive_names)

        # Build response
        return self.calculator.build_readiness_response(
            tenant_id=tenant_id,
            pack_id=pack_id,
            pack_version=pack_version,
            canonical_freshness=canonical_freshness,
            decision_health=decision_health,
            rollup_integrity=rollup_integrity,
        )

