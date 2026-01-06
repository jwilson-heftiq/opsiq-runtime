"""Core readiness calculation logic."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from opsiq_runtime.app.api.services.pack_readiness.models import (
    CanonicalFreshnessResult,
    DecisionHealthResult,
    PackReadinessResponse,
    RollupIntegrityResult,
)

logger = logging.getLogger(__name__)


class PackReadinessCalculator:
    """Calculates pack readiness metrics from raw data."""

    def __init__(
        self,
        freshness_threshold_hours: float = 36.0,
        unknown_rate_warn_threshold: float = 0.30,
        unknown_rate_fail_threshold: float = 0.60,
        integrity_warn_threshold: float = 0.95,
        integrity_fail_threshold: float = 0.80,
    ) -> None:
        """
        Initialize calculator with thresholds.

        Args:
            freshness_threshold_hours: Hours before canonical data is considered stale (default 36)
            unknown_rate_warn_threshold: Unknown rate threshold for WARN (default 0.30 = 30%)
            unknown_rate_fail_threshold: Unknown rate threshold for FAIL (default 0.60 = 60%)
            integrity_warn_threshold: Integrity pass rate threshold for WARN (default 0.95 = 95%)
            integrity_fail_threshold: Integrity pass rate threshold for FAIL (default 0.80 = 80%)
        """
        self.freshness_threshold_hours = freshness_threshold_hours
        self.unknown_rate_warn_threshold = unknown_rate_warn_threshold
        self.unknown_rate_fail_threshold = unknown_rate_fail_threshold
        self.integrity_warn_threshold = integrity_warn_threshold
        self.integrity_fail_threshold = integrity_fail_threshold

    def calculate_canonical_freshness(
        self, table_name: str, last_as_of_ts: datetime | None
    ) -> CanonicalFreshnessResult:
        """
        Calculate canonical freshness status for a table.

        Args:
            table_name: Canonical table name
            last_as_of_ts: Last as_of_ts timestamp from the table (None if no data)

        Returns:
            CanonicalFreshnessResult with status
        """
        if last_as_of_ts is None:
            return CanonicalFreshnessResult(
                table=table_name,
                last_as_of_ts=None,
                hours_since_last_update=None,
                status="FAIL",
            )

        now = datetime.now(timezone.utc)
        hours_since = (now - last_as_of_ts).total_seconds() / 3600.0
        
        # Future snapshot timestamps are invalid and should be treated as FAIL
        if hours_since < 0:
            return CanonicalFreshnessResult(
                table=table_name,
                last_as_of_ts=last_as_of_ts,
                hours_since_last_update=hours_since,
                status="FAIL",
            )
            
        if hours_since <= self.freshness_threshold_hours:
            status = "PASS"
        else:
            status = "WARN"

        return CanonicalFreshnessResult(
            table=table_name,
            last_as_of_ts=last_as_of_ts,
            hours_since_last_update=hours_since,
            status=status,
        )

    def calculate_decision_health(
        self,
        primitive_name: str,
        total_decisions: int,
        state_counts: dict[str, int],
        last_computed_at: datetime | None,
    ) -> DecisionHealthResult:
        """
        Calculate decision health status for a primitive.

        Args:
            primitive_name: Primitive name
            total_decisions: Total number of decisions in time window
            state_counts: Dictionary of state -> count
            last_computed_at: Most recent computed_at timestamp

        Returns:
            DecisionHealthResult with status
        """
        if total_decisions == 0:
            return DecisionHealthResult(
                primitive_name=primitive_name,
                total_decisions=0,
                state_counts={},
                unknown_rate=0.0,
                last_computed_at=None,
                status="FAIL",
            )

        unknown_count = state_counts.get("UNKNOWN", 0)
        unknown_rate = unknown_count / total_decisions if total_decisions > 0 else 0.0

        if unknown_rate >= self.unknown_rate_fail_threshold:
            status = "FAIL"
        elif unknown_rate >= self.unknown_rate_warn_threshold:
            status = "WARN"
        else:
            status = "PASS"

        return DecisionHealthResult(
            primitive_name=primitive_name,
            total_decisions=total_decisions,
            state_counts=state_counts,
            unknown_rate=unknown_rate,
            last_computed_at=last_computed_at,
            status=status,
        )

    def calculate_rollup_integrity(
        self, check_name: str, total: int, passed: int, zero_total_status: str = "FAIL"
    ) -> RollupIntegrityResult:
        """
        Calculate rollup integrity check status.

        Args:
            check_name: Name of the integrity check
            total: Total number of decisions checked
            passed: Number of decisions that passed the check
            zero_total_status: Status to return when total==0 (default "FAIL", can be "WARN")

        Returns:
            RollupIntegrityResult with status
        """
        if total == 0:
            return RollupIntegrityResult(
                check=check_name,
                pass_rate=0.0,
                status=zero_total_status,
            )

        pass_rate = passed / total

        if pass_rate < self.integrity_fail_threshold:
            status = "FAIL"
        elif pass_rate < self.integrity_warn_threshold:
            status = "WARN"
        else:
            status = "PASS"

        return RollupIntegrityResult(
            check=check_name,
            pass_rate=pass_rate,
            status=status,
        )

    def aggregate_status(
        self,
        canonical_freshness: list[CanonicalFreshnessResult],
        decision_health: list[DecisionHealthResult],
        rollup_integrity: list[RollupIntegrityResult],
    ) -> str:
        """
        Aggregate overall status from all metrics.

        Rules:
        - If any metric has FAIL status → overall FAIL
        - Else if any metric has WARN status → overall WARN
        - Else → overall PASS

        Args:
            canonical_freshness: List of canonical freshness results
            decision_health: List of decision health results
            rollup_integrity: List of rollup integrity results

        Returns:
            Overall status: "PASS", "WARN", or "FAIL"
        """
        all_results = list(canonical_freshness) + list(decision_health) + list(rollup_integrity)

        if not all_results:
            return "FAIL"

        # Check for any FAIL
        if any(r.status == "FAIL" for r in all_results):
            return "FAIL"

        # Check for any WARN
        if any(r.status == "WARN" for r in all_results):
            return "WARN"

        # All PASS
        return "PASS"

    def build_readiness_response(
        self,
        tenant_id: str,
        pack_id: str,
        pack_version: str,
        canonical_freshness: list[CanonicalFreshnessResult],
        decision_health: list[DecisionHealthResult],
        rollup_integrity: list[RollupIntegrityResult],
    ) -> PackReadinessResponse:
        """
        Build complete readiness response.

        Args:
            tenant_id: Tenant ID
            pack_id: Pack ID
            pack_version: Pack version
            canonical_freshness: List of canonical freshness results
            decision_health: List of decision health results
            rollup_integrity: List of rollup integrity results

        Returns:
            PackReadinessResponse with overall status
        """
        overall_status = self.aggregate_status(canonical_freshness, decision_health, rollup_integrity)

        return PackReadinessResponse(
            tenant_id=tenant_id,
            pack_id=pack_id,
            pack_version=pack_version,
            overall_status=overall_status,
            canonical_freshness=canonical_freshness,
            decision_health=decision_health,
            rollup_integrity=rollup_integrity,
            computed_at=datetime.now(timezone.utc),
        )

