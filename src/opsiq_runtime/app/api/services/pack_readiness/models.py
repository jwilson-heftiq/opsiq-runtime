"""Pydantic models for pack readiness responses."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class CanonicalFreshnessResult(BaseModel):
    """Canonical input freshness result for a single table."""

    table: str = Field(..., description="Canonical table name")
    last_as_of_ts: datetime | None = Field(None, description="Last as_of_ts timestamp in the table")
    hours_since_last_update: float | None = Field(None, description="Hours since last update")
    status: str = Field(..., description="PASS, WARN, or FAIL")


class DecisionHealthResult(BaseModel):
    """Decision output health result for a single primitive."""

    primitive_name: str = Field(..., description="Primitive name")
    total_decisions: int = Field(..., description="Total decisions in last 24 hours")
    state_counts: dict[str, int] = Field(..., description="Count of decisions by state")
    unknown_rate: float = Field(..., description="Rate of UNKNOWN decisions (0.0 to 1.0)")
    last_computed_at: datetime | None = Field(None, description="Most recent computed_at timestamp")
    status: str = Field(..., description="PASS, WARN, or FAIL")


class RollupIntegrityResult(BaseModel):
    """Rollup integrity check result."""

    check: str = Field(..., description="Check name (e.g., 'order_line_has_ordernum')")
    pass_rate: float = Field(..., description="Pass rate (0.0 to 1.0)")
    status: str = Field(..., description="PASS, WARN, or FAIL")


class PackReadinessResponse(BaseModel):
    """Complete pack readiness response."""

    tenant_id: str = Field(..., description="Tenant ID")
    pack_id: str = Field(..., description="Pack ID")
    pack_version: str = Field(..., description="Pack version")
    overall_status: str = Field(..., description="Overall status: PASS, WARN, or FAIL")
    canonical_freshness: list[CanonicalFreshnessResult] = Field(
        default_factory=list, description="Canonical input freshness results"
    )
    decision_health: list[DecisionHealthResult] = Field(
        default_factory=list, description="Decision output health results"
    )
    rollup_integrity: list[RollupIntegrityResult] = Field(
        default_factory=list, description="Rollup integrity check results"
    )
    computed_at: datetime = Field(..., description="When readiness was computed")

