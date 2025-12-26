"""Pydantic models for decision-related API responses."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class DecisionKey(BaseModel):
    """Decision key identifying a unique decision."""

    tenant_id: str
    subject_type: str
    subject_id: str
    primitive_name: str
    primitive_version: str
    as_of_ts: datetime = Field(..., description="ISO timestamp")


class DecisionListItem(DecisionKey):
    """Decision item for list responses."""

    decision_state: str = Field(..., description="URGENT/WATCHLIST/HEALTHY/UNKNOWN")
    confidence: str = Field(..., description="HIGH/MEDIUM/LOW")
    computed_at: datetime = Field(..., description="ISO timestamp")
    drivers: list[str] = Field(default_factory=list, description="Parsed JSON array")
    metrics: dict[str, Any] = Field(default_factory=dict, description="Parsed JSON object")


class DecisionListResponse(BaseModel):
    """Response for decision list endpoints."""

    items: list[DecisionListItem]
    next_cursor: str | None = None


class DecisionDetail(DecisionKey):
    """Detailed decision information."""

    canonical_version: str
    config_version: str
    decision_state: str = Field(..., description="URGENT/WATCHLIST/HEALTHY/UNKNOWN")
    confidence: str = Field(..., description="HIGH/MEDIUM/LOW")
    computed_at: datetime = Field(..., description="ISO timestamp")
    valid_until: datetime | None = None
    drivers: list[str] = Field(default_factory=list, description="Parsed JSON array")
    metrics: dict[str, Any] = Field(default_factory=dict, description="Parsed JSON object")
    evidence_refs: list[str] = Field(default_factory=list, description="Parsed JSON array of evidence IDs")
    correlation_id: str | None = None


class EvidenceRecord(BaseModel):
    """Evidence record from the evidence table."""

    tenant_id: str
    evidence_id: str
    primitive_name: str
    primitive_version: str
    as_of_ts: datetime = Field(..., description="ISO timestamp")
    computed_at: datetime = Field(..., description="ISO timestamp")
    evidence: dict[str, Any] = Field(default_factory=dict, description="Parsed JSON object")


class DecisionBundle(BaseModel):
    """Decision bundle with composite, components, and evidence."""

    composite: DecisionDetail
    components: dict[str, DecisionDetail] = Field(
        default_factory=dict,
        description="Component decisions keyed by primitive_name (operational_risk, shopper_frequency_trend)",
    )
    evidence: dict[str, list[EvidenceRecord]] = Field(
        default_factory=dict,
        description="Evidence grouped by primitive_name (composite, operational_risk, shopper_frequency_trend)",
    )

