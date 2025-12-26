"""Pydantic models for run registry API responses."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class RunRegistryItem(BaseModel):
    """Run registry item representing a single runtime run."""

    correlation_id: str
    primitive_name: str
    primitive_version: str
    status: str = Field(..., description="STARTED | SUCCESS | FAILED")
    started_at: datetime = Field(..., description="ISO timestamp")
    completed_at: datetime | None = None
    duration_ms: int | None = None
    input_count: int | None = None
    decision_count: int | None = None
    at_risk_count: int | None = None
    unknown_count: int | None = None
    error_message: str | None = None


class RunRegistryResponse(BaseModel):
    """Response for run registry endpoint."""

    items: list[RunRegistryItem]
    next_cursor: str | None = None

