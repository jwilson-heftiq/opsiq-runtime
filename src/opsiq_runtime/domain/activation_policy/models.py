from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class ActivationItem:
    """Core item representation for activation policy processing."""

    item_group_id: str  # coalesce(linkcode, gtin)
    gtin: str | None = None
    linkcode: str | None = None
    category: str | None = None
    score: float = 0.0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ExclusionResult:
    """Result of an exclusion check on an ActivationItem."""

    excluded: bool
    reasons: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class PolicyConfig:
    """Configuration for activation policy processing."""

    exclude_by: str = "item_group_id"
    exclude_lookback_days: int = 14
    max_items: int = 20
    category_cap: int | None = None
    min_match_rate_for_high_confidence: float = 0.5


@dataclass(frozen=True)
class PolicyOutcome:
    """Final result of activation policy processing."""

    selected_items: list[ActivationItem]
    excluded_items: list[ActivationItem]
    excluded_count: int
    candidates_count: int
    match_rate: float
    drivers: list[str]
    computed_confidence: str  # "HIGH" | "MEDIUM" | "LOW"
