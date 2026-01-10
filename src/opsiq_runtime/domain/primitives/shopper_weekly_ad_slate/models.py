from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Optional

from opsiq_runtime.domain.common.ids import SubjectId, TenantId

if TYPE_CHECKING:
    from opsiq_runtime.domain.common.decision import DecisionResult
    from opsiq_runtime.domain.common.evidence import EvidenceSet


@dataclass(frozen=True)
class AdCandidate:
    """Represents a candidate ad item from the weekly ad canonical table."""

    ad_id: str
    ad_group_id: str
    scope_type: str
    scope_value: str
    as_of_ts: datetime
    gtin: str | None
    linkcode: str | None
    item_group_id: str  # COALESCE(linkcode, gtin)
    title: str | None
    promo_text: str | None
    primary_image_url: str | None
    promo_price: float | None
    ad_price_raw: str | None
    ad_price_uom: str | None
    ad_price_qualifier: str | None


@dataclass(frozen=True)
class ShopperAffinityRow:
    """Per-shopper affinity data from the top affinity feature table."""

    shopper_id: str
    as_of_ts: datetime
    top_affinity_items: list[dict]  # Array of structs with rank, item_group_id, affinity_score, etc.


@dataclass(frozen=True)
class RecentPurchaseKey:
    """Exclusion key per shopper from recent purchase history."""

    shopper_id: str
    item_group_id: str
    last_purchase_ts: datetime | None
    category: str | None


@dataclass(frozen=True)
class ShopperWeeklyAdSlateInput:
    """Combined input per shopper for weekly ad slate evaluation."""

    tenant_id: TenantId
    subject_type: str
    subject_id: SubjectId
    as_of_ts: datetime
    config_version: str
    canonical_version: str
    candidates: list[AdCandidate]  # Same for all shoppers
    shopper_affinity: ShopperAffinityRow | None
    recent_purchase_keys: set[str]  # item_group_ids to exclude

    @staticmethod
    def new(
        tenant_id: str,
        subject_id: str,
        as_of_ts: datetime,
        config_version: str,
        canonical_version: str = "v1",
        subject_type: str = "shopper",
        candidates: list[AdCandidate] | None = None,
        shopper_affinity: ShopperAffinityRow | None = None,
        recent_purchase_keys: set[str] | None = None,
    ) -> "ShopperWeeklyAdSlateInput":
        return ShopperWeeklyAdSlateInput(
            tenant_id=TenantId(tenant_id),
            subject_type=subject_type,
            subject_id=SubjectId(subject_id),
            as_of_ts=as_of_ts,
            config_version=config_version,
            canonical_version=canonical_version,
            candidates=candidates or [],
            shopper_affinity=shopper_affinity,
            recent_purchase_keys=recent_purchase_keys or set(),
        )


@dataclass(frozen=True)
class SlateItem:
    """Output item in the ranked slate."""

    rank: int
    item_group_id: str
    gtin: str | None
    linkcode: str | None
    score: float
    title: str | None
    promo_price: float | None
    ad_group_id: str
    reasons: list[str]


@dataclass(frozen=True)
class ShopperWeeklyAdSlateResult:
    """Evaluator output for shopper weekly ad slate."""

    decision: "DecisionResult"  # Forward reference
    evidence_set: "EvidenceSet"  # Forward reference
