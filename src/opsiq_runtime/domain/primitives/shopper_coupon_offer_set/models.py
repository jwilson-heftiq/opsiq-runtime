from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

from opsiq_runtime.domain.common.ids import SubjectId, TenantId

if TYPE_CHECKING:
    from opsiq_runtime.domain.common.decision import DecisionResult
    from opsiq_runtime.domain.common.evidence import EvidenceSet
    from opsiq_runtime.domain.primitives.shopper_weekly_ad_slate.models import (
        ShopperAffinityRow,
    )


@dataclass(frozen=True)
class CouponOffer:
    """A single coupon offer in the offer set."""

    rank: int
    item_group_id: str
    gtin: str | None
    linkcode: str | None
    affinity_score: float
    baseline_price: float
    offer_price: float
    reasons: list[str]


@dataclass(frozen=True)
class CouponOfferSetInput:
    """Combined input per shopper for coupon offer set evaluation."""

    tenant_id: TenantId
    subject_type: str
    subject_id: SubjectId
    as_of_ts: datetime
    config_version: str
    canonical_version: str
    shopper_affinity: "ShopperAffinityRow | None"
    weekly_ad_item_groups: set[str]  # item_group_ids to exclude (from weekly ad)
    eligible_map: dict[str, dict]  # item_group_id -> {gtin, linkcode, ineligible_reasons}
    recent_purchase_keys: set[str]  # item_group_ids to exclude (recent purchases)
    baseline_prices: dict[tuple[str, str], float]  # (shopper_id, item_group_id) -> baseline_price

    @staticmethod
    def new(
        tenant_id: str,
        subject_id: str,
        as_of_ts: datetime,
        config_version: str,
        canonical_version: str = "v1",
        subject_type: str = "shopper",
        shopper_affinity: "ShopperAffinityRow | None" = None,
        weekly_ad_item_groups: set[str] | None = None,
        eligible_map: dict[str, dict] | None = None,
        recent_purchase_keys: set[str] | None = None,
        baseline_prices: dict[tuple[str, str], float] | None = None,
    ) -> "CouponOfferSetInput":
        return CouponOfferSetInput(
            tenant_id=TenantId(tenant_id),
            subject_type=subject_type,
            subject_id=SubjectId(subject_id),
            as_of_ts=as_of_ts,
            config_version=config_version,
            canonical_version=canonical_version,
            shopper_affinity=shopper_affinity,
            weekly_ad_item_groups=weekly_ad_item_groups or set(),
            eligible_map=eligible_map or {},
            recent_purchase_keys=recent_purchase_keys or set(),
            baseline_prices=baseline_prices or {},
        )


@dataclass(frozen=True)
class CouponOfferSetResult:
    """Evaluator output for shopper coupon offer set."""

    decision: "DecisionResult"  # Forward reference
    evidence_set: "EvidenceSet"  # Forward reference
