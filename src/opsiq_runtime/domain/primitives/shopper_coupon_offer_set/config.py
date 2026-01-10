from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ShopperCouponOfferSetConfig:
    primitive_name: str = "shopper_coupon_offer_set"
    primitive_version: str = "1.0.0"
    canonical_version: str = "v1"
    max_offers: int = 10
    discount_pct: int = 25  # 25% discount means offer_price = baseline_price * 0.75
    affinity_top_k: int = 50
    exclude_lookback_days: int = 14
    min_match_rate_for_high_confidence: float = 0.50
    sparse_emission: bool = True
    ad_hours_window: int = 72  # For current ad exclusion set
    pricing_fallback_mode: str = "skip"  # "skip" items missing baseline_price
    category_cap: int | None = None  # Optional per-category cap
    ad_id: str = ""  # Required; set via config
    scope_type: str = ""  # Required; set via config
    scope_value: str = ""  # Required; set via config
    hours_window: int = 72  # For affinity and eligibility freshness
