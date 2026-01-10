from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ShopperWeeklyAdSlateConfig:
    primitive_name: str = "shopper_weekly_ad_slate"
    primitive_version: str = "1.0.0"
    canonical_version: str = "v1"
    slate_size_k: int = 20
    affinity_top_k: int = 50
    exclude_lookback_days: int = 14
    exclude_by: str = "item_group_id"  # or "gtin"
    category_cap: int | None = None
    min_match_rate_for_high_confidence: float = 0.50
    sparse_emission: bool = True
    ad_id: str = ""  # Required; set via config
    scope_type: str = ""  # Required; set via config
    scope_value: str = ""  # Required; set via config
    hours_window: int = 36  # For ad candidates and affinity freshness
