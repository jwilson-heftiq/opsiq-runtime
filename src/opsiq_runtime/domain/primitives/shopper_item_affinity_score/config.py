from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ShopperItemAffinityConfig:
    primitive_name: str = "shopper_item_affinity_score"
    primitive_version: str = "1.0.0"
    canonical_version: str = "v1"
    lookback_days: int = 90
    top_k: int = 50
