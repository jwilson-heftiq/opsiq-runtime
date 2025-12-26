from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ShopperHealthConfig:
    primitive_name: str = "shopper_health_classification"
    primitive_version: str = "1.0.0"
    canonical_version: str = "v1"

