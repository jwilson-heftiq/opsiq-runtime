from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CustomerImpactConfig:
    primitive_name: str = "customer_order_impact_risk"
    primitive_version: str = "1.0.0"
    canonical_version: str = "v1"
    high_threshold: int = 5
    medium_threshold: int = 2

