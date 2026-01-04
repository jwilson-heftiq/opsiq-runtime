from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class OrderRiskConfig:
    primitive_name: str = "order_fulfillment_risk"
    primitive_version: str = "1.0.0"
    canonical_version: str = "v1"

