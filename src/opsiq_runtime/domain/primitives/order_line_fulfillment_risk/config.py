from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class OrderLineFulfillmentRiskConfig:
    primitive_name: str = "order_line_fulfillment_risk"
    primitive_version: str = "1.0.0"
    canonical_version: str = "v1"
    closed_statuses: set[str] = None

    def __post_init__(self):
        if self.closed_statuses is None:
            object.__setattr__(self, "closed_statuses", {"CLOSED", "CANCELLED"})

