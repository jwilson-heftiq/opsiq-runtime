from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class OperationalRiskConfig:
    at_risk_days: int
    primitive_version: str = "1.0.0"
    canonical_version: str = "v1"

