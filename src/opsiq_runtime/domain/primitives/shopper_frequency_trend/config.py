from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ShopperFrequencyTrendConfig:
    primitive_name: str = "shopper_frequency_trend"
    primitive_version: str = "1.0.0"
    canonical_version: str = "v1"
    baseline_window_days: int = 90
    min_baseline_trips: int = 4
    decline_ratio_threshold: float = 1.5
    improve_ratio_threshold: float = 0.75
    max_reasonable_gap_days: int = 365

