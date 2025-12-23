from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from opsiq_runtime.domain.common.ids import SubjectId, TenantId


@dataclass(frozen=True)
class ShopperFrequencyInput:
    tenant_id: TenantId
    subject_type: str
    subject_id: SubjectId
    as_of_ts: datetime
    last_trip_ts: Optional[datetime]
    prev_trip_ts: Optional[datetime]
    recent_gap_days: Optional[float]
    baseline_avg_gap_days: Optional[float]
    baseline_trip_count: Optional[int]
    baseline_window_days: Optional[int]
    canonical_version: str
    config_version: str

    @staticmethod
    def new(
        tenant_id: str,
        subject_id: str,
        as_of_ts: datetime,
        last_trip_ts: Optional[datetime],
        prev_trip_ts: Optional[datetime],
        config_version: str,
        canonical_version: str = "v1",
        subject_type: str = "shopper",
        recent_gap_days: Optional[float] = None,
        baseline_avg_gap_days: Optional[float] = None,
        baseline_trip_count: Optional[int] = None,
        baseline_window_days: Optional[int] = None,
    ) -> "ShopperFrequencyInput":
        # Compute recent_gap_days if missing but last/prev exist
        computed_recent_gap = recent_gap_days
        if computed_recent_gap is None and last_trip_ts is not None and prev_trip_ts is not None:
            delta = last_trip_ts.date() - prev_trip_ts.date()
            computed_recent_gap = float(delta.days)

        return ShopperFrequencyInput(
            tenant_id=TenantId(tenant_id),
            subject_type=subject_type,
            subject_id=SubjectId(subject_id),
            as_of_ts=as_of_ts,
            last_trip_ts=last_trip_ts,
            prev_trip_ts=prev_trip_ts,
            recent_gap_days=computed_recent_gap,
            baseline_avg_gap_days=baseline_avg_gap_days,
            baseline_trip_count=baseline_trip_count,
            baseline_window_days=baseline_window_days,
            canonical_version=canonical_version,
            config_version=config_version,
        )

