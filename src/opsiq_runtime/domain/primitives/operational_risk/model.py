from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from opsiq_runtime.domain.common.ids import SubjectId, TenantId


@dataclass(frozen=True)
class OperationalRiskInput:
    tenant_id: TenantId
    subject_type: str
    subject_id: SubjectId
    as_of_ts: datetime
    last_trip_ts: Optional[datetime]
    days_since_last_trip: Optional[int]
    config_version: str
    canonical_version: str

    @staticmethod
    def new(
        tenant_id: str,
        subject_id: str,
        as_of_ts: datetime,
        last_trip_ts: Optional[datetime],
        config_version: str,
        canonical_version: str,
        subject_type: str = "shopper",
        days_since_last_trip: Optional[int] = None,
    ) -> "OperationalRiskInput":
        return OperationalRiskInput(
            tenant_id=TenantId(tenant_id),
            subject_type=subject_type,
            subject_id=SubjectId(subject_id),
            as_of_ts=as_of_ts,
            last_trip_ts=last_trip_ts,
            days_since_last_trip=days_since_last_trip,
            config_version=config_version,
            canonical_version=canonical_version,
        )

