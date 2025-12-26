from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from opsiq_runtime.domain.common.ids import SubjectId, TenantId


@dataclass(frozen=True)
class ShopperHealthInput:
    tenant_id: TenantId
    subject_type: str
    subject_id: SubjectId
    as_of_ts: datetime
    risk_state: Optional[str]
    trend_state: Optional[str]
    risk_evidence_refs: list[str]
    trend_evidence_refs: list[str]
    risk_source_as_of_ts: Optional[datetime]
    trend_source_as_of_ts: Optional[datetime]
    canonical_version: str
    config_version: str

    @staticmethod
    def new(
        tenant_id: str,
        subject_id: str,
        as_of_ts: datetime,
        config_version: str,
        canonical_version: str = "v1",
        subject_type: str = "shopper",
        risk_state: Optional[str] = None,
        trend_state: Optional[str] = None,
        risk_evidence_refs: Optional[list[str]] = None,
        trend_evidence_refs: Optional[list[str]] = None,
        risk_source_as_of_ts: Optional[datetime] = None,
        trend_source_as_of_ts: Optional[datetime] = None,
    ) -> "ShopperHealthInput":
        return ShopperHealthInput(
            tenant_id=TenantId(tenant_id),
            subject_type=subject_type,
            subject_id=SubjectId(subject_id),
            as_of_ts=as_of_ts,
            risk_state=risk_state,
            trend_state=trend_state,
            risk_evidence_refs=risk_evidence_refs or [],
            trend_evidence_refs=trend_evidence_refs or [],
            risk_source_as_of_ts=risk_source_as_of_ts,
            trend_source_as_of_ts=trend_source_as_of_ts,
            canonical_version=canonical_version,
            config_version=config_version,
        )

