from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from opsiq_runtime.domain.common.ids import SubjectId, TenantId


@dataclass(frozen=True)
class SourceOrderRef:
    """Reference to a source order decision."""

    order_subject_id: str
    decision_state: str
    evidence_refs: list[str]


@dataclass(frozen=True)
class CustomerImpactInput:
    tenant_id: TenantId
    subject_type: str
    subject_id: SubjectId
    as_of_ts: datetime
    order_count_total: int
    order_count_at_risk: int
    order_count_unknown: int
    at_risk_order_subject_ids: list[str]
    source_order_refs: list[SourceOrderRef]
    canonical_version: str
    config_version: str

    @staticmethod
    def new(
        tenant_id: str,
        subject_id: str,
        as_of_ts: datetime,
        config_version: str,
        canonical_version: str = "v1",
        subject_type: str = "customer",
        order_count_total: int = 0,
        order_count_at_risk: int = 0,
        order_count_unknown: int = 0,
        at_risk_order_subject_ids: Optional[list[str]] = None,
        source_order_refs: Optional[list[SourceOrderRef]] = None,
    ) -> "CustomerImpactInput":
        return CustomerImpactInput(
            tenant_id=TenantId(tenant_id),
            subject_type=subject_type,
            subject_id=SubjectId(subject_id),
            as_of_ts=as_of_ts,
            order_count_total=order_count_total,
            order_count_at_risk=order_count_at_risk,
            order_count_unknown=order_count_unknown,
            at_risk_order_subject_ids=at_risk_order_subject_ids or [],
            source_order_refs=source_order_refs or [],
            canonical_version=canonical_version,
            config_version=config_version,
        )

