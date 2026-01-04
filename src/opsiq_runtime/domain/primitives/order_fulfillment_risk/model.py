from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from opsiq_runtime.domain.common.ids import SubjectId, TenantId


@dataclass(frozen=True)
class SourceLineRef:
    """Reference to a source order_line decision."""

    line_subject_id: str
    decision_state: str
    evidence_refs: list[str]


@dataclass(frozen=True)
class OrderRiskInput:
    tenant_id: TenantId
    subject_type: str
    subject_id: SubjectId
    as_of_ts: datetime
    customer_id: Optional[str]
    order_line_count_total: int
    order_line_count_at_risk: int
    order_line_count_unknown: int
    order_line_count_not_at_risk: int
    at_risk_line_subject_ids: list[str]
    source_line_refs: list[SourceLineRef]
    canonical_version: str
    config_version: str

    @staticmethod
    def new(
        tenant_id: str,
        subject_id: str,
        as_of_ts: datetime,
        config_version: str,
        canonical_version: str = "v1",
        subject_type: str = "order",
        customer_id: Optional[str] = None,
        order_line_count_total: int = 0,
        order_line_count_at_risk: int = 0,
        order_line_count_unknown: int = 0,
        order_line_count_not_at_risk: int = 0,
        at_risk_line_subject_ids: Optional[list[str]] = None,
        source_line_refs: Optional[list[SourceLineRef]] = None,
    ) -> "OrderRiskInput":
        return OrderRiskInput(
            tenant_id=TenantId(tenant_id),
            subject_type=subject_type,
            subject_id=SubjectId(subject_id),
            as_of_ts=as_of_ts,
            customer_id=customer_id,
            order_line_count_total=order_line_count_total,
            order_line_count_at_risk=order_line_count_at_risk,
            order_line_count_unknown=order_line_count_unknown,
            order_line_count_not_at_risk=order_line_count_not_at_risk,
            at_risk_line_subject_ids=at_risk_line_subject_ids or [],
            source_line_refs=source_line_refs or [],
            canonical_version=canonical_version,
            config_version=config_version,
        )

