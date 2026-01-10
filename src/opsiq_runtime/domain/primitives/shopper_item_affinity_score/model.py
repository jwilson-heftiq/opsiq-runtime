from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from opsiq_runtime.domain.common.ids import SubjectId, TenantId


@dataclass(frozen=True)
class ShopperItemAffinityInput:
    tenant_id: TenantId
    subject_type: str
    subject_id: SubjectId
    as_of_ts: datetime
    top_affinity_items: Optional[list[dict]]
    config_version: str
    canonical_version: str
    lookback_days: Optional[int] = None
    top_k: Optional[int] = None

    @staticmethod
    def new(
        tenant_id: str,
        subject_id: str,
        as_of_ts: datetime,
        config_version: str,
        canonical_version: str = "v1",
        subject_type: str = "shopper",
        top_affinity_items: Optional[list[dict]] = None,
        lookback_days: Optional[int] = None,
        top_k: Optional[int] = None,
    ) -> "ShopperItemAffinityInput":
        return ShopperItemAffinityInput(
            tenant_id=TenantId(tenant_id),
            subject_type=subject_type,
            subject_id=SubjectId(subject_id),
            as_of_ts=as_of_ts,
            top_affinity_items=top_affinity_items,
            config_version=config_version,
            canonical_version=canonical_version,
            lookback_days=lookback_days,
            top_k=top_k,
        )
