from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from opsiq_runtime.domain.common.ids import CorrelationId, TenantId


@dataclass(frozen=True)
class RunContext:
    tenant_id: TenantId
    primitive_name: str
    primitive_version: str
    as_of_ts: datetime
    config_version: str
    correlation_id: CorrelationId

    @classmethod
    def from_args(
        cls,
        tenant_id: str,
        primitive_name: str,
        primitive_version: str,
        config_version: str,
        as_of_ts: Optional[datetime] = None,
        correlation_id: Optional[str] = None,
    ) -> "RunContext":
        return cls(
            tenant_id=TenantId(tenant_id),
            primitive_name=primitive_name,
            primitive_version=primitive_version,
            as_of_ts=as_of_ts or datetime.now(timezone.utc),
            config_version=config_version,
            correlation_id=CorrelationId(correlation_id or "auto"),
        )

