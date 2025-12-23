from __future__ import annotations

from datetime import datetime
from typing import Protocol

from opsiq_runtime.domain.common.ids import SubjectId, TenantId


class CommonInput(Protocol):
    """Protocol for common input fields shared across all primitives."""

    tenant_id: TenantId
    subject_type: str
    subject_id: SubjectId
    as_of_ts: datetime
    config_version: str

