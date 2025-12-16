from __future__ import annotations

from dataclasses import dataclass
from typing import NewType

TenantId = NewType("TenantId", str)
SubjectId = NewType("SubjectId", str)


@dataclass(frozen=True)
class CorrelationId:
    value: str

