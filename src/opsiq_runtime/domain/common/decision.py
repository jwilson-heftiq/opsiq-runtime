from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List

from opsiq_runtime.domain.common.versioning import VersionInfo


@dataclass(frozen=True)
class DecisionResult:
    state: str
    drivers: List[str]
    metrics: Dict[str, float]
    evidence_refs: List[str]
    versions: VersionInfo
    computed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    valid_until: datetime | None = None

