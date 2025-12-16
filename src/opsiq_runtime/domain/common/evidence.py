from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List


@dataclass(frozen=True)
class Evidence:
    evidence_id: str
    rule_ids: List[str]
    thresholds: Dict[str, Any]
    references: Dict[str, Any]
    observed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass(frozen=True)
class EvidenceSet:
    evidence: List[Evidence]

