from __future__ import annotations

from typing import Iterable, Optional, Protocol

from opsiq_runtime.application.run_context import RunContext
from opsiq_runtime.domain.common.decision import DecisionResult
from opsiq_runtime.domain.common.evidence import EvidenceSet
from opsiq_runtime.domain.primitives.operational_risk.model import OperationalRiskInput


class OutputsRepository(Protocol):
    def write_decisions(
        self, ctx: RunContext, decisions: Iterable[DecisionResult], inputs: Optional[list[OperationalRiskInput]] = None
    ) -> None: ...

    def write_evidence(
        self, ctx: RunContext, evidence_sets: Iterable[EvidenceSet], inputs: Optional[list[OperationalRiskInput]] = None
    ) -> None: ...

