from __future__ import annotations

import json
import sys
from typing import Iterable, Optional

from opsiq_runtime.application.run_context import RunContext
from opsiq_runtime.domain.common.decision import DecisionResult
from opsiq_runtime.domain.common.evidence import EvidenceSet
from opsiq_runtime.domain.primitives.operational_risk.model import OperationalRiskInput
from opsiq_runtime.ports.outputs_repository import OutputsRepository


class StdoutOutputsRepository(OutputsRepository):
    def write_decisions(
        self, ctx: RunContext, decisions: Iterable[DecisionResult], inputs: Optional[list[OperationalRiskInput]] = None
    ) -> None:
        payload = [
            {
                "state": d.state,
                "confidence": d.confidence,
                "drivers": d.drivers,
                "metrics": d.metrics,
                "evidence_refs": d.evidence_refs,
                "versions": {
                    "primitive_version": d.versions.primitive_version,
                    "canonical_version": d.versions.canonical_version,
                    "config_version": d.versions.config_version,
                },
                "computed_at": d.computed_at.isoformat(),
            }
            for d in decisions
        ]
        sys.stdout.write(json.dumps({"type": "decisions", "data": payload}) + "\n")

    def write_evidence(
        self, ctx: RunContext, evidence_sets: Iterable[EvidenceSet], inputs: Optional[list[OperationalRiskInput]] = None
    ) -> None:
        payload = []
        for e_set in evidence_sets:
            for evidence in e_set.evidence:
                payload.append(
                    {
                        "evidence_id": evidence.evidence_id,
                        "rule_ids": evidence.rule_ids,
                        "thresholds": evidence.thresholds,
                        "references": evidence.references,
                        "observed_at": evidence.observed_at.isoformat(),
                    }
                )
        sys.stdout.write(json.dumps({"type": "evidence", "data": payload}) + "\n")

