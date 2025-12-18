from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable, Optional

from opsiq_runtime.application.run_context import RunContext
from opsiq_runtime.domain.common.decision import DecisionResult
from opsiq_runtime.domain.common.evidence import EvidenceSet
from opsiq_runtime.domain.primitives.operational_risk.model import OperationalRiskInput
from opsiq_runtime.ports.outputs_repository import OutputsRepository
from opsiq_runtime.settings import get_settings


class FileOutputsRepository(OutputsRepository):
    def __init__(self, output_dir: str | None = None) -> None:
        settings = get_settings()
        self.output_dir = Path(output_dir or settings.output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def _write_jsonl(self, path: Path, items: Iterable[dict]) -> None:
        with path.open("w") as f:
            for item in items:
                f.write(json.dumps(item))
                f.write("\n")

    def write_decisions(
        self, ctx: RunContext, decisions: Iterable[DecisionResult], inputs: Optional[list[OperationalRiskInput]] = None
    ) -> None:
        path = self.output_dir / f"{ctx.primitive_name}_decisions.jsonl"
        serializable = [
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
                "valid_until": d.valid_until.isoformat() if d.valid_until else None,
            }
            for d in decisions
        ]
        self._write_jsonl(path, serializable)

    def write_evidence(
        self, ctx: RunContext, evidence_sets: Iterable[EvidenceSet], inputs: Optional[list[OperationalRiskInput]] = None
    ) -> None:
        path = self.output_dir / f"{ctx.primitive_name}_evidence.jsonl"
        serializable = []
        for e_set in evidence_sets:
            for evidence in e_set.evidence:
                serializable.append(
                    {
                        "evidence_id": evidence.evidence_id,
                        "rule_ids": evidence.rule_ids,
                        "thresholds": evidence.thresholds,
                        "references": evidence.references,
                        "observed_at": evidence.observed_at.isoformat(),
                    }
                )
        self._write_jsonl(path, serializable)

