from __future__ import annotations

import time
from typing import Iterable, List

from opsiq_runtime.application.registry import Registry
from opsiq_runtime.application.run_context import RunContext
from opsiq_runtime.domain.common.decision import DecisionResult
from opsiq_runtime.domain.common.evidence import EvidenceSet
from opsiq_runtime.domain.primitives.operational_risk.config import OperationalRiskConfig
from opsiq_runtime.domain.primitives.operational_risk.evaluator import OperationalRiskResult
from opsiq_runtime.domain.primitives.operational_risk.model import OperationalRiskInput
from opsiq_runtime.ports.config_provider import ConfigProvider
from opsiq_runtime.ports.event_publisher import EventPublisher
from opsiq_runtime.ports.inputs_repository import InputsRepository
from opsiq_runtime.ports.lock_manager import LockManager
from opsiq_runtime.ports.outputs_repository import OutputsRepository


class Runner:
    def __init__(
        self,
        config_provider: ConfigProvider,
        inputs_repo: InputsRepository,
        outputs_repo: OutputsRepository,
        event_publisher: EventPublisher,
        lock_manager: LockManager,
        registry: Registry,
    ) -> None:
        self.config_provider = config_provider
        self.inputs_repo = inputs_repo
        self.outputs_repo = outputs_repo
        self.event_publisher = event_publisher
        self.lock_manager = lock_manager
        self.registry = registry

    def run(self, ctx: RunContext) -> dict:
        start = time.time()
        self.lock_manager.acquire(ctx)
        try:
            config = self.config_provider.get_config(ctx.tenant_id, ctx.config_version)
            self.registry.ensure_version(ctx.primitive_name, ctx.primitive_version, ctx.config_version)
            evaluator = self.registry.get(ctx.primitive_name, ctx.primitive_version)

            # Collect inputs and results together to maintain pairing for outputs repository
            inputs_list: List[OperationalRiskInput] = []
            results: List[OperationalRiskResult] = []
            for input_row in self.inputs_repo.fetch_operational_risk_inputs(ctx):
                inputs_list.append(input_row)
                results.append(evaluator(input_row, config))

            decisions: List[DecisionResult] = [r.decision for r in results]
            evidence_sets: List[EvidenceSet] = [r.evidence_set for r in results]

            self.outputs_repo.write_decisions(ctx, decisions, inputs_list)
            self.outputs_repo.write_evidence(ctx, evidence_sets, inputs_list)

            summary = {
                "tenant_id": ctx.tenant_id,
                "primitive_name": ctx.primitive_name,
                "primitive_version": ctx.primitive_version,
                "config_version": ctx.config_version,
                "count": len(results),
                "duration_ms": int((time.time() - start) * 1000),
            }
            self.event_publisher.publish_decision_ready(ctx, summary)
            return summary
        finally:
            self.lock_manager.release(ctx)

