from __future__ import annotations

from datetime import datetime, timezone
from typing import List

from opsiq_runtime.application.registry import Registry
from opsiq_runtime.application.run_context import RunContext
from opsiq_runtime.domain.common.decision import DecisionResult
from opsiq_runtime.domain.common.evidence import EvidenceSet
from opsiq_runtime.domain.primitives.operational_risk.evaluator import OperationalRiskResult
from opsiq_runtime.domain.primitives.operational_risk.model import OperationalRiskInput
from opsiq_runtime.domain.primitives.operational_risk import rules
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
        started_at = datetime.now(timezone.utc)
        self.lock_manager.acquire(ctx)

        # Get config early so we can use canonical_version for run registry
        config = self.config_provider.get_config(ctx.tenant_id, ctx.config_version)

        # Register run started (if outputs_repo supports it)
        if hasattr(self.outputs_repo, "register_run_started"):
            self.outputs_repo.register_run_started(ctx, config.canonical_version)

        try:
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

            # Count decision states
            at_risk_count = sum(1 for d in decisions if d.state == rules.AT_RISK)
            not_at_risk_count = sum(1 for d in decisions if d.state == rules.NOT_AT_RISK)
            unknown_count = sum(1 for d in decisions if d.state == rules.UNKNOWN)

            # Register run completed (if outputs_repo supports it)
            if hasattr(self.outputs_repo, "register_run_completed"):
                self.outputs_repo.register_run_completed(
                    ctx,
                    started_at=started_at,
                    input_count=len(inputs_list),
                    decision_count=len(decisions),
                    at_risk_count=at_risk_count,
                    not_at_risk_count=not_at_risk_count,
                    unknown_count=unknown_count,
                )

            duration_ms = int((datetime.now(timezone.utc) - started_at).total_seconds() * 1000)
            summary = {
                "tenant_id": ctx.tenant_id,
                "primitive_name": ctx.primitive_name,
                "primitive_version": ctx.primitive_version,
                "config_version": ctx.config_version,
                "count": len(results),
                "at_risk_count": at_risk_count,
                "not_at_risk_count": not_at_risk_count,
                "unknown_count": unknown_count,
                "duration_ms": duration_ms,
            }
            self.event_publisher.publish_decision_ready(ctx, summary)
            return summary

        except Exception as e:
            # Register run failed (if outputs_repo supports it)
            if hasattr(self.outputs_repo, "register_run_failed"):
                self.outputs_repo.register_run_failed(ctx, started_at, e)
            raise

        finally:
            self.lock_manager.release(ctx)

