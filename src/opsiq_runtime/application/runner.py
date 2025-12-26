from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable, List, Optional

from opsiq_runtime.application.registry import Registry
from opsiq_runtime.application.run_context import RunContext
from opsiq_runtime.application.errors import RunCancelledError
from opsiq_runtime.domain.common.decision import DecisionResult
from opsiq_runtime.domain.common.evidence import EvidenceSet
from opsiq_runtime.domain.common.input_protocol import CommonInput
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
        cancellation_check: Optional[Callable[[], bool]] = None,
    ) -> None:
        self.config_provider = config_provider
        self.inputs_repo = inputs_repo
        self.outputs_repo = outputs_repo
        self.event_publisher = event_publisher
        self.lock_manager = lock_manager
        self.registry = registry
        self.cancellation_check = cancellation_check

    def run(self, ctx: RunContext) -> dict:
        started_at = datetime.now(timezone.utc)
        self.lock_manager.acquire(ctx)

        # Get config early so we can use canonical_version for run registry
        # Pass primitive_name to config provider
        config = self.config_provider.get_config(ctx.tenant_id, ctx.config_version, ctx.primitive_name)

        # Register run started (if outputs_repo supports it)
        if hasattr(self.outputs_repo, "register_run_started"):
            self.outputs_repo.register_run_started(ctx, config.canonical_version)

        try:
            self.registry.ensure_version(ctx.primitive_name, ctx.primitive_version, ctx.config_version)
            evaluator = self.registry.get(ctx.primitive_name, ctx.primitive_version)

            # Get the correct input fetch method for this primitive
            fetch_method_name = self.registry.get_input_fetch_method(ctx.primitive_name)
            fetch_method = getattr(self.inputs_repo, fetch_method_name)

            # Collect inputs and results together to maintain pairing for outputs repository
            inputs_list: List[CommonInput] = []
            results: List[Any] = []
            for input_row in fetch_method(ctx):
                # Check for cancellation
                if self.cancellation_check and self.cancellation_check():
                    raise RunCancelledError(f"Run cancelled for correlation_id={ctx.correlation_id}")
                inputs_list.append(input_row)
                results.append(evaluator(input_row, config))

            decisions: List[DecisionResult] = [r.decision for r in results]
            evidence_sets: List[EvidenceSet] = [r.evidence_set for r in results]

            self.outputs_repo.write_decisions(ctx, decisions, inputs_list)
            self.outputs_repo.write_evidence(ctx, evidence_sets, inputs_list, decisions)

            # Count decision states (primitive-agnostic)
            # For operational_risk: AT_RISK, NOT_AT_RISK, UNKNOWN
            # For shopper_frequency_trend: DECLINING, STABLE, IMPROVING, UNKNOWN
            state_counts: dict[str, int] = {}
            for d in decisions:
                state_counts[d.state] = state_counts.get(d.state, 0) + 1

            # Register run completed (if outputs_repo supports it)
            if hasattr(self.outputs_repo, "register_run_completed"):
                # For backward compatibility, extract counts for operational_risk states
                at_risk_count = state_counts.get("AT_RISK", 0)
                not_at_risk_count = state_counts.get("NOT_AT_RISK", 0)
                unknown_count = state_counts.get("UNKNOWN", 0)
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
                "state_counts": state_counts,
                "duration_ms": duration_ms,
            }
            # For backward compatibility, include operational_risk specific counts
            if ctx.primitive_name == "operational_risk":
                summary["at_risk_count"] = state_counts.get("AT_RISK", 0)
                summary["not_at_risk_count"] = state_counts.get("NOT_AT_RISK", 0)
                summary["unknown_count"] = state_counts.get("UNKNOWN", 0)
            self.event_publisher.publish_decision_ready(ctx, summary)
            return summary

        except RunCancelledError as e:
            # Register run cancelled (if outputs_repo supports it)
            if hasattr(self.outputs_repo, "register_run_failed"):
                self.outputs_repo.register_run_failed(ctx, started_at, e)
            raise
        except Exception as e:
            # Register run failed (if outputs_repo supports it)
            if hasattr(self.outputs_repo, "register_run_failed"):
                self.outputs_repo.register_run_failed(ctx, started_at, e)
            raise

        finally:
            self.lock_manager.release(ctx)

