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
from opsiq_runtime.settings import Settings


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
        settings: Optional[Settings] = None,
        pack_loader: Optional[Any] = None,  # PackLoaderService
        readiness_service: Optional[Any] = None,  # PackReadinessService
    ) -> None:
        self.config_provider = config_provider
        self.inputs_repo = inputs_repo
        self.outputs_repo = outputs_repo
        self.event_publisher = event_publisher
        self.lock_manager = lock_manager
        self.registry = registry
        self.cancellation_check = cancellation_check
        self.settings = settings
        self.pack_loader = pack_loader
        self.readiness_service = readiness_service

    def run(self, ctx: RunContext) -> dict:
        started_at = datetime.now(timezone.utc)
        self.lock_manager.acquire(ctx)

        # Check pack readiness if enforcement is enabled
        if (
            self.settings
            and self.settings.enforce_readiness
            and self.pack_loader
            and self.readiness_service
        ):
            self._check_pack_readiness(ctx)

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

            # Filter out None results (sparse emission) while maintaining input/output pairing
            filtered_pairs = [(inp, res) for inp, res in zip(inputs_list, results) if res is not None]
            filtered_inputs: List[CommonInput] = [inp for inp, _ in filtered_pairs]
            filtered_results: List[Any] = [res for _, res in filtered_pairs]

            decisions: List[DecisionResult] = [r.decision for r in filtered_results]
            evidence_sets: List[EvidenceSet] = [r.evidence_set for r in filtered_results]

            self.outputs_repo.write_decisions(ctx, decisions, filtered_inputs)
            self.outputs_repo.write_evidence(ctx, evidence_sets, filtered_inputs, decisions)

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
                "count": len(filtered_results),  # Emitted count
                "evaluated_count": len(inputs_list),  # Total inputs evaluated
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

    def _check_pack_readiness(self, ctx: RunContext) -> None:
        """
        Check pack readiness before running a primitive.

        If enforce_readiness is enabled and the pack is in FAIL status, raises an exception.
        """
        try:
            # Find the pack containing this primitive
            tenant_enablement = self.pack_loader.get_tenant_enablement(ctx.tenant_id)

            for pack_item in tenant_enablement["enabled_packs"]:
                if not pack_item["enabled"]:
                    continue

                pack_id = pack_item["pack_id"]
                pack_version = pack_item["pack_version"]
                pack_def = self.pack_loader.get_pack_definition(pack_id, pack_version)

                # Check if this primitive belongs to this pack
                primitive_names = [
                    p.get("primitive_name") for p in pack_def.get("primitives", [])
                ]
                if ctx.primitive_name not in primitive_names:
                    continue

                # Check readiness
                readiness = self.readiness_service.calculate_pack_readiness(
                    tenant_id=ctx.tenant_id,
                    pack_id=pack_id,
                    pack_version=pack_version,
                    pack_definition=pack_def,
                )

                if readiness.overall_status == "FAIL":
                    # Emit event
                    self.event_publisher.publish_decision_ready(
                        ctx,
                        {
                            "event_type": "opsiq.pack.readiness_failed",
                            "pack_id": pack_id,
                            "pack_version": pack_version,
                            "primitive_name": ctx.primitive_name,
                            "readiness_status": "FAIL",
                        },
                    )
                    raise RuntimeError(
                        f"Pack {pack_id} v{pack_version} is in FAIL readiness status. "
                        f"Primitive {ctx.primitive_name} cannot be executed. "
                        f"Check /v1/tenants/{ctx.tenant_id}/packs/{pack_id}/readiness for details."
                    )

                # Found the pack and checked readiness, exit
                return

        except Exception as e:
            # If we can't check readiness, log warning but don't block execution
            # (unless it's the RuntimeError we raised above)
            if isinstance(e, RuntimeError) and "readiness status" in str(e):
                raise
            import logging

            logger = logging.getLogger(__name__)
            logger.warning(f"Failed to check pack readiness: {e}")

