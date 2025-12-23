from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Iterable, List, Optional

from opsiq_runtime.application.run_context import RunContext
from opsiq_runtime.domain.primitives.operational_risk.model import OperationalRiskInput
from opsiq_runtime.domain.primitives.shopper_frequency_trend.model import ShopperFrequencyInput
from opsiq_runtime.ports.inputs_repository import InputsRepository


class InMemoryInputsRepository(InputsRepository):
    def __init__(self, items: Optional[List[OperationalRiskInput]] = None) -> None:
        self.items = items or []

    def fetch_operational_risk_inputs(self, ctx: RunContext) -> Iterable[OperationalRiskInput]:
        if self.items:
            return list(self.items)
        # Provide a minimal default example when none supplied
        now = datetime.now(timezone.utc)
        return [
            OperationalRiskInput.new(
                tenant_id=str(ctx.tenant_id),
                subject_id="demo-subject",
                as_of_ts=ctx.as_of_ts or now,
                last_trip_ts=now,
                config_version=ctx.config_version,
                canonical_version="v1",
            )
        ]

    def fetch_shopper_frequency_inputs(self, ctx: RunContext) -> Iterable[ShopperFrequencyInput]:
        # Provide a minimal default example when none supplied
        now = datetime.now(timezone.utc)
        last_trip = now - timedelta(days=5)
        prev_trip = now - timedelta(days=10)
        return [
            ShopperFrequencyInput.new(
                tenant_id=str(ctx.tenant_id),
                subject_id="demo-subject",
                as_of_ts=ctx.as_of_ts or now,
                last_trip_ts=last_trip,
                prev_trip_ts=prev_trip,
                config_version=ctx.config_version,
                canonical_version="v1",
                baseline_trip_count=5,
                baseline_avg_gap_days=10.0,
                recent_gap_days=5.0,
            )
        ]

