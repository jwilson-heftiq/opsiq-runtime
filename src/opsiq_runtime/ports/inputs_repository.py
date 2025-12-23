from __future__ import annotations

from typing import Iterable, Protocol

from opsiq_runtime.domain.primitives.operational_risk.model import OperationalRiskInput
from opsiq_runtime.domain.primitives.shopper_frequency_trend.model import ShopperFrequencyInput
from opsiq_runtime.application.run_context import RunContext


class InputsRepository(Protocol):
    def fetch_operational_risk_inputs(self, ctx: RunContext) -> Iterable[OperationalRiskInput]: ...

    def fetch_shopper_frequency_inputs(self, ctx: RunContext) -> Iterable[ShopperFrequencyInput]: ...

