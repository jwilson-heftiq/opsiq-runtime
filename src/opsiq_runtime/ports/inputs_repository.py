from __future__ import annotations

from typing import Iterable, Protocol

from opsiq_runtime.domain.primitives.operational_risk.model import OperationalRiskInput
from opsiq_runtime.domain.primitives.order_line_fulfillment_risk.model import OrderLineFulfillmentInput
from opsiq_runtime.domain.primitives.order_fulfillment_risk.model import OrderRiskInput
from opsiq_runtime.domain.primitives.customer_order_impact_risk.model import CustomerImpactInput
from opsiq_runtime.domain.primitives.shopper_frequency_trend.model import ShopperFrequencyInput
from opsiq_runtime.domain.primitives.shopper_health_classification.model import ShopperHealthInput
from opsiq_runtime.application.run_context import RunContext


class InputsRepository(Protocol):
    def fetch_operational_risk_inputs(self, ctx: RunContext) -> Iterable[OperationalRiskInput]: ...

    def fetch_shopper_frequency_inputs(self, ctx: RunContext) -> Iterable[ShopperFrequencyInput]: ...

    def fetch_shopper_health_inputs(self, ctx: RunContext) -> Iterable[ShopperHealthInput]: ...

    def fetch_order_line_fulfillment_inputs(self, ctx: RunContext) -> Iterable[OrderLineFulfillmentInput]: ...

    def fetch_order_risk_inputs(self, ctx: RunContext) -> Iterable[OrderRiskInput]: ...

    def fetch_customer_impact_inputs(self, ctx: RunContext) -> Iterable[CustomerImpactInput]: ...

