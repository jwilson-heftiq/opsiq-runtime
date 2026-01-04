from __future__ import annotations

from typing import Protocol, Union

from opsiq_runtime.domain.primitives.operational_risk.config import OperationalRiskConfig
from opsiq_runtime.domain.primitives.shopper_frequency_trend.config import ShopperFrequencyTrendConfig
from opsiq_runtime.domain.primitives.shopper_health_classification.config import ShopperHealthConfig
from opsiq_runtime.domain.primitives.order_line_fulfillment_risk.config import OrderLineFulfillmentRiskConfig
from opsiq_runtime.domain.primitives.order_fulfillment_risk.config import OrderRiskConfig
from opsiq_runtime.domain.primitives.customer_order_impact_risk.config import CustomerImpactConfig


class ConfigProvider(Protocol):
    def get_config(
        self, tenant_id: str, config_version: str, primitive_name: str | None = None
    ) -> Union[
        OperationalRiskConfig,
        ShopperFrequencyTrendConfig,
        ShopperHealthConfig,
        OrderLineFulfillmentRiskConfig,
        OrderRiskConfig,
        CustomerImpactConfig,
    ]: ...

