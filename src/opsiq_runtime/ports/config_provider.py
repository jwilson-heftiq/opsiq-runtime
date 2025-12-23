from __future__ import annotations

from typing import Protocol, Union

from opsiq_runtime.domain.primitives.operational_risk.config import OperationalRiskConfig
from opsiq_runtime.domain.primitives.shopper_frequency_trend.config import ShopperFrequencyTrendConfig


class ConfigProvider(Protocol):
    def get_config(
        self, tenant_id: str, config_version: str, primitive_name: str | None = None
    ) -> Union[OperationalRiskConfig, ShopperFrequencyTrendConfig]: ...

