from __future__ import annotations

from typing import Protocol

from opsiq_runtime.domain.primitives.operational_risk.config import OperationalRiskConfig


class ConfigProvider(Protocol):
    def get_config(self, tenant_id: str, config_version: str) -> OperationalRiskConfig: ...

