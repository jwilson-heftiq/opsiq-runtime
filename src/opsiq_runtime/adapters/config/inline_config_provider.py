from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

from opsiq_runtime.domain.primitives.operational_risk.config import OperationalRiskConfig
from opsiq_runtime.ports.config_provider import ConfigProvider
from opsiq_runtime.settings import get_settings


class InlineConfigProvider(ConfigProvider):
    """Returns a config from JSON file or defaults."""

    def __init__(self, config_path: Optional[str] = None) -> None:
        self.config_path = Path(config_path) if config_path else None

    def get_config(self, tenant_id: str, config_version: str) -> OperationalRiskConfig:
        if self.config_path and self.config_path.exists():
            with self.config_path.open() as f:
                data = json.load(f)
            tenant_cfg = data.get(tenant_id, {}).get(config_version) or data.get("default", {})
            if tenant_cfg:
                return OperationalRiskConfig(
                    at_risk_days=int(tenant_cfg.get("at_risk_days", get_settings().default_at_risk_days))
                )
        return OperationalRiskConfig(at_risk_days=get_settings().default_at_risk_days)

