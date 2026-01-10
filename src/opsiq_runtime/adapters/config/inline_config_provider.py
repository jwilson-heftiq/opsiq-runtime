from __future__ import annotations

import json
from pathlib import Path
from typing import Optional, Union

from opsiq_runtime.domain.primitives.operational_risk.config import OperationalRiskConfig
from opsiq_runtime.domain.primitives.order_line_fulfillment_risk.config import OrderLineFulfillmentRiskConfig
from opsiq_runtime.domain.primitives.order_fulfillment_risk.config import OrderRiskConfig
from opsiq_runtime.domain.primitives.customer_order_impact_risk.config import CustomerImpactConfig
from opsiq_runtime.domain.primitives.shopper_frequency_trend.config import ShopperFrequencyTrendConfig
from opsiq_runtime.domain.primitives.shopper_health_classification.config import ShopperHealthConfig
from opsiq_runtime.domain.primitives.shopper_weekly_ad_slate.config import (
    ShopperWeeklyAdSlateConfig,
)
from opsiq_runtime.ports.config_provider import ConfigProvider
from opsiq_runtime.settings import get_settings


class InlineConfigProvider(ConfigProvider):
    """Returns a config from JSON file or defaults."""

    def __init__(self, config_path: Optional[str] = None) -> None:
        self.config_path = Path(config_path) if config_path else None

    def get_config(
        self, tenant_id: str, config_version: str, primitive_name: str | None = None
    ) -> Union[OperationalRiskConfig, ShopperFrequencyTrendConfig, ShopperHealthConfig, OrderLineFulfillmentRiskConfig, OrderRiskConfig, CustomerImpactConfig, ShopperWeeklyAdSlateConfig]:
        settings = get_settings()
        
        # Determine which primitive based on primitive_name
        if primitive_name == "order_line_fulfillment_risk":
            closed_statuses_str = settings.default_order_line_closed_statuses
            closed_statuses = {s.strip().upper() for s in closed_statuses_str.split(",") if s.strip()}
            return OrderLineFulfillmentRiskConfig(closed_statuses=closed_statuses)
        elif primitive_name == "order_fulfillment_risk":
            return OrderRiskConfig()
        elif primitive_name == "customer_order_impact_risk":
            return CustomerImpactConfig()
        elif primitive_name == "shopper_health_classification":
            return ShopperHealthConfig()
        elif primitive_name == "shopper_weekly_ad_slate":
            # Load config from JSON file if available, otherwise use defaults
            if self.config_path and self.config_path.exists():
                with self.config_path.open() as f:
                    data = json.load(f)
                tenant_cfg = data.get(tenant_id, {}).get(config_version) or data.get("default", {})
                if tenant_cfg:
                    return ShopperWeeklyAdSlateConfig(
                        slate_size_k=int(tenant_cfg.get("slate_size_k", 20)),
                        affinity_top_k=int(tenant_cfg.get("affinity_top_k", 50)),
                        exclude_lookback_days=int(tenant_cfg.get("exclude_lookback_days", 14)),
                        exclude_by=str(tenant_cfg.get("exclude_by", "item_group_id")),
                        category_cap=tenant_cfg.get("category_cap"),  # Can be None
                        min_match_rate_for_high_confidence=float(tenant_cfg.get("min_match_rate_for_high_confidence", 0.50)),
                        sparse_emission=bool(tenant_cfg.get("sparse_emission", True)),
                        ad_id=str(tenant_cfg.get("ad_id", "")),
                        scope_type=str(tenant_cfg.get("scope_type", "")),
                        scope_value=str(tenant_cfg.get("scope_value", "")),
                        hours_window=int(tenant_cfg.get("hours_window", 36)),
                    )
            return ShopperWeeklyAdSlateConfig()
        elif primitive_name == "shopper_frequency_trend":
            if self.config_path and self.config_path.exists():
                with self.config_path.open() as f:
                    data = json.load(f)
                tenant_cfg = data.get(tenant_id, {}).get(config_version) or data.get("default", {})
                if tenant_cfg:
                    return ShopperFrequencyTrendConfig(
                        baseline_window_days=int(tenant_cfg.get("baseline_window_days", settings.default_baseline_window_days)),
                        min_baseline_trips=int(tenant_cfg.get("min_baseline_trips", settings.default_min_baseline_trips)),
                        decline_ratio_threshold=float(tenant_cfg.get("decline_ratio_threshold", settings.default_decline_ratio_threshold)),
                        improve_ratio_threshold=float(tenant_cfg.get("improve_ratio_threshold", settings.default_improve_ratio_threshold)),
                        max_reasonable_gap_days=int(tenant_cfg.get("max_reasonable_gap_days", settings.default_max_reasonable_gap_days)),
                    )
            return ShopperFrequencyTrendConfig(
                baseline_window_days=settings.default_baseline_window_days,
                min_baseline_trips=settings.default_min_baseline_trips,
                decline_ratio_threshold=settings.default_decline_ratio_threshold,
                improve_ratio_threshold=settings.default_improve_ratio_threshold,
                max_reasonable_gap_days=settings.default_max_reasonable_gap_days,
            )
        else:
            # Default to operational_risk
            if self.config_path and self.config_path.exists():
                with self.config_path.open() as f:
                    data = json.load(f)
                tenant_cfg = data.get(tenant_id, {}).get(config_version) or data.get("default", {})
                if tenant_cfg:
                    return OperationalRiskConfig(
                        at_risk_days=int(tenant_cfg.get("at_risk_days", settings.default_at_risk_days))
                    )
            return OperationalRiskConfig(at_risk_days=settings.default_at_risk_days)

