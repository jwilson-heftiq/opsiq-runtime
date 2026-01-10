from __future__ import annotations

from typing import Any, Callable, Dict, Tuple

from opsiq_runtime.application.errors import PrimitiveVersionMismatch, UnknownPrimitiveError
from opsiq_runtime.domain.primitives.operational_risk.evaluator import evaluate_operational_risk
from opsiq_runtime.domain.primitives.order_line_fulfillment_risk.evaluator import evaluate_order_line_fulfillment_risk
from opsiq_runtime.domain.primitives.order_fulfillment_risk.evaluator import evaluate_order_fulfillment_risk
from opsiq_runtime.domain.primitives.customer_order_impact_risk.evaluator import evaluate_customer_order_impact_risk
from opsiq_runtime.domain.primitives.shopper_frequency_trend.evaluator import evaluate_shopper_frequency_trend
from opsiq_runtime.domain.primitives.shopper_health_classification.evaluator import evaluate_shopper_health_classification
from opsiq_runtime.domain.primitives.shopper_item_affinity_score.evaluator import (
    evaluate_shopper_item_affinity_score,
)
from opsiq_runtime.domain.primitives.shopper_weekly_ad_slate.evaluator import (
    evaluate_shopper_weekly_ad_slate,
)
from opsiq_runtime.domain.primitives.shopper_coupon_offer_set.evaluator import (
    evaluate_shopper_coupon_offer_set,
)

EvaluatorFn = Callable[..., Any]


class Registry:
    def __init__(self) -> None:
        self._evaluators: Dict[Tuple[str, str], EvaluatorFn] = {}
        self._input_fetchers: Dict[str, str] = {}  # Maps primitive_name to fetch method name
        self.register("operational_risk", "1.0.0", evaluate_operational_risk, "fetch_operational_risk_inputs")
        self.register("shopper_frequency_trend", "1.0.0", evaluate_shopper_frequency_trend, "fetch_shopper_frequency_inputs")
        self.register("shopper_health_classification", "1.0.0", evaluate_shopper_health_classification, "fetch_shopper_health_inputs")
        self.register("shopper_item_affinity_score", "1.0.0", evaluate_shopper_item_affinity_score, "fetch_shopper_item_affinity_inputs")
        self.register("order_line_fulfillment_risk", "1.0.0", evaluate_order_line_fulfillment_risk, "fetch_order_line_fulfillment_inputs")
        self.register("order_fulfillment_risk", "1.0.0", evaluate_order_fulfillment_risk, "fetch_order_risk_inputs")
        self.register("customer_order_impact_risk", "1.0.0", evaluate_customer_order_impact_risk, "fetch_customer_impact_inputs")
        self.register("shopper_weekly_ad_slate", "1.0.0", evaluate_shopper_weekly_ad_slate, "fetch_shopper_weekly_ad_slate_inputs")
        self.register("shopper_coupon_offer_set", "1.0.0", evaluate_shopper_coupon_offer_set, "fetch_shopper_coupon_offer_set_inputs")

    def register(
        self, primitive_name: str, primitive_version: str, fn: EvaluatorFn, input_fetch_method: str
    ) -> None:
        self._evaluators[(primitive_name, primitive_version)] = fn
        self._input_fetchers[primitive_name] = input_fetch_method

    def get(self, primitive_name: str, primitive_version: str) -> EvaluatorFn:
        key = (primitive_name, primitive_version)
        if key not in self._evaluators:
            raise UnknownPrimitiveError(f"Primitive {primitive_name} version {primitive_version} not registered")
        return self._evaluators[key]

    def get_input_fetch_method(self, primitive_name: str) -> str:
        """Get the input fetch method name for a primitive."""
        if primitive_name not in self._input_fetchers:
            raise UnknownPrimitiveError(f"Primitive {primitive_name} has no registered input fetch method")
        return self._input_fetchers[primitive_name]

    def ensure_version(self, primitive_name: str, requested_version: str, config_version: str) -> None:
        if (primitive_name, requested_version) not in self._evaluators:
            raise PrimitiveVersionMismatch(
                f"Primitive {primitive_name} version {requested_version} not supported for config {config_version}"
            )

