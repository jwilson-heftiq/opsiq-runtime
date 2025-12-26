from __future__ import annotations

from typing import Any, Callable, Dict, Tuple

from opsiq_runtime.application.errors import PrimitiveVersionMismatch, UnknownPrimitiveError
from opsiq_runtime.domain.primitives.operational_risk.evaluator import evaluate_operational_risk
from opsiq_runtime.domain.primitives.shopper_frequency_trend.evaluator import evaluate_shopper_frequency_trend
from opsiq_runtime.domain.primitives.shopper_health_classification.evaluator import evaluate_shopper_health_classification

EvaluatorFn = Callable[..., Any]


class Registry:
    def __init__(self) -> None:
        self._evaluators: Dict[Tuple[str, str], EvaluatorFn] = {}
        self._input_fetchers: Dict[str, str] = {}  # Maps primitive_name to fetch method name
        self.register("operational_risk", "1.0.0", evaluate_operational_risk, "fetch_operational_risk_inputs")
        self.register("shopper_frequency_trend", "1.0.0", evaluate_shopper_frequency_trend, "fetch_shopper_frequency_inputs")
        self.register("shopper_health_classification", "1.0.0", evaluate_shopper_health_classification, "fetch_shopper_health_inputs")

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

