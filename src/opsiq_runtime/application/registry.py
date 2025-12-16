from __future__ import annotations

from typing import Callable, Dict, Tuple

from opsiq_runtime.application.errors import PrimitiveVersionMismatch, UnknownPrimitiveError
from opsiq_runtime.domain.primitives.operational_risk.config import OperationalRiskConfig
from opsiq_runtime.domain.primitives.operational_risk.evaluator import (
    OperationalRiskResult,
    evaluate_operational_risk,
)
from opsiq_runtime.domain.primitives.operational_risk.model import OperationalRiskInput

EvaluatorFn = Callable[[OperationalRiskInput, OperationalRiskConfig], OperationalRiskResult]


class Registry:
    def __init__(self) -> None:
        self._evaluators: Dict[Tuple[str, str], EvaluatorFn] = {}
        self.register("operational_risk", "1.0.0", evaluate_operational_risk)

    def register(self, primitive_name: str, primitive_version: str, fn: EvaluatorFn) -> None:
        self._evaluators[(primitive_name, primitive_version)] = fn

    def get(self, primitive_name: str, primitive_version: str) -> EvaluatorFn:
        key = (primitive_name, primitive_version)
        if key not in self._evaluators:
            raise UnknownPrimitiveError(f"Primitive {primitive_name} version {primitive_version} not registered")
        return self._evaluators[key]

    def ensure_version(self, primitive_name: str, requested_version: str, config_version: str) -> None:
        if (primitive_name, requested_version) not in self._evaluators:
            raise PrimitiveVersionMismatch(
                f"Primitive {primitive_name} version {requested_version} not supported for config {config_version}"
            )

