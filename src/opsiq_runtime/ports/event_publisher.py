from __future__ import annotations

from typing import Protocol

from opsiq_runtime.application.run_context import RunContext


class EventPublisher(Protocol):
    def publish_decision_ready(self, ctx: RunContext, summary: dict) -> None: ...

