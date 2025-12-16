from __future__ import annotations

from opsiq_runtime.application.run_context import RunContext
from opsiq_runtime.ports.event_publisher import EventPublisher


class NoopEventPublisher(EventPublisher):
    def publish_decision_ready(self, ctx: RunContext, summary: dict) -> None:
        return None

