from __future__ import annotations

from typing import Protocol

from opsiq_runtime.application.run_context import RunContext


class LockManager(Protocol):
    def acquire(self, ctx: RunContext) -> None: ...

    def release(self, ctx: RunContext) -> None: ...

