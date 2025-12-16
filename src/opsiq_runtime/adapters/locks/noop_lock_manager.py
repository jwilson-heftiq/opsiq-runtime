from __future__ import annotations

from opsiq_runtime.application.run_context import RunContext
from opsiq_runtime.ports.lock_manager import LockManager


class NoopLockManager(LockManager):
    def acquire(self, ctx: RunContext) -> None:
        return None

    def release(self, ctx: RunContext) -> None:
        return None

