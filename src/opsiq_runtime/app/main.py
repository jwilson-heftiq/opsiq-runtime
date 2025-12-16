from __future__ import annotations

from datetime import datetime, timezone

from fastapi import FastAPI
from pydantic import BaseModel

from opsiq_runtime.application.registry import Registry
from opsiq_runtime.application.run_context import RunContext
from opsiq_runtime.application.runner import Runner
from opsiq_runtime.app.factory import create_adapters
from opsiq_runtime.app.health import router as health_router
from opsiq_runtime.observability.logging import configure_logging

configure_logging()

app = FastAPI()
app.include_router(health_router)

registry = Registry()


class RunRequest(BaseModel):
    tenant_id: str
    primitive_name: str
    config_version: str
    as_of_ts: datetime | None = None
    correlation_id: str | None = None
    primitive_version: str | None = None


@app.post("/run")
def run_primitive(req: RunRequest) -> dict:
    config_provider, inputs_repo, outputs_repo, event_publisher, lock_manager = create_adapters(
        correlation_id=req.correlation_id
    )
    runner = Runner(
        config_provider=config_provider,
        inputs_repo=inputs_repo,
        outputs_repo=outputs_repo,
        event_publisher=event_publisher,
        lock_manager=lock_manager,
        registry=registry,
    )
    ctx = RunContext.from_args(
        tenant_id=req.tenant_id,
        primitive_name=req.primitive_name,
        primitive_version=req.primitive_version or "1.0.0",
        as_of_ts=req.as_of_ts or datetime.now(timezone.utc),
        config_version=req.config_version,
        correlation_id=req.correlation_id,
    )
    return runner.run(ctx)

