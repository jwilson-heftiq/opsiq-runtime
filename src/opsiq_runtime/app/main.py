from __future__ import annotations

from datetime import datetime, timezone

from fastapi import BackgroundTasks, FastAPI, HTTPException
from pydantic import BaseModel

from opsiq_runtime.application.errors import RunCancelledError
from opsiq_runtime.application.registry import Registry
from opsiq_runtime.application.run_context import RunContext
from opsiq_runtime.application.runner import Runner
from opsiq_runtime.app.api.routers import decisions_router, worklists_router
from opsiq_runtime.app.factory import create_adapters
from opsiq_runtime.app.health import router as health_router
from opsiq_runtime.app.job_manager import job_manager
from opsiq_runtime.observability.logging import configure_logging

configure_logging()

app = FastAPI()
app.include_router(health_router)
app.include_router(worklists_router, prefix="/v1", tags=["worklists"])
app.include_router(decisions_router, prefix="/v1", tags=["decisions"])

registry = Registry()


class RunRequest(BaseModel):
    tenant_id: str
    primitive_name: str
    config_version: str
    as_of_ts: datetime | None = None
    correlation_id: str | None = None
    primitive_version: str | None = None


def _run_job_in_background(
    req: RunRequest,
    correlation_id: str,
) -> None:
    """Run a job in the background."""
    try:
        config_provider, inputs_repo, outputs_repo, event_publisher, lock_manager = create_adapters(
            correlation_id=correlation_id
        )

        # Create cancellation check function
        def check_cancelled() -> bool:
            return job_manager.is_cancelled(correlation_id)

        runner = Runner(
            config_provider=config_provider,
            inputs_repo=inputs_repo,
            outputs_repo=outputs_repo,
            event_publisher=event_publisher,
            lock_manager=lock_manager,
            registry=registry,
            cancellation_check=check_cancelled,
        )
        ctx = RunContext.from_args(
            tenant_id=req.tenant_id,
            primitive_name=req.primitive_name,
            primitive_version=req.primitive_version or "1.0.0",
            as_of_ts=req.as_of_ts or datetime.now(timezone.utc),
            config_version=req.config_version,
            correlation_id=correlation_id,
        )
        result = runner.run(ctx)
        job_manager.complete_job(correlation_id, result)
    except RunCancelledError:
        # Job was cancelled, status already updated
        pass
    except Exception as e:
        job_manager.fail_job(correlation_id, str(e))


@app.post("/run")
async def run_primitive(req: RunRequest, background_tasks: BackgroundTasks) -> dict:
    """
    Start a primitive run. Returns immediately with correlation_id.
    The job runs in the background. Use /status/{correlation_id} to check progress.
    """
    import uuid

    # Generate correlation_id if not provided
    correlation_id = req.correlation_id or f"auto-{uuid.uuid4().hex[:8]}"

    # Register the job
    job_manager.register_job(correlation_id, req.tenant_id, req.primitive_name)

    # Run in background
    background_tasks.add_task(_run_job_in_background, req, correlation_id)

    return {
        "correlation_id": correlation_id,
        "status": "started",
        "message": "Job started. Use /status/{correlation_id} to check progress.",
    }


@app.post("/run/sync")
def run_primitive_sync(req: RunRequest) -> dict:
    """
    Run a primitive synchronously (blocks until completion).
    For backward compatibility.
    """
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


@app.post("/cancel/{correlation_id}")
def cancel_job(correlation_id: str) -> dict:
    """
    Cancel a running job.

    Returns:
        Status of the cancellation request
    """
    cancelled = job_manager.cancel_job(correlation_id)
    if not cancelled:
        raise HTTPException(
            status_code=404,
            detail=f"Job {correlation_id} not found or cannot be cancelled (may already be completed/failed)",
        )
    return {
        "correlation_id": correlation_id,
        "status": "cancelled",
        "message": "Job cancellation requested",
    }


@app.get("/status/{correlation_id}")
def get_job_status(correlation_id: str) -> dict:
    """
    Get the status of a job.

    Returns:
        Job status including result if completed
    """
    job = job_manager.get_job_status(correlation_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"Job {correlation_id} not found")

    response = {
        "correlation_id": job.correlation_id,
        "tenant_id": job.tenant_id,
        "primitive_name": job.primitive_name,
        "status": job.status,
        "started_at": job.started_at.isoformat(),
    }

    if job.completed_at:
        response["completed_at"] = job.completed_at.isoformat()
        duration_ms = int((job.completed_at - job.started_at).total_seconds() * 1000)
        response["duration_ms"] = duration_ms

    if job.result:
        response["result"] = job.result

    if job.error:
        response["error"] = job.error

    return response

