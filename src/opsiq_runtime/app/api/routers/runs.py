"""Router for run registry endpoints."""

from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query

from opsiq_runtime.adapters.databricks.client import DatabricksSqlClient
from opsiq_runtime.app.api.models.runs import RunRegistryResponse
from opsiq_runtime.app.api.repositories.runs_repo import RunsRepository
from opsiq_runtime.settings import Settings, get_settings

router = APIRouter()


def get_databricks_client(settings: Settings = Depends(get_settings)) -> DatabricksSqlClient:
    """Dependency to provide DatabricksSqlClient."""
    return DatabricksSqlClient(settings, correlation_id=None)


def get_runs_repository(
    client: DatabricksSqlClient = Depends(get_databricks_client),
    settings: Settings = Depends(get_settings),
) -> RunsRepository:
    """Dependency to provide RunsRepository."""
    return RunsRepository(client, settings)


@router.get("/tenants/{tenant_id}/runs", response_model=RunRegistryResponse)
def get_run_registry(
    tenant_id: str,
    primitive_name: str | None = Query(None, description="Filter by primitive name"),
    status: str | None = Query(None, description="Filter by status (STARTED/SUCCESS/FAILED)"),
    from_ts: datetime | None = Query(None, description="Start timestamp for started_at filter (ISO format)"),
    to_ts: datetime | None = Query(None, description="End timestamp for started_at filter (ISO format)"),
    limit: int = Query(50, ge=1, description="Maximum number of results"),
    cursor: str | None = Query(None, description="Pagination cursor"),
    repository: RunsRepository = Depends(get_runs_repository),
) -> RunRegistryResponse:
    """
    Get run registry for a tenant.

    Returns time-ordered runs (newest first) for the specified tenant.
    Supports optional filtering by primitive_name, status, and started_at range.
    Uses keyset pagination based on started_at and correlation_id.
    """
    try:
        return repository.get_run_registry(
            tenant_id=tenant_id,
            primitive_name=primitive_name,
            status=status,
            from_ts=from_ts,
            to_ts=to_ts,
            limit=limit,
            cursor=cursor,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching run registry: {str(e)}")

