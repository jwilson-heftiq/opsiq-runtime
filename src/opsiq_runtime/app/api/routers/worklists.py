"""Router for worklist endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query

from opsiq_runtime.adapters.databricks.client import DatabricksSqlClient
from opsiq_runtime.app.api.models.decisions import DecisionListResponse
from opsiq_runtime.app.api.repositories.decisions_repo import DecisionsRepository
from opsiq_runtime.settings import Settings, get_settings

router = APIRouter()


def get_databricks_client(settings: Settings = Depends(get_settings)) -> DatabricksSqlClient:
    """Dependency to provide DatabricksSqlClient."""
    return DatabricksSqlClient(settings, correlation_id=None)


def get_decisions_repository(
    client: DatabricksSqlClient = Depends(get_databricks_client),
    settings: Settings = Depends(get_settings),
) -> DecisionsRepository:
    """Dependency to provide DecisionsRepository."""
    return DecisionsRepository(client, settings)


@router.get("/tenants/{tenant_id}/worklists/shopper-health", response_model=DecisionListResponse)
def get_shopper_health_worklist(
    tenant_id: str,
    state: list[str] | None = Query(None, description="Filter by decision states (URGENT/WATCHLIST/HEALTHY/UNKNOWN)"),
    confidence: list[str] | None = Query(None, description="Filter by confidence levels (HIGH/MEDIUM/LOW)"),
    subject_id: str | None = Query(None, description="Substring match on subject_id"),
    limit: int = Query(50, ge=1, le=200, description="Maximum number of results"),
    cursor: str | None = Query(None, description="Pagination cursor"),
    repository: DecisionsRepository = Depends(get_decisions_repository),
) -> DecisionListResponse:
    """
    Get worklist of latest shopper health classification decisions per subject.

    Returns the latest decision per subject for shopper_health_classification primitive.
    Supports filtering by state, confidence, and subject_id substring matching.
    Uses keyset pagination based on computed_at and subject_id.
    """
    try:
        return repository.get_worklist(
            tenant_id=tenant_id,
            state=state,
            confidence=confidence,
            subject_id_filter=subject_id,
            limit=limit,
            cursor=cursor,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching worklist: {str(e)}")

