"""Router for decision bundle endpoints."""

from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query

from opsiq_runtime.adapters.databricks.client import DatabricksSqlClient
from opsiq_runtime.app.api.models.decisions import DecisionBundle
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


@router.get("/tenants/{tenant_id}/subjects/shopper/{subject_id}/decision-bundle", response_model=DecisionBundle)
def get_decision_bundle(
    tenant_id: str,
    subject_id: str,
    as_of_ts: datetime | None = Query(None, description="ISO timestamp for as_of_ts filter (if not provided, uses latest)"),
    include_evidence: bool = Query(True, description="Whether to include evidence records"),
    repository: DecisionsRepository = Depends(get_decisions_repository),
) -> DecisionBundle:
    """
    Get decision bundle for a subject including composite decision, component decisions, and evidence.

    Returns:
    - composite: The latest shopper_health_classification decision (or at specified as_of_ts)
    - components: Component decisions (operational_risk, shopper_frequency_trend) with computed_at <= composite.computed_at
    - evidence: Evidence records grouped by primitive_name (if include_evidence=true)
    """
    try:
        return repository.get_decision_bundle(
            tenant_id=tenant_id,
            subject_id=subject_id,
            as_of_ts=as_of_ts,
            include_evidence=include_evidence,
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching decision bundle: {str(e)}")

