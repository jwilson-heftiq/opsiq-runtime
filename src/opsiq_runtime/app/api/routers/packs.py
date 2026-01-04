"""Router for decision pack endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from opsiq_runtime.adapters.databricks.client import DatabricksSqlClient
from opsiq_runtime.app.api.models.packs import (
    DecisionPackDefinition,
    EnabledPackSummary,
    ReadinessCheckResult,
    SubjectDefinition,
    TenantReadinessResponse,
)
from opsiq_runtime.app.api.services.pack_loader import PackLoaderService
from opsiq_runtime.settings import Settings, get_settings

router = APIRouter()


def get_pack_loader_service(settings: Settings = Depends(get_settings)) -> PackLoaderService:
    """Dependency to provide PackLoaderService."""
    return PackLoaderService(settings)


def get_databricks_client(settings: Settings = Depends(get_settings)) -> DatabricksSqlClient | None:
    """Dependency to provide DatabricksSqlClient if configured."""
    if not settings.databricks_server_hostname:
        return None
    try:
        return DatabricksSqlClient(settings, correlation_id=None)
    except Exception:
        return None


@router.get("/tenants/{tenant_id}/decision-packs", response_model=list[EnabledPackSummary])
def get_tenant_decision_packs(
    tenant_id: str,
    pack_loader: PackLoaderService = Depends(get_pack_loader_service),
) -> list[EnabledPackSummary]:
    """
    Get list of enabled decision packs for a tenant.

    Returns enabled packs with summary information including subjects and primitives.
    """
    try:
        # Load tenant enablement
        tenant_enablement = pack_loader.get_tenant_enablement(tenant_id)

        # Filter enabled packs
        enabled_packs: list[EnabledPackSummary] = []
        for pack_item in tenant_enablement["enabled_packs"]:
            if not pack_item["enabled"]:
                continue

            pack_id = pack_item["pack_id"]
            pack_version = pack_item["pack_version"]

            # Load pack definition
            pack_def = pack_loader.get_pack_definition(pack_id, pack_version)

            # Substitute {tenantId} in ui_route
            subjects = []
            for subject in pack_def["subjects"]:
                worklist = subject["default_worklist"].copy()
                worklist["ui_route"] = worklist["ui_route"].replace("{tenantId}", tenant_id)
                subjects.append(
                    SubjectDefinition(
                        subject_type=subject["subject_type"],
                        default_worklist=worklist,
                    )
                )

            # Build summary
            summary = EnabledPackSummary(
                pack_id=pack_def["pack_id"],
                pack_version=pack_def["pack_version"],
                name=pack_def["name"],
                description=pack_def["description"],
                tags=pack_def.get("tags", []),
                subjects=subjects,
                primitives=[
                    {
                        "primitive_name": p["primitive_name"],
                        "primitive_version": p["primitive_version"],
                    }
                    for p in pack_def["primitives"]
                ],
            )
            enabled_packs.append(summary)

        return enabled_packs
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error loading packs: {str(e)}")


@router.get("/decision-packs/{pack_id}/{pack_version}", response_model=DecisionPackDefinition)
def get_decision_pack(
    pack_id: str,
    pack_version: str,
    pack_loader: PackLoaderService = Depends(get_pack_loader_service),
) -> DecisionPackDefinition:
    """
    Get full decision pack definition by ID and version.

    Returns the complete pack definition including all configuration.
    """
    try:
        pack_def = pack_loader.get_pack_definition(pack_id, pack_version)
        return DecisionPackDefinition(**pack_def)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error loading pack: {str(e)}")


@router.get("/tenants/{tenant_id}/readiness", response_model=TenantReadinessResponse)
def get_tenant_readiness(
    tenant_id: str,
    pack_loader: PackLoaderService = Depends(get_pack_loader_service),
    db_client: DatabricksSqlClient | None = Depends(get_databricks_client),
) -> TenantReadinessResponse:
    """
    Get readiness checklist for enabled packs for a tenant.

    Runs onboarding_checks for each enabled pack and returns status.
    Best-effort: if Databricks not configured, returns WARN status.
    """
    try:
        # Load tenant enablement
        tenant_enablement = pack_loader.get_tenant_enablement(tenant_id)

        checks: list[ReadinessCheckResult] = []

        for pack_item in tenant_enablement["enabled_packs"]:
            if not pack_item["enabled"]:
                continue

            pack_id = pack_item["pack_id"]
            pack_version = pack_item["pack_version"]

            # Load pack definition
            pack_def = pack_loader.get_pack_definition(pack_id, pack_version)

            # Run onboarding checks
            for check_def in pack_def.get("onboarding_checks", []):
                check_type = check_def["type"]
                severity = check_def["severity"]
                table = check_def.get("table")
                message = check_def.get("message", "")

                if check_type == "table_exists":
                    if not db_client:
                        checks.append(
                            ReadinessCheckResult(
                                pack_id=pack_id,
                                pack_version=pack_version,
                                check_type=check_type,
                                check_severity=severity,
                                status="WARN",
                                message="Databricks not configured",
                                table=table,
                            )
                        )
                        continue

                    # Build table name with prefix
                    settings = pack_loader.settings
                    table_name = f"{settings.databricks_table_prefix}{table}"
                    if settings.databricks_catalog:
                        if settings.databricks_schema:
                            table_name = f"{settings.databricks_catalog}.{settings.databricks_schema}.{table_name}"
                        else:
                            table_name = f"{settings.databricks_catalog}.{table_name}"
                    elif settings.databricks_schema:
                        table_name = f"{settings.databricks_schema}.{table_name}"

                    # Check if table exists
                    try:
                        db_client.describe_table(table_name)
                        checks.append(
                            ReadinessCheckResult(
                                pack_id=pack_id,
                                pack_version=pack_version,
                                check_type=check_type,
                                check_severity=severity,
                                status="PASS",
                                message=message or f"Table {table_name} exists",
                                table=table,
                            )
                        )
                    except Exception as e:
                        checks.append(
                            ReadinessCheckResult(
                                pack_id=pack_id,
                                pack_version=pack_version,
                                check_type=check_type,
                                check_severity=severity,
                                status="FAIL",
                                message=f"Table {table_name} does not exist or cannot be accessed: {str(e)}",
                                table=table,
                            )
                        )
                else:
                    # Unknown check type - mark as WARN
                    checks.append(
                        ReadinessCheckResult(
                            pack_id=pack_id,
                            pack_version=pack_version,
                            check_type=check_type,
                            check_severity=severity,
                            status="WARN",
                            message=message or f"Check type {check_type} not implemented",
                            table=table,
                        )
                    )

        return TenantReadinessResponse(tenant_id=tenant_id, checks=checks)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error checking readiness: {str(e)}")

