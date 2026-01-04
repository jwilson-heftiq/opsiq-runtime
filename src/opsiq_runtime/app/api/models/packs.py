"""Pydantic models for decision pack-related API responses."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class DefaultWorklist(BaseModel):
    """Default worklist configuration for a subject."""

    title: str
    primitive_name: str
    default_filters: dict[str, Any] = Field(default_factory=dict)
    ui_route: str


class SubjectDefinition(BaseModel):
    """Subject definition within a pack."""

    subject_type: str
    default_worklist: DefaultWorklist


class PrimitiveDependency(BaseModel):
    """Primitive dependency information."""

    canonical_inputs: list[str] = Field(default_factory=list)
    primitives: list[str] = Field(default_factory=list)


class PrimitiveDefinition(BaseModel):
    """Primitive definition within a pack."""

    primitive_name: str
    primitive_version: str
    canonical_version: str
    kind: str  # "primitive" or "composite"
    depends_on: PrimitiveDependency


class ActivationConfig(BaseModel):
    """Activation configuration for a pack."""

    eventbridge_enabled: bool = False
    event_types: list[str] = Field(default_factory=list)
    recommended_targets: list[str] = Field(default_factory=list)


class OnboardingCheck(BaseModel):
    """Onboarding check definition."""

    type: str  # "table_exists", "view_exists", "custom"
    severity: str  # "BLOCKER", "WARN", "INFO"
    table: str | None = None
    message: str | None = None


class DecisionPackDefinition(BaseModel):
    """Full decision pack definition matching the JSON schema."""

    pack_id: str
    pack_version: str
    name: str
    description: str
    status: str  # "ACTIVE", "DEPRECATED", "INTERNAL"
    tags: list[str] = Field(default_factory=list)
    subjects: list[SubjectDefinition]
    primitives: list[PrimitiveDefinition]
    activation: ActivationConfig | None = None
    onboarding_checks: list[OnboardingCheck] = Field(default_factory=list)


class TenantPackConfig(BaseModel):
    """Configuration for a pack within a tenant enablement."""

    default_config_version: str = "cfg_v1"
    worklist_defaults: dict[str, Any] = Field(default_factory=dict)


class EnabledPackItem(BaseModel):
    """An enabled pack item in tenant enablement."""

    pack_id: str
    pack_version: str
    enabled: bool
    config: TenantPackConfig


class TenantPackEnablement(BaseModel):
    """Tenant pack enablement configuration."""

    tenant_id: str
    enabled_packs: list[EnabledPackItem]


class EnabledPackSummary(BaseModel):
    """Summary of an enabled pack for API responses."""

    pack_id: str
    pack_version: str
    name: str
    description: str
    tags: list[str]
    subjects: list[SubjectDefinition]
    primitives: list[dict[str, str]] = Field(
        description="Simplified primitive info with primitive_name and primitive_version"
    )


class ReadinessCheckResult(BaseModel):
    """Result of a single readiness check."""

    pack_id: str
    pack_version: str
    check_type: str
    check_severity: str
    status: str  # "PASS", "FAIL", "WARN"
    message: str
    table: str | None = None


class TenantReadinessResponse(BaseModel):
    """Response for tenant readiness endpoint."""

    tenant_id: str
    checks: list[ReadinessCheckResult]

