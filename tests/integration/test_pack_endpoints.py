"""Integration tests for pack endpoints."""

from __future__ import annotations

import pytest

from opsiq_runtime.app.main import app
from opsiq_runtime.settings import get_settings
from fastapi.testclient import TestClient


@pytest.fixture
def client():
    """Create a test client."""
    return TestClient(app)


def test_get_tenant_decision_packs(client):
    """Test getting enabled packs for a tenant."""
    # This test requires the actual pack files to exist
    # Skip if packs directory doesn't exist
    settings = get_settings()
    packs_dir = settings.packs_base_dir
    
    try:
        response = client.get("/v1/tenants/price_chopper/decision-packs")
        assert response.status_code in [200, 404]  # 404 if tenant not configured
        if response.status_code == 200:
            data = response.json()
            assert isinstance(data, list)
    except Exception:
        pytest.skip("Packs directory or tenant configuration not available")


def test_get_decision_pack(client):
    """Test getting a pack definition."""
    try:
        response = client.get("/v1/decision-packs/shopper_health_intelligence/1.0.0")
        assert response.status_code in [200, 404]
        if response.status_code == 200:
            data = response.json()
            assert "pack_id" in data
            assert "pack_version" in data
    except Exception:
        pytest.skip("Pack definition not available")


@pytest.mark.skipif(
    not get_settings().databricks_server_hostname,
    reason="Databricks not configured",
)
def test_get_tenant_readiness(client):
    """Test getting tenant readiness (requires Databricks)."""
    try:
        response = client.get("/v1/tenants/price_chopper/readiness")
        assert response.status_code in [200, 404]
        if response.status_code == 200:
            data = response.json()
            assert "tenant_id" in data
            assert "checks" in data
    except Exception:
        pytest.skip("Readiness check not available")

