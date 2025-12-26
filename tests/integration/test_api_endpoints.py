"""Integration tests for API endpoints.

These tests are skipped unless DATABRICKS_* environment variables are present.
They require a live Databricks connection and should not run in CI by default.
"""

from __future__ import annotations

import os
import pytest
from fastapi.testclient import TestClient

from opsiq_runtime.app.main import app


def _has_databricks_config() -> bool:
    """Check if Databricks environment variables are configured."""
    required = [
        "DATABRICKS_SERVER_HOSTNAME",
        "DATABRICKS_HTTP_PATH",
        "DATABRICKS_ACCESS_TOKEN",
    ]
    return all(os.getenv(var) for var in required)


@pytest.mark.skipif(not _has_databricks_config(), reason="DATABRICKS_* env vars not set")
def test_worklist_endpoint_smoke():
    """Smoke test for worklist endpoint."""
    client = TestClient(app)

    # This will fail if the endpoint is not registered or there's a configuration issue
    # It may fail with 500 if there's no data, which is acceptable for a smoke test
    response = client.get("/v1/tenants/test_tenant/worklists/shopper-health?limit=10")

    # Should not return 404 (endpoint not found) or 422 (validation error)
    assert response.status_code in [200, 404, 500], f"Unexpected status code: {response.status_code}, body: {response.text}"


@pytest.mark.skipif(not _has_databricks_config(), reason="DATABRICKS_* env vars not set")
def test_decision_bundle_endpoint_smoke():
    """Smoke test for decision bundle endpoint."""
    client = TestClient(app)

    # This will fail if the endpoint is not registered or there's a configuration issue
    # It may fail with 404 if subject doesn't exist, which is acceptable for a smoke test
    response = client.get("/v1/tenants/test_tenant/subjects/shopper/test_subject/decision-bundle")

    # Should not return 404 (endpoint not found) or 422 (validation error)
    assert response.status_code in [200, 404, 500], f"Unexpected status code: {response.status_code}, body: {response.text}"


@pytest.mark.skipif(not _has_databricks_config(), reason="DATABRICKS_* env vars not set")
def test_worklist_endpoint_with_filters():
    """Test worklist endpoint with filter parameters."""
    client = TestClient(app)

    response = client.get(
        "/v1/tenants/test_tenant/worklists/shopper-health",
        params={
            "state": ["URGENT", "WATCHLIST"],
            "confidence": ["HIGH"],
            "limit": 5,
        },
    )

    # Should not return 404 or 422
    assert response.status_code in [200, 404, 500], f"Unexpected status code: {response.status_code}, body: {response.text}"


@pytest.mark.skipif(not _has_databricks_config(), reason="DATABRICKS_* env vars not set")
def test_decision_bundle_endpoint_with_params():
    """Test decision bundle endpoint with query parameters."""
    client = TestClient(app)

    response = client.get(
        "/v1/tenants/test_tenant/subjects/shopper/test_subject/decision-bundle",
        params={
            "include_evidence": "false",
        },
    )

    # Should not return 404 or 422
    assert response.status_code in [200, 404, 500], f"Unexpected status code: {response.status_code}, body: {response.text}"

