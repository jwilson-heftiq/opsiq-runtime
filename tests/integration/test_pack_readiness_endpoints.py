"""Integration tests for pack readiness endpoints."""

from __future__ import annotations

from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch

import pytest

from opsiq_runtime.app.api.services.pack_readiness.models import (
    CanonicalFreshnessResult,
    DecisionHealthResult,
    PackReadinessResponse,
    RollupIntegrityResult,
)


@pytest.fixture
def mock_databricks_client():
    """Create a mock Databricks client."""
    client = MagicMock()
    return client


@pytest.fixture
def mock_pack_readiness_service(mock_databricks_client):
    """Create a mock pack readiness service."""
    from opsiq_runtime.app.api.services.pack_readiness import PackReadinessService
    from opsiq_runtime.settings import Settings

    settings = Settings()
    service = PackReadinessService(settings, mock_databricks_client)

    return service


class TestPackReadinessEndpoints:
    """Test pack readiness API endpoints."""

    def test_get_pack_readiness_success(self, client, mock_databricks_client):
        """Test successful pack readiness retrieval."""
        # Mock Databricks responses
        mock_databricks_client.query.side_effect = [
            # Canonical freshness query
            [{"last_as_of_ts": datetime.now(timezone.utc) - timedelta(hours=12)}],
            # Decision health query
            [
                {
                    "primitive_name": "order_line_fulfillment_risk",
                    "total_decisions": 1000,
                    "at_risk_count": 50,
                    "not_at_risk_count": 940,
                    "unknown_count": 10,
                    "last_computed_at": datetime.now(timezone.utc),
                }
            ],
            # Rollup integrity query
            [{"total": 1000, "has_ordernum": 990}],
        ]

        response = client.get("/v1/tenants/test_tenant/packs/order_fulfillment_risk/readiness")

        assert response.status_code == 200
        data = response.json()
        assert data["pack_id"] == "order_fulfillment_risk"
        assert data["tenant_id"] == "test_tenant"
        assert "overall_status" in data
        assert "canonical_freshness" in data
        assert "decision_health" in data
        assert "rollup_integrity" in data

    def test_get_pack_readiness_not_found(self, client):
        """Test pack readiness for non-existent pack."""
        response = client.get("/v1/tenants/test_tenant/packs/nonexistent_pack/readiness")

        assert response.status_code == 404

    def test_get_all_packs_readiness(self, client, mock_databricks_client):
        """Test getting readiness for all enabled packs."""
        # Mock Databricks responses (will be called multiple times for each pack)
        mock_databricks_client.query.side_effect = [
            # For each pack: canonical freshness, decision health, rollup integrity
            [{"last_as_of_ts": datetime.now(timezone.utc) - timedelta(hours=12)}],
            [
                {
                    "primitive_name": "order_line_fulfillment_risk",
                    "total_decisions": 1000,
                    "at_risk_count": 50,
                    "not_at_risk_count": 940,
                    "unknown_count": 10,
                    "last_computed_at": datetime.now(timezone.utc),
                }
            ],
            [{"total": 1000, "has_ordernum": 990}],
        ]

        response = client.get("/v1/tenants/test_tenant/packs/readiness")

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        # Should return readiness for all enabled packs

    def test_get_pack_readiness_no_databricks(self, client):
        """Test pack readiness when Databricks is not configured."""
        # When Databricks is not available, should return WARN status
        response = client.get("/v1/tenants/test_tenant/packs/order_fulfillment_risk/readiness")

        # Should still return 200, but with WARN statuses
        assert response.status_code == 200
        data = response.json()
        # At least one metric should have WARN status when Databricks unavailable
        assert any(
            item.get("status") == "WARN"
            for section in [data.get("canonical_freshness", []), data.get("decision_health", [])]
            for item in section
        )

    def test_get_pack_readiness_empty_results(self, client, mock_databricks_client):
        """Test pack readiness with empty Databricks results."""
        # Mock empty results
        mock_databricks_client.query.side_effect = [
            [],  # No canonical data
            [],  # No decision data
            [],  # No integrity data
        ]

        response = client.get("/v1/tenants/test_tenant/packs/order_fulfillment_risk/readiness")

        assert response.status_code == 200
        data = response.json()
        # Should handle empty results gracefully
        assert "overall_status" in data

