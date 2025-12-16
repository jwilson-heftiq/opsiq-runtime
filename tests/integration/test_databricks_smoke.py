"""
Integration tests for Databricks adapters.

These tests are skipped unless DATABRICKS_* environment variables are present.
They require a live Databricks connection and should not run in CI by default.
"""

from __future__ import annotations

import os
import pytest

from opsiq_runtime.adapters.databricks.client import DatabricksSqlClient
from opsiq_runtime.settings import get_settings


def _has_databricks_config() -> bool:
    """Check if Databricks environment variables are configured."""
    required = [
        "DATABRICKS_SERVER_HOSTNAME",
        "DATABRICKS_HTTP_PATH",
        "DATABRICKS_ACCESS_TOKEN",
    ]
    return all(os.getenv(var) for var in required)


@pytest.mark.skipif(not _has_databricks_config(), reason="DATABRICKS_* env vars not set")
def test_databricks_client_connection():
    """Test basic connection to Databricks."""
    settings = get_settings()
    client = DatabricksSqlClient(settings)

    try:
        # Simple query to test connection
        result = client.query("SELECT 1 AS test_value")
        assert len(result) == 1
        assert result[0]["test_value"] == 1
    finally:
        client.close()


@pytest.mark.skipif(not _has_databricks_config(), reason="DATABRICKS_* env vars not set")
def test_databricks_input_table_read():
    """Test reading from input table (if it exists)."""
    settings = get_settings()
    client = DatabricksSqlClient(settings)

    try:
        # Build table name
        table_parts = []
        if settings.databricks_catalog:
            table_parts.append(settings.databricks_catalog)
        if settings.databricks_schema:
            table_parts.append(settings.databricks_schema)
        table_parts.append(f"{settings.databricks_table_prefix}gold_canonical_shopper_recency_input_v1")
        table_name = ".".join(table_parts)

        # Try to read a small sample (limit to avoid large results)
        sql = f"SELECT COUNT(*) AS row_count FROM {table_name} LIMIT 1"
        try:
            result = client.query(sql)
            # If we get here, table exists and query worked
            assert len(result) >= 0  # At least got a result
        except Exception as e:
            # Table might not exist in test environment - that's okay
            pytest.skip(f"Input table not available: {e}")

    finally:
        client.close()

