"""Unit tests for DatabricksInputsRepository.fetch_shopper_weekly_ad_slate_inputs SQL builder."""

from unittest.mock import MagicMock, Mock, patch

from opsiq_runtime.adapters.databricks.client import DatabricksSqlClient
from opsiq_runtime.adapters.databricks.inputs_repo import DatabricksInputsRepository
from opsiq_runtime.application.run_context import RunContext
from opsiq_runtime.domain.common.ids import CorrelationId, TenantId
from opsiq_runtime.settings import Settings


def test_fetch_current_ad_candidates_sql_builder():
    """Test that SQL query includes correct table name and filters."""
    # Mock Databricks client
    mock_client = Mock(spec=DatabricksSqlClient)
    mock_client.query.return_value = iter([])
    
    # Create repository with mock client
    settings = Settings()
    settings.databricks_catalog = "opsiq_dev"
    settings.databricks_schema = "gold"
    settings.databricks_table_prefix = ""
    
    repo = DatabricksInputsRepository(mock_client, settings)
    
    # Call fetch method
    repo.fetch_current_ad_candidates(
        tenant_id="test_tenant",
        ad_id="ad_001",
        scope_type="store",
        scope_value="store_123",
        hours_window=36,
    )
    
    # Verify query was called
    assert mock_client.query.called
    
    # Get the SQL query that was executed
    call_args = mock_client.query.call_args
    sql = call_args[0][0]  # First positional argument is SQL
    params = call_args[1]["params"]  # Parameters are passed as kwargs
    
    # Verify table name is in SQL
    assert "opsiq_dev.gold.gold_canonical_weekly_ad_item_v1" in sql
    
    # Verify WHERE filters
    assert "tenant_id = ?" in sql or "tenant_id=?" in sql.replace(" ", "")
    assert "ad_id = ?" in sql or "ad_id=?" in sql.replace(" ", "")
    assert "scope_type = ?" in sql or "scope_type=?" in sql.replace(" ", "")
    assert "scope_value = ?" in sql or "scope_value=?" in sql.replace(" ", "")
    
    # Verify time window filter
    assert "interval" in sql.lower() or "hours" in sql.lower()
    
    # Verify required columns are selected
    assert "ad_id" in sql.lower()
    assert "item_group_id" in sql.lower()
    assert "promo_price" in sql.lower()
    
    # Verify parameters
    assert params[0] == "test_tenant"
    assert params[1] == "ad_001"
    assert params[2] == "store"
    assert params[3] == "store_123"
    assert params[4] == 36  # hours_window


def test_fetch_shopper_top_affinity_sql_builder():
    """Test that SQL query includes correct table name and filters."""
    # Mock Databricks client
    mock_client = Mock(spec=DatabricksSqlClient)
    mock_client.query.return_value = iter([])
    
    # Create repository
    settings = Settings()
    settings.databricks_catalog = "opsiq_dev"
    settings.databricks_schema = "gold"
    settings.databricks_table_prefix = ""
    
    repo = DatabricksInputsRepository(mock_client, settings)
    
    # Call fetch method
    repo.fetch_shopper_top_affinity(
        tenant_id="test_tenant",
        hours_window=36,
        shopper_ids=None,
    )
    
    # Verify query was called
    assert mock_client.query.called
    
    # Get the SQL query
    call_args = mock_client.query.call_args
    sql = call_args[0][0]
    params = call_args[1]["params"]
    
    # Verify table name
    assert "opsiq_dev.gold.gold_feature_shopper_top_affinity_v1" in sql
    
    # Verify WHERE filters
    assert "tenant_id = ?" in sql or "tenant_id=?" in sql.replace(" ", "")
    assert "interval" in sql.lower() or "hours" in sql.lower()
    
    # Verify required columns
    assert "shopper_id" in sql.lower()
    assert "top_affinity_items" in sql.lower()
    
    # Verify parameters
    assert params[0] == "test_tenant"
    assert params[1] == 36


def test_fetch_shopper_top_affinity_with_shopper_ids_filter():
    """Test that SQL includes shopper_id IN filter when shopper_ids provided."""
    # Mock Databricks client
    mock_client = Mock(spec=DatabricksSqlClient)
    mock_client.query.return_value = iter([])
    
    # Create repository
    settings = Settings()
    repo = DatabricksInputsRepository(mock_client, settings)
    
    # Call with shopper_ids
    repo.fetch_shopper_top_affinity(
        tenant_id="test_tenant",
        hours_window=36,
        shopper_ids=["s1", "s2", "s3"],
    )
    
    # Verify query was called
    assert mock_client.query.called
    
    # Get the SQL query
    call_args = mock_client.query.call_args
    sql = call_args[0][0]
    params = call_args[1]["params"]
    
    # Verify IN clause is present
    assert "shopper_id IN" in sql or "shopper_id in" in sql.lower()
    
    # Verify parameters include shopper_ids
    assert params[0] == "test_tenant"
    assert params[1] == 36
    assert "s1" in params
    assert "s2" in params
    assert "s3" in params


def test_fetch_recent_purchase_keys_sql_builder():
    """Test that SQL query includes correct table name, COALESCE, and filters."""
    # Mock Databricks client
    mock_client = Mock(spec=DatabricksSqlClient)
    mock_client.query.return_value = iter([])
    
    # Create repository
    settings = Settings()
    repo = DatabricksInputsRepository(mock_client, settings)
    
    # Call fetch method
    repo.fetch_recent_purchase_keys(
        tenant_id="test_tenant",
        exclude_days=14,
        shopper_ids=None,
    )
    
    # Verify query was called
    assert mock_client.query.called
    
    # Get the SQL query
    call_args = mock_client.query.call_args
    sql = call_args[0][0]
    params = call_args[1]["params"]
    
    # Verify table name
    assert "opsiq_dev.gold.gold_canonical_trip_item_enriched_v1" in sql
    
    # Verify COALESCE logic
    assert "COALESCE(linkcode, gtin)" in sql or "coalesce(linkcode, gtin)" in sql.lower()
    
    # Verify WHERE filters
    assert "tenant_id = ?" in sql or "tenant_id=?" in sql.replace(" ", "")
    assert "interval" in sql.lower() or "days" in sql.lower()
    
    # Verify GROUP BY
    assert "GROUP BY" in sql or "group by" in sql.lower()
    
    # Verify parameters
    assert params[0] == "test_tenant"
    assert params[1] == 14  # exclude_days


def test_fetch_recent_purchase_keys_with_shopper_ids_filter():
    """Test that SQL includes shopper_id IN filter when shopper_ids provided."""
    # Mock Databricks client
    mock_client = Mock(spec=DatabricksSqlClient)
    mock_client.query.return_value = iter([])
    
    # Create repository
    settings = Settings()
    repo = DatabricksInputsRepository(mock_client, settings)
    
    # Call with shopper_ids
    repo.fetch_recent_purchase_keys(
        tenant_id="test_tenant",
        exclude_days=14,
        shopper_ids=["s1", "s2"],
    )
    
    # Verify query was called
    assert mock_client.query.called
    
    # Get the SQL query
    call_args = mock_client.query.call_args
    sql = call_args[0][0]
    params = call_args[1]["params"]
    
    # Verify IN clause is present
    assert "shopper_id IN" in sql or "shopper_id in" in sql.lower()
    
    # Verify parameters include shopper_ids
    assert params[0] == "test_tenant"
    assert params[1] == 14
    assert "s1" in params
    assert "s2" in params


def test_fetch_shopper_weekly_ad_slate_inputs_integration():
    """Test the main fetch method integrates all three helper methods."""
    from datetime import datetime, timezone
    import json
    
    # Mock Databricks client
    mock_client = Mock(spec=DatabricksSqlClient)
    
    # Mock ad candidates query
    mock_ad_candidates = [
        {
            "ad_id": "ad_001",
            "ad_group_id": "ad_group_1",
            "scope_type": "store",
            "scope_value": "store_123",
            "as_of_ts": "2024-01-10T12:00:00Z",
            "gtin": "GTIN_001",
            "linkcode": None,
            "item_group_id": "item_001",
            "title": "Item 1",
            "promo_text": None,
            "primary_image_url": None,
            "promo_price": 10.0,
            "ad_price_raw": None,
            "ad_price_uom": None,
            "ad_price_qualifier": None,
        }
    ]
    
    # Mock affinity query
    mock_affinity = [
        {
            "shopper_id": "s1",
            "as_of_ts": "2024-01-10T12:00:00Z",
            "top_affinity_items": json.dumps([
                {"rank": 1, "item_group_id": "item_001", "affinity_score": 0.9},
            ]),
        }
    ]
    
    # Mock purchases query
    mock_purchases = [
        {
            "shopper_id": "s1",
            "item_group_id": "item_002",
            "last_purchase_ts": "2024-01-05T12:00:00Z",
            "category": "Dairy",
        }
    ]
    
    # Set up mock to return different results for different queries
    def mock_query_side_effect(sql, params=None):
        sql_lower = sql.lower()
        if "gold_canonical_weekly_ad_item_v1" in sql_lower:
            return iter(mock_ad_candidates)
        elif "gold_feature_shopper_top_affinity_v1" in sql_lower:
            return iter(mock_affinity)
        elif "gold_canonical_trip_item_enriched_v1" in sql_lower:
            return iter(mock_purchases)
        return iter([])
    
    mock_client.query.side_effect = mock_query_side_effect
    
    # Create repository
    settings = Settings()
    repo = DatabricksInputsRepository(mock_client, settings)
    
    # Create run context
    ctx = RunContext.from_args(
        tenant_id="test_tenant",
        primitive_name="shopper_weekly_ad_slate",
        primitive_version="1.0.0",
        config_version="cfg_v1",
        as_of_ts=datetime(2024, 1, 10, 12, 0, 0, tzinfo=timezone.utc),
        correlation_id="test_corr",
    )
    
    # Mock config provider
    with patch(
        "opsiq_runtime.adapters.databricks.inputs_repo.InlineConfigProvider"
    ) as mock_config_provider_class:
        from opsiq_runtime.domain.primitives.shopper_weekly_ad_slate.config import (
            ShopperWeeklyAdSlateConfig,
        )
        
        mock_config = ShopperWeeklyAdSlateConfig(
            ad_id="ad_001",
            scope_type="store",
            scope_value="store_123",
        )
        mock_config_provider = Mock()
        mock_config_provider.get_config.return_value = mock_config
        mock_config_provider_class.return_value = mock_config_provider
        
        # Call fetch method
        inputs = list(repo.fetch_shopper_weekly_ad_slate_inputs(ctx))
        
        # Verify inputs were created
        assert len(inputs) == 1
        input_obj = inputs[0]
        assert input_obj.subject_id == "s1"
        assert len(input_obj.candidates) == 1
        assert input_obj.shopper_affinity is not None
        assert input_obj.shopper_affinity.shopper_id == "s1"
        assert "item_002" in input_obj.recent_purchase_keys  # Excluded item
