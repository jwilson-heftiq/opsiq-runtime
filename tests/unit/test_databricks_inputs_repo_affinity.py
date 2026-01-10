"""Unit tests for DatabricksInputsRepository.fetch_shopper_item_affinity_inputs SQL builder."""

from unittest.mock import MagicMock, Mock, patch

from opsiq_runtime.adapters.databricks.client import DatabricksSqlClient
from opsiq_runtime.adapters.databricks.inputs_repo import DatabricksInputsRepository
from opsiq_runtime.application.run_context import RunContext
from opsiq_runtime.domain.common.ids import CorrelationId, TenantId
from opsiq_runtime.settings import Settings


def test_fetch_shopper_item_affinity_inputs_sql_builder():
    """Test that SQL query includes correct table name and filters."""
    # Mock Databricks client
    mock_client = Mock(spec=DatabricksSqlClient)
    
    # Mock query results - empty for now, just testing SQL construction
    mock_client.query.return_value = iter([])
    
    # Create repository with mock client
    settings = Settings()
    settings.databricks_catalog = "opsiq_dev"
    settings.databricks_schema = "gold"
    settings.databricks_table_prefix = ""
    
    repo = DatabricksInputsRepository(mock_client, settings)
    
    # Create run context
    from datetime import datetime, timezone
    
    ctx = RunContext.from_args(
        tenant_id="test_tenant",
        primitive_name="shopper_item_affinity_score",
        primitive_version="1.0.0",
        config_version="cfg_v1",
        as_of_ts=datetime(2024, 1, 10, 12, 0, 0, tzinfo=timezone.utc),
        correlation_id="test_corr",
    )
    
    # Call fetch method
    list(repo.fetch_shopper_item_affinity_inputs(ctx))
    
    # Verify query was called
    assert mock_client.query.called
    
    # Get the SQL query that was executed
    call_args = mock_client.query.call_args
    sql = call_args[0][0]  # First positional argument is SQL
    params = call_args[1]["params"]  # Parameters are passed as kwargs
    
    # Verify table name is in SQL
    assert "gold_feature_shopper_top_affinity_v1" in sql.lower()
    
    # Verify tenant_id filter is present
    assert "tenant_id = ?" in sql or "tenant_id=?" in sql.replace(" ", "")
    
    # Verify time window filter is present (36h default)
    assert "interval" in sql.lower() or "hours" in sql.lower()
    
    # Verify required columns are selected
    assert "top_affinity_items" in sql.lower()
    assert "lookback_days" in sql.lower()
    assert "top_k" in sql.lower()
    assert "as_of_ts" in sql.lower()
    
    # Verify parameters include tenant_id
    assert params[0] == "test_tenant"
    assert params[1] == 36  # Default hours_window


def test_fetch_shopper_item_affinity_inputs_parses_array_column():
    """Test that array column (top_affinity_items) is parsed correctly."""
    import json
    
    # Mock Databricks client
    mock_client = Mock(spec=DatabricksSqlClient)
    
    # Mock query results with array column as JSON string
    mock_row = {
        "tenant_id": "test_tenant",
        "subject_type": "shopper",
        "subject_id": "s1",
        "as_of_ts": "2024-01-10T12:00:00Z",
        "top_affinity_items": json.dumps([
            {
                "rank": 1,
                "item_group_id": "item_001",
                "affinity_score": 0.95,
                "trip_count": 10,
                "days_since_last_purchase": 5,
                "total_sales": 150.0,
            }
        ]),
        "lookback_days": 90,
        "top_k": 50,
        "config_version": "cfg_v1",
    }
    mock_client.query.return_value = iter([mock_row])
    
    # Create repository
    settings = Settings()
    settings.databricks_catalog = "opsiq_dev"
    settings.databricks_schema = "gold"
    settings.databricks_table_prefix = ""
    
    repo = DatabricksInputsRepository(mock_client, settings)
    
    # Create run context
    from datetime import datetime, timezone
    
    ctx = RunContext(
        tenant_id=TenantId("test_tenant"),
        primitive_name="shopper_item_affinity_score",
        primitive_version="1.0.0",
        as_of_ts=datetime(2024, 1, 10, 12, 0, 0, tzinfo=timezone.utc),
        config_version="cfg_v1",
        correlation_id=CorrelationId("test_corr"),
    )
    
    # Call fetch method
    inputs = list(repo.fetch_shopper_item_affinity_inputs(ctx))
    
    # Verify input was created and array was parsed
    assert len(inputs) == 1
    input_row = inputs[0]
    assert input_row.subject_id == "s1"
    assert input_row.top_affinity_items is not None
    assert len(input_row.top_affinity_items) == 1
    assert input_row.top_affinity_items[0]["item_group_id"] == "item_001"
    assert input_row.top_affinity_items[0]["affinity_score"] == 0.95
    assert input_row.lookback_days == 90
    assert input_row.top_k == 50


def test_fetch_shopper_item_affinity_inputs_handles_list_array():
    """Test that array column as Python list (not JSON string) is handled."""
    # Mock Databricks client
    mock_client = Mock(spec=DatabricksSqlClient)
    
    # Mock query results with array column as Python list
    mock_row = {
        "tenant_id": "test_tenant",
        "subject_type": "shopper",
        "subject_id": "s1",
        "as_of_ts": "2024-01-10T12:00:00Z",
        "top_affinity_items": [
            {
                "rank": 1,
                "item_group_id": "item_001",
                "affinity_score": 0.85,
            }
        ],
        "lookback_days": None,
        "top_k": None,
        "config_version": "cfg_v1",
    }
    mock_client.query.return_value = iter([mock_row])
    
    # Create repository
    settings = Settings()
    settings.databricks_catalog = "opsiq_dev"
    settings.databricks_schema = "gold"
    settings.databricks_table_prefix = ""
    
    repo = DatabricksInputsRepository(mock_client, settings)
    
    # Create run context
    from datetime import datetime, timezone
    
    ctx = RunContext(
        tenant_id=TenantId("test_tenant"),
        primitive_name="shopper_item_affinity_score",
        primitive_version="1.0.0",
        as_of_ts=datetime(2024, 1, 10, 12, 0, 0, tzinfo=timezone.utc),
        config_version="cfg_v1",
        correlation_id=CorrelationId("test_corr"),
    )
    
    # Call fetch method
    inputs = list(repo.fetch_shopper_item_affinity_inputs(ctx))
    
    # Verify input was created and list was used as-is
    assert len(inputs) == 1
    input_row = inputs[0]
    assert input_row.top_affinity_items is not None
    assert len(input_row.top_affinity_items) == 1
    assert input_row.top_affinity_items[0]["item_group_id"] == "item_001"
    assert input_row.lookback_days is None
    assert input_row.top_k is None
