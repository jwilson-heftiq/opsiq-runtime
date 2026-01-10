"""Unit tests for DatabricksInputsRepository coupon offer set SQL builders."""

from unittest.mock import Mock

from opsiq_runtime.adapters.databricks.client import DatabricksSqlClient
from opsiq_runtime.adapters.databricks.inputs_repo import DatabricksInputsRepository
from opsiq_runtime.settings import Settings


def test_fetch_weekly_ad_item_groups_sql_builder():
    """Test that fetch_weekly_ad_item_groups SQL contains correct WHERE constraints and COALESCE."""
    mock_client = Mock(spec=DatabricksSqlClient)
    mock_client.query.return_value = iter([])
    
    settings = Settings()
    repo = DatabricksInputsRepository(mock_client, settings)
    
    # Call method
    repo.fetch_weekly_ad_item_groups(
        tenant_id="t1",
        ad_id="ad_001",
        scope_type="store",
        scope_value="store_123",
        hours_window=72,
    )
    
    # Verify query was called
    assert mock_client.query.called
    
    call_args = mock_client.query.call_args
    sql = call_args[0][0]
    params = call_args[1]["params"]
    
    # Verify table name
    assert "gold_canonical_weekly_ad_item_v1" in sql.lower()
    
    # Verify WHERE constraints
    assert "tenant_id = ?" in sql or "tenant_id=?" in sql.replace(" ", "")
    assert "ad_id = ?" in sql or "ad_id=?" in sql.replace(" ", "")
    assert "scope_type = ?" in sql or "scope_type=?" in sql.replace(" ", "")
    assert "scope_value = ?" in sql or "scope_value=?" in sql.replace(" ", "")
    
    # Verify COALESCE pattern for item_group_id
    assert "coalesce(linkcode, gtin)" in sql.lower() or "coalesce(gtin, linkcode)" in sql.lower()
    
    # Verify INTERVAL usage
    assert "interval" in sql.lower()
    assert "hours" in sql.lower()
    
    # Verify DISTINCT
    assert "distinct" in sql.lower()
    
    # Verify NOT NULL check
    assert "not null" in sql.lower() or "is not null" in sql.lower()
    
    # Verify parameters
    assert params[0] == "t1"
    assert params[1] == "ad_001"
    assert params[2] == "store"
    assert params[3] == "store_123"
    assert params[4] == 72


def test_fetch_weekly_ad_item_groups_returns_set():
    """Test that fetch_weekly_ad_item_groups returns a set of item_group_ids."""
    mock_client = Mock(spec=DatabricksSqlClient)
    mock_client.query.return_value = iter([
        {"item_group_id": "item_001"},
        {"item_group_id": "item_002"},
        {"item_group_id": "item_001"},  # Duplicate should be deduplicated
    ])
    
    settings = Settings()
    repo = DatabricksInputsRepository(mock_client, settings)
    
    result = repo.fetch_weekly_ad_item_groups(
        tenant_id="t1",
        ad_id="ad_001",
        scope_type="store",
        scope_value="store_123",
    )
    
    assert isinstance(result, set)
    assert len(result) == 2  # Duplicates removed
    assert "item_001" in result
    assert "item_002" in result


def test_fetch_coupon_eligible_items_sql_builder():
    """Test that fetch_coupon_eligible_items SQL contains correct WHERE constraints."""
    mock_client = Mock(spec=DatabricksSqlClient)
    mock_client.query.return_value = iter([])
    
    settings = Settings()
    repo = DatabricksInputsRepository(mock_client, settings)
    
    # Call method
    repo.fetch_coupon_eligible_items(tenant_id="t1", hours_window=72)
    
    # Verify query was called
    assert mock_client.query.called
    
    call_args = mock_client.query.call_args
    sql = call_args[0][0]
    params = call_args[1]["params"]
    
    # Verify table name
    assert "gold_policy_item_eligibility_v1" in sql.lower()
    
    # Verify WHERE constraints
    assert "tenant_id = ?" in sql or "tenant_id=?" in sql.replace(" ", "")
    assert "is_coupon_eligible = true" in sql.lower() or "is_coupon_eligible=true" in sql.lower().replace(" ", "")
    
    # Verify INTERVAL usage
    assert "interval" in sql.lower()
    assert "hours" in sql.lower()
    
    # Verify required columns
    assert "item_group_id" in sql.lower()
    assert "gtin" in sql.lower()
    assert "linkcode" in sql.lower()
    assert "ineligible_reasons" in sql.lower()
    
    # Verify parameters
    assert params[0] == "t1"
    assert params[1] == 72


def test_fetch_coupon_eligible_items_returns_dict():
    """Test that fetch_coupon_eligible_items returns dict keyed by item_group_id."""
    import json
    
    mock_client = Mock(spec=DatabricksSqlClient)
    mock_client.query.return_value = iter([
        {
            "item_group_id": "item_001",
            "gtin": "GTIN_001",
            "linkcode": None,
            "ineligible_reasons": json.dumps([]),
        },
        {
            "item_group_id": "item_002",
            "gtin": "GTIN_002",
            "linkcode": "LINK_002",
            "ineligible_reasons": None,
        },
    ])
    
    settings = Settings()
    repo = DatabricksInputsRepository(mock_client, settings)
    
    result = repo.fetch_coupon_eligible_items(tenant_id="t1")
    
    assert isinstance(result, dict)
    assert "item_001" in result
    assert "item_002" in result
    assert result["item_001"]["gtin"] == "GTIN_001"
    assert result["item_002"]["linkcode"] == "LINK_002"


def test_fetch_baseline_prices_sql_builder():
    """Test that fetch_baseline_prices SQL contains CTE with COALESCE and unit_price calculation."""
    mock_client = Mock(spec=DatabricksSqlClient)
    mock_client.query.return_value = iter([])
    
    settings = Settings()
    repo = DatabricksInputsRepository(mock_client, settings)
    
    # Call method
    repo.fetch_baseline_prices(tenant_id="t1", exclude_days=90)
    
    # Verify query was called
    assert mock_client.query.called
    
    call_args = mock_client.query.call_args
    sql = call_args[0][0]
    params = call_args[1]["params"]
    
    # Verify table name
    assert "gold_canonical_trip_item_enriched_v1" in sql.lower()
    
    # Verify CTE structure
    assert "with base as" in sql.lower() or "with base as (" in sql.lower()
    assert "ranked as" in sql.lower() or "ranked as (" in sql.lower()
    
    # Verify COALESCE pattern for item_group_id
    assert "coalesce(linkcode, gtin)" in sql.lower() or "coalesce(gtin, linkcode)" in sql.lower()
    
    # Verify unit_price calculation logic
    assert "unit_price" in sql.lower()
    assert "amount" in sql.lower()
    assert "quantity" in sql.lower()
    assert "case" in sql.lower() or "when" in sql.lower()
    
    # Verify ROW_NUMBER for ranking
    assert "row_number()" in sql.lower() or "row_number (" in sql.lower()
    assert "partition by" in sql.lower()
    assert "order by" in sql.lower()
    assert "trip_ts desc" in sql.lower()
    
    # Verify INTERVAL usage for days
    assert "interval" in sql.lower()
    assert "days" in sql.lower()
    
    # Verify parameters
    assert params[0] == "t1"
    assert params[1] == 90


def test_fetch_baseline_prices_with_shopper_ids_filter():
    """Test that fetch_baseline_prices includes shopper_ids filter when provided."""
    mock_client = Mock(spec=DatabricksSqlClient)
    mock_client.query.return_value = iter([])
    
    settings = Settings()
    repo = DatabricksInputsRepository(mock_client, settings)
    
    # Call method with shopper_ids
    repo.fetch_baseline_prices(
        tenant_id="t1",
        exclude_days=90,
        shopper_ids=["s1", "s2", "s3"],
    )
    
    # Verify query was called
    assert mock_client.query.called
    
    call_args = mock_client.query.call_args
    sql = call_args[0][0]
    params = call_args[1]["params"]
    
    # Verify IN clause for shopper_ids
    assert "shopper_id in" in sql.lower() or "shopper_id in (" in sql.lower()
    
    # Verify parameters include shopper_ids
    assert params[0] == "t1"
    assert params[1] == 90
    assert "s1" in params
    assert "s2" in params
    assert "s3" in params


def test_fetch_baseline_prices_returns_dict_with_tuple_keys():
    """Test that fetch_baseline_prices returns dict keyed by (shopper_id, item_group_id) tuple."""
    mock_client = Mock(spec=DatabricksSqlClient)
    mock_client.query.return_value = iter([
        {
            "shopper_id": "s1",
            "item_group_id": "item_001",
            "baseline_price": 10.0,
            "baseline_price_ts": "2024-01-10T12:00:00Z",
        },
        {
            "shopper_id": "s1",
            "item_group_id": "item_002",
            "baseline_price": 15.0,
            "baseline_price_ts": "2024-01-10T12:00:00Z",
        },
    ])
    
    settings = Settings()
    repo = DatabricksInputsRepository(mock_client, settings)
    
    result = repo.fetch_baseline_prices(tenant_id="t1")
    
    assert isinstance(result, dict)
    assert ("s1", "item_001") in result
    assert ("s1", "item_002") in result
    assert result[("s1", "item_001")] == 10.0
    assert result[("s1", "item_002")] == 15.0


def test_fetch_baseline_prices_handles_unit_price_calculation():
    """Test that baseline prices are calculated correctly from unit_price or amount/quantity."""
    mock_client = Mock(spec=DatabricksSqlClient)
    # Mock results with both unit_price and computed prices
    mock_client.query.return_value = iter([
        {
            "shopper_id": "s1",
            "item_group_id": "item_001",
            "baseline_price": 10.5,  # From unit_price column
            "baseline_price_ts": "2024-01-10T12:00:00Z",
        },
        {
            "shopper_id": "s1",
            "item_group_id": "item_002",
            "baseline_price": 7.5,  # From amount/quantity calculation
            "baseline_price_ts": "2024-01-09T12:00:00Z",
        },
    ])
    
    settings = Settings()
    repo = DatabricksInputsRepository(mock_client, settings)
    
    result = repo.fetch_baseline_prices(tenant_id="t1")
    
    assert isinstance(result, dict)
    assert result[("s1", "item_001")] == 10.5
    assert result[("s1", "item_002")] == 7.5


def test_fetch_baseline_prices_filters_invalid_prices():
    """Test that invalid or non-positive prices are filtered out."""
    mock_client = Mock(spec=DatabricksSqlClient)
    mock_client.query.return_value = iter([
        {
            "shopper_id": "s1",
            "item_group_id": "item_001",
            "baseline_price": 10.0,  # Valid
            "baseline_price_ts": "2024-01-10T12:00:00Z",
        },
        {
            "shopper_id": "s1",
            "item_group_id": "item_002",
            "baseline_price": 0.0,  # Invalid (should be filtered)
            "baseline_price_ts": "2024-01-10T12:00:00Z",
        },
        {
            "shopper_id": "s1",
            "item_group_id": "item_003",
            "baseline_price": -5.0,  # Invalid (should be filtered)
            "baseline_price_ts": "2024-01-10T12:00:00Z",
        },
    ])
    
    settings = Settings()
    repo = DatabricksInputsRepository(mock_client, settings)
    
    result = repo.fetch_baseline_prices(tenant_id="t1")
    
    # Only positive prices should be included
    assert ("s1", "item_001") in result
    assert ("s1", "item_002") not in result
    assert ("s1", "item_003") not in result
    assert result[("s1", "item_001")] == 10.0
