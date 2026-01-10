from datetime import datetime, timezone

from opsiq_runtime.domain.common.decision import CONFIDENCE_HIGH, CONFIDENCE_LOW
from opsiq_runtime.domain.primitives.shopper_item_affinity_score.config import (
    ShopperItemAffinityConfig,
)
from opsiq_runtime.domain.primitives.shopper_item_affinity_score.evaluator import (
    evaluate_shopper_item_affinity_score,
)
from opsiq_runtime.domain.primitives.shopper_item_affinity_score.model import (
    ShopperItemAffinityInput,
)
from opsiq_runtime.domain.primitives.shopper_item_affinity_score import rules


def make_input(
    top_affinity_items: list[dict] | None = None,
    lookback_days: int | None = None,
    top_k: int | None = None,
    subject_id: str = "s1",
) -> ShopperItemAffinityInput:
    as_of_ts = datetime(2024, 1, 10, 12, 0, 0, tzinfo=timezone.utc)
    return ShopperItemAffinityInput.new(
        tenant_id="t1",
        subject_id=subject_id,
        as_of_ts=as_of_ts,
        config_version="cfg",
        canonical_version="v1",
        top_affinity_items=top_affinity_items,
        lookback_days=lookback_days,
        top_k=top_k,
    )


def test_computed_when_has_items():
    """Test COMPUTED when top_affinity_items is non-empty."""
    cfg = ShopperItemAffinityConfig()
    top_items = [
        {
            "rank": 1,
            "item_group_id": "item_001",
            "affinity_score": 0.95,
            "trip_count": 10,
            "days_since_last_purchase": 5,
            "total_sales": 150.0,
            "gtin_sample": "1234567890123",
            "linkcode_sample": "LINK001",
            "category": "Dairy",
            "brand": "Brand A",
            "item_name": "Milk",
            "image_url": "https://example.com/milk.jpg",
        }
    ]
    res = evaluate_shopper_item_affinity_score(make_input(top_affinity_items=top_items), cfg)

    assert res.decision.state == rules.COMPUTED
    assert res.decision.confidence == CONFIDENCE_HIGH
    assert rules.DRIVER_TOP_AFFINITY_COMPUTED in res.decision.drivers
    assert rules.RULE_COMPUTED_HAS_ITEMS in res.evidence_set.evidence[0].rule_ids


def test_unknown_when_empty_items():
    """Test UNKNOWN when top_affinity_items is empty list."""
    cfg = ShopperItemAffinityConfig()
    res = evaluate_shopper_item_affinity_score(make_input(top_affinity_items=[]), cfg)

    assert res.decision.state == rules.UNKNOWN
    assert res.decision.confidence == CONFIDENCE_LOW
    assert rules.DRIVER_NO_AFFINITY_ITEMS in res.decision.drivers
    assert rules.RULE_UNKNOWN_NO_ITEMS in res.evidence_set.evidence[0].rule_ids


def test_unknown_when_null_items():
    """Test UNKNOWN when top_affinity_items is None."""
    cfg = ShopperItemAffinityConfig()
    res = evaluate_shopper_item_affinity_score(make_input(top_affinity_items=None), cfg)

    assert res.decision.state == rules.UNKNOWN
    assert res.decision.confidence == CONFIDENCE_LOW
    assert rules.DRIVER_NO_AFFINITY_ITEMS in res.decision.drivers
    assert rules.RULE_UNKNOWN_NO_ITEMS in res.evidence_set.evidence[0].rule_ids


def test_evidence_id_format():
    """Test that evidence ID follows the correct format."""
    cfg = ShopperItemAffinityConfig()
    top_items = [{"rank": 1, "item_group_id": "item_001", "affinity_score": 0.95}]
    res = evaluate_shopper_item_affinity_score(
        make_input(top_affinity_items=top_items, subject_id="shopper_123"), cfg
    )

    evidence = res.evidence_set.evidence[0]
    assert evidence.evidence_id == "evidence-shopper_123-affinity-v1"
    assert evidence.evidence_id in res.decision.evidence_refs


def test_metrics_json_structure_with_items():
    """Test that metrics_json has correct structure when items are present."""
    cfg = ShopperItemAffinityConfig()
    top_items = [
        {
            "rank": 1,
            "item_group_id": "item_001",
            "affinity_score": 0.85,
            "trip_count": 8,
            "days_since_last_purchase": 3,
            "total_sales": 120.50,
            "gtin_sample": "GTIN001",
            "linkcode_sample": "LINK001",
            "category": "Produce",
            "brand": "Brand B",
            "item_name": "Apples",
            "image_url": "https://example.com/apples.jpg",
        },
        {
            "rank": 2,
            "item_group_id": "item_002",
            "affinity_score": 0.75,
            "trip_count": 5,
            "days_since_last_purchase": 7,
            "total_sales": 80.25,
            "gtin_sample": None,
            "linkcode_sample": None,
            "category": None,
            "brand": None,
            "item_name": None,
            "image_url": None,
        },
    ]
    as_of_ts = datetime(2024, 1, 10, 12, 0, 0, tzinfo=timezone.utc)
    input_row = make_input(top_affinity_items=top_items, lookback_days=60, top_k=20)
    res = evaluate_shopper_item_affinity_score(input_row, cfg)

    metrics = res.decision.metrics
    assert "lookback_days" in metrics
    assert "top_k" in metrics
    assert "as_of_ts" in metrics
    assert "top_items" in metrics

    assert metrics["lookback_days"] == 60  # From input row
    assert metrics["top_k"] == 20  # From input row
    assert metrics["as_of_ts"] == as_of_ts.isoformat()
    assert isinstance(metrics["top_items"], list)
    assert len(metrics["top_items"]) == 2

    # Check first item structure
    item1 = metrics["top_items"][0]
    assert item1["rank"] == 1
    assert item1["item_group_id"] == "item_001"
    assert item1["affinity_score"] == 0.85
    assert item1["trip_count"] == 8
    assert item1["days_since_last_purchase"] == 3
    assert item1["total_sales"] == 120.50
    assert item1["gtin_sample"] == "GTIN001"
    assert item1["linkcode_sample"] == "LINK001"
    assert item1["category"] == "Produce"
    assert item1["brand"] == "Brand B"
    assert item1["item_name"] == "Apples"
    assert item1["image_url"] == "https://example.com/apples.jpg"

    # Check second item with null values
    item2 = metrics["top_items"][1]
    assert item2["rank"] == 2
    assert item2["gtin_sample"] is None
    assert item2["linkcode_sample"] is None
    assert item2["category"] is None
    assert item2["brand"] is None
    assert item2["item_name"] is None
    assert item2["image_url"] is None


def test_metrics_json_uses_config_defaults():
    """Test that metrics_json uses config defaults when input row doesn't provide values."""
    cfg = ShopperItemAffinityConfig(lookback_days=90, top_k=50)
    top_items = [{"rank": 1, "item_group_id": "item_001", "affinity_score": 0.95}]
    res = evaluate_shopper_item_affinity_score(make_input(top_affinity_items=top_items), cfg)

    metrics = res.decision.metrics
    assert metrics["lookback_days"] == 90  # From config default
    assert metrics["top_k"] == 50  # From config default


def test_evidence_json_contains_source_table():
    """Test that evidence references contain source_table and source_as_of_ts."""
    cfg = ShopperItemAffinityConfig()
    as_of_ts = datetime(2024, 1, 10, 12, 0, 0, tzinfo=timezone.utc)
    top_items = [{"rank": 1, "item_group_id": "item_001", "affinity_score": 0.95}]
    input_row = make_input(top_affinity_items=top_items)
    res = evaluate_shopper_item_affinity_score(input_row, cfg)

    evidence = res.evidence_set.evidence[0]
    references = evidence.references

    assert "source_table" in references
    assert references["source_table"] == "opsiq_dev.gold.gold_feature_shopper_top_affinity_v1"
    assert "source_as_of_ts" in references
    assert references["source_as_of_ts"] == as_of_ts.isoformat()


def test_multiple_items_preserved():
    """Test that multiple items in top_affinity_items are all preserved in metrics."""
    cfg = ShopperItemAffinityConfig()
    top_items = [
        {"rank": i, "item_group_id": f"item_{i:03d}", "affinity_score": 0.9 - i * 0.1}
        for i in range(1, 6)
    ]
    res = evaluate_shopper_item_affinity_score(make_input(top_affinity_items=top_items), cfg)

    metrics = res.decision.metrics
    assert len(metrics["top_items"]) == 5
    for i, item in enumerate(metrics["top_items"], start=1):
        assert item["rank"] == i
        assert item["item_group_id"] == f"item_{i:03d}"


def test_empty_items_metrics_structure():
    """Test metrics structure when items are empty."""
    cfg = ShopperItemAffinityConfig()
    res = evaluate_shopper_item_affinity_score(make_input(top_affinity_items=[]), cfg)

    metrics = res.decision.metrics
    assert "lookback_days" in metrics
    assert "top_k" in metrics
    assert "as_of_ts" in metrics
    assert "top_items" in metrics
    assert isinstance(metrics["top_items"], list)
    assert len(metrics["top_items"]) == 0
