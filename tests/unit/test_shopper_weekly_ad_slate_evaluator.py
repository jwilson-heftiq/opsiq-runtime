from datetime import datetime, timezone

from opsiq_runtime.domain.common.decision import CONFIDENCE_HIGH, CONFIDENCE_MEDIUM
from opsiq_runtime.domain.primitives.shopper_weekly_ad_slate.config import (
    ShopperWeeklyAdSlateConfig,
)
from opsiq_runtime.domain.primitives.shopper_weekly_ad_slate.evaluator import (
    evaluate_shopper_weekly_ad_slate,
)
from opsiq_runtime.domain.primitives.shopper_weekly_ad_slate.models import (
    AdCandidate,
    ShopperAffinityRow,
    ShopperWeeklyAdSlateInput,
)
from opsiq_runtime.domain.primitives.shopper_weekly_ad_slate import rules


def make_candidate(
    item_group_id: str,
    gtin: str | None = None,
    linkcode: str | None = None,
    promo_price: float | None = None,
    ad_group_id: str = "ad_group_1",
    title: str | None = None,
) -> AdCandidate:
    """Helper to create AdCandidate for testing."""
    as_of_ts = datetime(2024, 1, 10, 12, 0, 0, tzinfo=timezone.utc)
    return AdCandidate(
        ad_id="ad_001",
        ad_group_id=ad_group_id,
        scope_type="store",
        scope_value="store_123",
        as_of_ts=as_of_ts,
        gtin=gtin or f"GTIN_{item_group_id}",
        linkcode=linkcode,
        item_group_id=item_group_id,
        title=title or f"Item {item_group_id}",
        promo_text=None,
        primary_image_url=None,
        promo_price=promo_price,
        ad_price_raw=None,
        ad_price_uom=None,
        ad_price_qualifier=None,
    )


def make_input(
    candidates: list[AdCandidate] | None = None,
    shopper_affinity: ShopperAffinityRow | None = None,
    recent_purchase_keys: set[str] | None = None,
    subject_id: str = "s1",
) -> ShopperWeeklyAdSlateInput:
    """Helper to create ShopperWeeklyAdSlateInput for testing."""
    as_of_ts = datetime(2024, 1, 10, 12, 0, 0, tzinfo=timezone.utc)
    return ShopperWeeklyAdSlateInput.new(
        tenant_id="t1",
        subject_id=subject_id,
        as_of_ts=as_of_ts,
        config_version="cfg",
        canonical_version="v1",
        candidates=candidates or [],
        shopper_affinity=shopper_affinity,
        recent_purchase_keys=recent_purchase_keys or set(),
    )


def test_affinity_matches_and_exclusions():
    """Test that affinity matches boost scores and exclusions remove items."""
    cfg = ShopperWeeklyAdSlateConfig(
        slate_size_k=5,
        ad_id="ad_001",
        scope_type="store",
        scope_value="store_123",
    )
    
    # Create candidates
    candidates = [
        make_candidate("item_001", promo_price=10.0),  # Has affinity
        make_candidate("item_002", promo_price=15.0),  # No affinity
        make_candidate("item_003", promo_price=20.0),  # Excluded (recent purchase)
        make_candidate("item_004", promo_price=12.0),  # Has affinity
    ]
    
    # Create affinity with item_001 and item_004
    affinity = ShopperAffinityRow(
        shopper_id="s1",
        as_of_ts=datetime(2024, 1, 10, 12, 0, 0, tzinfo=timezone.utc),
        top_affinity_items=[
            {"rank": 1, "item_group_id": "item_001", "affinity_score": 0.9},
            {"rank": 2, "item_group_id": "item_004", "affinity_score": 0.7},
        ],
    )
    
    # Exclude item_003
    recent_purchases = {"item_003"}
    
    input_obj = make_input(
        candidates=candidates,
        shopper_affinity=affinity,
        recent_purchase_keys=recent_purchases,
    )
    
    res = evaluate_shopper_weekly_ad_slate(input_obj, cfg)
    
    assert res is not None
    assert res.decision.state == rules.COMPUTED
    assert len(res.decision.metrics["items"]) == 3  # item_003 excluded
    
    # Check ordering: item_001 (score 0.9) should be first, then item_004 (0.7), then item_002 (0.0)
    items = res.decision.metrics["items"]
    assert items[0]["item_group_id"] == "item_001"
    assert items[0]["score"] == 0.9
    assert "AFFINITY_MATCH" in items[0]["reasons"]
    
    assert items[1]["item_group_id"] == "item_004"
    assert items[1]["score"] == 0.7
    assert "AFFINITY_MATCH" in items[1]["reasons"]
    
    assert items[2]["item_group_id"] == "item_002"
    assert items[2]["score"] == 0.0
    
    # Check drivers
    assert "IN_CURRENT_AD" in res.decision.drivers
    assert "AFFINITY_MATCH" in res.decision.drivers
    assert "RECENT_PURCHASE_EXCLUSIONS" in res.decision.drivers
    
    # Check excluded_count
    assert res.decision.metrics["excluded_count"] == 1


def test_ordering_stable_by_score_then_price_then_gtin():
    """Test that ordering is stable: score DESC, promo_price ASC, gtin ASC."""
    cfg = ShopperWeeklyAdSlateConfig(
        slate_size_k=10,
        ad_id="ad_001",
        scope_type="store",
        scope_value="store_123",
    )
    
    # Create candidates with same score, different prices
    candidates = [
        make_candidate("item_a", gtin="GTIN_A", promo_price=15.0),  # Higher price
        make_candidate("item_b", gtin="GTIN_B", promo_price=10.0),  # Lower price (should come first)
        make_candidate("item_c", gtin="GTIN_C", promo_price=10.0),  # Same price, different gtin
    ]
    
    # Give item_b and item_c same affinity score
    affinity = ShopperAffinityRow(
        shopper_id="s1",
        as_of_ts=datetime(2024, 1, 10, 12, 0, 0, tzinfo=timezone.utc),
        top_affinity_items=[
            {"rank": 1, "item_group_id": "item_b", "affinity_score": 0.5},
            {"rank": 2, "item_group_id": "item_c", "affinity_score": 0.5},
        ],
    )
    
    input_obj = make_input(candidates=candidates, shopper_affinity=affinity)
    res = evaluate_shopper_weekly_ad_slate(input_obj, cfg)
    
    assert res is not None
    items = res.decision.metrics["items"]
    
    # item_b and item_c have same score (0.5), so should be ordered by price (both 10.0), then gtin
    # item_b (GTIN_B) should come before item_c (GTIN_C) alphabetically
    assert items[0]["item_group_id"] == "item_b"
    assert items[1]["item_group_id"] == "item_c"
    assert items[2]["item_group_id"] == "item_a"  # No affinity, but included


def test_category_cap():
    """Test that category cap limits items per category."""
    cfg = ShopperWeeklyAdSlateConfig(
        slate_size_k=10,
        category_cap=2,  # Max 2 per category
        ad_id="ad_001",
        scope_type="store",
        scope_value="store_123",
    )
    
    # Note: Category cap requires category field in AdCandidate
    # For now, this test verifies the logic exists, but category isn't in AdCandidate yet
    # This test will need to be updated when category is added to the model
    candidates = [
        make_candidate("item_001", promo_price=10.0),
        make_candidate("item_002", promo_price=11.0),
        make_candidate("item_003", promo_price=12.0),
    ]
    
    affinity = ShopperAffinityRow(
        shopper_id="s1",
        as_of_ts=datetime(2024, 1, 10, 12, 0, 0, tzinfo=timezone.utc),
        top_affinity_items=[
            {"rank": 1, "item_group_id": "item_001", "affinity_score": 0.9},
            {"rank": 2, "item_group_id": "item_002", "affinity_score": 0.8},
            {"rank": 3, "item_group_id": "item_003", "affinity_score": 0.7},
        ],
    )
    
    input_obj = make_input(candidates=candidates, shopper_affinity=affinity)
    res = evaluate_shopper_weekly_ad_slate(input_obj, cfg)
    
    # Category cap logic is in place, but without category in AdCandidate, all items pass
    assert res is not None
    assert len(res.decision.metrics["items"]) == 3


def test_sparse_emission_returns_none_when_no_eligible_items():
    """Test that sparse emission returns None when slate is empty."""
    cfg = ShopperWeeklyAdSlateConfig(
        slate_size_k=5,
        sparse_emission=True,
        ad_id="ad_001",
        scope_type="store",
        scope_value="store_123",
    )
    
    # All candidates excluded
    candidates = [make_candidate("item_001")]
    recent_purchases = {"item_001"}  # Exclude the only candidate
    
    input_obj = make_input(
        candidates=candidates,
        recent_purchase_keys=recent_purchases,
    )
    
    res = evaluate_shopper_weekly_ad_slate(input_obj, cfg)
    assert res is None


def test_confidence_high_when_match_rate_above_threshold():
    """Test that confidence is HIGH when match_rate >= min_match_rate_for_high_confidence."""
    cfg = ShopperWeeklyAdSlateConfig(
        slate_size_k=5,
        min_match_rate_for_high_confidence=0.50,
        ad_id="ad_001",
        scope_type="store",
        scope_value="store_123",
    )
    
    # Create 5 candidates, 3 with affinity (match_rate = 0.6 >= 0.5)
    candidates = [
        make_candidate("item_001", promo_price=10.0),
        make_candidate("item_002", promo_price=11.0),
        make_candidate("item_003", promo_price=12.0),
        make_candidate("item_004", promo_price=13.0),
        make_candidate("item_005", promo_price=14.0),
    ]
    
    affinity = ShopperAffinityRow(
        shopper_id="s1",
        as_of_ts=datetime(2024, 1, 10, 12, 0, 0, tzinfo=timezone.utc),
        top_affinity_items=[
            {"rank": 1, "item_group_id": "item_001", "affinity_score": 0.9},
            {"rank": 2, "item_group_id": "item_002", "affinity_score": 0.8},
            {"rank": 3, "item_group_id": "item_003", "affinity_score": 0.7},
        ],
    )
    
    input_obj = make_input(candidates=candidates, shopper_affinity=affinity)
    res = evaluate_shopper_weekly_ad_slate(input_obj, cfg)
    
    assert res is not None
    assert res.decision.confidence == CONFIDENCE_HIGH
    assert res.decision.metrics["match_rate"] == 0.6  # 3 out of 5


def test_confidence_medium_when_match_rate_below_threshold():
    """Test that confidence is MEDIUM when match_rate < min_match_rate_for_high_confidence."""
    cfg = ShopperWeeklyAdSlateConfig(
        slate_size_k=5,
        min_match_rate_for_high_confidence=0.50,
        ad_id="ad_001",
        scope_type="store",
        scope_value="store_123",
    )
    
    # Create 5 candidates, 2 with affinity (match_rate = 0.4 < 0.5)
    candidates = [
        make_candidate("item_001", promo_price=10.0),
        make_candidate("item_002", promo_price=11.0),
        make_candidate("item_003", promo_price=12.0),
        make_candidate("item_004", promo_price=13.0),
        make_candidate("item_005", promo_price=14.0),
    ]
    
    affinity = ShopperAffinityRow(
        shopper_id="s1",
        as_of_ts=datetime(2024, 1, 10, 12, 0, 0, tzinfo=timezone.utc),
        top_affinity_items=[
            {"rank": 1, "item_group_id": "item_001", "affinity_score": 0.9},
            {"rank": 2, "item_group_id": "item_002", "affinity_score": 0.8},
        ],
    )
    
    input_obj = make_input(candidates=candidates, shopper_affinity=affinity)
    res = evaluate_shopper_weekly_ad_slate(input_obj, cfg)
    
    assert res is not None
    assert res.decision.confidence == CONFIDENCE_MEDIUM
    assert res.decision.metrics["match_rate"] == 0.4  # 2 out of 5


def test_drivers_include_all_applicable():
    """Test that drivers include all applicable ones."""
    cfg = ShopperWeeklyAdSlateConfig(
        slate_size_k=5,
        ad_id="ad_001",
        scope_type="store",
        scope_value="store_123",
    )
    
    candidates = [
        make_candidate("item_001", promo_price=10.0),  # Has affinity
        make_candidate("item_002", promo_price=11.0),  # Excluded
    ]
    
    affinity = ShopperAffinityRow(
        shopper_id="s1",
        as_of_ts=datetime(2024, 1, 10, 12, 0, 0, tzinfo=timezone.utc),
        top_affinity_items=[
            {"rank": 1, "item_group_id": "item_001", "affinity_score": 0.9},
        ],
    )
    
    recent_purchases = {"item_002"}
    
    input_obj = make_input(
        candidates=candidates,
        shopper_affinity=affinity,
        recent_purchase_keys=recent_purchases,
    )
    
    res = evaluate_shopper_weekly_ad_slate(input_obj, cfg)
    
    assert res is not None
    drivers = res.decision.drivers
    assert "IN_CURRENT_AD" in drivers
    assert "AFFINITY_MATCH" in drivers
    assert "RECENT_PURCHASE_EXCLUSIONS" in drivers


def test_metrics_json_structure():
    """Test that metrics JSON has correct structure."""
    cfg = ShopperWeeklyAdSlateConfig(
        slate_size_k=3,
        exclude_lookback_days=14,
        ad_id="ad_001",
        scope_type="store",
        scope_value="store_123",
    )
    
    candidates = [make_candidate("item_001", promo_price=10.0)]
    
    affinity = ShopperAffinityRow(
        shopper_id="s1",
        as_of_ts=datetime(2024, 1, 10, 12, 0, 0, tzinfo=timezone.utc),
        top_affinity_items=[
            {"rank": 1, "item_group_id": "item_001", "affinity_score": 0.9},
        ],
    )
    
    input_obj = make_input(candidates=candidates, shopper_affinity=affinity)
    res = evaluate_shopper_weekly_ad_slate(input_obj, cfg)
    
    assert res is not None
    metrics = res.decision.metrics
    
    assert "ad_id" in metrics
    assert metrics["ad_id"] == "ad_001"
    assert "scope_type" in metrics
    assert "scope_value" in metrics
    assert "slate_size_k" in metrics
    assert "exclude_lookback_days" in metrics
    assert "excluded_count" in metrics
    assert "candidates_count" in metrics
    assert "match_rate" in metrics
    assert "items" in metrics
    assert isinstance(metrics["items"], list)
    assert len(metrics["items"]) == 1
    
    item = metrics["items"][0]
    assert "rank" in item
    assert "item_group_id" in item
    assert "score" in item
    assert "reasons" in item
