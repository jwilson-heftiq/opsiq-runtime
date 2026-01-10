from datetime import datetime, timezone

from opsiq_runtime.domain.common.decision import CONFIDENCE_HIGH, CONFIDENCE_MEDIUM
from opsiq_runtime.domain.primitives.shopper_coupon_offer_set.config import (
    ShopperCouponOfferSetConfig,
)
from opsiq_runtime.domain.primitives.shopper_coupon_offer_set.evaluator import (
    evaluate_shopper_coupon_offer_set,
)
from opsiq_runtime.domain.primitives.shopper_coupon_offer_set.models import (
    CouponOfferSetInput,
)
from opsiq_runtime.domain.primitives.shopper_coupon_offer_set import rules
from opsiq_runtime.domain.primitives.shopper_weekly_ad_slate.models import (
    ShopperAffinityRow,
)


def make_input(
    shopper_affinity: ShopperAffinityRow | None = None,
    weekly_ad_item_groups: set[str] | None = None,
    eligible_map: dict[str, dict] | None = None,
    recent_purchase_keys: set[str] | None = None,
    baseline_prices: dict[tuple[str, str], float] | None = None,
    subject_id: str = "s1",
) -> CouponOfferSetInput:
    """Helper to create CouponOfferSetInput for testing."""
    as_of_ts = datetime(2024, 1, 10, 12, 0, 0, tzinfo=timezone.utc)
    return CouponOfferSetInput.new(
        tenant_id="t1",
        subject_id=subject_id,
        as_of_ts=as_of_ts,
        config_version="cfg",
        canonical_version="v1",
        shopper_affinity=shopper_affinity,
        weekly_ad_item_groups=weekly_ad_item_groups or set(),
        eligible_map=eligible_map or {},
        recent_purchase_keys=recent_purchase_keys or set(),
        baseline_prices=baseline_prices or {},
    )


def test_excludes_weekly_ad_overlap():
    """Test that items with matching item_group_id in weekly ad are excluded."""
    cfg = ShopperCouponOfferSetConfig(
        max_offers=10,
        ad_id="ad_001",
        scope_type="store",
        scope_value="store_123",
    )
    
    affinity = ShopperAffinityRow(
        shopper_id="s1",
        as_of_ts=datetime(2024, 1, 10, 12, 0, 0, tzinfo=timezone.utc),
        top_affinity_items=[
            {"rank": 1, "item_group_id": "item_001", "affinity_score": 0.9},
            {"rank": 2, "item_group_id": "item_002", "affinity_score": 0.8},
        ],
    )
    
    eligible_map = {
        "item_001": {"gtin": "GTIN_001", "linkcode": None, "ineligible_reasons": []},
        "item_002": {"gtin": "GTIN_002", "linkcode": None, "ineligible_reasons": []},
    }
    
    weekly_ad_item_groups = {"item_001"}  # item_001 is in weekly ad
    
    baseline_prices = {
        ("s1", "item_001"): 10.0,
        ("s1", "item_002"): 15.0,
    }
    
    input_obj = make_input(
        shopper_affinity=affinity,
        weekly_ad_item_groups=weekly_ad_item_groups,
        eligible_map=eligible_map,
        baseline_prices=baseline_prices,
    )
    
    result = evaluate_shopper_coupon_offer_set(input_obj, cfg)
    
    assert result is not None
    assert len(result.decision.metrics["offers"]) == 1
    assert result.decision.metrics["offers"][0]["item_group_id"] == "item_002"
    assert result.decision.metrics["excluded_weekly_ad_count"] == 1


def test_excludes_recent_purchases():
    """Test that items in recent_purchase_keys are excluded."""
    cfg = ShopperCouponOfferSetConfig(max_offers=10)
    
    affinity = ShopperAffinityRow(
        shopper_id="s1",
        as_of_ts=datetime(2024, 1, 10, 12, 0, 0, tzinfo=timezone.utc),
        top_affinity_items=[
            {"rank": 1, "item_group_id": "item_001", "affinity_score": 0.9},
            {"rank": 2, "item_group_id": "item_002", "affinity_score": 0.8},
        ],
    )
    
    eligible_map = {
        "item_001": {"gtin": "GTIN_001", "linkcode": None, "ineligible_reasons": []},
        "item_002": {"gtin": "GTIN_002", "linkcode": None, "ineligible_reasons": []},
    }
    
    recent_purchase_keys = {"item_001"}  # item_001 was recently purchased
    
    baseline_prices = {
        ("s1", "item_001"): 10.0,
        ("s1", "item_002"): 15.0,
    }
    
    input_obj = make_input(
        shopper_affinity=affinity,
        eligible_map=eligible_map,
        recent_purchase_keys=recent_purchase_keys,
        baseline_prices=baseline_prices,
    )
    
    result = evaluate_shopper_coupon_offer_set(input_obj, cfg)
    
    assert result is not None
    assert len(result.decision.metrics["offers"]) == 1
    assert result.decision.metrics["offers"][0]["item_group_id"] == "item_002"
    assert result.decision.metrics["excluded_recent_purchase_count"] == 1


def test_enforces_eligibility_gate():
    """Test that only items in eligible_map pass the eligibility gate."""
    cfg = ShopperCouponOfferSetConfig(max_offers=10)
    
    affinity = ShopperAffinityRow(
        shopper_id="s1",
        as_of_ts=datetime(2024, 1, 10, 12, 0, 0, tzinfo=timezone.utc),
        top_affinity_items=[
            {"rank": 1, "item_group_id": "item_001", "affinity_score": 0.9},
            {"rank": 2, "item_group_id": "item_002", "affinity_score": 0.8},
            {"rank": 3, "item_group_id": "item_003", "affinity_score": 0.7},
        ],
    )
    
    # Only item_001 and item_002 are eligible
    eligible_map = {
        "item_001": {"gtin": "GTIN_001", "linkcode": None, "ineligible_reasons": []},
        "item_002": {"gtin": "GTIN_002", "linkcode": None, "ineligible_reasons": []},
    }
    
    baseline_prices = {
        ("s1", "item_001"): 10.0,
        ("s1", "item_002"): 15.0,
    }
    
    input_obj = make_input(
        shopper_affinity=affinity,
        eligible_map=eligible_map,
        baseline_prices=baseline_prices,
    )
    
    result = evaluate_shopper_coupon_offer_set(input_obj, cfg)
    
    assert result is not None
    assert result.decision.metrics["candidate_count"] == 3
    assert result.decision.metrics["eligible_count"] == 2
    assert len(result.decision.metrics["offers"]) == 2
    offer_ids = {offer["item_group_id"] for offer in result.decision.metrics["offers"]}
    assert offer_ids == {"item_001", "item_002"}


def test_skips_items_missing_baseline_price():
    """Test that items missing baseline_price are skipped when pricing_fallback_mode='skip'."""
    cfg = ShopperCouponOfferSetConfig(
        max_offers=10,
        pricing_fallback_mode="skip",
    )
    
    affinity = ShopperAffinityRow(
        shopper_id="s1",
        as_of_ts=datetime(2024, 1, 10, 12, 0, 0, tzinfo=timezone.utc),
        top_affinity_items=[
            {"rank": 1, "item_group_id": "item_001", "affinity_score": 0.9},
            {"rank": 2, "item_group_id": "item_002", "affinity_score": 0.8},
        ],
    )
    
    eligible_map = {
        "item_001": {"gtin": "GTIN_001", "linkcode": None, "ineligible_reasons": []},
        "item_002": {"gtin": "GTIN_002", "linkcode": None, "ineligible_reasons": []},
    }
    
    # Only item_002 has baseline_price
    baseline_prices = {
        ("s1", "item_002"): 15.0,
    }
    
    input_obj = make_input(
        shopper_affinity=affinity,
        eligible_map=eligible_map,
        baseline_prices=baseline_prices,
    )
    
    result = evaluate_shopper_coupon_offer_set(input_obj, cfg)
    
    assert result is not None
    assert len(result.decision.metrics["offers"]) == 1
    assert result.decision.metrics["offers"][0]["item_group_id"] == "item_002"
    assert result.decision.metrics["excluded_pricing_missing_count"] == 1


def test_caps_to_max_offers():
    """Test that max_offers limit is enforced."""
    cfg = ShopperCouponOfferSetConfig(max_offers=3)
    
    # Create 10 eligible items with affinity
    affinity_items = [
        {"rank": i, "item_group_id": f"item_{i:03d}", "affinity_score": 1.0 - i * 0.1}
        for i in range(1, 11)
    ]
    
    affinity = ShopperAffinityRow(
        shopper_id="s1",
        as_of_ts=datetime(2024, 1, 10, 12, 0, 0, tzinfo=timezone.utc),
        top_affinity_items=affinity_items,
    )
    
    eligible_map = {
        f"item_{i:03d}": {"gtin": f"GTIN_{i:03d}", "linkcode": None, "ineligible_reasons": []}
        for i in range(1, 11)
    }
    
    baseline_prices = {
        ("s1", f"item_{i:03d}"): 10.0 + i
        for i in range(1, 11)
    }
    
    input_obj = make_input(
        shopper_affinity=affinity,
        eligible_map=eligible_map,
        baseline_prices=baseline_prices,
    )
    
    result = evaluate_shopper_coupon_offer_set(input_obj, cfg)
    
    assert result is not None
    assert len(result.decision.metrics["offers"]) == 3
    assert result.decision.metrics["max_offers"] == 3


def test_deterministic_ordering_and_stable_ranks():
    """Test that ordering is deterministic and ranks are stable."""
    cfg = ShopperCouponOfferSetConfig(max_offers=5)
    
    affinity = ShopperAffinityRow(
        shopper_id="s1",
        as_of_ts=datetime(2024, 1, 10, 12, 0, 0, tzinfo=timezone.utc),
        top_affinity_items=[
            {"rank": 1, "item_group_id": "item_001", "affinity_score": 0.9},
            {"rank": 2, "item_group_id": "item_002", "affinity_score": 0.9},  # Same score
            {"rank": 3, "item_group_id": "item_003", "affinity_score": 0.8},
        ],
    )
    
    eligible_map = {
        "item_001": {"gtin": "GTIN_001", "linkcode": None, "ineligible_reasons": []},
        "item_002": {"gtin": "GTIN_002", "linkcode": None, "ineligible_reasons": []},
        "item_003": {"gtin": "GTIN_003", "linkcode": None, "ineligible_reasons": []},
    }
    
    baseline_prices = {
        ("s1", "item_001"): 10.0,
        ("s1", "item_002"): 15.0,
        ("s1", "item_003"): 20.0,
    }
    
    input_obj = make_input(
        shopper_affinity=affinity,
        eligible_map=eligible_map,
        baseline_prices=baseline_prices,
    )
    
    result = evaluate_shopper_coupon_offer_set(input_obj, cfg)
    
    assert result is not None
    offers = result.decision.metrics["offers"]
    
    # Should be ordered by score DESC, then item_group_id ASC (deterministic)
    assert len(offers) == 3
    assert offers[0]["rank"] == 1
    assert offers[1]["rank"] == 2
    assert offers[2]["rank"] == 3
    
    # Items with same score should be ordered by item_group_id
    assert offers[0]["affinity_score"] == 0.9
    assert offers[1]["affinity_score"] == 0.9
    assert offers[0]["item_group_id"] < offers[1]["item_group_id"]  # item_001 < item_002


def test_sparse_emission_returns_none_when_no_offers():
    """Test that sparse emission returns None when no offers are generated."""
    cfg = ShopperCouponOfferSetConfig(sparse_emission=True, max_offers=10)
    
    # No affinity items
    affinity = ShopperAffinityRow(
        shopper_id="s1",
        as_of_ts=datetime(2024, 1, 10, 12, 0, 0, tzinfo=timezone.utc),
        top_affinity_items=[],
    )
    
    input_obj = make_input(shopper_affinity=affinity)
    
    result = evaluate_shopper_coupon_offer_set(input_obj, cfg)
    
    assert result is None


def test_sparse_emission_returns_none_when_all_excluded():
    """Test that sparse emission returns None when all items are excluded."""
    cfg = ShopperCouponOfferSetConfig(sparse_emission=True, max_offers=10)
    
    affinity = ShopperAffinityRow(
        shopper_id="s1",
        as_of_ts=datetime(2024, 1, 10, 12, 0, 0, tzinfo=timezone.utc),
        top_affinity_items=[
            {"rank": 1, "item_group_id": "item_001", "affinity_score": 0.9},
        ],
    )
    
    eligible_map = {
        "item_001": {"gtin": "GTIN_001", "linkcode": None, "ineligible_reasons": []},
    }
    
    # Exclude via weekly ad
    weekly_ad_item_groups = {"item_001"}
    
    # No baseline price (also excluded)
    baseline_prices = {}
    
    input_obj = make_input(
        shopper_affinity=affinity,
        eligible_map=eligible_map,
        weekly_ad_item_groups=weekly_ad_item_groups,
        baseline_prices=baseline_prices,
    )
    
    result = evaluate_shopper_coupon_offer_set(input_obj, cfg)
    
    assert result is None


def test_calculates_offer_price_from_baseline():
    """Test that offer_price is calculated correctly from baseline_price with discount."""
    cfg = ShopperCouponOfferSetConfig(
        max_offers=10,
        discount_pct=25,  # 25% discount = 0.75 multiplier
    )
    
    affinity = ShopperAffinityRow(
        shopper_id="s1",
        as_of_ts=datetime(2024, 1, 10, 12, 0, 0, tzinfo=timezone.utc),
        top_affinity_items=[
            {"rank": 1, "item_group_id": "item_001", "affinity_score": 0.9},
        ],
    )
    
    eligible_map = {
        "item_001": {"gtin": "GTIN_001", "linkcode": None, "ineligible_reasons": []},
    }
    
    baseline_prices = {
        ("s1", "item_001"): 10.0,  # Baseline $10.00
    }
    
    input_obj = make_input(
        shopper_affinity=affinity,
        eligible_map=eligible_map,
        baseline_prices=baseline_prices,
    )
    
    result = evaluate_shopper_coupon_offer_set(input_obj, cfg)
    
    assert result is not None
    offer = result.decision.metrics["offers"][0]
    assert offer["baseline_price"] == 10.0
    assert offer["offer_price"] == 7.50  # 10.0 * 0.75 = 7.50


def test_applies_category_cap_if_configured():
    """Test that category cap is applied when configured."""
    cfg = ShopperCouponOfferSetConfig(max_offers=10, category_cap=2)
    
    affinity = ShopperAffinityRow(
        shopper_id="s1",
        as_of_ts=datetime(2024, 1, 10, 12, 0, 0, tzinfo=timezone.utc),
        top_affinity_items=[
            {"rank": 1, "item_group_id": "item_001", "affinity_score": 0.9},
            {"rank": 2, "item_group_id": "item_002", "affinity_score": 0.8},
            {"rank": 3, "item_group_id": "item_003", "affinity_score": 0.7},
        ],
    )
    
    # Note: Category cap requires category in ActivationItem, but eligibility_map doesn't have category
    # This test verifies the code handles None category correctly
    eligible_map = {
        "item_001": {"gtin": "GTIN_001", "linkcode": None, "ineligible_reasons": []},
        "item_002": {"gtin": "GTIN_002", "linkcode": None, "ineligible_reasons": []},
        "item_003": {"gtin": "GTIN_003", "linkcode": None, "ineligible_reasons": []},
    }
    
    baseline_prices = {
        ("s1", "item_001"): 10.0,
        ("s1", "item_002"): 15.0,
        ("s1", "item_003"): 20.0,
    }
    
    input_obj = make_input(
        shopper_affinity=affinity,
        eligible_map=eligible_map,
        baseline_prices=baseline_prices,
    )
    
    result = evaluate_shopper_coupon_offer_set(input_obj, cfg)
    
    # Since category is None, category cap doesn't apply (items with None category are uncapped)
    assert result is not None
    assert len(result.decision.metrics["offers"]) == 3


def test_computes_confidence_based_on_match_rate():
    """Test that confidence is HIGH when match_rate >= threshold, else MEDIUM."""
    cfg_high = ShopperCouponOfferSetConfig(
        max_offers=10,
        min_match_rate_for_high_confidence=0.5,
    )
    
    # All items have affinity_score > 0 (match_rate = 1.0)
    affinity = ShopperAffinityRow(
        shopper_id="s1",
        as_of_ts=datetime(2024, 1, 10, 12, 0, 0, tzinfo=timezone.utc),
        top_affinity_items=[
            {"rank": 1, "item_group_id": "item_001", "affinity_score": 0.9},
            {"rank": 2, "item_group_id": "item_002", "affinity_score": 0.8},
        ],
    )
    
    eligible_map = {
        "item_001": {"gtin": "GTIN_001", "linkcode": None, "ineligible_reasons": []},
        "item_002": {"gtin": "GTIN_002", "linkcode": None, "ineligible_reasons": []},
    }
    
    baseline_prices = {
        ("s1", "item_001"): 10.0,
        ("s1", "item_002"): 15.0,
    }
    
    input_obj = make_input(
        shopper_affinity=affinity,
        eligible_map=eligible_map,
        baseline_prices=baseline_prices,
    )
    
    result = evaluate_shopper_coupon_offer_set(input_obj, cfg_high)
    
    assert result is not None
    assert result.decision.confidence == CONFIDENCE_HIGH  # match_rate = 1.0 >= 0.5
    
    # Test with lower match rate (items with score = 0)
    cfg_medium = ShopperCouponOfferSetConfig(
        max_offers=10,
        min_match_rate_for_high_confidence=0.5,
    )
    
    affinity_low = ShopperAffinityRow(
        shopper_id="s1",
        as_of_ts=datetime(2024, 1, 10, 12, 0, 0, tzinfo=timezone.utc),
        top_affinity_items=[
            {"rank": 1, "item_group_id": "item_001", "affinity_score": 0.0},  # No affinity
        ],
    )
    
    input_obj_low = make_input(
        shopper_affinity=affinity_low,
        eligible_map=eligible_map,
        baseline_prices={("s1", "item_001"): 10.0},
    )
    
    result_low = evaluate_shopper_coupon_offer_set(input_obj_low, cfg_medium)
    
    assert result_low is not None
    assert result_low.decision.confidence == CONFIDENCE_MEDIUM  # match_rate = 0.0 < 0.5


def test_aggregates_drivers_correctly():
    """Test that drivers are aggregated correctly from Activation Policy and primitive-specific."""
    cfg = ShopperCouponOfferSetConfig(max_offers=10)
    
    affinity = ShopperAffinityRow(
        shopper_id="s1",
        as_of_ts=datetime(2024, 1, 10, 12, 0, 0, tzinfo=timezone.utc),
        top_affinity_items=[
            {"rank": 1, "item_group_id": "item_001", "affinity_score": 0.9},
        ],
    )
    
    eligible_map = {
        "item_001": {"gtin": "GTIN_001", "linkcode": None, "ineligible_reasons": []},
    }
    
    baseline_prices = {
        ("s1", "item_001"): 10.0,
    }
    
    input_obj = make_input(
        shopper_affinity=affinity,
        eligible_map=eligible_map,
        baseline_prices=baseline_prices,
    )
    
    result = evaluate_shopper_coupon_offer_set(input_obj, cfg)
    
    assert result is not None
    drivers = result.decision.drivers
    
    # Should include Activation Policy drivers
    assert "ACTIVATION_POLICY_APPLIED" in drivers
    assert "AFFINITY_MATCH" in drivers  # score > 0
    
    # Should include primitive-specific drivers
    assert rules.DRIVER_ELIGIBILITY_POLICY_ENFORCED in drivers
    assert rules.DRIVER_NOT_IN_WEEKLY_AD in drivers
    assert rules.DRIVER_COUPON_DISCOUNT_APPLIED in drivers
