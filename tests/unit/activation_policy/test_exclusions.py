from opsiq_runtime.domain.activation_policy.exclusions import (
    apply_exclusions,
    exclude_if_in_set,
    exclude_if_recent_purchase,
)
from opsiq_runtime.domain.activation_policy.identity import build_activation_item


def test_exclude_if_in_set_excludes_when_in_set():
    """Test that item is excluded when item_group_id is in excluded set."""
    item = build_activation_item(linkcode="LINK001", score=0.9)
    excluded_set = {"LINK001", "LINK002"}
    
    result = exclude_if_in_set(item, excluded_set, "WEEKLY_AD_OVERLAP_EXCLUSION")
    
    assert result.excluded is True
    assert "WEEKLY_AD_OVERLAP_EXCLUSION" in result.reasons


def test_exclude_if_in_set_not_excluded_when_not_in_set():
    """Test that item is not excluded when item_group_id is not in excluded set."""
    item = build_activation_item(linkcode="LINK003", score=0.9)
    excluded_set = {"LINK001", "LINK002"}
    
    result = exclude_if_in_set(item, excluded_set)
    
    assert result.excluded is False
    assert result.reasons == []


def test_exclude_if_recent_purchase_excludes_when_in_set():
    """Test that item is excluded when in recent purchase set."""
    item = build_activation_item(linkcode="LINK001", score=0.9)
    recent_purchases = {"LINK001"}
    
    result = exclude_if_recent_purchase(item, recent_purchases)
    
    assert result.excluded is True
    assert "RECENT_PURCHASE_EXCLUSION" in result.reasons


def test_exclude_if_recent_purchase_not_excluded_when_not_in_set():
    """Test that item is not excluded when not in recent purchase set."""
    item = build_activation_item(linkcode="LINK003", score=0.9)
    recent_purchases = {"LINK001", "LINK002"}
    
    result = exclude_if_recent_purchase(item, recent_purchases)
    
    assert result.excluded is False
    assert result.reasons == []


def test_exclude_if_in_set_custom_reason():
    """Test that custom exclusion reason can be provided."""
    item = build_activation_item(linkcode="LINK001")
    excluded_set = {"LINK001"}
    
    result = exclude_if_in_set(item, excluded_set, "CUSTOM_EXCLUSION")
    
    assert result.excluded is True
    assert result.reasons == ["CUSTOM_EXCLUSION"]


def test_apply_exclusions_single_check():
    """Test applying a single exclusion check."""
    items = [
        build_activation_item(linkcode="LINK001", score=0.9),
        build_activation_item(linkcode="LINK002", score=0.8),
        build_activation_item(linkcode="LINK003", score=0.7),
    ]
    excluded_set = {"LINK002"}
    
    def check(item):
        return exclude_if_in_set(item, excluded_set)
    
    eligible, excluded, reason_counts = apply_exclusions(items, [check])
    
    assert len(eligible) == 2
    assert eligible[0].item_group_id == "LINK001"
    assert eligible[1].item_group_id == "LINK003"
    
    assert len(excluded) == 1
    assert excluded[0].item_group_id == "LINK002"
    assert "WEEKLY_AD_OVERLAP_EXCLUSION" in excluded[0].metadata["excluded_reasons"]
    
    assert reason_counts["WEEKLY_AD_OVERLAP_EXCLUSION"] == 1


def test_apply_exclusions_multiple_checks():
    """Test applying multiple exclusion checks."""
    items = [
        build_activation_item(linkcode="LINK001", score=0.9),
        build_activation_item(linkcode="LINK002", score=0.8),
        build_activation_item(linkcode="LINK003", score=0.7),
    ]
    weekly_ad_overlap = {"LINK001"}
    recent_purchases = {"LINK002"}
    
    def check_overlap(item):
        return exclude_if_in_set(item, weekly_ad_overlap, "WEEKLY_AD_OVERLAP_EXCLUSION")
    
    def check_recent(item):
        return exclude_if_recent_purchase(item, recent_purchases)
    
    eligible, excluded, reason_counts = apply_exclusions(items, [check_overlap, check_recent])
    
    assert len(eligible) == 1
    assert eligible[0].item_group_id == "LINK003"
    
    assert len(excluded) == 2
    excluded_ids = {item.item_group_id for item in excluded}
    assert excluded_ids == {"LINK001", "LINK002"}
    
    assert reason_counts["WEEKLY_AD_OVERLAP_EXCLUSION"] == 1
    assert reason_counts["RECENT_PURCHASE_EXCLUSION"] == 1


def test_apply_exclusions_multiple_reasons_for_same_item():
    """Test that an item can be excluded for multiple reasons."""
    item = build_activation_item(linkcode="LINK001", score=0.9)
    excluded_set = {"LINK001"}
    recent_purchases = {"LINK001"}
    
    def check_overlap(item):
        return exclude_if_in_set(item, excluded_set, "WEEKLY_AD_OVERLAP_EXCLUSION")
    
    def check_recent(item):
        return exclude_if_recent_purchase(item, recent_purchases)
    
    eligible, excluded, reason_counts = apply_exclusions([item], [check_overlap, check_recent])
    
    assert len(eligible) == 0
    assert len(excluded) == 1
    assert excluded[0].item_group_id == "LINK001"
    
    excluded_reasons = excluded[0].metadata["excluded_reasons"]
    assert "WEEKLY_AD_OVERLAP_EXCLUSION" in excluded_reasons
    assert "RECENT_PURCHASE_EXCLUSION" in excluded_reasons
    
    assert reason_counts["WEEKLY_AD_OVERLAP_EXCLUSION"] == 1
    assert reason_counts["RECENT_PURCHASE_EXCLUSION"] == 1


def test_apply_exclusions_empty_items():
    """Test applying exclusions to empty list."""
    eligible, excluded, reason_counts = apply_exclusions([], [])
    
    assert len(eligible) == 0
    assert len(excluded) == 0
    assert reason_counts == {}


def test_apply_exclusions_no_checks():
    """Test applying no exclusion checks (all items eligible)."""
    items = [
        build_activation_item(linkcode="LINK001", score=0.9),
        build_activation_item(linkcode="LINK002", score=0.8),
    ]
    
    eligible, excluded, reason_counts = apply_exclusions(items, [])
    
    assert len(eligible) == 2
    assert len(excluded) == 0
    assert reason_counts == {}


def test_apply_exclusions_preserves_item_attributes():
    """Test that excluded items preserve original attributes except metadata."""
    item = build_activation_item(
        linkcode="LINK001",
        gtin="GTIN001",
        category="Dairy",
        score=0.9,
        metadata={"title": "Milk"},
    )
    excluded_set = {"LINK001"}
    
    def check(item):
        return exclude_if_in_set(item, excluded_set)
    
    eligible, excluded, _ = apply_exclusions([item], [check])
    
    assert len(excluded) == 1
    excluded_item = excluded[0]
    assert excluded_item.item_group_id == "LINK001"
    assert excluded_item.gtin == "GTIN001"
    assert excluded_item.category == "Dairy"
    assert excluded_item.score == 0.9
    assert excluded_item.metadata["title"] == "Milk"
    assert "excluded_reasons" in excluded_item.metadata
