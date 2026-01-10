from opsiq_runtime.domain.activation_policy.identity import build_activation_item
from opsiq_runtime.domain.activation_policy.selection import (
    apply_category_cap,
    apply_max_items,
)


def test_apply_max_items_truncates():
    """Test that apply_max_items truncates to first max_items."""
    items = [
        build_activation_item(linkcode="LINK001", score=0.9),
        build_activation_item(linkcode="LINK002", score=0.8),
        build_activation_item(linkcode="LINK003", score=0.7),
        build_activation_item(linkcode="LINK004", score=0.6),
    ]
    
    result = apply_max_items(items, max_items=2)
    
    assert len(result) == 2
    assert result[0].item_group_id == "LINK001"
    assert result[1].item_group_id == "LINK002"


def test_apply_max_items_preserves_order():
    """Test that apply_max_items preserves original order."""
    items = [
        build_activation_item(linkcode="LINK003", score=0.7),
        build_activation_item(linkcode="LINK001", score=0.9),
        build_activation_item(linkcode="LINK002", score=0.8),
    ]
    
    result = apply_max_items(items, max_items=2)
    
    assert len(result) == 2
    assert result[0].item_group_id == "LINK003"  # Original order preserved
    assert result[1].item_group_id == "LINK001"


def test_apply_max_items_returns_all_when_fewer_than_max():
    """Test that all items are returned when fewer than max_items."""
    items = [
        build_activation_item(linkcode="LINK001", score=0.9),
        build_activation_item(linkcode="LINK002", score=0.8),
    ]
    
    result = apply_max_items(items, max_items=5)
    
    assert len(result) == 2
    assert result[0].item_group_id == "LINK001"
    assert result[1].item_group_id == "LINK002"


def test_apply_max_items_zero_max():
    """Test that zero max_items returns empty list."""
    items = [
        build_activation_item(linkcode="LINK001", score=0.9),
        build_activation_item(linkcode="LINK002", score=0.8),
    ]
    
    result = apply_max_items(items, max_items=0)
    assert len(result) == 0


def test_apply_max_items_empty_list():
    """Test that empty list returns empty list."""
    result = apply_max_items([], max_items=5)
    assert result == []


def test_apply_category_cap_enforces_per_category():
    """Test that category cap limits items per category."""
    items = [
        build_activation_item(linkcode="LINK001", category="Dairy", score=0.9),
        build_activation_item(linkcode="LINK002", category="Dairy", score=0.8),
        build_activation_item(linkcode="LINK003", category="Dairy", score=0.7),
        build_activation_item(linkcode="LINK004", category="Produce", score=0.9),
        build_activation_item(linkcode="LINK005", category="Produce", score=0.8),
    ]
    
    result = apply_category_cap(items, cap=2)
    
    # Should have max 2 from Dairy and max 2 from Produce
    dairy_items = [item for item in result if item.category == "Dairy"]
    produce_items = [item for item in result if item.category == "Produce"]
    
    assert len(dairy_items) == 2
    assert len(produce_items) == 2
    assert len(result) == 4


def test_apply_category_cap_preserves_ordering():
    """Test that category cap preserves original ordering."""
    items = [
        build_activation_item(linkcode="LINK001", category="Dairy", score=0.9),
        build_activation_item(linkcode="LINK002", category="Dairy", score=0.8),
        build_activation_item(linkcode="LINK003", category="Dairy", score=0.7),
        build_activation_item(linkcode="LINK004", category="Produce", score=0.9),
    ]
    
    result = apply_category_cap(items, cap=2)
    
    # Should preserve order: first 2 Dairy items, then Produce
    assert result[0].item_group_id == "LINK001"
    assert result[1].item_group_id == "LINK002"
    assert result[2].item_group_id == "LINK004"
    # LINK003 should be excluded (third Dairy item)


def test_apply_category_cap_none_category_uncapped():
    """Test that items with category=None are uncapped."""
    items = [
        build_activation_item(linkcode="LINK001", category=None, score=0.9),
        build_activation_item(linkcode="LINK002", category=None, score=0.8),
        build_activation_item(linkcode="LINK003", category=None, score=0.7),
        build_activation_item(linkcode="LINK004", category="Dairy", score=0.9),
        build_activation_item(linkcode="LINK005", category="Dairy", score=0.8),
        build_activation_item(linkcode="LINK006", category="Dairy", score=0.7),
    ]
    
    result = apply_category_cap(items, cap=2)
    
    # All None category items should be included (uncapped)
    none_category_items = [item for item in result if item.category is None]
    dairy_items = [item for item in result if item.category == "Dairy"]
    
    assert len(none_category_items) == 3  # All included
    assert len(dairy_items) == 2  # Capped at 2


def test_apply_category_cap_all_none_category():
    """Test category cap when all items have category=None."""
    items = [
        build_activation_item(linkcode="LINK001", category=None, score=0.9),
        build_activation_item(linkcode="LINK002", category=None, score=0.8),
        build_activation_item(linkcode="LINK003", category=None, score=0.7),
    ]
    
    result = apply_category_cap(items, cap=2)
    
    # All should be included since None category is uncapped
    assert len(result) == 3


def test_apply_category_cap_empty_list():
    """Test that empty list returns empty list."""
    result = apply_category_cap([], cap=2)
    assert result == []


def test_apply_category_cap_zero_cap():
    """Test that zero cap excludes all items with categories (but None category still included)."""
    items = [
        build_activation_item(linkcode="LINK001", category="Dairy", score=0.9),
        build_activation_item(linkcode="LINK002", category="Produce", score=0.8),
        build_activation_item(linkcode="LINK003", category=None, score=0.7),
    ]
    
    result = apply_category_cap(items, cap=0)
    
    # Only None category items should be included
    assert len(result) == 1
    assert result[0].item_group_id == "LINK003"
    assert result[0].category is None


def test_apply_category_cap_multiple_categories():
    """Test category cap with multiple categories."""
    items = [
        build_activation_item(linkcode="LINK001", category="Dairy", score=0.9),
        build_activation_item(linkcode="LINK002", category="Dairy", score=0.8),
        build_activation_item(linkcode="LINK003", category="Produce", score=0.9),
        build_activation_item(linkcode="LINK004", category="Produce", score=0.8),
        build_activation_item(linkcode="LINK005", category="Meat", score=0.9),
        build_activation_item(linkcode="LINK006", category="Meat", score=0.8),
        build_activation_item(linkcode="LINK007", category="Meat", score=0.7),
    ]
    
    result = apply_category_cap(items, cap=2)
    
    # Should have max 2 from each category
    dairy_items = [item for item in result if item.category == "Dairy"]
    produce_items = [item for item in result if item.category == "Produce"]
    meat_items = [item for item in result if item.category == "Meat"]
    
    assert len(dairy_items) == 2
    assert len(produce_items) == 2
    assert len(meat_items) == 2
    assert len(result) == 6
