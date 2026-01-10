from opsiq_runtime.domain.activation_policy.identity import build_activation_item
from opsiq_runtime.domain.activation_policy.ordering import (
    compute_match_rate,
    stable_rank,
)


def test_stable_rank_sorts_by_score_desc():
    """Test that items are sorted by score descending."""
    items = [
        build_activation_item(linkcode="LINK001", score=0.5),
        build_activation_item(linkcode="LINK002", score=0.9),
        build_activation_item(linkcode="LINK003", score=0.3),
    ]
    
    ranked = stable_rank(items)
    
    assert ranked[0].score == 0.9
    assert ranked[0].item_group_id == "LINK002"
    assert ranked[1].score == 0.5
    assert ranked[2].score == 0.3


def test_stable_rank_tie_breaker_by_ad_position():
    """Test that items with same score are sorted by ad_position ASC."""
    items = [
        build_activation_item(
            linkcode="LINK001", score=0.5, metadata={"ad_position": 3}
        ),
        build_activation_item(
            linkcode="LINK002", score=0.5, metadata={"ad_position": 1}
        ),
        build_activation_item(
            linkcode="LINK003", score=0.5, metadata={"ad_position": 2}
        ),
    ]
    
    ranked = stable_rank(items)
    
    assert ranked[0].item_group_id == "LINK002"  # ad_position 1
    assert ranked[1].item_group_id == "LINK003"  # ad_position 2
    assert ranked[2].item_group_id == "LINK001"  # ad_position 3


def test_stable_rank_tie_breaker_by_gtin_when_no_ad_position():
    """Test that items with same score and no ad_position are sorted by gtin ASC."""
    items = [
        build_activation_item(linkcode="LINK001", gtin="GTIN_C", score=0.5),
        build_activation_item(linkcode="LINK002", gtin="GTIN_A", score=0.5),
        build_activation_item(linkcode="LINK003", gtin="GTIN_B", score=0.5),
    ]
    
    ranked = stable_rank(items)
    
    assert ranked[0].item_group_id == "LINK002"  # GTIN_A
    assert ranked[1].item_group_id == "LINK003"  # GTIN_B
    assert ranked[2].item_group_id == "LINK001"  # GTIN_C


def test_stable_rank_no_ad_position_sorts_last():
    """Test that items without ad_position are sorted after items with ad_position."""
    items = [
        build_activation_item(linkcode="LINK001", score=0.5),  # No ad_position
        build_activation_item(
            linkcode="LINK002", score=0.5, metadata={"ad_position": 1}
        ),
        build_activation_item(linkcode="LINK003", score=0.5),  # No ad_position
    ]
    
    ranked = stable_rank(items)
    
    # Items with ad_position come first
    assert ranked[0].item_group_id == "LINK002"
    # Items without ad_position come after, sorted by gtin/item_group_id
    assert ranked[1].item_group_id in ["LINK001", "LINK003"]
    assert ranked[2].item_group_id in ["LINK001", "LINK003"]


def test_stable_rank_final_tie_breaker_by_item_group_id_when_no_gtin():
    """Test that item_group_id is used as tie-breaker when gtin is None."""
    items = [
        build_activation_item(linkcode="LINK_C", gtin=None, score=0.5),
        build_activation_item(linkcode="LINK_A", gtin=None, score=0.5),
        build_activation_item(linkcode="LINK_B", gtin=None, score=0.5),
    ]
    
    ranked = stable_rank(items)
    
    assert ranked[0].item_group_id == "LINK_A"
    assert ranked[1].item_group_id == "LINK_B"
    assert ranked[2].item_group_id == "LINK_C"


def test_stable_rank_complex_sorting():
    """Test complex sorting with score, ad_position, and gtin."""
    items = [
        build_activation_item(
            linkcode="LINK001", gtin="GTIN_Z", score=0.8, metadata={"ad_position": 2}
        ),
        build_activation_item(
            linkcode="LINK002", gtin="GTIN_A", score=0.8, metadata={"ad_position": 1}
        ),
        build_activation_item(
            linkcode="LINK003", gtin="GTIN_B", score=0.9, metadata={"ad_position": 3}
        ),
        build_activation_item(linkcode="LINK004", gtin="GTIN_C", score=0.7),
    ]
    
    ranked = stable_rank(items)
    
    # Highest score first
    assert ranked[0].item_group_id == "LINK003"  # score 0.9
    # Then score 0.8 items sorted by ad_position
    assert ranked[1].item_group_id == "LINK002"  # score 0.8, ad_position 1
    assert ranked[2].item_group_id == "LINK001"  # score 0.8, ad_position 2
    # Then score 0.7
    assert ranked[3].item_group_id == "LINK004"  # score 0.7


def test_stable_rank_deterministic():
    """Test that stable_rank produces same output on repeated calls."""
    items = [
        build_activation_item(linkcode="LINK001", score=0.5),
        build_activation_item(linkcode="LINK002", score=0.9),
        build_activation_item(linkcode="LINK003", score=0.3),
    ]
    
    ranked1 = stable_rank(items)
    ranked2 = stable_rank(items)
    
    assert len(ranked1) == len(ranked2)
    for i in range(len(ranked1)):
        assert ranked1[i].item_group_id == ranked2[i].item_group_id
        assert ranked1[i].score == ranked2[i].score


def test_stable_rank_empty_list():
    """Test that stable_rank handles empty list."""
    ranked = stable_rank([])
    assert ranked == []


def test_compute_match_rate_all_match():
    """Test match rate when all items have score > 0."""
    items = [
        build_activation_item(linkcode="LINK001", score=0.9),
        build_activation_item(linkcode="LINK002", score=0.8),
        build_activation_item(linkcode="LINK003", score=0.7),
    ]
    
    match_rate = compute_match_rate(items)
    assert match_rate == 1.0


def test_compute_match_rate_none_match():
    """Test match rate when no items have score > 0."""
    items = [
        build_activation_item(linkcode="LINK001", score=0.0),
        build_activation_item(linkcode="LINK002", score=0.0),
    ]
    
    match_rate = compute_match_rate(items)
    assert match_rate == 0.0


def test_compute_match_rate_partial_match():
    """Test match rate when some items have score > 0."""
    items = [
        build_activation_item(linkcode="LINK001", score=0.9),
        build_activation_item(linkcode="LINK002", score=0.0),
        build_activation_item(linkcode="LINK003", score=0.8),
        build_activation_item(linkcode="LINK004", score=0.0),
    ]
    
    match_rate = compute_match_rate(items)
    assert match_rate == 0.5  # 2 out of 4


def test_compute_match_rate_empty_list():
    """Test match rate for empty list returns 0.0."""
    match_rate = compute_match_rate([])
    assert match_rate == 0.0


def test_compute_match_rate_score_zero_not_counted():
    """Test that score exactly 0.0 is not counted as a match."""
    items = [
        build_activation_item(linkcode="LINK001", score=0.0),
        build_activation_item(linkcode="LINK002", score=0.1),
    ]
    
    match_rate = compute_match_rate(items)
    assert match_rate == 0.5  # Only one item with score > 0
