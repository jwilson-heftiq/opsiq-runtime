from opsiq_runtime.domain.activation_policy.identity import build_activation_item
from opsiq_runtime.domain.activation_policy.reasons import (
    add_excluded_reason,
    add_reason,
    aggregate_drivers,
)


def test_add_reason_creates_reasons_list():
    """Test that add_reason creates reasons list in metadata."""
    item = build_activation_item(linkcode="LINK001", score=0.9)
    
    result = add_reason(item, "AFFINITY_MATCH")
    
    assert "reasons" in result.metadata
    assert result.metadata["reasons"] == ["AFFINITY_MATCH"]


def test_add_reason_appends_to_existing_reasons():
    """Test that add_reason appends to existing reasons list."""
    item = build_activation_item(
        linkcode="LINK001",
        score=0.9,
        metadata={"reasons": ["IN_CURRENT_AD"]},
    )
    
    result = add_reason(item, "AFFINITY_MATCH")
    
    assert "AFFINITY_MATCH" in result.metadata["reasons"]
    assert "IN_CURRENT_AD" in result.metadata["reasons"]
    assert len(result.metadata["reasons"]) == 2


def test_add_reason_no_duplicates():
    """Test that add_reason does not add duplicate reasons."""
    item = build_activation_item(
        linkcode="LINK001",
        score=0.9,
        metadata={"reasons": ["AFFINITY_MATCH"]},
    )
    
    result = add_reason(item, "AFFINITY_MATCH")
    
    assert result.metadata["reasons"] == ["AFFINITY_MATCH"]
    assert len(result.metadata["reasons"]) == 1


def test_add_reason_preserves_other_metadata():
    """Test that add_reason preserves other metadata fields."""
    item = build_activation_item(
        linkcode="LINK001",
        score=0.9,
        metadata={"promo_price": 10.0, "title": "Milk"},
    )
    
    result = add_reason(item, "AFFINITY_MATCH")
    
    assert result.metadata["promo_price"] == 10.0
    assert result.metadata["title"] == "Milk"
    assert "AFFINITY_MATCH" in result.metadata["reasons"]


def test_add_excluded_reason_creates_excluded_reasons_list():
    """Test that add_excluded_reason creates excluded_reasons list in metadata."""
    item = build_activation_item(linkcode="LINK001", score=0.9)
    
    result = add_excluded_reason(item, "RECENT_PURCHASE_EXCLUSION")
    
    assert "excluded_reasons" in result.metadata
    assert result.metadata["excluded_reasons"] == ["RECENT_PURCHASE_EXCLUSION"]


def test_add_excluded_reason_appends_to_existing():
    """Test that add_excluded_reason appends to existing excluded_reasons list."""
    item = build_activation_item(
        linkcode="LINK001",
        score=0.9,
        metadata={"excluded_reasons": ["WEEKLY_AD_OVERLAP_EXCLUSION"]},
    )
    
    result = add_excluded_reason(item, "RECENT_PURCHASE_EXCLUSION")
    
    assert "RECENT_PURCHASE_EXCLUSION" in result.metadata["excluded_reasons"]
    assert "WEEKLY_AD_OVERLAP_EXCLUSION" in result.metadata["excluded_reasons"]
    assert len(result.metadata["excluded_reasons"]) == 2


def test_add_excluded_reason_no_duplicates():
    """Test that add_excluded_reason does not add duplicate reasons."""
    item = build_activation_item(
        linkcode="LINK001",
        score=0.9,
        metadata={"excluded_reasons": ["RECENT_PURCHASE_EXCLUSION"]},
    )
    
    result = add_excluded_reason(item, "RECENT_PURCHASE_EXCLUSION")
    
    assert result.metadata["excluded_reasons"] == ["RECENT_PURCHASE_EXCLUSION"]
    assert len(result.metadata["excluded_reasons"]) == 1


def test_aggregate_drivers_always_includes_activation_policy_applied():
    """Test that aggregate_drivers always includes ACTIVATION_POLICY_APPLIED."""
    selected = [build_activation_item(linkcode="LINK001", score=0.0)]
    excluded = []
    
    drivers = aggregate_drivers(selected, excluded)
    
    assert "ACTIVATION_POLICY_APPLIED" in drivers


def test_aggregate_drivers_includes_affinity_match_when_scores_positive():
    """Test that AFFINITY_MATCH is included when any selected item has score > 0."""
    selected = [
        build_activation_item(linkcode="LINK001", score=0.9),
        build_activation_item(linkcode="LINK002", score=0.0),
    ]
    excluded = []
    
    drivers = aggregate_drivers(selected, excluded)
    
    assert "AFFINITY_MATCH" in drivers
    assert "ACTIVATION_POLICY_APPLIED" in drivers


def test_aggregate_drivers_no_affinity_match_when_all_scores_zero():
    """Test that AFFINITY_MATCH is not included when all selected items have score 0."""
    selected = [
        build_activation_item(linkcode="LINK001", score=0.0),
        build_activation_item(linkcode="LINK002", score=0.0),
    ]
    excluded = []
    
    drivers = aggregate_drivers(selected, excluded)
    
    assert "AFFINITY_MATCH" not in drivers
    assert "ACTIVATION_POLICY_APPLIED" in drivers


def test_aggregate_drivers_includes_exclusions_applied_when_excluded():
    """Test that EXCLUSIONS_APPLIED is included when there are excluded items."""
    selected = [build_activation_item(linkcode="LINK001", score=0.9)]
    excluded = [build_activation_item(linkcode="LINK002", score=0.8)]
    
    drivers = aggregate_drivers(selected, excluded)
    
    assert "EXCLUSIONS_APPLIED" in drivers
    assert "ACTIVATION_POLICY_APPLIED" in drivers


def test_aggregate_drivers_no_exclusions_applied_when_no_excluded():
    """Test that EXCLUSIONS_APPLIED is not included when no items excluded."""
    selected = [build_activation_item(linkcode="LINK001", score=0.9)]
    excluded = []
    
    drivers = aggregate_drivers(selected, excluded)
    
    assert "EXCLUSIONS_APPLIED" not in drivers
    assert "ACTIVATION_POLICY_APPLIED" in drivers


def test_aggregate_drivers_includes_all_applicable():
    """Test that all applicable drivers are included."""
    selected = [build_activation_item(linkcode="LINK001", score=0.9)]
    excluded = [build_activation_item(linkcode="LINK002", score=0.8)]
    
    drivers = aggregate_drivers(selected, excluded)
    
    assert "ACTIVATION_POLICY_APPLIED" in drivers
    assert "AFFINITY_MATCH" in drivers
    assert "EXCLUSIONS_APPLIED" in drivers
    assert len(drivers) == 3


def test_aggregate_drivers_stable_order():
    """Test that drivers are returned in stable order."""
    selected = [build_activation_item(linkcode="LINK001", score=0.9)]
    excluded = [build_activation_item(linkcode="LINK002", score=0.8)]
    
    drivers1 = aggregate_drivers(selected, excluded)
    drivers2 = aggregate_drivers(selected, excluded)
    
    assert drivers1 == drivers2


def test_aggregate_drivers_empty_selected():
    """Test aggregate_drivers with empty selected list."""
    selected = []
    excluded = [build_activation_item(linkcode="LINK002", score=0.8)]
    
    drivers = aggregate_drivers(selected, excluded)
    
    assert "ACTIVATION_POLICY_APPLIED" in drivers
    assert "AFFINITY_MATCH" not in drivers
    assert "EXCLUSIONS_APPLIED" in drivers


def test_aggregate_drivers_empty_all():
    """Test aggregate_drivers with empty selected and excluded."""
    selected = []
    excluded = []
    
    drivers = aggregate_drivers(selected, excluded)
    
    assert drivers == ["ACTIVATION_POLICY_APPLIED"]
