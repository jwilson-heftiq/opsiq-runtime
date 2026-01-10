from opsiq_runtime.domain.activation_policy import (
    ActivationItem,
    PolicyConfig,
    PolicyOutcome,
    build_activation_item,
    build_policy_outcome,
    compute_match_rate,
    aggregate_drivers,
)


def test_build_policy_outcome_confidence_low_when_empty_selected():
    """Test that confidence is LOW when no items are selected."""
    config = PolicyConfig(min_match_rate_for_high_confidence=0.5)
    
    outcome = build_policy_outcome(
        selected_items=[],
        excluded_items=[],
        candidates_count=10,
        match_rate=0.0,
        drivers=["ACTIVATION_POLICY_APPLIED"],
        config=config,
    )
    
    assert outcome.computed_confidence == "LOW"
    assert len(outcome.selected_items) == 0
    assert outcome.candidates_count == 10


def test_build_policy_outcome_confidence_high_when_match_rate_above_threshold():
    """Test that confidence is HIGH when match_rate >= min_match_rate_for_high_confidence."""
    config = PolicyConfig(min_match_rate_for_high_confidence=0.5)
    selected = [
        build_activation_item(linkcode="LINK001", score=0.9),
        build_activation_item(linkcode="LINK002", score=0.8),
        build_activation_item(linkcode="LINK003", score=0.0),  # One with score 0
    ]
    match_rate = compute_match_rate(selected)  # Should be 2/3 = 0.667
    
    outcome = build_policy_outcome(
        selected_items=selected,
        excluded_items=[],
        candidates_count=5,
        match_rate=match_rate,
        drivers=aggregate_drivers(selected, []),
        config=config,
    )
    
    assert outcome.computed_confidence == "HIGH"
    assert match_rate >= 0.5


def test_build_policy_outcome_confidence_high_when_match_rate_exactly_at_threshold():
    """Test that confidence is HIGH when match_rate exactly equals threshold."""
    config = PolicyConfig(min_match_rate_for_high_confidence=0.5)
    selected = [
        build_activation_item(linkcode="LINK001", score=0.9),
        build_activation_item(linkcode="LINK002", score=0.0),
    ]
    match_rate = 0.5  # Exactly at threshold
    
    outcome = build_policy_outcome(
        selected_items=selected,
        excluded_items=[],
        candidates_count=2,
        match_rate=match_rate,
        drivers=aggregate_drivers(selected, []),
        config=config,
    )
    
    assert outcome.computed_confidence == "HIGH"


def test_build_policy_outcome_confidence_medium_when_match_rate_below_threshold():
    """Test that confidence is MEDIUM when 0 < match_rate < min_match_rate_for_high_confidence."""
    config = PolicyConfig(min_match_rate_for_high_confidence=0.5)
    selected = [
        build_activation_item(linkcode="LINK001", score=0.9),
        build_activation_item(linkcode="LINK002", score=0.0),
        build_activation_item(linkcode="LINK003", score=0.0),
    ]
    match_rate = compute_match_rate(selected)  # Should be 1/3 = 0.333
    
    outcome = build_policy_outcome(
        selected_items=selected,
        excluded_items=[],
        candidates_count=5,
        match_rate=match_rate,
        drivers=aggregate_drivers(selected, []),
        config=config,
    )
    
    assert outcome.computed_confidence == "MEDIUM"
    assert 0 < match_rate < 0.5


def test_build_policy_outcome_confidence_low_when_match_rate_zero():
    """Test that confidence is LOW when match_rate is 0.0 even with selected items."""
    config = PolicyConfig(min_match_rate_for_high_confidence=0.5)
    selected = [
        build_activation_item(linkcode="LINK001", score=0.0),
        build_activation_item(linkcode="LINK002", score=0.0),
    ]
    match_rate = 0.0
    
    outcome = build_policy_outcome(
        selected_items=selected,
        excluded_items=[],
        candidates_count=2,
        match_rate=match_rate,
        drivers=aggregate_drivers(selected, []),
        config=config,
    )
    
    assert outcome.computed_confidence == "LOW"


def test_build_policy_outcome_includes_all_fields():
    """Test that PolicyOutcome includes all required fields."""
    config = PolicyConfig()
    selected = [build_activation_item(linkcode="LINK001", score=0.9)]
    excluded = [build_activation_item(linkcode="LINK002", score=0.8)]
    match_rate = 1.0
    drivers = ["ACTIVATION_POLICY_APPLIED", "AFFINITY_MATCH"]
    
    outcome = build_policy_outcome(
        selected_items=selected,
        excluded_items=excluded,
        candidates_count=5,
        match_rate=match_rate,
        drivers=drivers,
        config=config,
    )
    
    assert outcome.selected_items == selected
    assert outcome.excluded_items == excluded
    assert outcome.excluded_count == 1
    assert outcome.candidates_count == 5
    assert outcome.match_rate == match_rate
    assert outcome.drivers == drivers
    assert outcome.computed_confidence in ["HIGH", "MEDIUM", "LOW"]


def test_build_policy_outcome_excluded_count_matches_excluded_items():
    """Test that excluded_count matches the length of excluded_items."""
    config = PolicyConfig()
    selected = [build_activation_item(linkcode="LINK001", score=0.9)]
    excluded = [
        build_activation_item(linkcode="LINK002", score=0.8),
        build_activation_item(linkcode="LINK003", score=0.7),
    ]
    
    outcome = build_policy_outcome(
        selected_items=selected,
        excluded_items=excluded,
        candidates_count=3,
        match_rate=1.0,
        drivers=["ACTIVATION_POLICY_APPLIED"],
        config=config,
    )
    
    assert outcome.excluded_count == 2
    assert outcome.excluded_count == len(excluded)
