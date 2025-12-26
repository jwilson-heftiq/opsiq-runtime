from datetime import datetime, timezone

from opsiq_runtime.domain.primitives.shopper_health_classification.config import ShopperHealthConfig
from opsiq_runtime.domain.primitives.shopper_health_classification.evaluator import evaluate_shopper_health_classification
from opsiq_runtime.domain.primitives.shopper_health_classification.model import ShopperHealthInput
from opsiq_runtime.domain.primitives.shopper_health_classification import rules


def make_input(
    risk_state: str | None = None,
    trend_state: str | None = None,
    risk_evidence_refs: list[str] | None = None,
    trend_evidence_refs: list[str] | None = None,
    risk_source_as_of_ts: datetime | None = None,
    trend_source_as_of_ts: datetime | None = None,
) -> ShopperHealthInput:
    as_of_ts = datetime(2024, 1, 10, tzinfo=timezone.utc)
    return ShopperHealthInput.new(
        tenant_id="t1",
        subject_id="s1",
        as_of_ts=as_of_ts,
        config_version="cfg",
        canonical_version="v1",
        risk_state=risk_state,
        trend_state=trend_state,
        risk_evidence_refs=risk_evidence_refs or [],
        trend_evidence_refs=trend_evidence_refs or [],
        risk_source_as_of_ts=risk_source_as_of_ts or as_of_ts,
        trend_source_as_of_ts=trend_source_as_of_ts or as_of_ts,
    )


def test_urgent_when_at_risk():
    """Test URGENT when risk_state == 'AT_RISK' (Rule 1)."""
    cfg = ShopperHealthConfig()
    res = evaluate_shopper_health_classification(
        make_input(risk_state="AT_RISK", trend_state="STABLE"), cfg
    )
    assert res.decision.state == rules.URGENT
    assert res.decision.confidence == "HIGH"
    assert rules.DRIVER_LAPSE_RISK in res.decision.drivers
    assert rules.RULE_URGENT_AT_RISK in res.evidence_set.evidence[0].rule_ids


def test_urgent_when_at_risk_regardless_of_trend():
    """Test URGENT when risk_state == 'AT_RISK' regardless of trend_state."""
    cfg = ShopperHealthConfig()
    # AT_RISK should dominate even with DECLINING trend
    res = evaluate_shopper_health_classification(
        make_input(risk_state="AT_RISK", trend_state="DECLINING"), cfg
    )
    assert res.decision.state == rules.URGENT
    assert res.decision.confidence == "HIGH"


def test_unknown_when_both_unknown():
    """Test UNKNOWN when both risk_state and trend_state are UNKNOWN (Rule 2)."""
    cfg = ShopperHealthConfig()
    res = evaluate_shopper_health_classification(
        make_input(risk_state="UNKNOWN", trend_state="UNKNOWN"), cfg
    )
    assert res.decision.state == rules.UNKNOWN
    assert res.decision.confidence == "LOW"
    assert rules.DRIVER_INSUFFICIENT_SIGNALS in res.decision.drivers
    assert rules.RULE_UNKNOWN_INSUFFICIENT_SIGNALS in res.evidence_set.evidence[0].rule_ids


def test_watchlist_when_not_at_risk_and_declining():
    """Test WATCHLIST when NOT_AT_RISK + DECLINING (Rule 3)."""
    cfg = ShopperHealthConfig()
    res = evaluate_shopper_health_classification(
        make_input(risk_state="NOT_AT_RISK", trend_state="DECLINING"), cfg
    )
    assert res.decision.state == rules.WATCHLIST
    assert res.decision.confidence == "MEDIUM"
    assert rules.DRIVER_CADENCE_DECLINING in res.decision.drivers
    assert rules.RULE_WATCHLIST_DECLINING in res.evidence_set.evidence[0].rule_ids


def test_watchlist_when_unknown_and_declining():
    """Test WATCHLIST when UNKNOWN + DECLINING (Rule 4)."""
    cfg = ShopperHealthConfig()
    res = evaluate_shopper_health_classification(
        make_input(risk_state="UNKNOWN", trend_state="DECLINING"), cfg
    )
    assert res.decision.state == rules.WATCHLIST
    assert res.decision.confidence == "LOW"
    assert rules.DRIVER_CADENCE_DECLINING in res.decision.drivers
    assert rules.DRIVER_RISK_UNKNOWN in res.decision.drivers
    assert rules.RULE_WATCHLIST_DECLINING_RISK_UNKNOWN in res.evidence_set.evidence[0].rule_ids


def test_healthy_when_not_at_risk_and_stable():
    """Test HEALTHY when NOT_AT_RISK + STABLE (Rule 5)."""
    cfg = ShopperHealthConfig()
    res = evaluate_shopper_health_classification(
        make_input(risk_state="NOT_AT_RISK", trend_state="STABLE"), cfg
    )
    assert res.decision.state == rules.HEALTHY
    assert res.decision.confidence == "HIGH"
    assert rules.DRIVER_RISK_OK in res.decision.drivers
    assert rules.DRIVER_CADENCE_OK in res.decision.drivers
    assert rules.RULE_HEALTHY_OK in res.evidence_set.evidence[0].rule_ids


def test_healthy_when_not_at_risk_and_improving():
    """Test HEALTHY when NOT_AT_RISK + IMPROVING (Rule 5)."""
    cfg = ShopperHealthConfig()
    res = evaluate_shopper_health_classification(
        make_input(risk_state="NOT_AT_RISK", trend_state="IMPROVING"), cfg
    )
    assert res.decision.state == rules.HEALTHY
    assert res.decision.confidence == "HIGH"
    assert rules.DRIVER_RISK_OK in res.decision.drivers
    assert rules.DRIVER_CADENCE_OK in res.decision.drivers


def test_unknown_when_partial_signals():
    """Test UNKNOWN for other combinations (Rule 6 - catch-all)."""
    cfg = ShopperHealthConfig()
    
    # NOT_AT_RISK + UNKNOWN
    res = evaluate_shopper_health_classification(
        make_input(risk_state="NOT_AT_RISK", trend_state="UNKNOWN"), cfg
    )
    assert res.decision.state == rules.UNKNOWN
    assert res.decision.confidence == "MEDIUM"
    assert rules.DRIVER_PARTIAL_SIGNALS in res.decision.drivers
    assert rules.RULE_UNKNOWN_PARTIAL_SIGNALS in res.evidence_set.evidence[0].rule_ids
    
    # AT_RISK is handled by Rule 1, but test other edge case: UNKNOWN + STABLE
    res = evaluate_shopper_health_classification(
        make_input(risk_state="UNKNOWN", trend_state="STABLE"), cfg
    )
    assert res.decision.state == rules.UNKNOWN
    assert res.decision.confidence == "MEDIUM"


def test_metrics_include_states_and_timestamps():
    """Test that metrics include risk_state, trend_state, and source timestamps."""
    cfg = ShopperHealthConfig()
    risk_ts = datetime(2024, 1, 9, tzinfo=timezone.utc)
    trend_ts = datetime(2024, 1, 8, tzinfo=timezone.utc)
    res = evaluate_shopper_health_classification(
        make_input(
            risk_state="NOT_AT_RISK",
            trend_state="STABLE",
            risk_source_as_of_ts=risk_ts,
            trend_source_as_of_ts=trend_ts,
        ),
        cfg,
    )
    
    assert res.decision.metrics["risk_state"] == "NOT_AT_RISK"
    assert res.decision.metrics["trend_state"] == "STABLE"
    # Metrics store timestamps as ISO strings
    assert res.decision.metrics["risk_source_as_of_ts"] == risk_ts.isoformat()
    assert res.decision.metrics["trend_source_as_of_ts"] == trend_ts.isoformat()


def test_evidence_includes_applied_rule_id_and_source_primitives():
    """Test that evidence includes applied_rule_id and source_primitives array."""
    cfg = ShopperHealthConfig()
    res = evaluate_shopper_health_classification(
        make_input(
            risk_state="NOT_AT_RISK",
            trend_state="DECLINING",
            risk_evidence_refs=["evidence-risk-1"],
            trend_evidence_refs=["evidence-trend-1"],
        ),
        cfg,
    )
    
    assert len(res.evidence_set.evidence) == 1
    evidence = res.evidence_set.evidence[0]
    
    # Check applied_rule_id
    assert evidence.references["applied_rule_id"] == rules.RULE_WATCHLIST_DECLINING
    
    # Check source_primitives array
    assert "source_primitives" in evidence.references
    source_primitives = evidence.references["source_primitives"]
    assert len(source_primitives) == 2
    
    # Check operational_risk source
    risk_source = next(
        (sp for sp in source_primitives if sp["primitive_name"] == "operational_risk"),
        None
    )
    assert risk_source is not None
    assert risk_source["primitive_version"] == "1.0.0"
    assert risk_source["evidence_refs"] == ["evidence-risk-1"]
    
    # Check shopper_frequency_trend source
    trend_source = next(
        (sp for sp in source_primitives if sp["primitive_name"] == "shopper_frequency_trend"),
        None
    )
    assert trend_source is not None
    assert trend_source["primitive_version"] == "1.0.0"
    assert trend_source["evidence_refs"] == ["evidence-trend-1"]
    
    # Check composition_inputs
    assert "composition_inputs" in evidence.references
    assert evidence.references["composition_inputs"]["risk_state"] == "NOT_AT_RISK"
    assert evidence.references["composition_inputs"]["trend_state"] == "DECLINING"


def test_evidence_handles_missing_primitives():
    """Test that evidence handles missing primitives correctly."""
    cfg = ShopperHealthConfig()
    
    # Only risk_state available
    res = evaluate_shopper_health_classification(
        make_input(
            risk_state="AT_RISK",
            trend_state=None,  # Missing trend
            risk_evidence_refs=["evidence-risk-1"],
        ),
        cfg,
    )
    
    evidence = res.evidence_set.evidence[0]
    source_primitives = evidence.references["source_primitives"]
    # Should only have operational_risk source
    assert len(source_primitives) == 1
    assert source_primitives[0]["primitive_name"] == "operational_risk"
    
    # Composition inputs should show UNKNOWN for missing trend
    assert evidence.references["composition_inputs"]["risk_state"] == "AT_RISK"
    assert evidence.references["composition_inputs"]["trend_state"] == "UNKNOWN"


def test_none_states_normalized_to_unknown():
    """Test that None states are normalized to UNKNOWN."""
    cfg = ShopperHealthConfig()
    
    # Both None should result in UNKNOWN (Rule 2)
    res = evaluate_shopper_health_classification(
        make_input(risk_state=None, trend_state=None), cfg
    )
    assert res.decision.state == rules.UNKNOWN
    assert res.decision.confidence == "LOW"
    assert rules.DRIVER_INSUFFICIENT_SIGNALS in res.decision.drivers

