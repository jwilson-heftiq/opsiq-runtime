from datetime import datetime, timezone

from opsiq_runtime.domain.primitives.customer_order_impact_risk.config import CustomerImpactConfig
from opsiq_runtime.domain.primitives.customer_order_impact_risk.evaluator import evaluate_customer_order_impact_risk
from opsiq_runtime.domain.primitives.customer_order_impact_risk.model import CustomerImpactInput, SourceOrderRef
from opsiq_runtime.domain.primitives.customer_order_impact_risk import rules

# Note: These tests verify the evaluator logic. The evaluator now sources from
# order_fulfillment_risk decisions (subject_type='order'), not order_line_fulfillment_risk.
# The inputs repository handles the sourcing change; these unit tests verify the
# aggregation logic works correctly with order-level inputs.


def make_input(
    order_count_total: int = 0,
    order_count_at_risk: int = 0,
    order_count_unknown: int = 0,
    at_risk_order_subject_ids: list[str] | None = None,
    source_order_refs: list[SourceOrderRef] | None = None,
    high_threshold: int = 5,
    medium_threshold: int = 2,
) -> CustomerImpactInput:
    as_of_ts = datetime(2024, 1, 10, tzinfo=timezone.utc)
    cfg = CustomerImpactConfig(high_threshold=high_threshold, medium_threshold=medium_threshold)
    input_obj = CustomerImpactInput.new(
        tenant_id="t1",
        subject_id="customer1",
        as_of_ts=as_of_ts,
        config_version="cfg",
        canonical_version="v1",
        order_count_total=order_count_total,
        order_count_at_risk=order_count_at_risk,
        order_count_unknown=order_count_unknown,
        at_risk_order_subject_ids=at_risk_order_subject_ids or [],
        source_order_refs=source_order_refs or [],
    )
    return input_obj, cfg


def test_unknown_when_no_orders_found():
    """Test UNKNOWN when total == 0 (Rule 1)."""
    input_obj, cfg = make_input(order_count_total=0)
    res = evaluate_customer_order_impact_risk(input_obj, cfg)
    assert res.decision.state == rules.UNKNOWN
    assert res.decision.confidence == "LOW"
    assert rules.DRIVER_NO_ORDERS_FOUND in res.decision.drivers
    assert rules.RULE_UNKNOWN_NO_ORDERS_FOUND in res.evidence_set.evidence[0].rule_ids


def test_high_impact_when_at_risk_above_high_threshold():
    """Test HIGH_IMPACT when at_risk >= high_threshold (Rule 2)."""
    input_obj, cfg = make_input(
        order_count_total=10,
        order_count_at_risk=5,  # At high_threshold
        order_count_unknown=0,
        at_risk_order_subject_ids=["order1", "order2", "order3", "order4", "order5"],
        high_threshold=5,
    )
    res = evaluate_customer_order_impact_risk(input_obj, cfg)
    assert res.decision.state == rules.HIGH_IMPACT
    assert res.decision.confidence == "HIGH"
    assert rules.DRIVER_HIGH_IMPACT in res.decision.drivers
    assert rules.RULE_HIGH_IMPACT in res.evidence_set.evidence[0].rule_ids
    
    # Test above threshold
    input_obj, cfg = make_input(
        order_count_total=10,
        order_count_at_risk=7,  # Above high_threshold
        order_count_unknown=0,
        high_threshold=5,
    )
    res = evaluate_customer_order_impact_risk(input_obj, cfg)
    assert res.decision.state == rules.HIGH_IMPACT
    assert rules.DRIVER_HIGH_IMPACT in res.decision.drivers


def test_medium_impact_when_at_risk_above_medium_threshold():
    """Test MEDIUM_IMPACT when at_risk >= medium_threshold but < high_threshold (Rule 3)."""
    input_obj, cfg = make_input(
        order_count_total=10,
        order_count_at_risk=3,  # Above medium_threshold but below high_threshold
        order_count_unknown=0,
        at_risk_order_subject_ids=["order1", "order2", "order3"],
        high_threshold=5,
        medium_threshold=2,
    )
    res = evaluate_customer_order_impact_risk(input_obj, cfg)
    assert res.decision.state == rules.MEDIUM_IMPACT
    assert res.decision.confidence == "HIGH"
    assert rules.DRIVER_MEDIUM_IMPACT in res.decision.drivers
    assert rules.RULE_MEDIUM_IMPACT in res.evidence_set.evidence[0].rule_ids
    
    # Test at threshold
    input_obj, cfg = make_input(
        order_count_total=10,
        order_count_at_risk=2,  # At medium_threshold
        order_count_unknown=0,
        high_threshold=5,
        medium_threshold=2,
    )
    res = evaluate_customer_order_impact_risk(input_obj, cfg)
    assert res.decision.state == rules.MEDIUM_IMPACT
    assert rules.DRIVER_MEDIUM_IMPACT in res.decision.drivers


def test_low_impact_when_at_risk_below_medium_threshold():
    """Test LOW_IMPACT when at_risk > 0 but < medium_threshold (Rule 4)."""
    input_obj, cfg = make_input(
        order_count_total=10,
        order_count_at_risk=1,  # Below medium_threshold
        order_count_unknown=0,
        at_risk_order_subject_ids=["order1"],
        high_threshold=5,
        medium_threshold=2,
    )
    res = evaluate_customer_order_impact_risk(input_obj, cfg)
    assert res.decision.state == rules.LOW_IMPACT
    assert res.decision.confidence == "MEDIUM"
    assert rules.DRIVER_LOW_IMPACT in res.decision.drivers
    assert rules.RULE_LOW_IMPACT in res.evidence_set.evidence[0].rule_ids


def test_unknown_when_all_orders_unknown():
    """Test UNKNOWN when unknown == total (Rule 5)."""
    input_obj, cfg = make_input(
        order_count_total=5,
        order_count_at_risk=0,
        order_count_unknown=5,
        high_threshold=5,
        medium_threshold=2,
    )
    res = evaluate_customer_order_impact_risk(input_obj, cfg)
    assert res.decision.state == rules.UNKNOWN
    assert res.decision.confidence == "LOW"
    assert rules.DRIVER_ALL_ORDERS_UNKNOWN in res.decision.drivers
    assert rules.RULE_UNKNOWN_ALL_ORDERS_UNKNOWN in res.evidence_set.evidence[0].rule_ids


def test_low_impact_when_no_at_risk_orders():
    """Test LOW_IMPACT when at_risk == 0 (Rule 6)."""
    input_obj, cfg = make_input(
        order_count_total=5,
        order_count_at_risk=0,
        order_count_unknown=0,
        high_threshold=5,
        medium_threshold=2,
    )
    res = evaluate_customer_order_impact_risk(input_obj, cfg)
    assert res.decision.state == rules.LOW_IMPACT
    assert res.decision.confidence == "MEDIUM"
    assert rules.DRIVER_NO_AT_RISK_ORDERS in res.decision.drivers
    assert rules.RULE_LOW_IMPACT_NO_AT_RISK in res.evidence_set.evidence[0].rule_ids


def test_metrics_include_counts_and_at_risk_order_ids():
    """Test that metrics include all counts and at_risk_order_subject_ids."""
    at_risk_ids = ["order1", "order2", "order3"]
    input_obj, cfg = make_input(
        order_count_total=10,
        order_count_at_risk=3,
        order_count_unknown=2,
        at_risk_order_subject_ids=at_risk_ids,
    )
    res = evaluate_customer_order_impact_risk(input_obj, cfg)
    assert res.decision.metrics["order_count_total"] == 10
    assert res.decision.metrics["order_count_at_risk"] == 3
    assert res.decision.metrics["order_count_unknown"] == 2
    assert res.decision.metrics["at_risk_order_subject_ids"] == at_risk_ids


def test_evidence_includes_applied_rule_id_and_rollup_counts():
    """Test that evidence includes applied_rule_id, source_orders, rollup_counts, and thresholds."""
    source_orders = [
        SourceOrderRef(
            order_subject_id="order1",
            decision_state="AT_RISK",
            evidence_refs=["evidence1"],
        ),
        SourceOrderRef(
            order_subject_id="order2",
            decision_state="NOT_AT_RISK",
            evidence_refs=["evidence2"],
        ),
    ]
    input_obj, cfg = make_input(
        order_count_total=2,
        order_count_at_risk=1,
        order_count_unknown=0,
        source_order_refs=source_orders,
        high_threshold=5,
        medium_threshold=2,
    )
    res = evaluate_customer_order_impact_risk(input_obj, cfg)
    assert len(res.evidence_set.evidence) == 1
    evidence = res.evidence_set.evidence[0]
    assert evidence.references["applied_rule_id"] == rules.RULE_LOW_IMPACT
    assert "source_orders" in evidence.references
    assert len(evidence.references["source_orders"]) == 2
    assert "rollup_counts" in evidence.references
    assert evidence.references["rollup_counts"]["total"] == 2
    assert evidence.references["rollup_counts"]["at_risk"] == 1
    assert "thresholds" in evidence.references
    assert evidence.references["thresholds"]["high_threshold"] == 5
    assert evidence.references["thresholds"]["medium_threshold"] == 2
    assert evidence.thresholds["high_threshold"] == 5
    assert evidence.thresholds["medium_threshold"] == 2


def test_source_orders_capped_at_100():
    """Test that source_orders are capped at 100 to avoid excessive size."""
    source_orders = [
        SourceOrderRef(
            order_subject_id=f"order{i}",
            decision_state="AT_RISK",
            evidence_refs=[f"evidence{i}"],
        )
        for i in range(150)
    ]
    input_obj, cfg = make_input(
        order_count_total=150,
        order_count_at_risk=150,
        order_count_unknown=0,
        source_order_refs=source_orders,
    )
    res = evaluate_customer_order_impact_risk(input_obj, cfg)
    evidence = res.evidence_set.evidence[0]
    assert len(evidence.references["source_orders"]) == 100  # Capped at 100


def test_rule_priority_order():
    """Test that rules are evaluated in priority order."""
    # Rule 1: total == 0 should take precedence
    input_obj, cfg = make_input(order_count_total=0, order_count_at_risk=1)
    res = evaluate_customer_order_impact_risk(input_obj, cfg)
    assert res.decision.state == rules.UNKNOWN
    assert rules.DRIVER_NO_ORDERS_FOUND in res.decision.drivers
    
    # Rule 2: high threshold should take precedence
    input_obj, cfg = make_input(
        order_count_total=10,
        order_count_at_risk=5,  # At high threshold
        order_count_unknown=3,
        high_threshold=5,
    )
    res = evaluate_customer_order_impact_risk(input_obj, cfg)
    assert res.decision.state == rules.HIGH_IMPACT
    assert rules.DRIVER_HIGH_IMPACT in res.decision.drivers
    
    # Rule 3: medium threshold should take precedence over low
    input_obj, cfg = make_input(
        order_count_total=10,
        order_count_at_risk=3,  # Above medium but below high
        order_count_unknown=0,
        high_threshold=5,
        medium_threshold=2,
    )
    res = evaluate_customer_order_impact_risk(input_obj, cfg)
    assert res.decision.state == rules.MEDIUM_IMPACT
    assert rules.DRIVER_MEDIUM_IMPACT in res.decision.drivers
    
    # Rule 4: low impact (at_risk > 0) should take precedence over Rule 6
    input_obj, cfg = make_input(
        order_count_total=10,
        order_count_at_risk=1,
        order_count_unknown=0,
        high_threshold=5,
        medium_threshold=2,
    )
    res = evaluate_customer_order_impact_risk(input_obj, cfg)
    assert res.decision.state == rules.LOW_IMPACT
    assert rules.DRIVER_LOW_IMPACT in res.decision.drivers
    
    # Rule 5: all unknown should take precedence over Rule 6
    input_obj, cfg = make_input(
        order_count_total=5,
        order_count_at_risk=0,
        order_count_unknown=5,
    )
    res = evaluate_customer_order_impact_risk(input_obj, cfg)
    assert res.decision.state == rules.UNKNOWN
    assert rules.DRIVER_ALL_ORDERS_UNKNOWN in res.decision.drivers

