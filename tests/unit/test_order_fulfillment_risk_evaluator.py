from datetime import datetime, timezone

from opsiq_runtime.domain.primitives.order_fulfillment_risk.config import OrderRiskConfig
from opsiq_runtime.domain.primitives.order_fulfillment_risk.evaluator import evaluate_order_fulfillment_risk
from opsiq_runtime.domain.primitives.order_fulfillment_risk.model import OrderRiskInput, SourceLineRef
from opsiq_runtime.domain.primitives.order_fulfillment_risk import rules


def make_input(
    order_line_count_total: int = 0,
    order_line_count_at_risk: int = 0,
    order_line_count_unknown: int = 0,
    order_line_count_not_at_risk: int = 0,
    at_risk_line_subject_ids: list[str] | None = None,
    source_line_refs: list[SourceLineRef] | None = None,
    customer_id: str | None = None,
) -> OrderRiskInput:
    as_of_ts = datetime(2024, 1, 10, tzinfo=timezone.utc)
    return OrderRiskInput.new(
        tenant_id="t1",
        subject_id="order1",
        as_of_ts=as_of_ts,
        config_version="cfg",
        canonical_version="v1",
        customer_id=customer_id,
        order_line_count_total=order_line_count_total,
        order_line_count_at_risk=order_line_count_at_risk,
        order_line_count_unknown=order_line_count_unknown,
        order_line_count_not_at_risk=order_line_count_not_at_risk,
        at_risk_line_subject_ids=at_risk_line_subject_ids or [],
        source_line_refs=source_line_refs or [],
    )


def test_unknown_when_no_lines_found():
    """Test UNKNOWN when total == 0 (Rule 1)."""
    cfg = OrderRiskConfig()
    res = evaluate_order_fulfillment_risk(make_input(order_line_count_total=0), cfg)
    assert res.decision.state == rules.UNKNOWN
    assert res.decision.confidence == "LOW"
    assert rules.DRIVER_NO_LINES_FOUND in res.decision.drivers
    assert rules.RULE_UNKNOWN_NO_LINES_FOUND in res.evidence_set.evidence[0].rule_ids


def test_at_risk_when_has_at_risk_lines():
    """Test AT_RISK when at_risk > 0 (Rule 2)."""
    cfg = OrderRiskConfig()
    res = evaluate_order_fulfillment_risk(
        make_input(
            order_line_count_total=5,
            order_line_count_at_risk=2,
            order_line_count_unknown=0,
            order_line_count_not_at_risk=3,
            at_risk_line_subject_ids=["line1", "line2"],
        ),
        cfg,
    )
    assert res.decision.state == rules.AT_RISK
    assert res.decision.confidence == "HIGH"
    assert rules.DRIVER_HAS_AT_RISK_LINES in res.decision.drivers
    assert rules.RULE_AT_RISK_HAS_AT_RISK_LINES in res.evidence_set.evidence[0].rule_ids
    assert res.decision.metrics["order_line_count_at_risk"] == 2
    assert res.decision.metrics["at_risk_line_subject_ids"] == ["line1", "line2"]


def test_unknown_when_all_lines_unknown():
    """Test UNKNOWN when unknown == total (Rule 3)."""
    cfg = OrderRiskConfig()
    res = evaluate_order_fulfillment_risk(
        make_input(
            order_line_count_total=3,
            order_line_count_at_risk=0,
            order_line_count_unknown=3,
            order_line_count_not_at_risk=0,
        ),
        cfg,
    )
    assert res.decision.state == rules.UNKNOWN
    assert res.decision.confidence == "LOW"
    assert rules.DRIVER_ALL_LINES_UNKNOWN in res.decision.drivers
    assert rules.RULE_UNKNOWN_ALL_LINES_UNKNOWN in res.evidence_set.evidence[0].rule_ids


def test_not_at_risk_when_all_lines_ok():
    """Test NOT_AT_RISK when no at-risk lines (Rule 4)."""
    cfg = OrderRiskConfig()
    res = evaluate_order_fulfillment_risk(
        make_input(
            order_line_count_total=5,
            order_line_count_at_risk=0,
            order_line_count_unknown=0,
            order_line_count_not_at_risk=5,
        ),
        cfg,
    )
    assert res.decision.state == rules.NOT_AT_RISK
    assert res.decision.confidence == "HIGH"
    assert rules.DRIVER_ALL_LINES_OK in res.decision.drivers
    assert rules.RULE_NOT_AT_RISK_ALL_LINES_OK in res.evidence_set.evidence[0].rule_ids


def test_metrics_include_counts_and_at_risk_line_ids():
    """Test that metrics include all counts and at_risk_line_subject_ids."""
    cfg = OrderRiskConfig()
    at_risk_ids = ["line1", "line2", "line3"]
    res = evaluate_order_fulfillment_risk(
        make_input(
            order_line_count_total=10,
            order_line_count_at_risk=3,
            order_line_count_unknown=2,
            order_line_count_not_at_risk=5,
            at_risk_line_subject_ids=at_risk_ids,
        ),
        cfg,
    )
    assert res.decision.metrics["order_line_count_total"] == 10
    assert res.decision.metrics["order_line_count_at_risk"] == 3
    assert res.decision.metrics["order_line_count_unknown"] == 2
    assert res.decision.metrics["order_line_count_not_at_risk"] == 5
    assert res.decision.metrics["at_risk_line_subject_ids"] == at_risk_ids


def test_evidence_includes_applied_rule_id_and_rollup_counts():
    """Test that evidence includes applied_rule_id, source_lines, and rollup_counts."""
    cfg = OrderRiskConfig()
    source_lines = [
        SourceLineRef(
            line_subject_id="line1",
            decision_state="AT_RISK",
            evidence_refs=["evidence1"],
        ),
        SourceLineRef(
            line_subject_id="line2",
            decision_state="NOT_AT_RISK",
            evidence_refs=["evidence2"],
        ),
    ]
    res = evaluate_order_fulfillment_risk(
        make_input(
            order_line_count_total=2,
            order_line_count_at_risk=1,
            order_line_count_unknown=0,
            order_line_count_not_at_risk=1,
            source_line_refs=source_lines,
        ),
        cfg,
    )
    assert len(res.evidence_set.evidence) == 1
    evidence = res.evidence_set.evidence[0]
    assert evidence.references["applied_rule_id"] == rules.RULE_AT_RISK_HAS_AT_RISK_LINES
    assert "source_lines" in evidence.references
    assert len(evidence.references["source_lines"]) == 2
    assert "rollup_counts" in evidence.references
    assert evidence.references["rollup_counts"]["total"] == 2
    assert evidence.references["rollup_counts"]["at_risk"] == 1


def test_source_lines_capped_at_100():
    """Test that source_lines are capped at 100 to avoid excessive size."""
    cfg = OrderRiskConfig()
    source_lines = [
        SourceLineRef(
            line_subject_id=f"line{i}",
            decision_state="AT_RISK",
            evidence_refs=[f"evidence{i}"],
        )
        for i in range(150)
    ]
    res = evaluate_order_fulfillment_risk(
        make_input(
            order_line_count_total=150,
            order_line_count_at_risk=150,
            order_line_count_unknown=0,
            order_line_count_not_at_risk=0,
            source_line_refs=source_lines,
        ),
        cfg,
    )
    evidence = res.evidence_set.evidence[0]
    assert len(evidence.references["source_lines"]) == 100  # Capped at 100


def test_rule_priority_order():
    """Test that rules are evaluated in priority order."""
    cfg = OrderRiskConfig()
    
    # Rule 1: total == 0 should take precedence (even if other counts are non-zero)
    res = evaluate_order_fulfillment_risk(
        make_input(order_line_count_total=0, order_line_count_at_risk=1), cfg
    )
    assert res.decision.state == rules.UNKNOWN
    assert rules.DRIVER_NO_LINES_FOUND in res.decision.drivers
    
    # Rule 2: at_risk > 0 should take precedence over other conditions
    res = evaluate_order_fulfillment_risk(
        make_input(
            order_line_count_total=5,
            order_line_count_at_risk=1,
            order_line_count_unknown=4,  # Even if mostly unknown
        ),
        cfg,
    )
    assert res.decision.state == rules.AT_RISK
    assert rules.DRIVER_HAS_AT_RISK_LINES in res.decision.drivers
    
    # Rule 3: all unknown should take precedence over not_at_risk count
    res = evaluate_order_fulfillment_risk(
        make_input(
            order_line_count_total=3,
            order_line_count_at_risk=0,
            order_line_count_unknown=3,
            order_line_count_not_at_risk=0,
        ),
        cfg,
    )
    assert res.decision.state == rules.UNKNOWN
    assert rules.DRIVER_ALL_LINES_UNKNOWN in res.decision.drivers


def test_metrics_include_customer_id():
    """Test that metrics_json includes customer_id when provided."""
    cfg = OrderRiskConfig()
    
    # Test with customer_id
    res = evaluate_order_fulfillment_risk(
        make_input(
            order_line_count_total=5,
            order_line_count_at_risk=2,
            customer_id="CUST123",
        ),
        cfg,
    )
    assert "customer_id" in res.decision.metrics
    assert res.decision.metrics["customer_id"] == "CUST123"
    
    # Test without customer_id (should not be in metrics)
    res = evaluate_order_fulfillment_risk(
        make_input(
            order_line_count_total=5,
            order_line_count_at_risk=2,
            customer_id=None,
        ),
        cfg,
    )
    assert "customer_id" not in res.decision.metrics

