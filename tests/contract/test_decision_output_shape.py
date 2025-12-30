from datetime import date, datetime, timezone

from opsiq_runtime.domain.primitives.operational_risk.config import OperationalRiskConfig
from opsiq_runtime.domain.primitives.operational_risk.evaluator import evaluate_operational_risk
from opsiq_runtime.domain.primitives.operational_risk.model import OperationalRiskInput
from opsiq_runtime.domain.primitives.order_line_fulfillment_risk.config import OrderLineFulfillmentRiskConfig
from opsiq_runtime.domain.primitives.order_line_fulfillment_risk.evaluator import evaluate_order_line_fulfillment_risk
from opsiq_runtime.domain.primitives.order_line_fulfillment_risk.model import OrderLineFulfillmentInput
from opsiq_runtime.domain.primitives.order_line_fulfillment_risk import rules as order_line_rules
from opsiq_runtime.domain.primitives.shopper_frequency_trend.config import ShopperFrequencyTrendConfig
from opsiq_runtime.domain.primitives.shopper_frequency_trend.evaluator import evaluate_shopper_frequency_trend
from opsiq_runtime.domain.primitives.shopper_frequency_trend.model import ShopperFrequencyInput
from opsiq_runtime.domain.primitives.shopper_health_classification.config import ShopperHealthConfig
from opsiq_runtime.domain.primitives.shopper_health_classification.evaluator import evaluate_shopper_health_classification
from opsiq_runtime.domain.primitives.shopper_health_classification.model import ShopperHealthInput
from opsiq_runtime.domain.primitives.shopper_health_classification import rules


def test_operational_risk_decision_includes_versions_and_evidence_refs():
    """Test that operational_risk primitive produces decisions with versions and evidence refs."""
    cfg = OperationalRiskConfig(at_risk_days=5, primitive_version="1.0.0", canonical_version="v1")
    input_row = OperationalRiskInput.new(
        tenant_id="t1",
        subject_id="s1",
        as_of_ts=datetime(2024, 1, 10, tzinfo=timezone.utc),
        last_trip_ts=datetime(2024, 1, 1, tzinfo=timezone.utc),
        days_since_last_trip=None,
        config_version="cfg123",
        canonical_version="v1",
    )
    result = evaluate_operational_risk(input_row, cfg)
    decision = result.decision
    assert decision.versions.primitive_version == "1.0.0"
    assert decision.versions.canonical_version == "v1"
    assert decision.versions.config_version == "cfg123"
    assert decision.evidence_refs


def test_shopper_frequency_trend_decision_includes_versions_and_evidence_refs():
    """Test that shopper_frequency_trend primitive produces decisions with versions and evidence refs."""
    cfg = ShopperFrequencyTrendConfig(primitive_version="1.0.0", canonical_version="v1")
    input_row = ShopperFrequencyInput.new(
        tenant_id="t1",
        subject_id="s1",
        as_of_ts=datetime(2024, 1, 10, tzinfo=timezone.utc),
        last_trip_ts=datetime(2024, 1, 5, tzinfo=timezone.utc),
        prev_trip_ts=datetime(2024, 1, 1, tzinfo=timezone.utc),
        config_version="cfg123",
        canonical_version="v1",
        baseline_trip_count=5,
        baseline_avg_gap_days=10.0,
        recent_gap_days=15.0,
    )
    result = evaluate_shopper_frequency_trend(input_row, cfg)
    decision = result.decision
    assert decision.versions.primitive_version == "1.0.0"
    assert decision.versions.canonical_version == "v1"
    assert decision.versions.config_version == "cfg123"
    assert decision.evidence_refs


def test_shopper_health_classification_decision_includes_versions_and_evidence_refs():
    """Test that shopper_health_classification primitive produces decisions with versions and evidence refs."""
    cfg = ShopperHealthConfig(primitive_version="1.0.0", canonical_version="v1")
    input_row = ShopperHealthInput.new(
        tenant_id="t1",
        subject_id="s1",
        as_of_ts=datetime(2024, 1, 10, tzinfo=timezone.utc),
        config_version="cfg123",
        canonical_version="v1",
        risk_state="NOT_AT_RISK",
        trend_state="STABLE",
        risk_evidence_refs=["evidence-risk-1"],
        trend_evidence_refs=["evidence-trend-1"],
    )
    result = evaluate_shopper_health_classification(input_row, cfg)
    decision = result.decision
    assert decision.versions.primitive_version == "1.0.0"
    assert decision.versions.canonical_version == "v1"
    assert decision.versions.config_version == "cfg123"
    assert decision.evidence_refs
    # Verify decision state is one of the valid states
    assert decision.state in (rules.URGENT, rules.WATCHLIST, rules.HEALTHY, rules.UNKNOWN)
    # Verify confidence level
    assert decision.confidence in ("HIGH", "MEDIUM", "LOW")
    # Verify metrics include risk_state and trend_state
    assert "risk_state" in decision.metrics
    assert "trend_state" in decision.metrics


def test_order_line_fulfillment_risk_decision_includes_versions_and_evidence_refs():
    """Test that order_line_fulfillment_risk primitive produces decisions with versions and evidence refs."""
    cfg = OrderLineFulfillmentRiskConfig(primitive_version="1.0.0", canonical_version="v1")
    input_row = OrderLineFulfillmentInput.new(
        tenant_id="t1",
        subject_id="ol1",
        as_of_ts=datetime(2024, 1, 10, tzinfo=timezone.utc),
        need_by_date=date(2024, 1, 15),
        open_quantity=10.0,
        projected_available_quantity=5.0,
        config_version="cfg123",
        canonical_version="v1",
    )
    result = evaluate_order_line_fulfillment_risk(input_row, cfg)
    decision = result.decision
    assert decision.versions.primitive_version == "1.0.0"
    assert decision.versions.canonical_version == "v1"
    assert decision.versions.config_version == "cfg123"
    assert decision.evidence_refs
    # Verify decision state is one of the valid states
    assert decision.state in (order_line_rules.AT_RISK, order_line_rules.NOT_AT_RISK, order_line_rules.UNKNOWN)
    # Verify confidence level
    assert decision.confidence in ("HIGH", "MEDIUM", "LOW")
    # Verify metrics include required fields
    assert "open_quantity" in decision.metrics
    assert "projected_available_quantity" in decision.metrics
    assert "shortage_quantity" in decision.metrics

