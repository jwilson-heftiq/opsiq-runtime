from datetime import datetime, timezone

from opsiq_runtime.domain.primitives.operational_risk.config import OperationalRiskConfig
from opsiq_runtime.domain.primitives.operational_risk.evaluator import evaluate_operational_risk
from opsiq_runtime.domain.primitives.operational_risk.model import OperationalRiskInput
from opsiq_runtime.domain.primitives.shopper_frequency_trend.config import ShopperFrequencyTrendConfig
from opsiq_runtime.domain.primitives.shopper_frequency_trend.evaluator import evaluate_shopper_frequency_trend
from opsiq_runtime.domain.primitives.shopper_frequency_trend.model import ShopperFrequencyInput


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

