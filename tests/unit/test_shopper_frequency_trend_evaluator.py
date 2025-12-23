from datetime import datetime, timezone

from opsiq_runtime.domain.primitives.shopper_frequency_trend.config import ShopperFrequencyTrendConfig
from opsiq_runtime.domain.primitives.shopper_frequency_trend.evaluator import evaluate_shopper_frequency_trend
from opsiq_runtime.domain.primitives.shopper_frequency_trend.model import ShopperFrequencyInput
from opsiq_runtime.domain.primitives.shopper_frequency_trend import rules


def make_input(
    last_trip: datetime | None = None,
    prev_trip: datetime | None = None,
    recent_gap_days: float | None = None,
    baseline_avg_gap_days: float | None = None,
    baseline_trip_count: int | None = None,
    baseline_window_days: int | None = None,
) -> ShopperFrequencyInput:
    return ShopperFrequencyInput.new(
        tenant_id="t1",
        subject_id="s1",
        as_of_ts=datetime(2024, 1, 10, tzinfo=timezone.utc),
        last_trip_ts=last_trip,
        prev_trip_ts=prev_trip,
        recent_gap_days=recent_gap_days,
        baseline_avg_gap_days=baseline_avg_gap_days,
        baseline_trip_count=baseline_trip_count,
        baseline_window_days=baseline_window_days,
        config_version="cfg",
        canonical_version="v1",
    )


def test_unknown_when_last_trip_missing():
    """Test UNKNOWN when last_trip_ts is None."""
    cfg = ShopperFrequencyTrendConfig()
    res = evaluate_shopper_frequency_trend(make_input(last_trip=None), cfg)
    assert res.decision.state == rules.UNKNOWN
    assert res.decision.confidence == "LOW"
    assert rules.RULE_ID_INSUFFICIENT_TRIP_HISTORY in res.decision.drivers


def test_unknown_when_prev_trip_missing():
    """Test UNKNOWN when prev_trip_ts is None."""
    cfg = ShopperFrequencyTrendConfig()
    last_trip = datetime(2024, 1, 5, tzinfo=timezone.utc)
    res = evaluate_shopper_frequency_trend(make_input(last_trip=last_trip, prev_trip=None), cfg)
    assert res.decision.state == rules.UNKNOWN
    assert res.decision.confidence == "LOW"
    assert rules.RULE_ID_INSUFFICIENT_TRIP_HISTORY in res.decision.drivers


def test_unknown_when_baseline_trip_count_insufficient():
    """Test UNKNOWN when baseline_trip_count < min_baseline_trips."""
    cfg = ShopperFrequencyTrendConfig(min_baseline_trips=4)
    last_trip = datetime(2024, 1, 5, tzinfo=timezone.utc)
    prev_trip = datetime(2024, 1, 1, tzinfo=timezone.utc)
    res = evaluate_shopper_frequency_trend(
        make_input(
            last_trip=last_trip,
            prev_trip=prev_trip,
            baseline_trip_count=3,  # Less than min_baseline_trips
            baseline_avg_gap_days=10.0,
            recent_gap_days=15.0,
        ),
        cfg,
    )
    assert res.decision.state == rules.UNKNOWN
    assert res.decision.confidence == "LOW"
    assert rules.RULE_ID_INSUFFICIENT_BASELINE in res.decision.drivers


def test_unknown_when_baseline_trip_count_none():
    """Test UNKNOWN when baseline_trip_count is None."""
    cfg = ShopperFrequencyTrendConfig()
    last_trip = datetime(2024, 1, 5, tzinfo=timezone.utc)
    prev_trip = datetime(2024, 1, 1, tzinfo=timezone.utc)
    res = evaluate_shopper_frequency_trend(
        make_input(
            last_trip=last_trip,
            prev_trip=prev_trip,
            baseline_trip_count=None,
            baseline_avg_gap_days=10.0,
            recent_gap_days=15.0,
        ),
        cfg,
    )
    assert res.decision.state == rules.UNKNOWN
    assert res.decision.confidence == "LOW"
    assert rules.RULE_ID_INSUFFICIENT_BASELINE in res.decision.drivers


def test_unknown_when_baseline_avg_gap_invalid():
    """Test UNKNOWN when baseline_avg_gap_days is None or <= 0."""
    cfg = ShopperFrequencyTrendConfig()
    last_trip = datetime(2024, 1, 5, tzinfo=timezone.utc)
    prev_trip = datetime(2024, 1, 1, tzinfo=timezone.utc)
    
    # Test None
    res = evaluate_shopper_frequency_trend(
        make_input(
            last_trip=last_trip,
            prev_trip=prev_trip,
            baseline_trip_count=5,
            baseline_avg_gap_days=None,
            recent_gap_days=15.0,
        ),
        cfg,
    )
    assert res.decision.state == rules.UNKNOWN
    assert rules.RULE_ID_BASELINE_INVALID in res.decision.drivers
    
    # Test <= 0
    res = evaluate_shopper_frequency_trend(
        make_input(
            last_trip=last_trip,
            prev_trip=prev_trip,
            baseline_trip_count=5,
            baseline_avg_gap_days=0.0,
            recent_gap_days=15.0,
        ),
        cfg,
    )
    assert res.decision.state == rules.UNKNOWN
    assert rules.RULE_ID_BASELINE_INVALID in res.decision.drivers


def test_unknown_when_recent_gap_missing():
    """Test UNKNOWN when recent_gap_days is None."""
    cfg = ShopperFrequencyTrendConfig()
    last_trip = datetime(2024, 1, 5, tzinfo=timezone.utc)
    prev_trip = datetime(2024, 1, 1, tzinfo=timezone.utc)
    res = evaluate_shopper_frequency_trend(
        make_input(
            last_trip=last_trip,
            prev_trip=prev_trip,
            baseline_trip_count=5,
            baseline_avg_gap_days=10.0,
            recent_gap_days=None,
        ),
        cfg,
    )
    assert res.decision.state == rules.UNKNOWN
    assert res.decision.confidence == "LOW"
    assert rules.RULE_ID_RECENT_GAP_MISSING in res.decision.drivers


def test_unknown_when_recent_gap_out_of_range():
    """Test UNKNOWN when recent_gap_days > max_reasonable_gap_days."""
    cfg = ShopperFrequencyTrendConfig(max_reasonable_gap_days=365)
    last_trip = datetime(2024, 1, 5, tzinfo=timezone.utc)
    prev_trip = datetime(2024, 1, 1, tzinfo=timezone.utc)
    res = evaluate_shopper_frequency_trend(
        make_input(
            last_trip=last_trip,
            prev_trip=prev_trip,
            baseline_trip_count=5,
            baseline_avg_gap_days=10.0,
            recent_gap_days=400.0,  # Exceeds max_reasonable_gap_days
        ),
        cfg,
    )
    assert res.decision.state == rules.UNKNOWN
    assert res.decision.confidence == "LOW"
    assert rules.RULE_ID_RECENT_GAP_OUT_OF_RANGE in res.decision.drivers


def test_declining_when_ratio_exceeds_threshold():
    """Test DECLINING when ratio >= decline_ratio_threshold."""
    cfg = ShopperFrequencyTrendConfig(decline_ratio_threshold=1.5)
    last_trip = datetime(2024, 1, 5, tzinfo=timezone.utc)
    prev_trip = datetime(2024, 1, 1, tzinfo=timezone.utc)
    # recent_gap_days = 20, baseline_avg_gap_days = 10, ratio = 2.0 >= 1.5
    res = evaluate_shopper_frequency_trend(
        make_input(
            last_trip=last_trip,
            prev_trip=prev_trip,
            baseline_trip_count=5,
            baseline_avg_gap_days=10.0,
            recent_gap_days=20.0,
        ),
        cfg,
    )
    assert res.decision.state == rules.DECLINING
    assert res.decision.confidence == "HIGH"
    assert rules.RULE_ID_CADENCE_SLOWING in res.decision.drivers
    assert res.decision.metrics["ratio"] == 2.0


def test_improving_when_ratio_below_threshold():
    """Test IMPROVING when ratio <= improve_ratio_threshold."""
    cfg = ShopperFrequencyTrendConfig(improve_ratio_threshold=0.75)
    last_trip = datetime(2024, 1, 5, tzinfo=timezone.utc)
    prev_trip = datetime(2024, 1, 1, tzinfo=timezone.utc)
    # recent_gap_days = 5, baseline_avg_gap_days = 10, ratio = 0.5 <= 0.75
    res = evaluate_shopper_frequency_trend(
        make_input(
            last_trip=last_trip,
            prev_trip=prev_trip,
            baseline_trip_count=5,
            baseline_avg_gap_days=10.0,
            recent_gap_days=5.0,
        ),
        cfg,
    )
    assert res.decision.state == rules.IMPROVING
    assert res.decision.confidence == "HIGH"
    assert rules.RULE_ID_CADENCE_ACCELERATING in res.decision.drivers
    assert res.decision.metrics["ratio"] == 0.5


def test_stable_when_ratio_between_thresholds():
    """Test STABLE when ratio is between improve and decline thresholds."""
    cfg = ShopperFrequencyTrendConfig(
        decline_ratio_threshold=1.5,
        improve_ratio_threshold=0.75,
    )
    last_trip = datetime(2024, 1, 5, tzinfo=timezone.utc)
    prev_trip = datetime(2024, 1, 1, tzinfo=timezone.utc)
    # recent_gap_days = 10, baseline_avg_gap_days = 10, ratio = 1.0 (between 0.75 and 1.5)
    res = evaluate_shopper_frequency_trend(
        make_input(
            last_trip=last_trip,
            prev_trip=prev_trip,
            baseline_trip_count=5,
            baseline_avg_gap_days=10.0,
            recent_gap_days=10.0,
        ),
        cfg,
    )
    assert res.decision.state == rules.STABLE
    assert res.decision.confidence == "HIGH"
    assert rules.RULE_ID_CADENCE_STABLE in res.decision.drivers
    assert res.decision.metrics["ratio"] == 1.0


def test_recent_gap_computed_from_timestamps():
    """Test that recent_gap_days is computed from last_trip_ts and prev_trip_ts if missing."""
    cfg = ShopperFrequencyTrendConfig()
    last_trip = datetime(2024, 1, 5, tzinfo=timezone.utc)
    prev_trip = datetime(2024, 1, 1, tzinfo=timezone.utc)
    # Should compute recent_gap_days = 4 days
    res = evaluate_shopper_frequency_trend(
        make_input(
            last_trip=last_trip,
            prev_trip=prev_trip,
            recent_gap_days=None,  # Will be computed
            baseline_trip_count=5,
            baseline_avg_gap_days=10.0,
        ),
        cfg,
    )
    # Should compute 4 days from timestamps
    assert res.decision.state == rules.IMPROVING  # 4/10 = 0.4 <= 0.75
    assert res.decision.metrics["recent_gap_days"] == 4.0


def test_evidence_includes_all_required_fields():
    """Test that evidence includes rule_ids, thresholds, timestamps, and metrics."""
    cfg = ShopperFrequencyTrendConfig()
    last_trip = datetime(2024, 1, 5, tzinfo=timezone.utc)
    prev_trip = datetime(2024, 1, 1, tzinfo=timezone.utc)
    res = evaluate_shopper_frequency_trend(
        make_input(
            last_trip=last_trip,
            prev_trip=prev_trip,
            baseline_trip_count=5,
            baseline_avg_gap_days=10.0,
            recent_gap_days=15.0,
        ),
        cfg,
    )
    
    assert len(res.evidence_set.evidence) == 1
    evidence = res.evidence_set.evidence[0]
    
    # Check rule_ids
    assert len(evidence.rule_ids) > 0
    
    # Check thresholds
    assert "min_baseline_trips" in evidence.thresholds
    assert "decline_ratio_threshold" in evidence.thresholds
    assert "improve_ratio_threshold" in evidence.thresholds
    assert "max_reasonable_gap_days" in evidence.thresholds
    assert "baseline_window_days" in evidence.thresholds
    
    # Check references (timestamps and metrics)
    assert "as_of_ts" in evidence.references
    assert "last_trip_ts" in evidence.references
    assert "prev_trip_ts" in evidence.references
    assert "recent_gap_days" in evidence.references
    assert "baseline_avg_gap_days" in evidence.references
    assert "baseline_trip_count" in evidence.references
    assert "ratio" in evidence.references

