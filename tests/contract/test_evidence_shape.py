from datetime import datetime, timezone

from opsiq_runtime.domain.primitives.operational_risk.config import OperationalRiskConfig
from opsiq_runtime.domain.primitives.operational_risk.evaluator import evaluate_operational_risk
from opsiq_runtime.domain.primitives.operational_risk.model import OperationalRiskInput


def test_evidence_contains_thresholds_and_references():
    cfg = OperationalRiskConfig(at_risk_days=7)
    input_row = OperationalRiskInput.new(
        tenant_id="t1",
        subject_id="s1",
        as_of_ts=datetime(2024, 1, 10, tzinfo=timezone.utc),
        last_trip_ts=datetime(2024, 1, 1, tzinfo=timezone.utc),
        days_since_last_trip=None,
        config_version="cfgX",
        canonical_version="v1",
    )
    result = evaluate_operational_risk(input_row, cfg)
    evidence_set = result.evidence_set
    assert evidence_set.evidence
    evidence = evidence_set.evidence[0]
    assert "at_risk_days" in evidence.thresholds
    assert evidence.references.get("last_trip_ts") is not None
    assert evidence.references.get("days_since_last_trip") is not None

