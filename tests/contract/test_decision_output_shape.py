from datetime import datetime, timezone

from opsiq_runtime.domain.primitives.operational_risk.config import OperationalRiskConfig
from opsiq_runtime.domain.primitives.operational_risk.evaluator import evaluate_operational_risk
from opsiq_runtime.domain.primitives.operational_risk.model import OperationalRiskInput


def test_decision_includes_versions_and_evidence_refs():
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

