from datetime import datetime, timedelta, timezone

from opsiq_runtime.domain.primitives.operational_risk.config import OperationalRiskConfig
from opsiq_runtime.domain.primitives.operational_risk.evaluator import evaluate_operational_risk
from opsiq_runtime.domain.primitives.operational_risk.model import OperationalRiskInput
from opsiq_runtime.domain.primitives.operational_risk import rules


def make_input(last_trip: datetime | None, days: int | None = None) -> OperationalRiskInput:
    return OperationalRiskInput.new(
        tenant_id="t1",
        subject_id="s1",
        as_of_ts=datetime(2024, 1, 10, tzinfo=timezone.utc),
        last_trip_ts=last_trip,
        days_since_last_trip=days,
        config_version="cfg",
        canonical_version="v1",
    )


def test_unknown_when_last_trip_missing():
    cfg = OperationalRiskConfig(at_risk_days=7)
    res = evaluate_operational_risk(make_input(None), cfg)
    assert res.decision.state == rules.UNKNOWN


def test_at_risk_when_days_exceed_threshold():
    cfg = OperationalRiskConfig(at_risk_days=7)
    last_trip = datetime(2024, 1, 1, tzinfo=timezone.utc)
    res = evaluate_operational_risk(make_input(last_trip), cfg)
    assert res.decision.state == rules.AT_RISK


def test_not_at_risk_when_under_threshold():
    cfg = OperationalRiskConfig(at_risk_days=10)
    last_trip = datetime(2024, 1, 5, tzinfo=timezone.utc)
    as_of = datetime(2024, 1, 10, tzinfo=timezone.utc)
    res = evaluate_operational_risk(
        OperationalRiskInput.new(
            tenant_id="t1",
            subject_id="s1",
            as_of_ts=as_of,
            last_trip_ts=last_trip,
            days_since_last_trip=2,
            config_version="cfg",
            canonical_version="v1",
        ),
        cfg,
    )
    assert res.decision.state == rules.NOT_AT_RISK

