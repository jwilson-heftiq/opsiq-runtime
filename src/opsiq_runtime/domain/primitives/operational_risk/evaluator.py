from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Tuple

from opsiq_runtime.domain.common.decision import (
    CONFIDENCE_HIGH,
    CONFIDENCE_LOW,
    DecisionResult,
)
from opsiq_runtime.domain.common.evidence import Evidence, EvidenceSet
from opsiq_runtime.domain.common.versioning import VersionInfo
from opsiq_runtime.domain.primitives.operational_risk.config import OperationalRiskConfig
from opsiq_runtime.domain.primitives.operational_risk.model import OperationalRiskInput
from opsiq_runtime.domain.primitives.operational_risk import rules


@dataclass(frozen=True)
class OperationalRiskResult:
    decision: DecisionResult
    evidence_set: EvidenceSet


def _compute_days_since(last_trip: datetime, as_of: datetime) -> int:
    delta = as_of.date() - last_trip.date()
    return delta.days


def evaluate_operational_risk(
    input_row: OperationalRiskInput, config: OperationalRiskConfig
) -> OperationalRiskResult:
    as_of = input_row.as_of_ts
    last_trip = input_row.last_trip_ts
    decision_state = rules.UNKNOWN
    days_since_last_trip = input_row.days_since_last_trip

    if last_trip is None:
        decision_state = rules.UNKNOWN
        confidence = CONFIDENCE_LOW  # Low confidence when no data
    else:
        computed_days = days_since_last_trip or _compute_days_since(last_trip, as_of)
        days_since_last_trip = computed_days
        decision_state = (
            rules.AT_RISK if computed_days >= config.at_risk_days else rules.NOT_AT_RISK
        )
        confidence = CONFIDENCE_HIGH  # High confidence when we have data

    evidence_id = f"evidence-{input_row.subject_id}"
    evidence = Evidence(
        evidence_id=evidence_id,
        rule_ids=[rules.RULE_ID_AT_RISK],
        thresholds={"at_risk_days": config.at_risk_days},
        references={
            "last_trip_ts": last_trip.isoformat() if last_trip else None,
            "days_since_last_trip": days_since_last_trip,
            "as_of_ts": as_of.isoformat(),
        },
        observed_at=datetime.now(timezone.utc),
    )

    versions = VersionInfo(
        primitive_version=config.primitive_version,
        canonical_version=config.canonical_version,
        config_version=input_row.config_version,
    )
    decision = DecisionResult(
        state=decision_state,
        confidence=confidence,
        drivers=["days_since_last_trip"],
        metrics={"days_since_last_trip": float(days_since_last_trip or -1)},
        evidence_refs=[evidence_id],
        versions=versions,
        computed_at=datetime.now(timezone.utc),
        valid_until=None,
    )

    return OperationalRiskResult(decision=decision, evidence_set=EvidenceSet(evidence=[evidence]))

