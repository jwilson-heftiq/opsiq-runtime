from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from opsiq_runtime.domain.common.decision import (
    CONFIDENCE_HIGH,
    CONFIDENCE_LOW,
    DecisionResult,
)
from opsiq_runtime.domain.common.evidence import Evidence, EvidenceSet
from opsiq_runtime.domain.common.versioning import VersionInfo
from opsiq_runtime.domain.primitives.shopper_frequency_trend.config import ShopperFrequencyTrendConfig
from opsiq_runtime.domain.primitives.shopper_frequency_trend.model import ShopperFrequencyInput
from opsiq_runtime.domain.primitives.shopper_frequency_trend import rules


@dataclass(frozen=True)
class ShopperFrequencyTrendResult:
    decision: DecisionResult
    evidence_set: EvidenceSet


def evaluate_shopper_frequency_trend(
    input_row: ShopperFrequencyInput, config: ShopperFrequencyTrendConfig
) -> ShopperFrequencyTrendResult:
    """
    Evaluate shopper frequency trend based on deterministic rules.
    
    Returns DECLINING, STABLE, IMPROVING, or UNKNOWN based on:
    - Trip history availability
    - Baseline data sufficiency
    - Ratio of recent_gap_days to baseline_avg_gap_days
    """
    decision_state = rules.UNKNOWN
    driver_code = rules.RULE_ID_INSUFFICIENT_TRIP_HISTORY
    confidence = CONFIDENCE_LOW
    rule_ids = []
    ratio = None

    # Rule 1: Check for sufficient trip history
    if input_row.last_trip_ts is None or input_row.prev_trip_ts is None:
        decision_state = rules.UNKNOWN
        driver_code = rules.RULE_ID_INSUFFICIENT_TRIP_HISTORY
        confidence = CONFIDENCE_LOW
        rule_ids = [rules.RULE_ID_INSUFFICIENT_TRIP_HISTORY]
    # Rule 2: Check baseline trip count
    elif input_row.baseline_trip_count is None or input_row.baseline_trip_count < config.min_baseline_trips:
        decision_state = rules.UNKNOWN
        driver_code = rules.RULE_ID_INSUFFICIENT_BASELINE
        confidence = CONFIDENCE_LOW
        rule_ids = [rules.RULE_ID_INSUFFICIENT_BASELINE]
    # Rule 3: Check baseline validity
    elif input_row.baseline_avg_gap_days is None or input_row.baseline_avg_gap_days <= 0:
        decision_state = rules.UNKNOWN
        driver_code = rules.RULE_ID_BASELINE_INVALID
        confidence = CONFIDENCE_LOW
        rule_ids = [rules.RULE_ID_BASELINE_INVALID]
    # Rule 4: Check recent gap availability
    elif input_row.recent_gap_days is None:
        decision_state = rules.UNKNOWN
        driver_code = rules.RULE_ID_RECENT_GAP_MISSING
        confidence = CONFIDENCE_LOW
        rule_ids = [rules.RULE_ID_RECENT_GAP_MISSING]
    # Rule 5: Check recent gap reasonableness
    elif input_row.recent_gap_days > config.max_reasonable_gap_days:
        decision_state = rules.UNKNOWN
        driver_code = rules.RULE_ID_RECENT_GAP_OUT_OF_RANGE
        confidence = CONFIDENCE_LOW
        rule_ids = [rules.RULE_ID_RECENT_GAP_OUT_OF_RANGE]
    # Rule 6: Compute ratio and classify
    else:
        ratio = input_row.recent_gap_days / input_row.baseline_avg_gap_days
        if ratio >= config.decline_ratio_threshold:
            decision_state = rules.DECLINING
            driver_code = rules.RULE_ID_CADENCE_SLOWING
            confidence = CONFIDENCE_HIGH
            rule_ids = [rules.RULE_ID_CADENCE_SLOWING]
        elif ratio <= config.improve_ratio_threshold:
            decision_state = rules.IMPROVING
            driver_code = rules.RULE_ID_CADENCE_ACCELERATING
            confidence = CONFIDENCE_HIGH
            rule_ids = [rules.RULE_ID_CADENCE_ACCELERATING]
        else:
            decision_state = rules.STABLE
            driver_code = rules.RULE_ID_CADENCE_STABLE
            confidence = CONFIDENCE_HIGH
            rule_ids = [rules.RULE_ID_CADENCE_STABLE]

    # Build evidence
    evidence_id = f"evidence-{input_row.subject_id}"
    evidence = Evidence(
        evidence_id=evidence_id,
        rule_ids=rule_ids,
        thresholds={
            "min_baseline_trips": config.min_baseline_trips,
            "decline_ratio_threshold": config.decline_ratio_threshold,
            "improve_ratio_threshold": config.improve_ratio_threshold,
            "max_reasonable_gap_days": config.max_reasonable_gap_days,
            "baseline_window_days": config.baseline_window_days,
        },
        references={
            "as_of_ts": input_row.as_of_ts.isoformat(),
            "last_trip_ts": input_row.last_trip_ts.isoformat() if input_row.last_trip_ts else None,
            "prev_trip_ts": input_row.prev_trip_ts.isoformat() if input_row.prev_trip_ts else None,
            "recent_gap_days": input_row.recent_gap_days,
            "baseline_avg_gap_days": input_row.baseline_avg_gap_days,
            "baseline_trip_count": input_row.baseline_trip_count,
            "ratio": ratio,
        },
        observed_at=datetime.now(timezone.utc),
    )

    # Build decision
    versions = VersionInfo(
        primitive_version=config.primitive_version,
        canonical_version=config.canonical_version,
        config_version=input_row.config_version,
    )
    
    metrics = {
        "recent_gap_days": input_row.recent_gap_days if input_row.recent_gap_days is not None else -1.0,
        "baseline_avg_gap_days": input_row.baseline_avg_gap_days if input_row.baseline_avg_gap_days is not None else -1.0,
        "baseline_trip_count": float(input_row.baseline_trip_count if input_row.baseline_trip_count is not None else -1),
    }
    if ratio is not None:
        metrics["ratio"] = ratio

    decision = DecisionResult(
        state=decision_state,
        confidence=confidence,
        drivers=[driver_code],
        metrics=metrics,
        evidence_refs=[evidence_id],
        versions=versions,
        computed_at=datetime.now(timezone.utc),
        valid_until=None,
    )

    return ShopperFrequencyTrendResult(decision=decision, evidence_set=EvidenceSet(evidence=[evidence]))

