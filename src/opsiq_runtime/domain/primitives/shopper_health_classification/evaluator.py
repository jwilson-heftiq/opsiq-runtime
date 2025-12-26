from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from opsiq_runtime.domain.common.decision import (
    CONFIDENCE_HIGH,
    CONFIDENCE_LOW,
    CONFIDENCE_MEDIUM,
    DecisionResult,
)
from opsiq_runtime.domain.common.evidence import Evidence, EvidenceSet
from opsiq_runtime.domain.common.versioning import VersionInfo
from opsiq_runtime.domain.primitives.shopper_health_classification.config import ShopperHealthConfig
from opsiq_runtime.domain.primitives.shopper_health_classification.model import ShopperHealthInput
from opsiq_runtime.domain.primitives.shopper_health_classification import rules


@dataclass(frozen=True)
class ShopperHealthResult:
    decision: DecisionResult
    evidence_set: EvidenceSet


def evaluate_shopper_health_classification(
    input_row: ShopperHealthInput, config: ShopperHealthConfig
) -> ShopperHealthResult:
    """
    Evaluate shopper health classification based on composition of operational_risk and shopper_frequency_trend.
    
    Priority-ordered rules:
    1. AT_RISK => URGENT
    2. UNKNOWN + UNKNOWN => UNKNOWN (insufficient signals)
    3. NOT_AT_RISK + DECLINING => WATCHLIST
    4. UNKNOWN + DECLINING => WATCHLIST (low confidence)
    5. NOT_AT_RISK + (STABLE|IMPROVING) => HEALTHY
    6. Else => UNKNOWN (partial signals)
    """
    risk_state = input_row.risk_state or "UNKNOWN"
    trend_state = input_row.trend_state or "UNKNOWN"
    
    # Normalize states - handle None as UNKNOWN
    if risk_state is None:
        risk_state = "UNKNOWN"
    if trend_state is None:
        trend_state = "UNKNOWN"
    
    # Rule 1: AT_RISK dominates => URGENT
    if risk_state == "AT_RISK":
        decision_state = rules.URGENT
        confidence = CONFIDENCE_HIGH
        drivers = [rules.DRIVER_LAPSE_RISK]
        applied_rule_id = rules.RULE_URGENT_AT_RISK
    
    # Rule 2: Both UNKNOWN => UNKNOWN (insufficient signals)
    elif risk_state == "UNKNOWN" and trend_state == "UNKNOWN":
        decision_state = rules.UNKNOWN
        confidence = CONFIDENCE_LOW
        drivers = [rules.DRIVER_INSUFFICIENT_SIGNALS]
        applied_rule_id = rules.RULE_UNKNOWN_INSUFFICIENT_SIGNALS
    
    # Rule 3: NOT_AT_RISK + DECLINING => WATCHLIST
    elif risk_state == "NOT_AT_RISK" and trend_state == "DECLINING":
        decision_state = rules.WATCHLIST
        confidence = CONFIDENCE_MEDIUM
        drivers = [rules.DRIVER_CADENCE_DECLINING]
        applied_rule_id = rules.RULE_WATCHLIST_DECLINING
    
    # Rule 4: UNKNOWN + DECLINING => WATCHLIST (low confidence)
    elif risk_state == "UNKNOWN" and trend_state == "DECLINING":
        decision_state = rules.WATCHLIST
        confidence = CONFIDENCE_LOW
        drivers = [rules.DRIVER_CADENCE_DECLINING, rules.DRIVER_RISK_UNKNOWN]
        applied_rule_id = rules.RULE_WATCHLIST_DECLINING_RISK_UNKNOWN
    
    # Rule 5: NOT_AT_RISK + (STABLE|IMPROVING) => HEALTHY
    elif risk_state == "NOT_AT_RISK" and trend_state in ("STABLE", "IMPROVING"):
        decision_state = rules.HEALTHY
        confidence = CONFIDENCE_HIGH
        drivers = [rules.DRIVER_RISK_OK, rules.DRIVER_CADENCE_OK]
        applied_rule_id = rules.RULE_HEALTHY_OK
    
    # Rule 6: Else => UNKNOWN (partial signals)
    else:
        decision_state = rules.UNKNOWN
        confidence = CONFIDENCE_MEDIUM
        drivers = [rules.DRIVER_PARTIAL_SIGNALS]
        applied_rule_id = rules.RULE_UNKNOWN_PARTIAL_SIGNALS
    
    # Build source primitives array
    source_primitives = []
    
    # Add operational_risk source if available
    if input_row.risk_state is not None:
        source_primitives.append({
            "primitive_name": "operational_risk",
            "primitive_version": "1.0.0",  # Default version if not available from source
            "as_of_ts": input_row.risk_source_as_of_ts.isoformat() if input_row.risk_source_as_of_ts else input_row.as_of_ts.isoformat(),
            "evidence_refs": input_row.risk_evidence_refs,
        })
    
    # Add shopper_frequency_trend source if available
    if input_row.trend_state is not None:
        source_primitives.append({
            "primitive_name": "shopper_frequency_trend",
            "primitive_version": "1.0.0",  # Default version if not available from source
            "as_of_ts": input_row.trend_source_as_of_ts.isoformat() if input_row.trend_source_as_of_ts else input_row.as_of_ts.isoformat(),
            "evidence_refs": input_row.trend_evidence_refs,
        })
    
    # Build evidence
    evidence_id = f"evidence-{input_row.subject_id}"
    evidence = Evidence(
        evidence_id=evidence_id,
        rule_ids=[applied_rule_id],
        thresholds={},  # No thresholds for v1.0.0
        references={
            "applied_rule_id": applied_rule_id,
            "source_primitives": source_primitives,
            "composition_inputs": {
                "risk_state": risk_state,
                "trend_state": trend_state,
            },
        },
        observed_at=datetime.now(timezone.utc),
    )
    
    # Build metrics
    # Note: metrics is typed as Dict[str, float], but user requirements specify strings for states
    # JSON serialization will handle strings correctly even with this type hint
    metrics = {
        "risk_state": risk_state,
        "trend_state": trend_state,
    }
    if input_row.risk_source_as_of_ts:
        metrics["risk_source_as_of_ts"] = input_row.risk_source_as_of_ts.isoformat()
    if input_row.trend_source_as_of_ts:
        metrics["trend_source_as_of_ts"] = input_row.trend_source_as_of_ts.isoformat()
    
    # Build decision
    versions = VersionInfo(
        primitive_version=config.primitive_version,
        canonical_version=config.canonical_version,
        config_version=input_row.config_version,
    )
    decision = DecisionResult(
        state=decision_state,
        confidence=confidence,
        drivers=drivers,
        metrics=metrics,
        evidence_refs=[evidence_id],
        versions=versions,
        computed_at=datetime.now(timezone.utc),
        valid_until=None,
    )
    
    return ShopperHealthResult(decision=decision, evidence_set=EvidenceSet(evidence=[evidence]))

