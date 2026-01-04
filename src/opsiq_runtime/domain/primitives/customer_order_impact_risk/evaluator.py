from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from opsiq_runtime.domain.common.decision import (
    CONFIDENCE_HIGH,
    CONFIDENCE_LOW,
    CONFIDENCE_MEDIUM,
    DecisionResult,
)
from opsiq_runtime.domain.common.evidence import Evidence, EvidenceSet
from opsiq_runtime.domain.common.versioning import VersionInfo
from opsiq_runtime.domain.primitives.customer_order_impact_risk.config import CustomerImpactConfig
from opsiq_runtime.domain.primitives.customer_order_impact_risk.model import CustomerImpactInput
from opsiq_runtime.domain.primitives.customer_order_impact_risk import rules


@dataclass(frozen=True)
class CustomerImpactResult:
    decision: DecisionResult
    evidence_set: EvidenceSet


def evaluate_customer_order_impact_risk(
    input_row: CustomerImpactInput, config: CustomerImpactConfig
) -> CustomerImpactResult:
    """
    Evaluate customer order impact risk by aggregating order decisions.
    
    Rule order:
    1. total == 0 => UNKNOWN (NO_ORDERS_FOUND)
    2. at_risk >= high_threshold => HIGH_IMPACT
    3. at_risk >= medium_threshold => MEDIUM_IMPACT
    4. at_risk > 0 => LOW_IMPACT
    5. unknown == total => UNKNOWN (ALL_ORDERS_UNKNOWN)
    6. else => LOW_IMPACT (NO_AT_RISK_ORDERS)
    
    Confidence:
    - HIGH for HIGH_IMPACT/MEDIUM_IMPACT
    - MEDIUM for LOW_IMPACT
    - LOW for UNKNOWN
    """
    # Rule 1: No orders found
    if input_row.order_count_total == 0:
        decision_state = rules.UNKNOWN
        confidence = CONFIDENCE_LOW
        drivers = [rules.DRIVER_NO_ORDERS_FOUND]
        applied_rule_id = rules.RULE_UNKNOWN_NO_ORDERS_FOUND
    # Rule 2: High impact (at_risk >= high_threshold)
    elif input_row.order_count_at_risk >= config.high_threshold:
        decision_state = rules.HIGH_IMPACT
        confidence = CONFIDENCE_HIGH
        drivers = [rules.DRIVER_HIGH_IMPACT]
        applied_rule_id = rules.RULE_HIGH_IMPACT
    # Rule 3: Medium impact (at_risk >= medium_threshold)
    elif input_row.order_count_at_risk >= config.medium_threshold:
        decision_state = rules.MEDIUM_IMPACT
        confidence = CONFIDENCE_HIGH
        drivers = [rules.DRIVER_MEDIUM_IMPACT]
        applied_rule_id = rules.RULE_MEDIUM_IMPACT
    # Rule 4: Low impact (at_risk > 0)
    elif input_row.order_count_at_risk > 0:
        decision_state = rules.LOW_IMPACT
        confidence = CONFIDENCE_MEDIUM
        drivers = [rules.DRIVER_LOW_IMPACT]
        applied_rule_id = rules.RULE_LOW_IMPACT
    # Rule 5: All orders unknown
    elif input_row.order_count_unknown == input_row.order_count_total:
        decision_state = rules.UNKNOWN
        confidence = CONFIDENCE_LOW
        drivers = [rules.DRIVER_ALL_ORDERS_UNKNOWN]
        applied_rule_id = rules.RULE_UNKNOWN_ALL_ORDERS_UNKNOWN
    # Rule 6: No at-risk orders
    else:
        decision_state = rules.LOW_IMPACT
        confidence = CONFIDENCE_MEDIUM
        drivers = [rules.DRIVER_NO_AT_RISK_ORDERS]
        applied_rule_id = rules.RULE_LOW_IMPACT_NO_AT_RISK
    
    # Build metrics
    metrics: dict[str, int | list[str]] = {
        "order_count_total": input_row.order_count_total,
        "order_count_at_risk": input_row.order_count_at_risk,
        "order_count_unknown": input_row.order_count_unknown,
        "at_risk_order_subject_ids": input_row.at_risk_order_subject_ids,
    }
    
    # Build evidence references
    # Cap source_orders to avoid excessive size (keep first 100)
    source_orders_capped = input_row.source_order_refs[:100]
    source_orders_json = [
        {
            "order_subject_id": ref.order_subject_id,
            "decision_state": ref.decision_state,
            "evidence_refs": ref.evidence_refs,
        }
        for ref in source_orders_capped
    ]
    
    evidence_references: dict[str, Any] = {
        "applied_rule_id": applied_rule_id,
        "source_orders": source_orders_json,
        "rollup_counts": {
            "total": input_row.order_count_total,
            "at_risk": input_row.order_count_at_risk,
            "unknown": input_row.order_count_unknown,
        },
        "thresholds": {
            "high_threshold": config.high_threshold,
            "medium_threshold": config.medium_threshold,
        },
    }
    
    # Build evidence
    evidence_id = f"evidence-{input_row.subject_id}"
    evidence = Evidence(
        evidence_id=evidence_id,
        rule_ids=[applied_rule_id],
        thresholds={
            "high_threshold": config.high_threshold,
            "medium_threshold": config.medium_threshold,
        },
        references=evidence_references,
        observed_at=datetime.now(timezone.utc),
    )
    
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
    
    return CustomerImpactResult(decision=decision, evidence_set=EvidenceSet(evidence=[evidence]))

