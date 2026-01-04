from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from opsiq_runtime.domain.common.decision import (
    CONFIDENCE_HIGH,
    CONFIDENCE_LOW,
    DecisionResult,
)
from opsiq_runtime.domain.common.evidence import Evidence, EvidenceSet
from opsiq_runtime.domain.common.versioning import VersionInfo
from opsiq_runtime.domain.primitives.order_fulfillment_risk.config import OrderRiskConfig
from opsiq_runtime.domain.primitives.order_fulfillment_risk.model import OrderRiskInput
from opsiq_runtime.domain.primitives.order_fulfillment_risk import rules


@dataclass(frozen=True)
class OrderRiskResult:
    decision: DecisionResult
    evidence_set: EvidenceSet


def evaluate_order_fulfillment_risk(
    input_row: OrderRiskInput, config: OrderRiskConfig
) -> OrderRiskResult:
    """
    Evaluate order fulfillment risk by aggregating order_line decisions.
    
    Rule order:
    1. total == 0 => UNKNOWN (NO_LINES_FOUND)
    2. at_risk > 0 => AT_RISK (HAS_AT_RISK_LINES)
    3. unknown == total => UNKNOWN (ALL_LINES_UNKNOWN)
    4. else => NOT_AT_RISK (ALL_LINES_OK)
    
    Confidence:
    - HIGH for AT_RISK/NOT_AT_RISK
    - LOW for UNKNOWN
    """
    # Rule 1: No lines found
    if input_row.order_line_count_total == 0:
        decision_state = rules.UNKNOWN
        confidence = CONFIDENCE_LOW
        drivers = [rules.DRIVER_NO_LINES_FOUND]
        applied_rule_id = rules.RULE_UNKNOWN_NO_LINES_FOUND
    # Rule 2: Has at-risk lines
    elif input_row.order_line_count_at_risk > 0:
        decision_state = rules.AT_RISK
        confidence = CONFIDENCE_HIGH
        drivers = [rules.DRIVER_HAS_AT_RISK_LINES]
        applied_rule_id = rules.RULE_AT_RISK_HAS_AT_RISK_LINES
    # Rule 3: All lines unknown
    elif input_row.order_line_count_unknown == input_row.order_line_count_total:
        decision_state = rules.UNKNOWN
        confidence = CONFIDENCE_LOW
        drivers = [rules.DRIVER_ALL_LINES_UNKNOWN]
        applied_rule_id = rules.RULE_UNKNOWN_ALL_LINES_UNKNOWN
    # Rule 4: All lines OK
    else:
        decision_state = rules.NOT_AT_RISK
        confidence = CONFIDENCE_HIGH
        drivers = [rules.DRIVER_ALL_LINES_OK]
        applied_rule_id = rules.RULE_NOT_AT_RISK_ALL_LINES_OK
    
    # Build metrics
    metrics: dict[str, int | list[str] | str] = {
        "order_line_count_total": input_row.order_line_count_total,
        "order_line_count_at_risk": input_row.order_line_count_at_risk,
        "order_line_count_unknown": input_row.order_line_count_unknown,
        "order_line_count_not_at_risk": input_row.order_line_count_not_at_risk,
        "at_risk_line_subject_ids": input_row.at_risk_line_subject_ids,
    }
    # Include customer_id for customer rollup
    if input_row.customer_id:
        metrics["customer_id"] = input_row.customer_id
    
    # Build evidence references
    # Cap source_lines to avoid excessive size (keep first 100)
    source_lines_capped = input_row.source_line_refs[:100]
    source_lines_json = [
        {
            "line_subject_id": ref.line_subject_id,
            "decision_state": ref.decision_state,
            "evidence_refs": ref.evidence_refs,
        }
        for ref in source_lines_capped
    ]
    
    evidence_references: dict[str, Any] = {
        "applied_rule_id": applied_rule_id,
        "source_lines": source_lines_json,
        "rollup_counts": {
            "total": input_row.order_line_count_total,
            "at_risk": input_row.order_line_count_at_risk,
            "unknown": input_row.order_line_count_unknown,
            "not_at_risk": input_row.order_line_count_not_at_risk,
        },
    }
    
    # Build evidence
    evidence_id = f"evidence-{input_row.subject_id}"
    evidence = Evidence(
        evidence_id=evidence_id,
        rule_ids=[applied_rule_id],
        thresholds={},
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
    
    return OrderRiskResult(decision=decision, evidence_set=EvidenceSet(evidence=[evidence]))

