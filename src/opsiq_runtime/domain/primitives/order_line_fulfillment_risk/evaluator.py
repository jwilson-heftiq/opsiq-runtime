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
from opsiq_runtime.domain.primitives.order_line_fulfillment_risk.config import OrderLineFulfillmentRiskConfig
from opsiq_runtime.domain.primitives.order_line_fulfillment_risk.model import OrderLineFulfillmentInput
from opsiq_runtime.domain.primitives.order_line_fulfillment_risk import rules


@dataclass(frozen=True)
class OrderLineFulfillmentResult:
    decision: DecisionResult
    evidence_set: EvidenceSet


def evaluate_order_line_fulfillment_risk(
    input_row: OrderLineFulfillmentInput, config: OrderLineFulfillmentRiskConfig
) -> OrderLineFulfillmentResult:
    """
    Evaluate order line fulfillment risk based on deterministic rules.
    
    Rule order:
    1. Missing required inputs (need_by_date, open_quantity, projected_available_quantity) => UNKNOWN
    2. is_on_hold == True => AT_RISK
    3. order_status (upper) in closed_statuses => NOT_AT_RISK
    4. open_quantity <= 0 => NOT_AT_RISK
    5. projected_available_quantity < open_quantity => AT_RISK
    6. Else => NOT_AT_RISK
    """
    # Rule 1: Missing required inputs
    if (
        input_row.need_by_date is None
        or input_row.open_quantity is None
        or input_row.projected_available_quantity is None
    ):
        decision_state = rules.UNKNOWN
        confidence = CONFIDENCE_LOW
        drivers = [rules.DRIVER_MISSING_REQUIRED_INPUTS]
        applied_rule_id = rules.RULE_UNKNOWN_MISSING_INPUTS
        shortage_quantity = 0.0
    # Rule 2: On hold
    elif input_row.is_on_hold is True:
        decision_state = rules.AT_RISK
        confidence = CONFIDENCE_HIGH
        drivers = [rules.DRIVER_ON_HOLD]
        applied_rule_id = rules.RULE_AT_RISK_ON_HOLD
        shortage_quantity = max(
            (input_row.open_quantity or 0.0) - (input_row.projected_available_quantity or 0.0), 0.0
        )
    # Rule 3: Closed status
    elif (
        input_row.order_status is not None
        and input_row.order_status.upper() in config.closed_statuses
    ):
        decision_state = rules.NOT_AT_RISK
        confidence = CONFIDENCE_HIGH
        drivers = [rules.DRIVER_NOT_OPEN]
        applied_rule_id = rules.RULE_NOT_AT_RISK_NOT_OPEN
        shortage_quantity = 0.0
    # Rule 4: No open quantity
    elif input_row.open_quantity is not None and input_row.open_quantity <= 0:
        decision_state = rules.NOT_AT_RISK
        confidence = CONFIDENCE_HIGH
        drivers = [rules.DRIVER_NO_OPEN_QTY]
        applied_rule_id = rules.RULE_NOT_AT_RISK_NO_OPEN_QTY
        shortage_quantity = 0.0
    # Rule 5: Projected short
    elif (
        input_row.projected_available_quantity is not None
        and input_row.open_quantity is not None
        and input_row.projected_available_quantity < input_row.open_quantity
    ):
        decision_state = rules.AT_RISK
        confidence = CONFIDENCE_HIGH
        drivers = [rules.DRIVER_PROJECTED_SHORT]
        applied_rule_id = rules.RULE_AT_RISK_PROJECTED_SHORT
        shortage_quantity = input_row.open_quantity - input_row.projected_available_quantity
    # Rule 6: Sufficient supply
    else:
        decision_state = rules.NOT_AT_RISK
        confidence = CONFIDENCE_HIGH
        drivers = [rules.DRIVER_SUFFICIENT_SUPPLY]
        applied_rule_id = rules.RULE_NOT_AT_RISK_SUFFICIENT_SUPPLY
        shortage_quantity = max(
            (input_row.open_quantity or 0.0) - (input_row.projected_available_quantity or 0.0), 0.0
        )

    # Build metrics
    metrics: dict[str, float | str | int] = {
        "open_quantity": float(input_row.open_quantity or 0.0),
        "projected_available_quantity": float(input_row.projected_available_quantity or 0.0),
        "shortage_quantity": float(shortage_quantity),
    }
    if input_row.need_by_date:
        metrics["need_by_date"] = input_row.need_by_date.isoformat()
    if input_row.release_shortage_qty is not None:
        metrics["release_shortage_qty"] = float(input_row.release_shortage_qty)
    if input_row.plant_shortage_qty is not None:
        metrics["plant_shortage_qty"] = float(input_row.plant_shortage_qty)
    if input_row.projected_onhand_qty_eod is not None:
        metrics["projected_onhand_qty_eod"] = float(input_row.projected_onhand_qty_eod)
    if input_row.supply_qty is not None:
        metrics["supply_qty"] = float(input_row.supply_qty)
    if input_row.demand_qty is not None:
        metrics["demand_qty"] = float(input_row.demand_qty)
    # Include ordernum, orderline, orderrelnum, and customer_id for aggregation and traceability
    if input_row.ordernum is not None:
        metrics["ordernum"] = input_row.ordernum
    if input_row.orderline is not None:
        metrics["orderline"] = input_row.orderline
    if input_row.orderrelnum is not None:
        metrics["orderrelnum"] = input_row.orderrelnum
    if input_row.customer_id:
        metrics["customer_id"] = input_row.customer_id

    # Build evidence references
    evidence_references: dict[str, Any] = {
        "applied_rule_id": applied_rule_id,
        "need_by_date": input_row.need_by_date.isoformat() if input_row.need_by_date else None,
        "open_quantity": input_row.open_quantity,
        "projected_available_quantity": input_row.projected_available_quantity,
        "order_status": input_row.order_status,
        "is_on_hold": input_row.is_on_hold,
        "closed_statuses": list(config.closed_statuses),
    }
    if input_row.partnum:
        evidence_references["partnum"] = input_row.partnum
    if input_row.customer_id:
        evidence_references["customer_id"] = input_row.customer_id
    if input_row.plant:
        evidence_references["plant"] = input_row.plant
    if input_row.warehouse:
        evidence_references["warehouse"] = input_row.warehouse

    # Build evidence
    evidence_id = f"evidence-{input_row.subject_id}"
    evidence = Evidence(
        evidence_id=evidence_id,
        rule_ids=[applied_rule_id],
        thresholds={"closed_statuses": list(config.closed_statuses)},
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

    return OrderLineFulfillmentResult(decision=decision, evidence_set=EvidenceSet(evidence=[evidence]))

