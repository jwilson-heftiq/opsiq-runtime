from datetime import date, datetime, timezone

from opsiq_runtime.domain.primitives.order_line_fulfillment_risk.config import OrderLineFulfillmentRiskConfig
from opsiq_runtime.domain.primitives.order_line_fulfillment_risk.evaluator import evaluate_order_line_fulfillment_risk
from opsiq_runtime.domain.primitives.order_line_fulfillment_risk.model import OrderLineFulfillmentInput
from opsiq_runtime.domain.primitives.order_line_fulfillment_risk import rules


def make_input(
    need_by_date: date | None = None,
    open_quantity: float | None = None,
    projected_available_quantity: float | None = None,
    order_status: str | None = None,
    is_on_hold: bool | None = None,
    release_shortage_qty: float | None = None,
    plant_shortage_qty: float | None = None,
    projected_onhand_qty_eod: float | None = None,
    supply_qty: float | None = None,
    demand_qty: float | None = None,
    partnum: str | None = None,
    customer_id: str | None = None,
    plant: str | None = None,
    warehouse: str | None = None,
) -> OrderLineFulfillmentInput:
    as_of_ts = datetime(2024, 1, 10, tzinfo=timezone.utc)
    return OrderLineFulfillmentInput.new(
        tenant_id="t1",
        subject_id="ol1",
        as_of_ts=as_of_ts,
        config_version="cfg",
        canonical_version="v1",
        need_by_date=need_by_date,
        open_quantity=open_quantity,
        projected_available_quantity=projected_available_quantity,
        order_status=order_status,
        is_on_hold=is_on_hold,
        release_shortage_qty=release_shortage_qty,
        plant_shortage_qty=plant_shortage_qty,
        projected_onhand_qty_eod=projected_onhand_qty_eod,
        supply_qty=supply_qty,
        demand_qty=demand_qty,
        partnum=partnum,
        customer_id=customer_id,
        plant=plant,
        warehouse=warehouse,
    )


def test_unknown_when_missing_required_inputs():
    """Test UNKNOWN when need_by_date, open_quantity, or projected_available_quantity is None."""
    cfg = OrderLineFulfillmentRiskConfig()
    
    # Missing need_by_date
    res = evaluate_order_line_fulfillment_risk(
        make_input(open_quantity=10.0, projected_available_quantity=5.0), cfg
    )
    assert res.decision.state == rules.UNKNOWN
    assert res.decision.confidence == "LOW"
    assert rules.DRIVER_MISSING_REQUIRED_INPUTS in res.decision.drivers
    assert rules.RULE_UNKNOWN_MISSING_INPUTS in res.evidence_set.evidence[0].rule_ids
    
    # Missing open_quantity
    res = evaluate_order_line_fulfillment_risk(
        make_input(need_by_date=date(2024, 1, 15), projected_available_quantity=5.0), cfg
    )
    assert res.decision.state == rules.UNKNOWN
    assert rules.DRIVER_MISSING_REQUIRED_INPUTS in res.decision.drivers
    
    # Missing projected_available_quantity
    res = evaluate_order_line_fulfillment_risk(
        make_input(need_by_date=date(2024, 1, 15), open_quantity=10.0), cfg
    )
    assert res.decision.state == rules.UNKNOWN
    assert rules.DRIVER_MISSING_REQUIRED_INPUTS in res.decision.drivers


def test_at_risk_when_on_hold():
    """Test AT_RISK when is_on_hold == True."""
    cfg = OrderLineFulfillmentRiskConfig()
    res = evaluate_order_line_fulfillment_risk(
        make_input(
            need_by_date=date(2024, 1, 15),
            open_quantity=10.0,
            projected_available_quantity=15.0,
            is_on_hold=True,
        ),
        cfg,
    )
    assert res.decision.state == rules.AT_RISK
    assert res.decision.confidence == "HIGH"
    assert rules.DRIVER_ON_HOLD in res.decision.drivers
    assert rules.RULE_AT_RISK_ON_HOLD in res.evidence_set.evidence[0].rule_ids


def test_not_at_risk_when_closed_status():
    """Test NOT_AT_RISK when order_status is in closed_statuses."""
    cfg = OrderLineFulfillmentRiskConfig(closed_statuses={"CLOSED", "CANCELLED"})
    
    # CLOSED status
    res = evaluate_order_line_fulfillment_risk(
        make_input(
            need_by_date=date(2024, 1, 15),
            open_quantity=10.0,
            projected_available_quantity=5.0,
            order_status="CLOSED",
        ),
        cfg,
    )
    assert res.decision.state == rules.NOT_AT_RISK
    assert res.decision.confidence == "HIGH"
    assert rules.DRIVER_NOT_OPEN in res.decision.drivers
    assert rules.RULE_NOT_AT_RISK_NOT_OPEN in res.evidence_set.evidence[0].rule_ids
    
    # CANCELLED status (case insensitive)
    res = evaluate_order_line_fulfillment_risk(
        make_input(
            need_by_date=date(2024, 1, 15),
            open_quantity=10.0,
            projected_available_quantity=5.0,
            order_status="cancelled",
        ),
        cfg,
    )
    assert res.decision.state == rules.NOT_AT_RISK
    assert rules.DRIVER_NOT_OPEN in res.decision.drivers


def test_not_at_risk_when_no_open_quantity():
    """Test NOT_AT_RISK when open_quantity <= 0."""
    cfg = OrderLineFulfillmentRiskConfig()
    
    # Zero quantity
    res = evaluate_order_line_fulfillment_risk(
        make_input(
            need_by_date=date(2024, 1, 15),
            open_quantity=0.0,
            projected_available_quantity=5.0,
        ),
        cfg,
    )
    assert res.decision.state == rules.NOT_AT_RISK
    assert res.decision.confidence == "HIGH"
    assert rules.DRIVER_NO_OPEN_QTY in res.decision.drivers
    assert rules.RULE_NOT_AT_RISK_NO_OPEN_QTY in res.evidence_set.evidence[0].rule_ids
    
    # Negative quantity
    res = evaluate_order_line_fulfillment_risk(
        make_input(
            need_by_date=date(2024, 1, 15),
            open_quantity=-1.0,
            projected_available_quantity=5.0,
        ),
        cfg,
    )
    assert res.decision.state == rules.NOT_AT_RISK
    assert rules.DRIVER_NO_OPEN_QTY in res.decision.drivers


def test_at_risk_when_projected_short():
    """Test AT_RISK when projected_available_quantity < open_quantity."""
    cfg = OrderLineFulfillmentRiskConfig()
    res = evaluate_order_line_fulfillment_risk(
        make_input(
            need_by_date=date(2024, 1, 15),
            open_quantity=10.0,
            projected_available_quantity=5.0,
        ),
        cfg,
    )
    assert res.decision.state == rules.AT_RISK
    assert res.decision.confidence == "HIGH"
    assert rules.DRIVER_PROJECTED_SHORT in res.decision.drivers
    assert rules.RULE_AT_RISK_PROJECTED_SHORT in res.evidence_set.evidence[0].rule_ids
    # Check shortage quantity calculation
    assert res.decision.metrics["shortage_quantity"] == 5.0


def test_not_at_risk_when_sufficient_supply():
    """Test NOT_AT_RISK when projected_available_quantity >= open_quantity."""
    cfg = OrderLineFulfillmentRiskConfig()
    
    # Equal quantities
    res = evaluate_order_line_fulfillment_risk(
        make_input(
            need_by_date=date(2024, 1, 15),
            open_quantity=10.0,
            projected_available_quantity=10.0,
        ),
        cfg,
    )
    assert res.decision.state == rules.NOT_AT_RISK
    assert res.decision.confidence == "HIGH"
    assert rules.DRIVER_SUFFICIENT_SUPPLY in res.decision.drivers
    assert rules.RULE_NOT_AT_RISK_SUFFICIENT_SUPPLY in res.evidence_set.evidence[0].rule_ids
    assert res.decision.metrics["shortage_quantity"] == 0.0
    
    # More than needed
    res = evaluate_order_line_fulfillment_risk(
        make_input(
            need_by_date=date(2024, 1, 15),
            open_quantity=10.0,
            projected_available_quantity=15.0,
        ),
        cfg,
    )
    assert res.decision.state == rules.NOT_AT_RISK
    assert rules.DRIVER_SUFFICIENT_SUPPLY in res.decision.drivers
    assert res.decision.metrics["shortage_quantity"] == 0.0


def test_shortage_quantity_calculation():
    """Test shortage_quantity is calculated correctly."""
    cfg = OrderLineFulfillmentRiskConfig()
    
    # Shortage case
    res = evaluate_order_line_fulfillment_risk(
        make_input(
            need_by_date=date(2024, 1, 15),
            open_quantity=10.0,
            projected_available_quantity=7.0,
        ),
        cfg,
    )
    assert res.decision.metrics["shortage_quantity"] == 3.0
    
    # No shortage case
    res = evaluate_order_line_fulfillment_risk(
        make_input(
            need_by_date=date(2024, 1, 15),
            open_quantity=10.0,
            projected_available_quantity=12.0,
        ),
        cfg,
    )
    assert res.decision.metrics["shortage_quantity"] == 0.0


def test_metrics_include_all_fields():
    """Test that metrics include all required and optional fields."""
    cfg = OrderLineFulfillmentRiskConfig()
    res = evaluate_order_line_fulfillment_risk(
        make_input(
            need_by_date=date(2024, 1, 15),
            open_quantity=10.0,
            projected_available_quantity=5.0,
            release_shortage_qty=2.0,
            plant_shortage_qty=1.0,
            projected_onhand_qty_eod=3.0,
            supply_qty=8.0,
            demand_qty=12.0,
        ),
        cfg,
    )
    
    assert "need_by_date" in res.decision.metrics
    assert res.decision.metrics["need_by_date"] == "2024-01-15"
    assert "open_quantity" in res.decision.metrics
    assert res.decision.metrics["open_quantity"] == 10.0
    assert "projected_available_quantity" in res.decision.metrics
    assert res.decision.metrics["projected_available_quantity"] == 5.0
    assert "shortage_quantity" in res.decision.metrics
    assert "release_shortage_qty" in res.decision.metrics
    assert res.decision.metrics["release_shortage_qty"] == 2.0
    assert "plant_shortage_qty" in res.decision.metrics
    assert res.decision.metrics["plant_shortage_qty"] == 1.0
    assert "projected_onhand_qty_eod" in res.decision.metrics
    assert res.decision.metrics["projected_onhand_qty_eod"] == 3.0
    assert "supply_qty" in res.decision.metrics
    assert res.decision.metrics["supply_qty"] == 8.0
    assert "demand_qty" in res.decision.metrics
    assert res.decision.metrics["demand_qty"] == 12.0


def test_evidence_includes_context_fields():
    """Test that evidence includes optional context fields when available."""
    cfg = OrderLineFulfillmentRiskConfig()
    res = evaluate_order_line_fulfillment_risk(
        make_input(
            need_by_date=date(2024, 1, 15),
            open_quantity=10.0,
            projected_available_quantity=5.0,
            partnum="PART123",
            customer_id="CUST456",
            plant="PLANT1",
            warehouse="WH1",
        ),
        cfg,
    )
    
    evidence = res.evidence_set.evidence[0]
    assert evidence.references["partnum"] == "PART123"
    assert evidence.references["customer_id"] == "CUST456"
    assert evidence.references["plant"] == "PLANT1"
    assert evidence.references["warehouse"] == "WH1"
    assert "closed_statuses" in evidence.thresholds
    assert isinstance(evidence.thresholds["closed_statuses"], list)


def test_evidence_includes_applied_rule_id():
    """Test that evidence includes applied_rule_id in references."""
    cfg = OrderLineFulfillmentRiskConfig()
    res = evaluate_order_line_fulfillment_risk(
        make_input(
            need_by_date=date(2024, 1, 15),
            open_quantity=10.0,
            projected_available_quantity=5.0,
        ),
        cfg,
    )
    
    evidence = res.evidence_set.evidence[0]
    assert "applied_rule_id" in evidence.references
    assert evidence.references["applied_rule_id"] == rules.RULE_AT_RISK_PROJECTED_SHORT


def test_rule_priority_order():
    """Test that rules are applied in the correct priority order."""
    cfg = OrderLineFulfillmentRiskConfig(closed_statuses={"CLOSED", "CANCELLED"})
    
    # On hold should take precedence over projected short
    res = evaluate_order_line_fulfillment_risk(
        make_input(
            need_by_date=date(2024, 1, 15),
            open_quantity=10.0,
            projected_available_quantity=5.0,
            is_on_hold=True,
        ),
        cfg,
    )
    assert res.decision.state == rules.AT_RISK
    assert rules.DRIVER_ON_HOLD in res.decision.drivers
    
    # Closed status should take precedence over projected short
    res = evaluate_order_line_fulfillment_risk(
        make_input(
            need_by_date=date(2024, 1, 15),
            open_quantity=10.0,
            projected_available_quantity=5.0,
            order_status="CLOSED",
        ),
        cfg,
    )
    assert res.decision.state == rules.NOT_AT_RISK
    assert rules.DRIVER_NOT_OPEN in res.decision.drivers
    
    # No open quantity should take precedence over projected short
    res = evaluate_order_line_fulfillment_risk(
        make_input(
            need_by_date=date(2024, 1, 15),
            open_quantity=0.0,
            projected_available_quantity=5.0,
        ),
        cfg,
    )
    assert res.decision.state == rules.NOT_AT_RISK
    assert rules.DRIVER_NO_OPEN_QTY in res.decision.drivers

