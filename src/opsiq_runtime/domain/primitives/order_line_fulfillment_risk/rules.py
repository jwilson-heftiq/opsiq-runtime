from __future__ import annotations

# Decision states
AT_RISK = "AT_RISK"
NOT_AT_RISK = "NOT_AT_RISK"
UNKNOWN = "UNKNOWN"

# Rule IDs
RULE_UNKNOWN_MISSING_INPUTS = "order_line_fulfillment_risk.unknown_missing_inputs"
RULE_AT_RISK_ON_HOLD = "order_line_fulfillment_risk.at_risk_on_hold"
RULE_NOT_AT_RISK_NOT_OPEN = "order_line_fulfillment_risk.not_at_risk_not_open"
RULE_NOT_AT_RISK_NO_OPEN_QTY = "order_line_fulfillment_risk.not_at_risk_no_open_qty"
RULE_AT_RISK_PROJECTED_SHORT = "order_line_fulfillment_risk.at_risk_projected_short"
RULE_NOT_AT_RISK_SUFFICIENT_SUPPLY = "order_line_fulfillment_risk.not_at_risk_sufficient_supply"

# Driver codes
DRIVER_MISSING_REQUIRED_INPUTS = "MISSING_REQUIRED_INPUTS"
DRIVER_ON_HOLD = "ON_HOLD"
DRIVER_NOT_OPEN = "NOT_OPEN"
DRIVER_NO_OPEN_QTY = "NO_OPEN_QTY"
DRIVER_PROJECTED_SHORT = "PROJECTED_SHORT"
DRIVER_SUFFICIENT_SUPPLY = "SUFFICIENT_SUPPLY"

