from __future__ import annotations

# Decision states
AT_RISK = "AT_RISK"
NOT_AT_RISK = "NOT_AT_RISK"
UNKNOWN = "UNKNOWN"

# Rule IDs
RULE_UNKNOWN_NO_LINES_FOUND = "order_fulfillment_risk.unknown_no_lines_found"
RULE_AT_RISK_HAS_AT_RISK_LINES = "order_fulfillment_risk.at_risk_has_at_risk_lines"
RULE_UNKNOWN_ALL_LINES_UNKNOWN = "order_fulfillment_risk.unknown_all_lines_unknown"
RULE_NOT_AT_RISK_ALL_LINES_OK = "order_fulfillment_risk.not_at_risk_all_lines_ok"

# Driver codes
DRIVER_NO_LINES_FOUND = "NO_LINES_FOUND"
DRIVER_HAS_AT_RISK_LINES = "HAS_AT_RISK_LINES"
DRIVER_ALL_LINES_UNKNOWN = "ALL_LINES_UNKNOWN"
DRIVER_ALL_LINES_OK = "ALL_LINES_OK"

