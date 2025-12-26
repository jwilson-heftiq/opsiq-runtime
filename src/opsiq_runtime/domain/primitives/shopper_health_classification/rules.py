from __future__ import annotations

# Decision states
URGENT = "URGENT"
WATCHLIST = "WATCHLIST"
HEALTHY = "HEALTHY"
UNKNOWN = "UNKNOWN"

# Rule IDs
RULE_URGENT_AT_RISK = "shopper_health_classification.urgent_at_risk"
RULE_UNKNOWN_INSUFFICIENT_SIGNALS = "shopper_health_classification.unknown_insufficient_signals"
RULE_WATCHLIST_DECLINING = "shopper_health_classification.watchlist_declining"
RULE_WATCHLIST_DECLINING_RISK_UNKNOWN = "shopper_health_classification.watchlist_declining_risk_unknown"
RULE_HEALTHY_OK = "shopper_health_classification.healthy_ok"
RULE_UNKNOWN_PARTIAL_SIGNALS = "shopper_health_classification.unknown_partial_signals"

# Driver codes
DRIVER_LAPSE_RISK = "LAPSE_RISK"
DRIVER_INSUFFICIENT_SIGNALS = "INSUFFICIENT_SIGNALS"
DRIVER_CADENCE_DECLINING = "CADENCE_DECLINING"
DRIVER_RISK_UNKNOWN = "RISK_UNKNOWN"
DRIVER_RISK_OK = "RISK_OK"
DRIVER_CADENCE_OK = "CADENCE_OK"
DRIVER_PARTIAL_SIGNALS = "PARTIAL_SIGNALS"

