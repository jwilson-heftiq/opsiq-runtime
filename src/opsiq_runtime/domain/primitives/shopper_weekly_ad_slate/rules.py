from __future__ import annotations

# Decision states
COMPUTED = "COMPUTED"
UNKNOWN = "UNKNOWN"

# Rule IDs
RULE_COMPUTED_SLATE = "shopper_weekly_ad_slate.computed_slate"
RULE_UNKNOWN_NO_ITEMS = "shopper_weekly_ad_slate.unknown_no_items"

# Driver codes
DRIVER_IN_CURRENT_AD = "IN_CURRENT_AD"
DRIVER_AFFINITY_MATCH = "AFFINITY_MATCH"
DRIVER_RECENT_PURCHASE_EXCLUSIONS = "RECENT_PURCHASE_EXCLUSIONS"
DRIVER_NO_ELIGIBLE_AD_ITEMS = "NO_ELIGIBLE_AD_ITEMS"
