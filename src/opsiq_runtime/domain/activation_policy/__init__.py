from __future__ import annotations

"""
Activation Policy & Guardrail Module

This module provides reusable, deterministic policy functions for activation primitives.
All functions are pure (no I/O, no side effects) and domain-only (no infrastructure dependencies).

Usage Example:
    ```python
    from opsiq_runtime.domain.activation_policy import (
        ActivationItem,
        ExclusionResult,
        PolicyConfig,
        build_activation_item,
        exclude_if_in_set,
        exclude_if_recent_purchase,
        apply_exclusions,
        stable_rank,
        apply_max_items,
        apply_category_cap,
        compute_match_rate,
        aggregate_drivers,
        build_policy_outcome,
    )
    
    # 1. Build candidates as ActivationItems
    candidates = [
        build_activation_item(
            linkcode="LINK001",
            gtin="1234567890123",
            category="Dairy",
            score=0.9,
            metadata={"promo_price": 10.0, "title": "Milk"}
        ),
        build_activation_item(
            linkcode="LINK002",
            gtin="9876543210987",
            category="Produce",
            score=0.7,
            metadata={"promo_price": 15.0, "title": "Apples"}
        ),
    ]
    
    # 2. Apply exclusions
    weekly_ad_overlap = {"LINK001"}
    recent_purchases = {"LINK003"}
    
    def check_overlap(item: ActivationItem) -> ExclusionResult:
        return exclude_if_in_set(item, weekly_ad_overlap, "WEEKLY_AD_OVERLAP_EXCLUSION")
    
    def check_recent_purchase(item: ActivationItem) -> ExclusionResult:
        return exclude_if_recent_purchase(item, recent_purchases, "RECENT_PURCHASE_EXCLUSION")
    
    eligible, excluded, reason_counts = apply_exclusions(
        candidates,
        [check_overlap, check_recent_purchase]
    )
    
    # 3. Stable rank eligible items
    ranked = stable_rank(eligible)
    
    # 4. Apply category cap + max_items
    config = PolicyConfig(max_items=5, category_cap=2)
    after_category_cap = apply_category_cap(ranked, config.category_cap or 999)
    final_selected = apply_max_items(after_category_cap, config.max_items)
    
    # 5. Compute match_rate, drivers, confidence
    match_rate = compute_match_rate(final_selected)
    drivers = aggregate_drivers(final_selected, excluded)
    
    # 6. Build and return PolicyOutcome
    outcome = build_policy_outcome(
        selected_items=final_selected,
        excluded_items=excluded,
        candidates_count=len(candidates),
        match_rate=match_rate,
        drivers=drivers,
        config=config,
    )
    ```
"""

from opsiq_runtime.domain.activation_policy.identity import (
    build_activation_item,
    resolve_item_group_id,
)
from opsiq_runtime.domain.activation_policy.models import (
    ActivationItem,
    ExclusionResult,
    PolicyConfig,
    PolicyOutcome,
)
from opsiq_runtime.domain.activation_policy.exclusions import (
    apply_exclusions,
    exclude_if_in_set,
    exclude_if_recent_purchase,
)
from opsiq_runtime.domain.activation_policy.ordering import (
    compute_match_rate,
    stable_rank,
)
from opsiq_runtime.domain.activation_policy.reasons import (
    add_excluded_reason,
    add_reason,
    aggregate_drivers,
)
from opsiq_runtime.domain.activation_policy.selection import (
    apply_category_cap,
    apply_max_items,
)

__all__ = [
    # Models
    "ActivationItem",
    "PolicyConfig",
    "PolicyOutcome",
    "ExclusionResult",
    # Identity
    "resolve_item_group_id",
    "build_activation_item",
    # Exclusions
    "exclude_if_in_set",
    "exclude_if_recent_purchase",
    "apply_exclusions",
    # Ordering
    "stable_rank",
    "compute_match_rate",
    # Selection
    "apply_max_items",
    "apply_category_cap",
    # Reasons
    "add_reason",
    "add_excluded_reason",
    "aggregate_drivers",
    # Outcome builder
    "build_policy_outcome",
]


def build_policy_outcome(
    selected_items: list[ActivationItem],
    excluded_items: list[ActivationItem],
    candidates_count: int,
    match_rate: float,
    drivers: list[str],
    config: PolicyConfig,
) -> PolicyOutcome:
    """
    Build a PolicyOutcome from selected and excluded items with confidence calculation.
    
    Confidence rules:
    - Empty selected => "LOW"
    - match_rate >= config.min_match_rate_for_high_confidence => "HIGH"
    - match_rate > 0 => "MEDIUM"
    - else => "LOW"
    """
    if not selected_items:
        computed_confidence = "LOW"
    elif match_rate >= config.min_match_rate_for_high_confidence:
        computed_confidence = "HIGH"
    elif match_rate > 0:
        computed_confidence = "MEDIUM"
    else:
        computed_confidence = "LOW"
    
    return PolicyOutcome(
        selected_items=selected_items,
        excluded_items=excluded_items,
        excluded_count=len(excluded_items),
        candidates_count=candidates_count,
        match_rate=match_rate,
        drivers=drivers,
        computed_confidence=computed_confidence,
    )
