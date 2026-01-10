from __future__ import annotations

from collections.abc import Callable

from opsiq_runtime.domain.activation_policy.models import ActivationItem, ExclusionResult


def exclude_if_in_set(
    item: ActivationItem,
    excluded_group_ids: set[str],
    reason: str = "WEEKLY_AD_OVERLAP_EXCLUSION",
) -> ExclusionResult:
    """
    Check if item should be excluded based on item_group_id being in excluded set.
    
    Args:
        item: ActivationItem to check
        excluded_group_ids: Set of item_group_ids to exclude
        reason: Exclusion reason string
        
    Returns:
        ExclusionResult with excluded=True if item_group_id is in set
    """
    if item.item_group_id in excluded_group_ids:
        return ExclusionResult(excluded=True, reasons=[reason])
    return ExclusionResult(excluded=False, reasons=[])


def exclude_if_recent_purchase(
    item: ActivationItem,
    recent_purchase_group_ids: set[str],
    reason: str = "RECENT_PURCHASE_EXCLUSION",
) -> ExclusionResult:
    """
    Check if item should be excluded based on recent purchase.
    
    Args:
        item: ActivationItem to check
        recent_purchase_group_ids: Set of item_group_ids from recent purchases
        reason: Exclusion reason string
        
    Returns:
        ExclusionResult with excluded=True if item_group_id is in recent purchases
    """
    if item.item_group_id in recent_purchase_group_ids:
        return ExclusionResult(excluded=True, reasons=[reason])
    return ExclusionResult(excluded=False, reasons=[])


def apply_exclusions(
    items: list[ActivationItem],
    exclusion_checks: list[Callable[[ActivationItem], ExclusionResult]],
) -> tuple[list[ActivationItem], list[ActivationItem], dict[str, int]]:
    """
    Apply multiple exclusion checks to items and return eligible, excluded, and reason counts.
    
    An item can be excluded for multiple reasons; all reasons are accumulated in
    item.metadata["excluded_reasons"].
    
    Args:
        items: List of ActivationItems to check
        exclusion_checks: List of exclusion check functions
        
    Returns:
        Tuple of (eligible_items, excluded_items, reason_counts)
        - eligible_items: Items that passed all exclusion checks
        - excluded_items: Items that failed at least one exclusion check
        - reason_counts: Dictionary mapping reason strings to counts
    """
    eligible: list[ActivationItem] = []
    excluded: list[ActivationItem] = []
    reason_counts: dict[str, int] = {}
    
    for item in items:
        all_reasons: list[str] = []
        is_excluded = False
        
        # Apply all exclusion checks
        for check in exclusion_checks:
            result = check(item)
            if result.excluded:
                is_excluded = True
                all_reasons.extend(result.reasons)
                # Count reasons
                for reason in result.reasons:
                    reason_counts[reason] = reason_counts.get(reason, 0) + 1
        
        if is_excluded:
            # Add all exclusion reasons to metadata
            excluded_metadata = dict(item.metadata)
            excluded_metadata["excluded_reasons"] = all_reasons
            excluded_item = ActivationItem(
                item_group_id=item.item_group_id,
                gtin=item.gtin,
                linkcode=item.linkcode,
                category=item.category,
                score=item.score,
                metadata=excluded_metadata,
            )
            excluded.append(excluded_item)
        else:
            eligible.append(item)
    
    return eligible, excluded, reason_counts
