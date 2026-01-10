from __future__ import annotations

from opsiq_runtime.domain.activation_policy.models import ActivationItem


def add_reason(item: ActivationItem, reason: str) -> ActivationItem:
    """
    Add a reason to an item's metadata["reasons"] list.
    
    If "reasons" doesn't exist, creates it. If reason already exists, it's not duplicated.
    
    Args:
        item: ActivationItem to add reason to
        reason: Reason string to add
        
    Returns:
        New ActivationItem with reason added to metadata
    """
    reasons = list(item.metadata.get("reasons", []))
    if reason not in reasons:
        reasons.append(reason)
    
    new_metadata = dict(item.metadata)
    new_metadata["reasons"] = reasons
    
    return ActivationItem(
        item_group_id=item.item_group_id,
        gtin=item.gtin,
        linkcode=item.linkcode,
        category=item.category,
        score=item.score,
        metadata=new_metadata,
    )


def add_excluded_reason(item: ActivationItem, reason: str) -> ActivationItem:
    """
    Add an exclusion reason to an item's metadata["excluded_reasons"] list.
    
    If "excluded_reasons" doesn't exist, creates it. If reason already exists, it's not duplicated.
    
    Args:
        item: ActivationItem to add exclusion reason to
        reason: Exclusion reason string to add
        
    Returns:
        New ActivationItem with exclusion reason added to metadata
    """
    excluded_reasons = list(item.metadata.get("excluded_reasons", []))
    if reason not in excluded_reasons:
        excluded_reasons.append(reason)
    
    new_metadata = dict(item.metadata)
    new_metadata["excluded_reasons"] = excluded_reasons
    
    return ActivationItem(
        item_group_id=item.item_group_id,
        gtin=item.gtin,
        linkcode=item.linkcode,
        category=item.category,
        score=item.score,
        metadata=new_metadata,
    )


def aggregate_drivers(
    selected: list[ActivationItem],
    excluded: list[ActivationItem],
) -> list[str]:
    """
    Aggregate primitive-level drivers from selected and excluded items.
    
    Drivers included:
    - Always: "ACTIVATION_POLICY_APPLIED"
    - If any selected item has score > 0: "AFFINITY_MATCH"
    - If any items were excluded: "EXCLUSIONS_APPLIED"
    
    Args:
        selected: List of selected ActivationItems
        excluded: List of excluded ActivationItems
        
    Returns:
        List of driver strings in stable order (no duplicates)
    """
    drivers: list[str] = []
    
    # Always include this
    drivers.append("ACTIVATION_POLICY_APPLIED")
    
    # Check for affinity matches
    has_affinity_match = any(item.score > 0 for item in selected)
    if has_affinity_match:
        drivers.append("AFFINITY_MATCH")
    
    # Check for exclusions
    if excluded:
        drivers.append("EXCLUSIONS_APPLIED")
    
    return drivers
