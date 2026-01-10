from __future__ import annotations

from opsiq_runtime.domain.activation_policy.models import ActivationItem


def stable_rank(items: list[ActivationItem]) -> list[ActivationItem]:
    """
    Sort items deterministically by score DESC, then ad_position ASC, then gtin ASC.
    
    Sort key priority:
    1. score DESC (highest first)
    2. metadata.get("ad_position") ASC (if present, else use large value to sort last)
    3. gtin ASC (or item_group_id ASC if gtin is None) as final tie-breaker
    
    Args:
        items: List of ActivationItems to sort
        
    Returns:
        New sorted list (items are immutable, so returns new list with same items)
    """
    def sort_key(item: ActivationItem) -> tuple:
        # 1. Score DESC (negate for descending)
        score_key = -item.score
        
        # 2. ad_position ASC (if present, else use large value)
        ad_position = item.metadata.get("ad_position")
        if ad_position is not None:
            ad_position_key = ad_position
        else:
            ad_position_key = float("inf")
        
        # 3. gtin ASC (or item_group_id ASC) as tie-breaker
        tie_breaker = item.gtin or item.item_group_id or ""
        
        return (score_key, ad_position_key, tie_breaker)
    
    # Sort in-place would be fine since items are immutable, but return new list for clarity
    return sorted(items, key=sort_key)


def compute_match_rate(items: list[ActivationItem]) -> float:
    """
    Compute match rate as fraction of items with score > 0.
    
    Args:
        items: List of ActivationItems
        
    Returns:
        Match rate (0.0 to 1.0), or 0.0 if items list is empty
    """
    if not items:
        return 0.0
    
    match_count = sum(1 for item in items if item.score > 0)
    return match_count / len(items)
