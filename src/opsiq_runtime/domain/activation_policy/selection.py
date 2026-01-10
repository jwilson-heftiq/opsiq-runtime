from __future__ import annotations

from opsiq_runtime.domain.activation_policy.models import ActivationItem


def apply_max_items(items: list[ActivationItem], max_items: int) -> list[ActivationItem]:
    """
    Apply maximum items constraint, preserving order.
    
    Takes the first max_items items from the list.
    
    Args:
        items: List of ActivationItems (should already be sorted/ranked)
        max_items: Maximum number of items to return
        
    Returns:
        First max_items items, or all items if fewer than max_items
    """
    return items[:max_items]


def apply_category_cap(items: list[ActivationItem], cap: int) -> list[ActivationItem]:
    """
    Apply per-category cap constraint while preserving ordering.
    
    Items are processed in order, and the first cap items per category are kept.
    Items with category=None are treated as uncapped (their own bucket, unlimited).
    
    Args:
        items: List of ActivationItems (should already be sorted/ranked)
        cap: Maximum number of items per category
        
    Returns:
        Filtered list preserving order, with at most cap items per category
        (items with category=None are not capped)
    """
    category_counts: dict[str | None, int] = {}
    result: list[ActivationItem] = []
    
    for item in items:
        category = item.category
        current_count = category_counts.get(category, 0)
        
        # Items with category=None are uncapped
        if category is None:
            result.append(item)
        elif current_count < cap:
            result.append(item)
            category_counts[category] = current_count + 1
    
    return result
