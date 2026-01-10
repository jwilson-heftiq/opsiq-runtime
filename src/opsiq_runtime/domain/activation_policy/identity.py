from __future__ import annotations

from opsiq_runtime.domain.activation_policy.models import ActivationItem


def resolve_item_group_id(linkcode: str | None, gtin: str | None) -> str | None:
    """
    Resolve item_group_id from linkcode and gtin.
    
    Priority: linkcode if non-empty, else gtin if non-empty, else None.
    
    Args:
        linkcode: Linkcode identifier (preferred)
        gtin: GTIN identifier (fallback)
        
    Returns:
        Resolved item_group_id or None if both are empty/None
    """
    if linkcode:
        return linkcode
    if gtin:
        return gtin
    return None


def build_activation_item(
    item_group_id: str | None = None,
    linkcode: str | None = None,
    gtin: str | None = None,
    category: str | None = None,
    score: float = 0.0,
    metadata: dict | None = None,
) -> ActivationItem:
    """
    Build an ActivationItem with resolved item_group_id.
    
    If item_group_id is not provided, it will be resolved from linkcode/gtin.
    
    Args:
        item_group_id: Pre-resolved item_group_id (optional, will be resolved if not provided)
        linkcode: Linkcode identifier
        gtin: GTIN identifier
        category: Item category
        score: Affinity or ranking score
        metadata: Additional metadata dictionary
        
    Returns:
        ActivationItem with resolved item_group_id
        
    Raises:
        ValueError: If item_group_id cannot be resolved (all identifiers are empty/None)
    """
    resolved_id = item_group_id or resolve_item_group_id(linkcode, gtin)
    if not resolved_id:
        raise ValueError("Cannot resolve item_group_id: both linkcode and gtin are empty/None")
    
    return ActivationItem(
        item_group_id=resolved_id,
        gtin=gtin,
        linkcode=linkcode,
        category=category,
        score=score,
        metadata=metadata or {},
    )
