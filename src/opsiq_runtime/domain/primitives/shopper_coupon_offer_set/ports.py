from __future__ import annotations

"""
Outbound port interfaces for shopper_coupon_offer_set primitive.

These interfaces define the expected signatures for data access methods
that should be implemented by adapters (e.g., DatabricksInputsRepository).
"""

from typing import Protocol

from opsiq_runtime.domain.primitives.shopper_weekly_ad_slate.models import (
    ShopperAffinityRow,
)


class CouponOfferSetDataPort(Protocol):
    """Protocol defining expected data access methods for coupon offer set primitive."""

    def get_weekly_ad_item_groups(
        self,
        tenant_id: str,
        ad_id: str,
        scope_type: str,
        scope_value: str,
        hours_window: int = 72,
    ) -> set[str]:
        """
        Fetch weekly ad item group IDs to exclude from coupon offers.
        
        Args:
            tenant_id: Tenant identifier
            ad_id: Ad identifier
            scope_type: Scope type (e.g., "store", "region")
            scope_value: Scope value (e.g., "store_123")
            hours_window: Hours window for freshness
            
        Returns:
            Set of item_group_ids (COALESCE(linkcode, gtin)) from weekly ad
        """
        ...

    def get_coupon_eligible_map(
        self,
        tenant_id: str,
        hours_window: int = 72,
    ) -> dict[str, dict]:
        """
        Fetch coupon-eligible items keyed by item_group_id.
        
        Args:
            tenant_id: Tenant identifier
            hours_window: Hours window for freshness
            
        Returns:
            Dictionary mapping item_group_id -> {gtin, linkcode, ineligible_reasons}
        """
        ...

    def get_recent_purchase_keys(
        self,
        tenant_id: str,
        exclude_days: int = 14,
        shopper_ids: list[str] | None = None,
    ) -> dict[str, set[str]]:
        """
        Fetch recent purchase item group IDs per shopper for exclusion.
        
        Args:
            tenant_id: Tenant identifier
            exclude_days: Days lookback for recent purchases
            shopper_ids: Optional list of shopper IDs to filter
            
        Returns:
            Dictionary mapping shopper_id -> set of item_group_ids
        """
        ...

    def get_baseline_prices(
        self,
        tenant_id: str,
        exclude_days: int = 90,
        shopper_ids: list[str] | None = None,
    ) -> dict[tuple[str, str], float]:
        """
        Fetch baseline unit prices for items per shopper.
        
        Args:
            tenant_id: Tenant identifier
            exclude_days: Days lookback for pricing data
            shopper_ids: Optional list of shopper IDs to filter
            
        Returns:
            Dictionary mapping (shopper_id, item_group_id) -> baseline_price (float)
        """
        ...

    def get_shopper_affinity_rows(
        self,
        tenant_id: str,
        hours_window: int = 72,
        shopper_ids: list[str] | None = None,
    ) -> list[ShopperAffinityRow]:
        """
        Fetch shopper affinity rows (defines which shoppers to evaluate).
        
        Args:
            tenant_id: Tenant identifier
            hours_window: Hours window for freshness
            shopper_ids: Optional list of shopper IDs to filter
            
        Returns:
            List of ShopperAffinityRow objects
        """
        ...
