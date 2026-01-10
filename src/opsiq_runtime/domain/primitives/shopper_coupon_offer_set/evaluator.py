from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from opsiq_runtime.domain.activation_policy import (
    ActivationItem,
    PolicyConfig,
    PolicyOutcome,
    add_reason,
    aggregate_drivers,
    apply_category_cap,
    apply_exclusions,
    apply_max_items,
    build_activation_item,
    build_policy_outcome,
    compute_match_rate,
    exclude_if_in_set,
    exclude_if_recent_purchase,
    stable_rank,
)
from opsiq_runtime.domain.common.decision import (
    CONFIDENCE_HIGH,
    CONFIDENCE_MEDIUM,
    DecisionResult,
)
from opsiq_runtime.domain.common.evidence import Evidence, EvidenceSet
from opsiq_runtime.domain.common.versioning import VersionInfo
from opsiq_runtime.domain.primitives.shopper_coupon_offer_set.config import (
    ShopperCouponOfferSetConfig,
)
from opsiq_runtime.domain.primitives.shopper_coupon_offer_set.models import (
    CouponOffer,
    CouponOfferSetInput,
    CouponOfferSetResult,
)
from opsiq_runtime.domain.primitives.shopper_coupon_offer_set import rules


def evaluate_shopper_coupon_offer_set(
    input_row: CouponOfferSetInput, config: ShopperCouponOfferSetConfig
) -> CouponOfferSetResult | None:
    """
    Evaluate shopper coupon offer set by combining affinity scores, eligibility, exclusions, and pricing.
    
    Returns None if sparse_emission is True and no offers are generated.
    """
    shopper_id = str(input_row.subject_id)
    
    # 1. Build affinity map from top_affinity_items up to affinity_top_k
    affinity_score_map: dict[str, float] = {}
    affinity_metadata_map: dict[str, dict] = {}  # Store full item metadata for later use
    
    if input_row.shopper_affinity and input_row.shopper_affinity.top_affinity_items:
        for item in input_row.shopper_affinity.top_affinity_items[: config.affinity_top_k]:
            item_group_id = item.get("item_group_id")
            if item_group_id:
                affinity_score = item.get("affinity_score", 0.0)
                affinity_score_map[item_group_id] = affinity_score
                affinity_metadata_map[item_group_id] = item
    
    # 2. Build candidate list from affinity map (ordered by score DESC, item_group_id ASC for stability)
    candidate_items: list[tuple[str, float]] = sorted(
        affinity_score_map.items(),
        key=lambda x: (-x[1], x[0])  # Score DESC, item_group_id ASC
    )
    
    candidate_count = len(candidate_items)
    
    # 3. Apply eligibility gate: filter to items in eligible_map
    eligible_candidates: list[tuple[str, float, dict]] = []
    for item_group_id, score in candidate_items:
        if item_group_id in input_row.eligible_map:
            eligible_info = input_row.eligible_map[item_group_id]
            eligible_candidates.append((item_group_id, score, eligible_info))
    
    eligible_count = len(eligible_candidates)
    
    # 4. Build ActivationItems for all eligible candidates (pricing will be checked after exclusions)
    activation_items: list[ActivationItem] = []
    
    for item_group_id, score, eligible_info in eligible_candidates:
        gtin = eligible_info.get("gtin")
        linkcode = eligible_info.get("linkcode")
        
        # Get baseline price (may be None)
        price_key = (shopper_id, item_group_id)
        baseline_price = input_row.baseline_prices.get(price_key)
        
        # Build ActivationItem with pricing metadata (even if None for now)
        metadata = {
            "affinity_metadata": affinity_metadata_map.get(item_group_id, {}),
            "baseline_price": baseline_price,
        }
        
        item = build_activation_item(
            item_group_id=item_group_id,
            gtin=gtin,
            linkcode=linkcode,
            category=None,  # Category not in eligibility table currently
            score=score,
            metadata=metadata,
        )
        activation_items.append(item)
    
    # 5. Apply exclusions using Activation Policy (weekly ad overlap, recent purchases)
    exclusion_checks = [
        lambda item: exclude_if_in_set(
            item, input_row.weekly_ad_item_groups, rules.DRIVER_WEEKLY_AD_OVERLAP_EXCLUSION
        ),
        lambda item: exclude_if_recent_purchase(
            item, input_row.recent_purchase_keys, rules.DRIVER_RECENT_PURCHASE_EXCLUSION
        ),
    ]
    
    eligible_after_exclusions, excluded_after_exclusions, reason_counts = apply_exclusions(
        activation_items, exclusion_checks
    )
    
    excluded_weekly_ad_count = reason_counts.get(rules.DRIVER_WEEKLY_AD_OVERLAP_EXCLUSION, 0)
    excluded_recent_purchase_count = reason_counts.get(rules.DRIVER_RECENT_PURCHASE_EXCLUSION, 0)
    
    # 6. Filter out items missing baseline_price (if pricing_fallback_mode="skip")
    pricing_missing_items: list[ActivationItem] = []
    items_with_pricing: list[ActivationItem] = []
    
    for item in eligible_after_exclusions:
        baseline_price = item.metadata.get("baseline_price")
        if baseline_price is None and config.pricing_fallback_mode == "skip":
            pricing_missing_items.append(item)
        else:
            items_with_pricing.append(item)
    
    excluded_pricing_missing_count = len(pricing_missing_items)
    
    # Combine all excluded items
    all_excluded = excluded_after_exclusions + pricing_missing_items
    
    # 7. Stable rank eligible items with pricing (already sorted but ensure deterministic)
    ranked = stable_rank(items_with_pricing)
    
    # 8. Apply category cap if configured
    if config.category_cap is not None and config.category_cap > 0:
        ranked = apply_category_cap(ranked, config.category_cap)
    
    # 9. Apply max_items constraint
    selected_activation_items = apply_max_items(ranked, config.max_offers)
    
    # 10. Compute match_rate
    match_rate = compute_match_rate(selected_activation_items)
    
    # 11. Build PolicyConfig and aggregate drivers
    policy_config = PolicyConfig(
        exclude_by="item_group_id",
        exclude_lookback_days=config.exclude_lookback_days,
        max_items=config.max_offers,
        category_cap=config.category_cap,
        min_match_rate_for_high_confidence=config.min_match_rate_for_high_confidence,
    )
    
    # Aggregate base drivers from Activation Policy
    base_drivers = aggregate_drivers(selected_activation_items, all_excluded)
    
    # Add primitive-specific drivers
    drivers = list(base_drivers)
    drivers.append(rules.DRIVER_ELIGIBILITY_POLICY_ENFORCED)
    drivers.append(rules.DRIVER_NOT_IN_WEEKLY_AD)
    drivers.append(rules.DRIVER_COUPON_DISCOUNT_APPLIED)
    
    # 11. Build PolicyOutcome to get confidence
    policy_outcome = build_policy_outcome(
        selected_items=selected_activation_items,
        excluded_items=all_excluded,
        candidates_count=candidate_count,
        match_rate=match_rate,
        drivers=drivers,
        config=policy_config,
    )
    
    # Override confidence: HIGH if match_rate >= threshold, else MEDIUM (if offers > 0)
    if len(selected_activation_items) > 0:
        if match_rate >= config.min_match_rate_for_high_confidence:
            confidence = CONFIDENCE_HIGH
        else:
            confidence = CONFIDENCE_MEDIUM
    else:
        confidence = CONFIDENCE_MEDIUM  # Shouldn't happen if sparse emission returns None
    
    # 12. Build CouponOffer list from selected ActivationItems (all have pricing)
    offers: list[CouponOffer] = []
    for rank, item in enumerate(selected_activation_items, start=1):
        baseline_price = item.metadata.get("baseline_price")
        if baseline_price is None:
            continue  # Skip if pricing missing (shouldn't happen but defensive)
        
        # Calculate offer_price = baseline_price * (1 - discount_pct/100)
        offer_price = round(baseline_price * (1 - config.discount_pct / 100), 2)
        
        # Build reasons list
        item_reasons = []
        if item.score > 0:
            item_reasons.append(rules.DRIVER_HIGH_AFFINITY)
        item_reasons.append(rules.DRIVER_NOT_IN_WEEKLY_AD)
        item_reasons.append(rules.DRIVER_COUPON_DISCOUNT_APPLIED)
        
        offer = CouponOffer(
            rank=rank,
            item_group_id=item.item_group_id,
            gtin=item.gtin,
            linkcode=item.linkcode,
            affinity_score=item.score,
            baseline_price=baseline_price,
            offer_price=offer_price,
            reasons=item_reasons,
        )
        offers.append(offer)
    
    # 13. Sparse emission: return None if offers list is empty
    if not offers and config.sparse_emission:
        return None
    
    # 14. Build metrics JSON
    metrics = {
        "max_offers": config.max_offers,
        "discount_pct": config.discount_pct,
        "candidate_count": candidate_count,
        "eligible_count": eligible_count,
        "excluded_weekly_ad_count": excluded_weekly_ad_count,
        "excluded_recent_purchase_count": excluded_recent_purchase_count,
        "excluded_pricing_missing_count": excluded_pricing_missing_count,
        "offers": [
            {
                "rank": offer.rank,
                "item_group_id": offer.item_group_id,
                "gtin": offer.gtin,
                "linkcode": offer.linkcode,
                "affinity_score": offer.affinity_score,
                "baseline_price": offer.baseline_price,
                "offer_price": offer.offer_price,
                "reasons": offer.reasons,
            }
            for offer in offers
        ],
    }
    
    # 15. Build evidence JSON
    evidence_references = {
        "sources": {
            "affinity_table": "opsiq_dev.gold.gold_feature_shopper_top_affinity_v1",
            "eligibility_table": "opsiq_dev.gold.gold_policy_item_eligibility_v1",
            "weekly_ad_table": "opsiq_dev.gold.gold_canonical_weekly_ad_item_v1",
            "purchases_table": "opsiq_dev.gold.gold_canonical_trip_item_enriched_v1",
        },
        "context": {
            "ad_id": config.ad_id,
            "scope_type": config.scope_type,
            "scope_value": config.scope_value,
        },
        "as_of": {
            "affinity_as_of_ts": (
                input_row.shopper_affinity.as_of_ts.isoformat()
                if input_row.shopper_affinity
                else input_row.as_of_ts.isoformat()
            ),
            "eligibility_as_of_ts": input_row.as_of_ts.isoformat(),
        },
    }
    
    # 16. Build evidence
    evidence_id = f"evidence-{shopper_id}-coupon-offer-set-v1"
    rule_id = rules.RULE_COMPUTED_OFFERS if offers else rules.RULE_UNKNOWN_NO_OFFERS
    evidence = Evidence(
        evidence_id=evidence_id,
        rule_ids=[rule_id],
        thresholds={},
        references=evidence_references,
        observed_at=datetime.now(timezone.utc),
    )
    
    # 17. Build decision
    decision_state = rules.COMPUTED if offers else rules.UNKNOWN
    versions = VersionInfo(
        primitive_version=config.primitive_version,
        canonical_version=config.canonical_version,
        config_version=input_row.config_version,
    )
    decision = DecisionResult(
        state=decision_state,
        confidence=confidence,
        drivers=drivers,
        metrics=metrics,
        evidence_refs=[evidence_id],
        versions=versions,
        computed_at=datetime.now(timezone.utc),
        valid_until=None,
    )
    
    return CouponOfferSetResult(
        decision=decision, evidence_set=EvidenceSet(evidence=[evidence])
    )
