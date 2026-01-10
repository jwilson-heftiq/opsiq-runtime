from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from opsiq_runtime.domain.common.decision import (
    CONFIDENCE_HIGH,
    CONFIDENCE_MEDIUM,
    DecisionResult,
)
from opsiq_runtime.domain.common.evidence import Evidence, EvidenceSet
from opsiq_runtime.domain.common.versioning import VersionInfo
from opsiq_runtime.domain.primitives.shopper_weekly_ad_slate.config import (
    ShopperWeeklyAdSlateConfig,
)
from opsiq_runtime.domain.primitives.shopper_weekly_ad_slate.models import (
    AdCandidate,
    ShopperWeeklyAdSlateInput,
    ShopperWeeklyAdSlateResult,
    SlateItem,
)
from opsiq_runtime.domain.primitives.shopper_weekly_ad_slate import rules


@dataclass(frozen=True)
class CandidateWithScore:
    """Internal helper for scoring and sorting candidates."""

    candidate: AdCandidate
    score: float
    reasons: list[str]
    category: str | None = None


def evaluate_shopper_weekly_ad_slate(
    input_row: ShopperWeeklyAdSlateInput, config: ShopperWeeklyAdSlateConfig
) -> ShopperWeeklyAdSlateResult | None:
    """
    Evaluate shopper weekly ad slate by combining ad candidates, affinity scores, and exclusions.
    
    Returns None if sparse_emission is True and slate is empty.
    """
    # Build affinity_score_map from shopper_affinity.top_affinity_items
    affinity_score_map: dict[str, dict] = {}
    if input_row.shopper_affinity and input_row.shopper_affinity.top_affinity_items:
        for item in input_row.shopper_affinity.top_affinity_items:
            item_group_id = item.get("item_group_id")
            if item_group_id:
                affinity_score_map[item_group_id] = {
                    "score": item.get("affinity_score", 0.0),
                    "rank": item.get("rank"),
                    "days_since_last_purchase": item.get("days_since_last_purchase"),
                    "total_sales": item.get("total_sales"),
                }

    # Score and filter candidates
    scored_candidates: list[CandidateWithScore] = []
    excluded_count = 0
    candidates_count = len(input_row.candidates)

    for candidate in input_row.candidates:
        match_key = candidate.item_group_id
        score = affinity_score_map.get(match_key, {}).get("score", 0.0) if match_key else 0.0
        reasons = ["IN_CURRENT_AD"]
        
        if score > 0:
            reasons.append("AFFINITY_MATCH")
        
        # Check for exclusion
        if match_key in input_row.recent_purchase_keys:
            excluded_count += 1
            continue  # Skip excluded items
        
        # Extract category from candidate if available (may need to add to AdCandidate model)
        # For now, we'll use None and handle category cap later if needed
        category = None
        
        scored_candidates.append(
            CandidateWithScore(
                candidate=candidate,
                score=score,
                reasons=reasons,
                category=category,
            )
        )

    # Sort by: score DESC, promo_price ASC NULLS LAST, gtin ASC
    def sort_key(cws: CandidateWithScore) -> tuple:
        promo_price = cws.candidate.promo_price
        # Use a large number for NULL prices to sort them last
        promo_price_sort = promo_price if promo_price is not None else float("inf")
        gtin = cws.candidate.gtin or ""
        return (-cws.score, promo_price_sort, gtin)

    scored_candidates.sort(key=sort_key)

    # Apply category cap if enabled
    if config.category_cap is not None and config.category_cap > 0:
        category_counts: dict[str, int] = {}
        capped_candidates: list[CandidateWithScore] = []
        
        for cws in scored_candidates:
            cat = cws.category or "unknown"
            count = category_counts.get(cat, 0)
            if count < config.category_cap:
                capped_candidates.append(cws)
                category_counts[cat] = count + 1
        
        scored_candidates = capped_candidates

    # Select top slate_size_k
    slate_items = scored_candidates[: config.slate_size_k]

    # Sparse emission: return None if slate is empty
    if not slate_items and config.sparse_emission:
        return None

    # Build slate items for metrics
    slate_items_metrics: list[dict] = []
    items_with_affinity = 0
    
    for rank, cws in enumerate(slate_items, start=1):
        candidate = cws.candidate
        slate_items_metrics.append({
            "rank": rank,
            "item_group_id": candidate.item_group_id,
            "gtin": candidate.gtin,
            "linkcode": candidate.linkcode,
            "score": cws.score,
            "title": candidate.title,
            "promo_price": candidate.promo_price,
            "ad_group_id": candidate.ad_group_id,
            "reasons": cws.reasons,
        })
        if cws.score > 0:
            items_with_affinity += 1

    # Compute confidence
    slate_size = len(slate_items)
    if slate_size > 0:
        match_rate = items_with_affinity / slate_size
        confidence = (
            CONFIDENCE_HIGH
            if match_rate >= config.min_match_rate_for_high_confidence
            else CONFIDENCE_MEDIUM
        )
    else:
        match_rate = 0.0
        confidence = CONFIDENCE_MEDIUM

    # Build drivers
    drivers = ["IN_CURRENT_AD"]
    if items_with_affinity > 0:
        drivers.append("AFFINITY_MATCH")
    if excluded_count > 0:
        drivers.append("RECENT_PURCHASE_EXCLUSIONS")

    # Determine decision state
    if slate_size == 0:
        decision_state = rules.UNKNOWN
        drivers = [rules.DRIVER_NO_ELIGIBLE_AD_ITEMS]
        applied_rule_id = rules.RULE_UNKNOWN_NO_ITEMS
    else:
        decision_state = rules.COMPUTED
        applied_rule_id = rules.RULE_COMPUTED_SLATE

    # Build metrics JSON
    metrics = {
        "ad_id": config.ad_id,
        "scope_type": config.scope_type,
        "scope_value": config.scope_value,
        "slate_size_k": config.slate_size_k,
        "exclude_lookback_days": config.exclude_lookback_days,
        "excluded_count": excluded_count,
        "candidates_count": candidates_count,
        "match_rate": match_rate,
        "items": slate_items_metrics,
    }

    # Build evidence JSON
    evidence_references = {
        "sources": {
            "ad_candidates_table": "opsiq_dev.gold.gold_canonical_weekly_ad_item_v1",
            "affinity_table": "opsiq_dev.gold.gold_feature_shopper_top_affinity_v1",
            "purchases_table": "opsiq_dev.gold.gold_canonical_trip_item_enriched_v1",
        },
        "as_of": {
            "ad_candidates_as_of_ts": (
                max((c.as_of_ts for c in input_row.candidates), default=input_row.as_of_ts).isoformat()
                if input_row.candidates
                else input_row.as_of_ts.isoformat()
            ),
            "affinity_as_of_ts": (
                input_row.shopper_affinity.as_of_ts.isoformat()
                if input_row.shopper_affinity
                else input_row.as_of_ts.isoformat()
            ),
        },
    }

    # Build evidence
    evidence_id = f"evidence-{input_row.subject_id}-weekly-ad-slate-v1"
    evidence = Evidence(
        evidence_id=evidence_id,
        rule_ids=[applied_rule_id],
        thresholds={},
        references=evidence_references,
        observed_at=datetime.now(timezone.utc),
    )

    # Build decision
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

    return ShopperWeeklyAdSlateResult(
        decision=decision, evidence_set=EvidenceSet(evidence=[evidence])
    )
