from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone

from opsiq_runtime.domain.common.decision import (
    CONFIDENCE_HIGH,
    CONFIDENCE_LOW,
    DecisionResult,
)
from opsiq_runtime.domain.common.evidence import Evidence, EvidenceSet
from opsiq_runtime.domain.common.versioning import VersionInfo
from opsiq_runtime.domain.primitives.shopper_item_affinity_score.config import (
    ShopperItemAffinityConfig,
)
from opsiq_runtime.domain.primitives.shopper_item_affinity_score.model import (
    ShopperItemAffinityInput,
)
from opsiq_runtime.domain.primitives.shopper_item_affinity_score import rules


@dataclass(frozen=True)
class ShopperItemAffinityResult:
    decision: DecisionResult
    evidence_set: EvidenceSet


def evaluate_shopper_item_affinity_score(
    input_row: ShopperItemAffinityInput, config: ShopperItemAffinityConfig
) -> ShopperItemAffinityResult:
    """
    Evaluate shopper item affinity score based on top affinity items.
    
    If top_affinity_items is empty/null: emit UNKNOWN with driver ["NO_AFFINITY_ITEMS"].
    Otherwise emit COMPUTED with HIGH confidence and driver ["TOP_AFFINITY_COMPUTED"].
    """
    top_items = input_row.top_affinity_items or []
    
    # Determine decision state and confidence
    if not top_items:
        decision_state = rules.UNKNOWN
        confidence = CONFIDENCE_LOW
        drivers = [rules.DRIVER_NO_AFFINITY_ITEMS]
        applied_rule_id = rules.RULE_UNKNOWN_NO_ITEMS
    else:
        decision_state = rules.COMPUTED
        confidence = CONFIDENCE_HIGH
        drivers = [rules.DRIVER_TOP_AFFINITY_COMPUTED]
        applied_rule_id = rules.RULE_COMPUTED_HAS_ITEMS
    
    # Build top_items array for metrics_json
    top_items_metrics = []
    for item in top_items:
        item_metric = {
            "rank": item.get("rank"),
            "item_group_id": item.get("item_group_id"),
            "affinity_score": item.get("affinity_score"),
            "trip_count": item.get("trip_count"),
            "days_since_last_purchase": item.get("days_since_last_purchase"),
            "total_sales": item.get("total_sales"),
            "gtin_sample": item.get("gtin_sample"),
            "linkcode_sample": item.get("linkcode_sample"),
            "category": item.get("category"),
            "brand": item.get("brand"),
            "item_name": item.get("item_name"),
            "image_url": item.get("image_url"),
        }
        top_items_metrics.append(item_metric)
    
    # Get lookback_days and top_k from input row or config defaults
    lookback_days = input_row.lookback_days if input_row.lookback_days is not None else config.lookback_days
    top_k = input_row.top_k if input_row.top_k is not None else config.top_k
    
    # Build metrics_json
    # Note: metrics field is typed as Dict[str, float] but stores JSON-compatible values
    metrics = {
        "lookback_days": lookback_days,
        "top_k": top_k,
        "as_of_ts": input_row.as_of_ts.isoformat(),
        "top_items": top_items_metrics,
    }
    
    # Build evidence
    evidence_id = f"evidence-{input_row.subject_id}-affinity-v1"
    
    # Build evidence references (evidence_json)
    # Use settings to build full table name, but for now use the specified format
    evidence_references = {
        "source_table": "opsiq_dev.gold.gold_feature_shopper_top_affinity_v1",
        "source_as_of_ts": input_row.as_of_ts.isoformat(),
    }
    
    evidence = Evidence(
        evidence_id=evidence_id,
        rule_ids=[applied_rule_id],
        thresholds={},  # No thresholds for v1.0.0
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
        metrics=metrics,  # This will store the full nested structure
        evidence_refs=[evidence_id],
        versions=versions,
        computed_at=datetime.now(timezone.utc),
        valid_until=None,
    )
    
    return ShopperItemAffinityResult(
        decision=decision, evidence_set=EvidenceSet(evidence=[evidence])
    )
