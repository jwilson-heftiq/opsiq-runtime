# OpsIQ Runtime

Decision Intelligence Runtime walking skeleton implementing decision primitives (`operational_risk`, `shopper_frequency_trend`, `shopper_health_classification`, `shopper_item_affinity_score`, `shopper_weekly_ad_slate`, `shopper_coupon_offer_set`, `order_line_fulfillment_risk`, `order_fulfillment_risk`, `customer_order_impact_risk`) with hexagonal architecture (ports & adapters).

## Quick start (local)
- Python 3.13 recommended.
- `python -m venv .venv && source .venv/bin/activate`
- `pip install uv && uv pip install -e .[dev]` or `pip install -e .[dev]`
- Run tests: `pytest`
- Lint/format: `ruff check .` and `ruff format .`

## Run services
- API: `uvicorn opsiq_runtime.app.main:app --host 0.0.0.0 --port 8080`
- CLI run-once: `python -m opsiq_runtime.app.cli run --tenant price_chopper --primitive operational_risk --config cfg_v1`
- Local helper: `scripts/run_local.sh`

## Docker

### Using docker-compose (recommended for local development)
- Start API: `docker-compose --profile api up`
- Run CLI once: `docker-compose --profile cli run cli --tenant price_chopper --primitive operational_risk --config cfg_v1`
- Build: `docker-compose build`
- Stop: `docker-compose down`

### Using docker directly
- Build: `docker build -f docker/Dockerfile -t opsiq-runtime .`
- Run API: `docker run -p 8080:8080 opsiq-runtime`
- Run CLI: `docker run opsiq-runtime run --tenant price_chopper --primitive operational_risk --config cfg_v1`

Example API calls:
```
# Async execution (default - returns immediately)
curl -X POST http://localhost:8080/run \
  -H "Content-Type: application/json" \
  -d '{"tenant_id":"price_chopper","primitive_name":"operational_risk","config_version":"cfg_v1","as_of_ts":"2024-01-01T00:00:00Z","correlation_id":"abc-123"}'

# Check job status
curl http://localhost:8080/status/abc-123

# Cancel a running job
curl -X POST http://localhost:8080/cancel/abc-123

# Synchronous execution (blocks until completion)
curl -X POST http://localhost:8080/run/sync \
  -H "Content-Type: application/json" \
  -d '{"tenant_id":"price_chopper","primitive_name":"operational_risk","config_version":"cfg_v1"}'
```

## Architecture notes
- Domain logic in `src/opsiq_runtime/domain/**` is pure and unit-testable.
- Application orchestration in `src/opsiq_runtime/application/`.
- Ports in `src/opsiq_runtime/ports/`; adapters under `src/opsiq_runtime/adapters/`.
- Observability and settings under `src/opsiq_runtime/observability/` and `src/opsiq_runtime/settings.py`.
- FastAPI app under `src/opsiq_runtime/app/`.

## Domain Modules

### activation_policy

A reusable, domain-only module providing deterministic policy functions for activation primitives. This module implements ADR-016 and provides shared guardrails for item identity resolution, exclusions, ordering, selection, and reason attribution.

**Location:** `src/opsiq_runtime/domain/activation_policy/`

**Features:**
- **Item Identity Resolution**: Resolves `item_group_id` from `linkcode` (preferred) or `gtin` (fallback)
- **Exclusion Policies**: Supports weekly ad overlap, recent purchase exclusions, and custom exclusion hooks
- **Deterministic Ordering**: Stable ranking by score DESC, ad_position ASC, gtin ASC tie-breaker
- **Selection Constraints**: Max items cap and per-category caps while preserving order
- **Reason Attribution**: Item-level reasons and primitive-level driver aggregation
- **Confidence Calculation**: Computes HIGH/MEDIUM/LOW confidence based on match rates

**Key Models:**
- `ActivationItem`: Core item representation with `item_group_id`, `gtin`, `linkcode`, `category`, `score`, and `metadata`
- `PolicyConfig`: Configuration with defaults for exclusions, caps, and confidence thresholds
- `PolicyOutcome`: Final result with selected/excluded items, match rates, drivers, and computed confidence
- `ExclusionResult`: Result of exclusion checks with reasons

**Core Functions:**
- `resolve_item_group_id()`: Resolves item identity (linkcode > gtin priority)
- `build_activation_item()`: Helper to create ActivationItem with resolved identity
- `exclude_if_in_set()`: Exclusion check for weekly ad overlap
- `exclude_if_recent_purchase()`: Exclusion check for recent purchases
- `apply_exclusions()`: Aggregates multiple exclusion checks with reason counting
- `stable_rank()`: Deterministic sorting by score, ad_position, gtin
- `compute_match_rate()`: Calculates fraction of items with score > 0
- `apply_max_items()`: Truncates to maximum items while preserving order
- `apply_category_cap()`: Enforces per-category limits (items with `category=None` are uncapped)
- `aggregate_drivers()`: Builds primitive-level driver list from selected/excluded items
- `build_policy_outcome()`: Assembles PolicyOutcome with confidence calculation

**Usage Pattern:**
```python
from opsiq_runtime.domain.activation_policy import (
    ActivationItem,
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
]

# 2. Apply exclusions
exclusion_checks = [
    lambda item: exclude_if_in_set(item, weekly_ad_overlap, "WEEKLY_AD_OVERLAP_EXCLUSION"),
    lambda item: exclude_if_recent_purchase(item, recent_purchases),
]
eligible, excluded, reason_counts = apply_exclusions(candidates, exclusion_checks)

# 3. Stable rank and apply constraints
ranked = stable_rank(eligible)
config = PolicyConfig(max_items=5, category_cap=2)
final_selected = apply_max_items(
    apply_category_cap(ranked, config.category_cap or 999),
    config.max_items
)

# 4. Build outcome
match_rate = compute_match_rate(final_selected)
drivers = aggregate_drivers(final_selected, excluded)
outcome = build_policy_outcome(
    selected_items=final_selected,
    excluded_items=excluded,
    candidates_count=len(candidates),
    match_rate=match_rate,
    drivers=drivers,
    config=config,
)
```

**Design Principles:**
- **Domain-Only**: No infrastructure imports (Databricks, boto3, FastAPI, etc.)
- **Pure Functions**: All functions are stateless with no I/O or side effects
- **Deterministic**: Same inputs always produce same outputs
- **Immutable**: All models use frozen dataclasses
- **Type-Safe**: Full Python 3.13+ type annotations

**Used By:**
- `shopper_weekly_ad_slate` primitive (can be refactored to use this module)
- `shopper_coupon_offer_set` primitive
- Any future activation primitives requiring policy guardrails

**Testing:**
Comprehensive unit test coverage in `tests/unit/activation_policy/` with 72+ tests covering:
- Identity resolution (linkcode precedence, gtin fallback, null handling)
- Exclusion logic (single/multiple exclusions, reason counting)
- Ordering (score sorting, tie-breakers, stability)
- Selection (max_items truncation, category caps, None category handling)
- Reason attribution (reason accumulation, driver aggregation)
- Confidence calculation (all confidence level rules)

## Environment
See `.env.example` for defaults such as log level and output directory.

## Databricks Mode

The runtime can be configured to use Databricks as the data source and sink instead of local file/in-memory adapters.

### Setup

Set the `RUNTIME_ADAPTERS` environment variable to `databricks` and configure the required Databricks connection settings:

```bash
export RUNTIME_ADAPTERS=databricks
export DATABRICKS_SERVER_HOSTNAME=your-workspace.cloud.databricks.com
export DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your-warehouse-id
export DATABRICKS_ACCESS_TOKEN=your-personal-access-token
```

Optional settings:
- `DATABRICKS_CATALOG` - Catalog name (default: None)
- `DATABRICKS_SCHEMA` - Schema name (default: None)
- `DATABRICKS_TABLE_PREFIX` - Prefix for table names (default: "")
- `DATABRICKS_WAREHOUSE_TIMEOUT_SECONDS` - Query timeout in seconds (default: 30)
- `DATABRICKS_USE_MERGE` - Use MERGE INTO for writes (default: true, set to false for DELETE+INSERT)

### Table Names

The Databricks adapters expect the following tables:

**Input Tables:**
- `{prefix}gold_canonical_shopper_recency_input_v1` (for `operational_risk` primitive)
  - Columns: `tenant_id`, `subject_type`, `subject_id`, `as_of_ts`, `last_trip_ts`, `days_since_last_trip`, `config_version`
  - Note: `canonical_version` is derived from `config_version` if not present in the table

- `{prefix}gold_canonical_shopper_frequency_input_v1` (for `shopper_frequency_trend` primitive)
  - Columns: `tenant_id`, `subject_type`, `subject_id`, `as_of_ts`, `last_trip_ts`, `prev_trip_ts`, `recent_gap_days`, `baseline_avg_gap_days`, `baseline_trip_count`, `baseline_window_days`, `config_version`

- `{prefix}gold_feature_shopper_top_affinity_v1` (for `shopper_item_affinity_score` primitive)
  - Location: Catalog `opsiq_dev`, Schema `gold`
  - Columns: `tenant_id`, `subject_type`, `subject_id`, `as_of_ts`, `top_affinity_items` (array<struct<...>>), `lookback_days` (optional), `top_k` (optional), `config_version`
  - Note: `top_affinity_items` is an array of structs containing item affinity data (rank, item_group_id, affinity_score, trip_count, days_since_last_purchase, total_sales, gtin_sample, linkcode_sample, category, brand, item_name, image_url)

- `{prefix}gold_decision_output_v1` (for `shopper_health_classification` primitive - composite primitive that reads from this table)
  - This primitive reads decision outputs from `operational_risk` and `shopper_frequency_trend` primitives stored in this table
  - Filters by `tenant_id`, `subject_type='shopper'`, and `primitive_name IN ('operational_risk','shopper_frequency_trend')`

- `{prefix}gold_canonical_order_line_fulfillment_input_v1` (for `order_line_fulfillment_risk` primitive)
  - Location: Catalog `opsiq_dev`, Schema `gold`
  - Columns: `tenant_id`, `subject_type`, `subject_id`, `as_of_ts`, `need_by_date`, `open_quantity`, `projected_available_quantity`, `order_status`, `is_on_hold`, `release_shortage_qty`, `plant_shortage_qty`, `projected_onhand_qty_eod`, `supply_qty`, `demand_qty`, `partnum`, `customer_id`, `ordernum`, `plant`, `warehouse`, `config_version`, `canonical_version`
  - Note: `ordernum` and `customer_id` are required for aggregation primitives (`order_fulfillment_risk`, `customer_order_impact_risk`)

- `opsiq_dev.gold.gold_canonical_weekly_ad_item_v1` (for `shopper_weekly_ad_slate` primitive)
  - Location: Catalog `opsiq_dev`, Schema `gold`
  - Columns: `tenant_id`, `ad_id`, `ad_group_id`, `scope_type`, `scope_value`, `as_of_ts`, `gtin`, `linkcode`, `item_group_id`, `title`, `promo_text`, `primary_image_url`, `promo_price`, `ad_price_raw`, `ad_price_uom`, `ad_price_qualifier`
  - Note: This table contains ad candidate items that are the same for all shoppers for a given (ad_id, scope_type, scope_value)

- `opsiq_dev.gold.gold_canonical_trip_item_enriched_v1` (for `shopper_weekly_ad_slate` and `shopper_coupon_offer_set` primitives)
  - Location: Catalog `opsiq_dev`, Schema `gold`
  - Columns: `tenant_id`, `shopper_id`, `trip_ts`, `gtin`, `linkcode`, `category`, `unit_price`, `amount`, `quantity`, and other trip item fields
  - Note: Used to exclude recently purchased items and fetch baseline pricing for coupon offers

- `opsiq_dev.gold.gold_policy_item_eligibility_v1` (for `shopper_coupon_offer_set` primitive)
  - Location: Catalog `opsiq_dev`, Schema `gold`
  - Columns: `tenant_id`, `as_of_ts`, `gtin`, `linkcode`, `item_group_id`, `is_coupon_eligible`, `ineligible_reasons`, and product attributes
  - Note: Continuity eligibility table used as eligibility gate for coupon offers

- `{prefix}gold_decision_output_v1` (for `order_fulfillment_risk` and `customer_order_impact_risk` primitives - composite primitives that read from this table)
  - These primitives read decision outputs from lower-level primitives stored in this table
  - `order_fulfillment_risk`: Filters by `primitive_name='order_line_fulfillment_risk'`, `subject_type='order_line'`, groups by `ordernum` from `metrics_json`
  - `customer_order_impact_risk`: Filters by `primitive_name='order_fulfillment_risk'`, `subject_type='order'`, groups by `customer_id` from `metrics_json`
  - Note: When using these primitives, set `DATABRICKS_CATALOG=opsiq_dev` and `DATABRICKS_SCHEMA=gold` environment variables

**Output Tables:**
- `{prefix}gold_decision_output_v1` (shared by all primitives)
  - Columns: `tenant_id`, `subject_type`, `subject_id`, `primitive_name`, `primitive_version`, `canonical_version`, `config_version`, `as_of_ts`, `decision_state`, `confidence`, `drivers_json`, `metrics_json`, `evidence_refs_json`, `computed_at`, `valid_until`, `correlation_id`

- `{prefix}gold_decision_evidence_v1` (shared by all primitives)
  - Columns: `tenant_id`, `subject_type`, `subject_id`, `primitive_name`, `primitive_version`, `canonical_version`, `config_version`, `as_of_ts`, `evidence_id`, `evidence_json`, `computed_at`, `correlation_id`

Where `{prefix}` is the value of `DATABRICKS_TABLE_PREFIX` (empty by default).

### Example: Running with Databricks

```bash
# Set environment variables
export RUNTIME_ADAPTERS=databricks
export DATABRICKS_SERVER_HOSTNAME=your-workspace.cloud.databricks.com
export DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your-warehouse-id
export DATABRICKS_ACCESS_TOKEN=your-token

# Run via API
uvicorn opsiq_runtime.app.main:app --host 0.0.0.0 --port 8080

# Or via CLI
python -m opsiq_runtime.app.cli run --tenant price_chopper --primitive operational_risk --config cfg_v1
```

**For `order_line_fulfillment_risk`, `shopper_item_affinity_score`, and `shopper_weekly_ad_slate` primitives, set catalog and schema:**
```bash
export DATABRICKS_CATALOG=opsiq_dev
export DATABRICKS_SCHEMA=gold
python -m opsiq_runtime.app.cli run --tenant vmc_group --primitive order_line_fulfillment_risk --config cfg_v1

# Or for shopper_item_affinity_score:
python -m opsiq_runtime.app.cli run --tenant price_chopper --primitive shopper_item_affinity_score --config cfg_v1

# Or for shopper_weekly_ad_slate:
python -m opsiq_runtime.app.cli run --tenant price_chopper --primitive shopper_weekly_ad_slate --config cfg_v1

# Or for shopper_coupon_offer_set:
python -m opsiq_runtime.app.cli run --tenant price_chopper --primitive shopper_coupon_offer_set --config cfg_v1
```

### API Request Reference

The `/run` endpoint accepts a JSON payload with the following fields:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `tenant_id` | string | **Yes** | Identifies the tenant/customer. Used to filter input data and partition output data. Example: `"price_chopper"` |
| `primitive_name` | string | **Yes** | The decision primitive to execute. Supported: `"operational_risk"`, `"shopper_frequency_trend"`, `"shopper_health_classification"`, `"shopper_item_affinity_score"`, `"shopper_weekly_ad_slate"`, `"shopper_coupon_offer_set"`, `"order_line_fulfillment_risk"`, `"order_fulfillment_risk"`, `"customer_order_impact_risk"` |
| `config_version` | string | **Yes** | Version of the configuration to use. Determines thresholds and rules. Example: `"cfg_v1"` |
| `as_of_ts` | string (ISO 8601) | No | The point-in-time for evaluation. Defaults to current time if omitted. See details below. |
| `correlation_id` | string | No | Unique identifier for this run, used for tracing and idempotency. Auto-generated if omitted. |
| `primitive_version` | string | No | Version of the primitive implementation. Defaults to `"1.0.0"` |

#### Understanding `as_of_ts`

The `as_of_ts` field specifies the **evaluation timestamp** - the point in time for which decisions are computed:

- **Current evaluation**: Omit or set to `null` to evaluate as of "now"
- **Historical analysis**: Set to a past date to compute what decisions *would have been* at that time
- **Batch processing**: Set to the business date being processed

For the `operational_risk` primitive, `as_of_ts` is used to:
1. Filter input rows from the canonical table
2. Compute `days_since_last_trip` (if not pre-computed) as `as_of_ts - last_trip_ts`
3. Determine if a shopper is AT_RISK based on the configured threshold

**Example**: If `as_of_ts = "2024-01-15T00:00:00Z"` and a shopper's `last_trip_ts = "2023-12-01"`, then `days_since_last_trip = 45`.

### Example API Request

**Async execution (default - returns immediately):**
```bash
curl -X POST http://localhost:8080/run \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_id": "price_chopper",
    "primitive_name": "operational_risk",
    "config_version": "cfg_v1",
    "as_of_ts": "2024-01-01T00:00:00Z",
    "correlation_id": "abc-123",
    "primitive_version": "1.0.0"
  }'
```

**Response (async):**
```json
{
  "correlation_id": "abc-123",
  "status": "started",
  "message": "Job started. Use /status/{correlation_id} to check progress."
}
```

**Synchronous execution (blocks until completion):**
```bash
curl -X POST http://localhost:8080/run/sync \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_id": "price_chopper",
    "primitive_name": "operational_risk",
    "config_version": "cfg_v1",
    "as_of_ts": "2024-01-01T00:00:00Z",
    "correlation_id": "abc-123"
  }'
```

**Response (sync):**
```json
{
  "tenant_id": "price_chopper",
  "primitive_name": "operational_risk",
  "primitive_version": "1.0.0",
  "config_version": "cfg_v1",
  "count": 774470,
  "at_risk_count": 123456,
  "not_at_risk_count": 650000,
  "unknown_count": 1014,
  "duration_ms": 45230
}
```

## Job Management

The runtime supports async job execution with cancellation and status tracking.

### Starting a Job

The `/run` endpoint (default) starts jobs asynchronously and returns immediately with a `correlation_id`. If you don't provide a `correlation_id`, one will be auto-generated.

### Checking Job Status

Use the `/status/{correlation_id}` endpoint to check the status of a running or completed job:

```bash
curl http://localhost:8080/status/abc-123
```

**Response:**
```json
{
  "correlation_id": "abc-123",
  "tenant_id": "price_chopper",
  "primitive_name": "operational_risk",
  "status": "completed",
  "started_at": "2024-01-01T00:00:00Z",
  "completed_at": "2024-01-01T00:00:45Z",
  "duration_ms": 45230,
  "result": {
    "tenant_id": "price_chopper",
    "primitive_name": "operational_risk",
    "count": 774470,
    "at_risk_count": 123456,
    "not_at_risk_count": 650000,
    "unknown_count": 1014,
    "duration_ms": 45230
  }
}
```

**Status values:**
- `running` - Job is currently executing
- `completed` - Job finished successfully
- `cancelled` - Job was cancelled
- `failed` - Job encountered an error

### Cancelling a Job

To stop a running job, use the `/cancel/{correlation_id}` endpoint:

```bash
curl -X POST http://localhost:8080/cancel/abc-123
```

**Response:**
```json
{
  "correlation_id": "abc-123",
  "status": "cancelled",
  "message": "Job cancellation requested"
}
```

**Notes:**
- Cancellation is checked between processing each input row, so it may take a moment to take effect
- Only jobs with status `running` can be cancelled
- Cancelled jobs will have their status updated in the run registry (if using Databricks adapters)

### What Happens During a Run

This will:
1. Read inputs from `gold_canonical_shopper_recency_input_v1` filtered by `tenant_id = 'price_chopper'`
2. Evaluate operational risk for each input row
3. Write decisions to `gold_decision_output_operational_risk_v1`
4. Write evidence to `gold_decision_evidence_operational_risk_v1`

All writes are idempotent using MERGE INTO (or DELETE+INSERT if `DATABRICKS_USE_MERGE=false`).

## Primitives

### operational_risk

Evaluates whether a shopper is at risk of churning based on days since last trip.

**Decision States:**
- `AT_RISK`: Shopper has not visited in `at_risk_days` or more
- `NOT_AT_RISK`: Shopper visited within `at_risk_days`
- `UNKNOWN`: Insufficient data (no `last_trip_ts`)

**Configuration:**
- `at_risk_days` (default: 30, env: `DEFAULT_AT_RISK_DAYS`)

**Example:**
```bash
python -m opsiq_runtime.app.cli run --tenant price_chopper --primitive operational_risk --config cfg_v1
```

### shopper_frequency_trend

Evaluates shopper trip frequency trends by comparing recent trip cadence to baseline.

**Decision States:**
- `DECLINING`: Recent trip cadence is slowing (ratio >= `decline_ratio_threshold`)
- `STABLE`: Recent trip cadence is similar to baseline (between thresholds)
- `IMPROVING`: Recent trip cadence is accelerating (ratio <= `improve_ratio_threshold`)
- `UNKNOWN`: Insufficient data or invalid baseline

**Configuration:**
- `baseline_window_days` (default: 90, env: `DEFAULT_BASELINE_WINDOW_DAYS`)
- `min_baseline_trips` (default: 4, env: `DEFAULT_MIN_BASELINE_TRIPS`)
- `decline_ratio_threshold` (default: 1.5, env: `DEFAULT_DECLINE_RATIO_THRESHOLD`)
- `improve_ratio_threshold` (default: 0.75, env: `DEFAULT_IMPROVE_RATIO_THRESHOLD`)
- `max_reasonable_gap_days` (default: 365, env: `DEFAULT_MAX_REASONABLE_GAP_DAYS`)

**Input Table:** `gold_canonical_shopper_frequency_input_v1`

**Evaluation Logic:**
1. If `last_trip_ts` or `prev_trip_ts` is missing → `UNKNOWN` (insufficient trip history)
2. If `baseline_trip_count < min_baseline_trips` → `UNKNOWN` (insufficient baseline)
3. If `baseline_avg_gap_days` is missing or <= 0 → `UNKNOWN` (invalid baseline)
4. If `recent_gap_days` is missing → `UNKNOWN` (recent gap missing)
5. If `recent_gap_days > max_reasonable_gap_days` → `UNKNOWN` (out of range)
6. Otherwise, compute `ratio = recent_gap_days / baseline_avg_gap_days`:
   - If `ratio >= decline_ratio_threshold` → `DECLINING` (cadence slowing)
   - Else if `ratio <= improve_ratio_threshold` → `IMPROVING` (cadence accelerating)
   - Else → `STABLE` (cadence stable)

**Example:**
```bash
python -m opsiq_runtime.app.cli run --tenant price_chopper --primitive shopper_frequency_trend --config cfg_v1
```

**Example API Request:**
```bash
curl -X POST http://localhost:8080/run \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_id": "price_chopper",
    "primitive_name": "shopper_frequency_trend",
    "config_version": "cfg_v1",
    "as_of_ts": "2024-01-01T00:00:00Z",
    "correlation_id": "abc-123"
  }'
```

**Example Response:**
```json
{
  "tenant_id": "price_chopper",
  "primitive_name": "shopper_frequency_trend",
  "primitive_version": "1.0.0",
  "config_version": "cfg_v1",
  "count": 774470,
  "state_counts": {
    "DECLINING": 50000,
    "STABLE": 650000,
    "IMPROVING": 70000,
    "UNKNOWN": 4470
  },
  "duration_ms": 45230
}
```

### shopper_health_classification

A composite primitive that combines outputs from `operational_risk` and `shopper_frequency_trend` to produce a single actionable shopper health classification.

**Decision States:**
- `URGENT`: Shopper is at risk of churning (risk_state = "AT_RISK")
- `WATCHLIST`: Shopper shows declining cadence or unknown risk with declining trend
- `HEALTHY`: Shopper is not at risk and has stable or improving cadence
- `UNKNOWN`: Insufficient signals from both primitives

**Configuration:**
- No thresholds required for v1.0.0

**Input Source:**
- Reads from `gold_decision_output_v1` table
- Filters for `operational_risk` and `shopper_frequency_trend` decision outputs
- Pivots results to combine risk_state and trend_state per shopper

**Composition Rules (priority-ordered):**
1. If `risk_state == "AT_RISK"` → `URGENT` (confidence: HIGH, drivers: ["LAPSE_RISK"])
2. If `risk_state == "UNKNOWN"` and `trend_state == "UNKNOWN"` → `UNKNOWN` (confidence: LOW, drivers: ["INSUFFICIENT_SIGNALS"])
3. If `risk_state == "NOT_AT_RISK"` and `trend_state == "DECLINING"` → `WATCHLIST` (confidence: MEDIUM, drivers: ["CADENCE_DECLINING"])
4. If `risk_state == "UNKNOWN"` and `trend_state == "DECLINING"` → `WATCHLIST` (confidence: LOW, drivers: ["CADENCE_DECLINING", "RISK_UNKNOWN"])
5. If `risk_state == "NOT_AT_RISK"` and `trend_state in ("STABLE","IMPROVING")` → `HEALTHY` (confidence: HIGH, drivers: ["RISK_OK", "CADENCE_OK"])
6. Else → `UNKNOWN` (confidence: MEDIUM, drivers: ["PARTIAL_SIGNALS"])

**Evidence Structure:**
- Evidence includes `applied_rule_id` identifying which composition rule was applied
- `source_primitives` array contains metadata from both source primitives (primitive_name, primitive_version, as_of_ts, evidence_refs)
- `composition_inputs` contains the risk_state and trend_state used for composition

**Metrics:**
- `risk_state`: The operational risk state from the source primitive
- `trend_state`: The frequency trend state from the source primitive
- `risk_source_as_of_ts`: Timestamp of the operational_risk decision (ISO format)
- `trend_source_as_of_ts`: Timestamp of the shopper_frequency_trend decision (ISO format)

**Example:**
```bash
python -m opsiq_runtime.app.cli run --tenant price_chopper --primitive shopper_health_classification --config cfg_v1
```

**Example API Request:**
```bash
curl -X POST http://localhost:8080/run \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_id": "price_chopper",
    "primitive_name": "shopper_health_classification",
    "config_version": "cfg_v1",
    "as_of_ts": "2024-01-01T00:00:00Z",
    "correlation_id": "abc-123"
  }'
```

**Example Response:**
```json
{
  "tenant_id": "price_chopper",
  "primitive_name": "shopper_health_classification",
  "primitive_version": "1.0.0",
  "config_version": "cfg_v1",
  "count": 774470,
  "state_counts": {
    "URGENT": 123456,
    "WATCHLIST": 50000,
    "HEALTHY": 600000,
    "UNKNOWN": 1014
  },
  "duration_ms": 45230
}
```

### shopper_item_affinity_score

Evaluates shopper item affinity by computing top affinity items based on purchase history and item preferences.

**Decision States:**
- `COMPUTED`: Top affinity items successfully computed and available
- `UNKNOWN`: No affinity items found (empty or null `top_affinity_items`)

**Configuration:**
- `lookback_days` (default: 90): Number of days to look back for affinity computation
- `top_k` (default: 50): Maximum number of top items to return

**Input Table:** `gold_feature_shopper_top_affinity_v1`

**Table Location:** Catalog `opsiq_dev`, Schema `gold`

**Evaluation Logic:**
1. If `top_affinity_items` is empty or null → `UNKNOWN` (confidence: LOW, driver: `["NO_AFFINITY_ITEMS"]`)
2. Otherwise → `COMPUTED` (confidence: HIGH, driver: `["TOP_AFFINITY_COMPUTED"]`)

**Metrics:**
The `metrics_json` field contains:
- `lookback_days`: Number of days used for affinity computation (from input row or config default)
- `top_k`: Maximum number of items returned (from input row or config default)
- `as_of_ts`: Timestamp of the affinity computation (ISO format)
- `top_items`: Array of top affinity items, each containing:
  - `rank`: Item rank (int, 1-based)
  - `item_group_id`: Item group identifier (string)
  - `affinity_score`: Affinity score (float)
  - `trip_count`: Number of trips where item was purchased (int)
  - `days_since_last_purchase`: Days since last purchase (int)
  - `total_sales`: Total sales amount for this item (float)
  - `gtin_sample`: Sample GTIN code (string, nullable)
  - `linkcode_sample`: Sample linkcode (string, nullable)
  - `category`: Item category (string, nullable)
  - `brand`: Item brand (string, nullable)
  - `item_name`: Item name (string, nullable)
  - `image_url`: Item image URL (string, nullable)

**Evidence:**
- Evidence ID format: `evidence-<shopper_id>-affinity-v1`
- Evidence references include:
  - `source_table`: Source table name (`opsiq_dev.gold.gold_feature_shopper_top_affinity_v1`)
  - `source_as_of_ts`: Timestamp of the source data (ISO format)

**Example:**
```bash
python -m opsiq_runtime.app.cli run --tenant price_chopper --primitive shopper_item_affinity_score --config cfg_v1
```

**Example API Request:**
```bash
curl -X POST http://localhost:8080/run \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_id": "price_chopper",
    "primitive_name": "shopper_item_affinity_score",
    "config_version": "cfg_v1",
    "as_of_ts": "2024-01-01T00:00:00Z",
    "correlation_id": "abc-123"
  }'
```

**Example Response:**
```json
{
  "tenant_id": "price_chopper",
  "primitive_name": "shopper_item_affinity_score",
  "primitive_version": "1.0.0",
  "config_version": "cfg_v1",
  "count": 774470,
  "state_counts": {
    "COMPUTED": 750000,
    "UNKNOWN": 24470
  },
  "duration_ms": 38200
}
```

**Example Decision Output (`metrics_json`):**
```json
{
  "lookback_days": 90,
  "top_k": 50,
  "as_of_ts": "2024-01-01T12:00:00Z",
  "top_items": [
    {
      "rank": 1,
      "item_group_id": "item_001",
      "affinity_score": 0.95,
      "trip_count": 10,
      "days_since_last_purchase": 5,
      "total_sales": 150.0,
      "gtin_sample": "1234567890123",
      "linkcode_sample": "LINK001",
      "category": "Dairy",
      "brand": "Brand A",
      "item_name": "Milk",
      "image_url": "https://example.com/milk.jpg"
    },
    {
      "rank": 2,
      "item_group_id": "item_002",
      "affinity_score": 0.85,
      "trip_count": 8,
      "days_since_last_purchase": 3,
      "total_sales": 120.50,
      "gtin_sample": "9876543210987",
      "linkcode_sample": "LINK002",
      "category": "Produce",
      "brand": "Brand B",
      "item_name": "Apples",
      "image_url": "https://example.com/apples.jpg"
    }
  ]
}
```

**Note:** For this primitive, set `DATABRICKS_CATALOG=opsiq_dev` and `DATABRICKS_SCHEMA=gold` environment variables to access the feature table.

### shopper_weekly_ad_slate

A composite primitive that produces a ranked, personalized list of ad items per shopper by combining ad candidates, affinity scores, and purchase exclusions.

**Decision States:**
- `COMPUTED`: Slate successfully computed with one or more items
- `UNKNOWN`: No eligible ad items after applying exclusions (only emitted if `sparse_emission=false`)

**Configuration:**
- `slate_size_k` (default: 20): Maximum number of items in the slate
- `affinity_top_k` (default: 50): Number of top affinity items to consider for scoring
- `exclude_lookback_days` (default: 14): Days to look back for recent purchase exclusions
- `exclude_by` (default: "item_group_id"): Field to use for exclusions ("item_group_id" or "gtin")
- `category_cap` (default: None): Maximum items per category (optional)
- `min_match_rate_for_high_confidence` (default: 0.50): Minimum match rate (items with affinity > 0) for HIGH confidence
- `sparse_emission` (default: True): If True, only emit shoppers with non-empty slates
- `ad_id` (required): Ad identifier to filter candidates
- `scope_type` (required): Scope type (e.g., "store", "region")
- `scope_value` (required): Scope value (e.g., "store_123")
- `hours_window` (default: 36): Hours window for ad candidates and affinity freshness

**Input Tables:**
- `opsiq_dev.gold.gold_canonical_weekly_ad_item_v1`: Ad candidate items (same for all shoppers)
- `opsiq_dev.gold.gold_feature_shopper_top_affinity_v1`: Shopper affinity scores (per shopper)
- `opsiq_dev.gold.gold_canonical_trip_item_enriched_v1`: Recent purchase history for exclusions (per shopper)

**Table Location:** Catalog `opsiq_dev`, Schema `gold`

**Evaluation Logic:**
1. Fetch ad candidates filtered by `ad_id`, `scope_type`, `scope_value`
2. For each shopper:
   - Build affinity score map from `top_affinity_items`
   - Score each candidate: affinity score if matched, 0.0 otherwise
   - Exclude candidates that match recent purchase keys (within `exclude_lookback_days`)
   - Sort remaining candidates by: score DESC, promo_price ASC NULLS LAST, gtin ASC
   - Apply category cap if enabled (keep at most N per category)
   - Select top `slate_size_k` items
3. If slate is empty and `sparse_emission=True`: return None (skip output)
4. Compute confidence: HIGH if match_rate >= `min_match_rate_for_high_confidence`, else MEDIUM

**Metrics:**
The `metrics_json` field contains:
- `ad_id`: Ad identifier
- `scope_type`: Scope type
- `scope_value`: Scope value
- `slate_size_k`: Maximum slate size
- `exclude_lookback_days`: Days for exclusion lookback
- `excluded_count`: Number of candidates excluded due to recent purchases
- `candidates_count`: Total number of ad candidates
- `match_rate`: Fraction of slate items with affinity score > 0
- `items`: Array of slate items, each containing:
  - `rank`: Item rank (int, 1-based)
  - `item_group_id`: Item group identifier (string)
  - `gtin`: GTIN code (string, nullable)
  - `linkcode`: Linkcode (string, nullable)
  - `score`: Affinity score (float)
  - `title`: Item title (string, nullable)
  - `promo_price`: Promotional price (float, nullable)
  - `ad_group_id`: Ad group identifier (string)
  - `reasons`: Array of reason codes (e.g., `["IN_CURRENT_AD", "AFFINITY_MATCH"]`)

**Evidence:**
- Evidence ID format: `evidence-<shopper_id>-weekly-ad-slate-v1`
- Evidence references include:
  - `sources`: Source table names for ad candidates, affinity, and purchases
  - `as_of`: Timestamps for ad candidates and affinity data

**Drivers:**
- `IN_CURRENT_AD`: Item is in the current ad (always present)
- `AFFINITY_MATCH`: Item has a matching affinity score > 0
- `RECENT_PURCHASE_EXCLUSIONS`: Some items were excluded due to recent purchases
- `NO_ELIGIBLE_AD_ITEMS`: No eligible items after exclusions (only if `sparse_emission=false`)

**Example:**
```bash
export DATABRICKS_CATALOG=opsiq_dev
export DATABRICKS_SCHEMA=gold
python -m opsiq_runtime.app.cli run --tenant price_chopper --primitive shopper_weekly_ad_slate --config cfg_v1
```

**Example API Request:**
```bash
curl -X POST http://localhost:8080/run \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_id": "price_chopper",
    "primitive_name": "shopper_weekly_ad_slate",
    "config_version": "cfg_v1",
    "as_of_ts": "2024-01-01T00:00:00Z",
    "correlation_id": "abc-123"
  }'
```

**Example Response:**
```json
{
  "tenant_id": "price_chopper",
  "primitive_name": "shopper_weekly_ad_slate",
  "primitive_version": "1.0.0",
  "config_version": "cfg_v1",
  "count": 450000,
  "evaluated_count": 500000,
  "state_counts": {
    "COMPUTED": 450000
  },
  "duration_ms": 125000
}
```

**Example Decision Output (`metrics_json`):**
```json
{
  "ad_id": "ad_001",
  "scope_type": "store",
  "scope_value": "store_123",
  "slate_size_k": 20,
  "exclude_lookback_days": 14,
  "excluded_count": 5,
  "candidates_count": 100,
  "match_rate": 0.75,
  "items": [
    {
      "rank": 1,
      "item_group_id": "item_001",
      "gtin": "1234567890123",
      "linkcode": "LINK001",
      "score": 0.9,
      "title": "Organic Milk",
      "promo_price": 4.99,
      "ad_group_id": "ad_group_1",
      "reasons": ["IN_CURRENT_AD", "AFFINITY_MATCH"]
    },
    {
      "rank": 2,
      "item_group_id": "item_002",
      "gtin": "9876543210987",
      "linkcode": null,
      "score": 0.8,
      "title": "Fresh Bread",
      "promo_price": 3.49,
      "ad_group_id": "ad_group_1",
      "reasons": ["IN_CURRENT_AD", "AFFINITY_MATCH"]
    }
  ]
}
```

**Note:** This primitive uses sparse emission by default. Shoppers with no eligible items after exclusions are not emitted. The `evaluated_count` in the response summary indicates the total number of shoppers evaluated, while `count` indicates the number of shoppers with non-empty slates.

### shopper_coupon_offer_set

A retail activation primitive that generates up to 10 personalized coupon offers per shopper by combining affinity scores, eligibility policies, exclusions, and baseline pricing with discount calculation.

**Decision States:**
- `COMPUTED`: Coupon offers successfully generated (one or more offers)
- `UNKNOWN`: No eligible offers generated (only emitted if `sparse_emission=false`)

**Configuration:**
- `max_offers` (default: 10): Maximum number of coupon offers per shopper
- `discount_pct` (default: 25): Discount percentage (offer_price = baseline_price * (1 - discount_pct/100))
- `affinity_top_k` (default: 50): Number of top affinity items to consider
- `exclude_lookback_days` (default: 14): Days to look back for recent purchase exclusions
- `min_match_rate_for_high_confidence` (default: 0.50): Minimum match rate (items with affinity > 0) for HIGH confidence
- `sparse_emission` (default: True): If True, only emit shoppers with non-empty offer sets
- `ad_hours_window` (default: 72): Hours window for weekly ad exclusion set freshness
- `pricing_fallback_mode` (default: "skip"): Behavior when baseline_price is missing ("skip" to exclude item)
- `category_cap` (default: None): Optional maximum items per category
- `ad_id` (required): Ad identifier for weekly ad exclusion
- `scope_type` (required): Scope type (e.g., "store", "region")
- `scope_value` (required): Scope value (e.g., "store_123")
- `hours_window` (default: 72): Hours window for affinity and eligibility freshness

**Input Tables:**
- `opsiq_dev.gold.gold_feature_shopper_top_affinity_v1`: Shopper affinity scores (primary iteration set)
- `opsiq_dev.gold.gold_policy_item_eligibility_v1`: Item eligibility gate (is_coupon_eligible=true)
- `opsiq_dev.gold.gold_canonical_weekly_ad_item_v1`: Weekly ad items for exclusion (overlap by item_group_id)
- `opsiq_dev.gold.gold_canonical_trip_item_enriched_v1`: Recent purchases for exclusion and baseline pricing

**Table Location:** Catalog `opsiq_dev`, Schema `gold`

**Evaluation Logic:**
1. Build affinity map from `top_affinity_items` up to `affinity_top_k` (ordered by score DESC)
2. Apply eligibility gate: filter to items in `gold_policy_item_eligibility_v1` where `is_coupon_eligible=true`
3. Build ActivationItems for eligible candidates with pricing metadata
4. Apply exclusions using Activation Policy module:
   - Exclude items overlapping weekly ad (by item_group_id)
   - Exclude recently purchased items (within `exclude_lookback_days`)
5. Filter out items missing baseline_price (if `pricing_fallback_mode="skip"`)
6. Calculate offer_price = round(baseline_price * (1 - discount_pct/100), 2)
7. Apply Activation Policy utilities:
   - Stable rank (deterministic ordering)
   - Category cap (if configured)
   - Max items constraint (`max_offers`)
   - Compute match_rate and aggregate drivers
8. Build CouponOfferSetResult with DecisionResult and EvidenceSet
9. **Sparse emission**: return None if offers list is empty (if `sparse_emission=True`)

**Metrics:**
The `metrics_json` field contains:
- `max_offers`: Maximum number of offers (int)
- `discount_pct`: Discount percentage applied (int)
- `candidate_count`: Total number of affinity candidate items (int)
- `eligible_count`: Number of items passing eligibility gate (int)
- `excluded_weekly_ad_count`: Number excluded due to weekly ad overlap (int)
- `excluded_recent_purchase_count`: Number excluded due to recent purchases (int)
- `excluded_pricing_missing_count`: Number excluded due to missing baseline_price (int)
- `offers`: Array of coupon offers, each containing:
  - `rank`: Offer rank (int, 1-based)
  - `item_group_id`: Item group identifier (string)
  - `gtin`: GTIN code (string, nullable)
  - `linkcode`: Linkcode (string, nullable)
  - `affinity_score`: Affinity score (float)
  - `baseline_price`: Baseline unit price (float)
  - `offer_price`: Calculated offer price (float, baseline_price * 0.75)
  - `reasons`: Array of reason codes (e.g., `["HIGH_AFFINITY", "NOT_IN_WEEKLY_AD", "COUPON_DISCOUNT_APPLIED"]`)

**Evidence:**
- Evidence ID format: `evidence-<shopper_id>-coupon-offer-set-v1`
- Evidence references include:
  - `sources`: Source table names for affinity, eligibility, weekly ad, and purchases
  - `context`: Ad context (ad_id, scope_type, scope_value)
  - `as_of`: Timestamps for affinity and eligibility data

**Drivers:**
- `ACTIVATION_POLICY_APPLIED`: Activation Policy module applied (always present)
- `ELIGIBILITY_POLICY_ENFORCED`: Eligibility gate enforced
- `NOT_IN_WEEKLY_AD`: Items not overlapping weekly ad
- `COUPON_DISCOUNT_APPLIED`: Discount calculation applied
- `AFFINITY_MATCH`: Items have affinity score > 0
- `EXCLUSIONS_APPLIED`: Some items were excluded

**Confidence:**
- `HIGH`: Match rate >= `min_match_rate_for_high_confidence` (default: 0.50)
- `MEDIUM`: Match rate > 0 but < threshold
- `LOW`: No items selected (should not occur with sparse emission)

**Example:**
```bash
export DATABRICKS_CATALOG=opsiq_dev
export DATABRICKS_SCHEMA=gold
python -m opsiq_runtime.app.cli run --tenant price_chopper --primitive shopper_coupon_offer_set --config cfg_v1
```

**Example API Request:**
```bash
curl -X POST http://localhost:8080/run \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_id": "price_chopper",
    "primitive_name": "shopper_coupon_offer_set",
    "config_version": "cfg_v1",
    "as_of_ts": "2024-01-01T00:00:00Z",
    "correlation_id": "abc-123"
  }'
```

**Example Response:**
```json
{
  "tenant_id": "price_chopper",
  "primitive_name": "shopper_coupon_offer_set",
  "primitive_version": "1.0.0",
  "config_version": "cfg_v1",
  "count": 350000,
  "evaluated_count": 500000,
  "state_counts": {
    "COMPUTED": 350000
  },
  "duration_ms": 145000
}
```

**Example Decision Output (`metrics_json`):**
```json
{
  "max_offers": 10,
  "discount_pct": 25,
  "candidate_count": 45,
  "eligible_count": 35,
  "excluded_weekly_ad_count": 5,
  "excluded_recent_purchase_count": 8,
  "excluded_pricing_missing_count": 2,
  "offers": [
    {
      "rank": 1,
      "item_group_id": "item_001",
      "gtin": "1234567890123",
      "linkcode": null,
      "affinity_score": 0.9,
      "baseline_price": 10.0,
      "offer_price": 7.5,
      "reasons": ["HIGH_AFFINITY", "NOT_IN_WEEKLY_AD", "COUPON_DISCOUNT_APPLIED"]
    },
    {
      "rank": 2,
      "item_group_id": "item_002",
      "gtin": "9876543210987",
      "linkcode": "LINK002",
      "affinity_score": 0.85,
      "baseline_price": 15.0,
      "offer_price": 11.25,
      "reasons": ["HIGH_AFFINITY", "NOT_IN_WEEKLY_AD", "COUPON_DISCOUNT_APPLIED"]
    }
  ]
}
```

**Note:** This primitive uses sparse emission by default. Shoppers with no eligible offers after exclusions and pricing checks are not emitted. The `evaluated_count` in the response summary indicates the total number of shoppers evaluated, while `count` indicates the number of shoppers with non-empty offer sets. The primitive integrates with the Activation Policy module (ADR-016) for deterministic exclusions, ordering, selection, and confidence computation.

### order_line_fulfillment_risk

Evaluates whether an order line is at risk of not being fulfilled on time based on projected available quantity versus open quantity.

**Decision States:**
- `AT_RISK`: Order line is on hold or projected available quantity is insufficient to fulfill open quantity
- `NOT_AT_RISK`: Order line is closed, has no open quantity, or has sufficient projected supply
- `UNKNOWN`: Missing required inputs (need_by_date, open_quantity, or projected_available_quantity)

**Configuration:**
- `closed_statuses` (default: `{"CLOSED", "CANCELLED"}`, env: `ORDER_LINE_CLOSED_STATUSES` - comma-separated list)

**Input Table:** `gold_canonical_order_line_fulfillment_input_v1`

**Evaluation Rules (priority-ordered):**
1. If `need_by_date`, `open_quantity`, or `projected_available_quantity` is missing → `UNKNOWN` (driver: `MISSING_REQUIRED_INPUTS`)
2. If `is_on_hold == True` → `AT_RISK` (driver: `ON_HOLD`)
3. If `order_status` (uppercase) is in `closed_statuses` → `NOT_AT_RISK` (driver: `NOT_OPEN`)
4. If `open_quantity <= 0` → `NOT_AT_RISK` (driver: `NO_OPEN_QTY`)
5. If `projected_available_quantity < open_quantity` → `AT_RISK` (driver: `PROJECTED_SHORT`)
6. Else → `NOT_AT_RISK` (driver: `SUFFICIENT_SUPPLY`)

**Metrics:**
- `need_by_date`: Target fulfillment date (ISO date string)
- `open_quantity`: Quantity still needed to fulfill the order line
- `projected_available_quantity`: Projected quantity available for fulfillment
- `shortage_quantity`: Computed as `max(open_quantity - projected_available_quantity, 0)`
- Optional: `release_shortage_qty`, `plant_shortage_qty`, `projected_onhand_qty_eod`, `supply_qty`, `demand_qty`
- `ordernum`: Order number (string or int) - required for aggregation
- `customer_id`: Customer identifier (string) - required for aggregation

**Evidence:**
- `applied_rule_id`: Identifies which evaluation rule was applied
- `closed_statuses`: List of statuses considered "closed"
- Context fields (when available): `partnum`, `customer_id`, `plant`, `warehouse`

**Example:**
```bash
python -m opsiq_runtime.app.cli run --tenant vmc_group --primitive order_line_fulfillment_risk --config cfg_v1
```

**Example API Request:**
```bash
curl -X POST http://localhost:8080/run \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_id": "vmc_group",
    "primitive_name": "order_line_fulfillment_risk",
    "config_version": "cfg_v1",
    "as_of_ts": "2024-01-01T00:00:00Z",
    "correlation_id": "abc-123"
  }'
```

**Example Response:**
```json
{
  "tenant_id": "vmc_group",
  "primitive_name": "order_line_fulfillment_risk",
  "primitive_version": "1.0.0",
  "config_version": "cfg_v1",
  "count": 125000,
  "state_counts": {
    "AT_RISK": 15000,
    "NOT_AT_RISK": 108000,
    "UNKNOWN": 2000
  },
  "duration_ms": 32100
}
```

**Worklist Endpoint:**
The runtime provides a dedicated worklist endpoint for order line fulfillment decisions:

```bash
curl "http://localhost:8080/v1/tenants/vmc_group/worklists/order-line-fulfillment?state=AT_RISK&limit=50"
```

This endpoint filters for `primitive_name="order_line_fulfillment_risk"` and `subject_type="order_line"`, returning the latest decision per order line with support for filtering by state, confidence, and subject_id substring matching.

**Decision Bundle Endpoint:**
Get detailed decision information for a specific order line:

```bash
curl "http://localhost:8080/v1/tenants/vmc_group/subjects/order_line/{subject_id}/decision-bundle"
```

Returns the primary decision and associated evidence for the order line.

### order_fulfillment_risk

Composite primitive that aggregates `order_line_fulfillment_risk` decisions to evaluate order-level risk.

**Decision States:**
- `AT_RISK`: Order has one or more order lines at risk
- `NOT_AT_RISK`: All order lines are not at risk
- `UNKNOWN`: No order lines found, or all order lines have unknown state

**Configuration:**
- No configuration parameters (uses defaults)

**Input Source:** Reads from `gold_decision_output_v1` where `primitive_name='order_line_fulfillment_risk'` and `subject_type='order_line'`, grouped by `ordernum` from `metrics_json`

**Evaluation Rules (priority-ordered):**
1. If `order_line_count_total == 0` → `UNKNOWN` (driver: `NO_LINES_FOUND`)
2. If `order_line_count_at_risk > 0` → `AT_RISK` (driver: `HAS_AT_RISK_LINES`)
3. If `order_line_count_unknown == order_line_count_total` → `UNKNOWN` (driver: `ALL_LINES_UNKNOWN`)
4. Else → `NOT_AT_RISK` (driver: `ALL_LINES_OK`)

**Metrics:**
- `order_line_count_total`: Total number of order lines in the order
- `order_line_count_at_risk`: Number of order lines at risk
- `order_line_count_unknown`: Number of order lines with unknown state
- `order_line_count_not_at_risk`: Number of order lines not at risk
- `at_risk_line_subject_ids`: List of order line subject IDs that are at risk (capped at 50)
- `customer_id`: Customer identifier (string, nullable) - extracted from line decisions, used for customer rollup

**Evidence:**
- `applied_rule_id`: Identifies which evaluation rule was applied
- `source_lines`: List of source order line references (capped at 50)
- `rollup_counts`: Aggregated counts per order

**Customer ID Resolution:**
- When multiple order lines have different `customer_id` values for the same order, the most frequent non-null value is selected
- If conflicts occur, a warning is logged and the chosen value is used

**Example:**
```bash
python -m opsiq_runtime.app.cli run --tenant vmc_group --primitive order_fulfillment_risk --config cfg_v1
```

**Example API Request:**
```bash
curl -X POST http://localhost:8080/run \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_id": "vmc_group",
    "primitive_name": "order_fulfillment_risk",
    "config_version": "cfg_v1",
    "as_of_ts": "2024-01-01T00:00:00Z",
    "correlation_id": "abc-123"
  }'
```

**Worklist Endpoint:**
```bash
curl "http://localhost:8080/v1/tenants/vmc_group/worklists/orders-at-risk?state=AT_RISK&limit=50"
```

This endpoint filters for `primitive_name="order_fulfillment_risk"` and `subject_type="order"`, returning the latest decision per order.

**Decision Bundle Endpoint:**
```bash
curl "http://localhost:8080/v1/tenants/vmc_group/subjects/order/{order_id}/decision-bundle"
```

Returns the primary decision and associated evidence for the order, including drilldown to at-risk order lines.

### customer_order_impact_risk

Composite primitive that aggregates `order_fulfillment_risk` decisions to evaluate customer-level impact.

**Decision States:**
- `HIGH_IMPACT`: Customer has 5 or more orders at risk (configurable via `high_threshold`)
- `MEDIUM_IMPACT`: Customer has 2-4 orders at risk (configurable via `medium_threshold`)
- `LOW_IMPACT`: Customer has 1 order at risk, or no orders at risk
- `UNKNOWN`: No orders found, or all orders have unknown state

**Configuration:**
- `high_threshold` (default: 5): Threshold for HIGH_IMPACT classification
- `medium_threshold` (default: 2): Threshold for MEDIUM_IMPACT classification

**Input Source:** Reads from `gold_decision_output_v1` where `primitive_name='order_fulfillment_risk'` and `subject_type='order'`, grouped by `customer_id` from `metrics_json`

**Evaluation Rules (priority-ordered):**
1. If `order_count_total == 0` → `UNKNOWN` (driver: `NO_ORDERS_FOUND`)
2. If `order_count_at_risk >= high_threshold` → `HIGH_IMPACT` (driver: `HIGH_IMPACT`)
3. If `order_count_at_risk >= medium_threshold` → `MEDIUM_IMPACT` (driver: `MEDIUM_IMPACT`)
4. If `order_count_at_risk > 0` → `LOW_IMPACT` (driver: `LOW_IMPACT`)
5. If `order_count_unknown == order_count_total` → `UNKNOWN` (driver: `ALL_ORDERS_UNKNOWN`)
6. Else → `LOW_IMPACT` (driver: `NO_AT_RISK_ORDERS`)

**Metrics:**
- `order_count_total`: Total number of orders for the customer
- `order_count_at_risk`: Number of orders at risk
- `order_count_unknown`: Number of orders with unknown state
- `at_risk_order_subject_ids`: List of order IDs that are at risk (capped at 100)

**Evidence:**
- `applied_rule_id`: Identifies which evaluation rule was applied
- `source_orders`: List of source order references (capped at 100) - references order-level evidence_refs
- `rollup_counts`: Aggregated counts per customer
- `thresholds`: Configuration thresholds used for classification

**Note:** This primitive sources from `order_fulfillment_risk` decisions (order-level), not directly from `order_line_fulfillment_risk` decisions. This creates a three-tier rollup hierarchy: order_line → order → customer.

**Example:**
```bash
python -m opsiq_runtime.app.cli run --tenant vmc_group --primitive customer_order_impact_risk --config cfg_v1
```

**Example API Request:**
```bash
curl -X POST http://localhost:8080/run \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_id": "vmc_group",
    "primitive_name": "customer_order_impact_risk",
    "config_version": "cfg_v1",
    "as_of_ts": "2024-01-01T00:00:00Z",
    "correlation_id": "abc-123"
  }'
```

**Worklist Endpoint:**
```bash
curl "http://localhost:8080/v1/tenants/vmc_group/worklists/customers-impacted?state=HIGH_IMPACT,MEDIUM_IMPACT&limit=50"
```

This endpoint filters for `primitive_name="customer_order_impact_risk"` and `subject_type="customer"`, returning the latest decision per customer.

**Decision Bundle Endpoint:**
```bash
curl "http://localhost:8080/v1/tenants/vmc_group/subjects/customer/{customer_id}/decision-bundle"
```

Returns the primary decision and associated evidence for the customer, including drilldown to at-risk orders.

## Decision Packs

The OpsIQ platform uses a file-based Decision Pack registry system to manage which decision capabilities are available to each tenant. Packs define primitives, subjects, worklists, and onboarding requirements.

### Pack Structure

Decision packs are defined as JSON files in the `decision_packs/` directory:

```
decision_packs/
├── _schemas/
│   ├── decision_pack.schema.json          # Schema for pack definitions
│   └── tenant_enablement.schema.json      # Schema for tenant enablement
├── shopper_health_intelligence/
│   └── 1.0.0/
│       └── pack.json                      # Pack definition
└── order_fulfillment_risk/
    └── 1.0.0/
        └── pack.json                      # Pack definition
```

Tenant enablement is configured in `tenants/{tenant_id}/packs.json`:

```
tenants/
├── price_chopper/
│   └── packs.json                         # Enabled packs for price_chopper
└── vmc_group/
    └── packs.json                         # Enabled packs for vmc_group
```

### Pack API Endpoints

**List Enabled Packs for a Tenant:**
```bash
curl "http://localhost:8080/v1/tenants/price_chopper/decision-packs"
```

Returns a list of enabled packs with summary information including subjects, worklists, and primitives.

**Get Pack Definition:**
```bash
curl "http://localhost:8080/v1/decision-packs/shopper_health_intelligence/1.0.0"
```

Returns the complete pack definition including all configuration, primitives, and onboarding checks.

**Check Tenant Readiness (Onboarding Checks):**
```bash
curl "http://localhost:8080/v1/tenants/price_chopper/readiness"
```

Runs onboarding checks (e.g., table existence) for all enabled packs and returns a readiness checklist with PASS/FAIL/WARN status. This endpoint validates that required tables/views exist.

**Note:** For comprehensive pack health monitoring including canonical freshness, decision output health, and rollup integrity, use the Pack Readiness endpoints (see Pack Readiness section below).

### Environment Configuration

- `OPSIQ_PACKS_BASE_DIR` - Base directory for pack files (default: repository root)
  - Set this if packs are located outside the repository
  - Example: `export OPSIQ_PACKS_BASE_DIR=/opt/opsiq/packs`

### Pack Validation

Validate all pack JSON files against their schemas:

```bash
python scripts/validate_packs.py
```

This script scans all `pack.json` and tenant `packs.json` files and validates them against the JSON schemas, exiting with an error code if any files are invalid.

### Pack Features

- **JSON Schema Validation**: All pack definitions are validated against schemas in `decision_packs/_schemas/`
- **Caching**: Pack definitions are cached in-memory with a 60-second TTL for performance
- **Fail-Fast**: Invalid JSON or schema violations raise exceptions immediately
- **Onboarding Checks**: Packs can define table/view existence checks that are validated via the readiness endpoint

### Pack Readiness

The OpsIQ platform provides comprehensive pack readiness monitoring to ensure decision packs are ready for production use. Readiness evaluates canonical input freshness, decision output health, and rollup integrity.

#### Readiness Metrics

**Canonical Freshness:**
- Monitors the last update timestamp (`as_of_ts`) for each canonical input table required by the pack
- Status thresholds:
  - `PASS`: Data updated within freshness threshold (default: 36 hours)
  - `WARN`: Data is stale (exceeds freshness threshold)
  - `FAIL`: No data found in table

**Decision Health:**
- Evaluates decision output quality for each primitive in the pack
- Metrics include:
  - Total decisions in last 24 hours
  - Decision state breakdown (AT_RISK, NOT_AT_RISK, UNKNOWN, etc.)
  - Unknown rate (percentage of UNKNOWN decisions)
  - Last computed timestamp
- Status thresholds:
  - `PASS`: Unknown rate < 30%
  - `WARN`: Unknown rate 30-60%
  - `FAIL`: Unknown rate > 60% or no decisions in last 24h

**Rollup Integrity** (Manufacturing Pack only):
- Validates that required JSON fields are present in `metrics_json` for aggregation
- Checks include:
  - `order_line_has_ordernum`: Verifies `ordernum` field in `order_line_fulfillment_risk` decisions
  - `order_has_customer_id`: Verifies `customer_id` field in `order_fulfillment_risk` decisions
  - `customer_has_impacted_orders`: Verifies `impacted_orders` array in `customer_order_impact_risk` decisions
- Status thresholds:
  - `PASS`: Pass rate ≥ 95%
  - `WARN`: Pass rate 80-95%
  - `FAIL`: Pass rate < 80%

**Overall Status:**
- Aggregated from all metrics using priority: `FAIL` > `WARN` > `PASS`
- If any metric is `FAIL`, overall status is `FAIL`
- If any metric is `WARN` (and none are `FAIL`), overall status is `WARN`
- Otherwise, overall status is `PASS`

#### Pack Readiness API Endpoints

**Get Pack Readiness:**
```bash
curl "http://localhost:8080/v1/tenants/vmc_group/packs/order_fulfillment_risk/readiness"
```

Returns detailed readiness metrics for a specific pack:
```json
{
  "tenant_id": "vmc_group",
  "pack_id": "order_fulfillment_risk",
  "pack_version": "1.0.0",
  "overall_status": "PASS",
  "canonical_freshness": [
    {
      "table": "gold_canonical_order_line_fulfillment_input_v1",
      "last_as_of_ts": "2024-01-15T10:30:00Z",
      "hours_since_last_update": 12.4,
      "status": "PASS"
    }
  ],
  "decision_health": [
    {
      "primitive_name": "order_line_fulfillment_risk",
      "total_decisions": 12450,
      "state_counts": {
        "AT_RISK": 320,
        "NOT_AT_RISK": 11980,
        "UNKNOWN": 150
      },
      "unknown_rate": 0.012,
      "last_computed_at": "2024-01-15T10:30:00Z",
      "status": "PASS"
    }
  ],
  "rollup_integrity": [
    {
      "check": "order_line_has_ordernum",
      "pass_rate": 1.0,
      "status": "PASS"
    }
  ],
  "computed_at": "2024-01-15T22:45:00Z"
}
```

**Get All Packs Readiness:**
```bash
curl "http://localhost:8080/v1/tenants/vmc_group/packs/readiness"
```

Returns readiness summary for all enabled packs for the tenant (array of pack readiness responses).

#### Canonical Mapping Kit

The platform includes canonical mapping specifications that serve as authoritative contracts for onboarding customers. These specifications document:

- Required and recommended fields
- Projection guidance (minimum viable vs enhanced)
- Field fallbacks (alternative field names)
- Unknown handling rules

**Location:** `canonical_models/manufacturing/order_line_fulfillment/order_line_fulfillment_input_v1.yaml`

**Example:**
```yaml
canonical_model: gold_canonical_order_line_fulfillment_input_v1
version: v1
subject_type: order_line
grain: one row per order line release

required_fields:
  - tenant_id (string)
  - subject_id (string)
  - as_of_ts (timestamp)
  - need_by_date (date)
  - open_quantity (number)
  - projected_available_quantity (number)
  - order_status (string)

recommended_fields:
  - ordernum (int)
  - customer_id (string)
  - partnum (string)
```

#### Quality Gates

The platform supports optional readiness enforcement to prevent execution of primitives when packs are in `FAIL` status.

**Configuration:**
```bash
export OPSIQ_ENFORCE_READINESS=true
```

When enabled:
- Runtime checks pack readiness before executing any primitive
- If pack status is `FAIL`, execution is blocked with an error
- Event `opsiq.pack.readiness_failed` is emitted
- Default: `false` (enforcement disabled)

**Example Error:**
```
RuntimeError: Pack order_fulfillment_risk v1.0.0 is in FAIL readiness status. 
Primitive order_line_fulfillment_risk cannot be executed. 
Check /v1/tenants/vmc_group/packs/order_fulfillment_risk/readiness for details.
```

#### Readiness UI

The Next.js frontend provides a dedicated Readiness page accessible via:
- Navigation: Admin → Readiness
- URL: `/{tenantId}/admin/readiness`

The UI displays:
- Overall pack status badges (PASS/WARN/FAIL)
- Expandable sections for each metric category
- Detailed tables with timestamps and rates
- Color-coded status indicators

#### Readiness Configuration

Thresholds are configurable via environment variables (with defaults):

- `OPSIQ_READINESS_FRESHNESS_THRESHOLD_HOURS` (default: 36) - Hours before canonical data is considered stale
- `OPSIQ_READINESS_UNKNOWN_RATE_WARN` (default: 0.30) - Unknown rate threshold for WARN (30%)
- `OPSIQ_READINESS_UNKNOWN_RATE_FAIL` (default: 0.60) - Unknown rate threshold for FAIL (60%)
- `OPSIQ_READINESS_INTEGRITY_WARN` (default: 0.95) - Integrity pass rate threshold for WARN (95%)
- `OPSIQ_READINESS_INTEGRITY_FAIL` (default: 0.80) - Integrity pass rate threshold for FAIL (80%)
- `OPSIQ_ENFORCE_READINESS` (default: false) - Enable quality gates to block FAIL packs

