# OpsIQ Runtime

Decision Intelligence Runtime walking skeleton implementing decision primitives (`operational_risk`, `shopper_frequency_trend`, `shopper_health_classification`, `order_line_fulfillment_risk`, `order_fulfillment_risk`, `customer_order_impact_risk`) with hexagonal architecture (ports & adapters).

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

- `{prefix}gold_decision_output_v1` (for `shopper_health_classification` primitive - composite primitive that reads from this table)
  - This primitive reads decision outputs from `operational_risk` and `shopper_frequency_trend` primitives stored in this table
  - Filters by `tenant_id`, `subject_type='shopper'`, and `primitive_name IN ('operational_risk','shopper_frequency_trend')`

- `{prefix}gold_canonical_order_line_fulfillment_input_v1` (for `order_line_fulfillment_risk` primitive)
  - Location: Catalog `opsiq_dev`, Schema `gold`
  - Columns: `tenant_id`, `subject_type`, `subject_id`, `as_of_ts`, `need_by_date`, `open_quantity`, `projected_available_quantity`, `order_status`, `is_on_hold`, `release_shortage_qty`, `plant_shortage_qty`, `projected_onhand_qty_eod`, `supply_qty`, `demand_qty`, `partnum`, `customer_id`, `ordernum`, `plant`, `warehouse`, `config_version`, `canonical_version`
  - Note: `ordernum` and `customer_id` are required for aggregation primitives (`order_fulfillment_risk`, `customer_order_impact_risk`)

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

**For `order_line_fulfillment_risk` primitive, set catalog and schema:**
```bash
export DATABRICKS_CATALOG=opsiq_dev
export DATABRICKS_SCHEMA=gold
python -m opsiq_runtime.app.cli run --tenant vmc_group --primitive order_line_fulfillment_risk --config cfg_v1
```

### API Request Reference

The `/run` endpoint accepts a JSON payload with the following fields:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `tenant_id` | string | **Yes** | Identifies the tenant/customer. Used to filter input data and partition output data. Example: `"price_chopper"` |
| `primitive_name` | string | **Yes** | The decision primitive to execute. Supported: `"operational_risk"`, `"shopper_frequency_trend"`, `"shopper_health_classification"`, `"order_line_fulfillment_risk"`, `"order_fulfillment_risk"`, `"customer_order_impact_risk"` |
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

**Check Tenant Readiness:**
```bash
curl "http://localhost:8080/v1/tenants/price_chopper/readiness"
```

Runs onboarding checks (e.g., table existence) for all enabled packs and returns a readiness checklist with PASS/FAIL/WARN status.

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

