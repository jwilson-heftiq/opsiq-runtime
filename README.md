# OpsIQ Runtime

Decision Intelligence Runtime walking skeleton implementing the `operational_risk` primitive with hexagonal architecture (ports & adapters).

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

Example API call:
```
curl -X POST http://localhost:8080/run \
  -H "Content-Type: application/json" \
  -d '{"tenant_id":"price_chopper","primitive_name":"operational_risk","config_version":"cfg_v1","as_of_ts":"2024-01-01T00:00:00Z","correlation_id":"abc-123"}'
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

**Input Table:**
- `{prefix}gold_canonical_shopper_recency_input_v1`
  - Columns: `tenant_id`, `subject_type`, `subject_id`, `as_of_ts`, `last_trip_ts`, `days_since_last_trip`, `config_version`
  - Note: `canonical_version` is derived from `config_version` if not present in the table

**Output Tables:**
- `{prefix}gold_decision_output_operational_risk_v1`
  - Columns: `tenant_id`, `subject_type`, `subject_id`, `primitive_name`, `primitive_version`, `canonical_version`, `config_version`, `as_of_ts`, `decision_state`, `confidence`, `drivers_json`, `metrics_json`, `evidence_refs_json`, `computed_at`, `valid_until`, `correlation_id`

- `{prefix}gold_decision_evidence_operational_risk_v1`
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

### API Request Reference

The `/run` endpoint accepts a JSON payload with the following fields:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `tenant_id` | string | **Yes** | Identifies the tenant/customer. Used to filter input data and partition output data. Example: `"price_chopper"` |
| `primitive_name` | string | **Yes** | The decision primitive to execute. Currently supported: `"operational_risk"` |
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

### Example Response

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

### What Happens During a Run

This will:
1. Read inputs from `gold_canonical_shopper_recency_input_v1` filtered by `tenant_id = 'price_chopper'`
2. Evaluate operational risk for each input row
3. Write decisions to `gold_decision_output_operational_risk_v1`
4. Write evidence to `gold_decision_evidence_operational_risk_v1`

All writes are idempotent using MERGE INTO (or DELETE+INSERT if `DATABRICKS_USE_MERGE=false`).

