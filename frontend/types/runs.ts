/**
 * TypeScript types matching the FastAPI backend Pydantic models for run registry.
 */

export interface RunRegistryItem {
  correlation_id: string
  primitive_name: string
  primitive_version: string
  status: "STARTED" | "SUCCESS" | "FAILED"
  started_at: string // ISO timestamp
  completed_at: string | null // ISO timestamp
  duration_ms: number | null
  input_count: number | null
  decision_count: number | null
  at_risk_count: number | null
  unknown_count: number | null
  error_message: string | null
}

export interface RunRegistryResponse {
  items: RunRegistryItem[]
  next_cursor: string | null
}

