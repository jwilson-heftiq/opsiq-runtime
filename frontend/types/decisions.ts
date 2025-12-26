/**
 * TypeScript types matching the FastAPI backend Pydantic models.
 */

export interface DecisionKey {
  tenant_id: string
  subject_type: string
  subject_id: string
  primitive_name: string
  primitive_version: string
  as_of_ts: string // ISO timestamp
}

export interface DecisionListItem extends DecisionKey {
  decision_state: "URGENT" | "WATCHLIST" | "HEALTHY" | "UNKNOWN"
  confidence: "HIGH" | "MEDIUM" | "LOW"
  computed_at: string // ISO timestamp
  drivers: string[]
  metrics: Record<string, any>
}

export interface DecisionListResponse {
  items: DecisionListItem[]
  next_cursor: string | null
}

export interface DecisionDetail extends DecisionKey {
  canonical_version: string
  config_version: string
  decision_state: "URGENT" | "WATCHLIST" | "HEALTHY" | "UNKNOWN"
  confidence: "HIGH" | "MEDIUM" | "LOW"
  computed_at: string // ISO timestamp
  valid_until: string | null
  drivers: string[]
  metrics: Record<string, any>
  evidence_refs: string[]
  correlation_id: string | null
}

export interface EvidenceRecord {
  tenant_id: string
  evidence_id: string
  primitive_name: string
  primitive_version: string
  as_of_ts: string // ISO timestamp
  computed_at: string // ISO timestamp
  evidence: Record<string, any>
}

export interface DecisionBundle {
  composite: DecisionDetail
  components: Record<string, DecisionDetail> // keyed by primitive_name
  evidence: Record<string, EvidenceRecord[]> // grouped by primitive_name
}

export interface DecisionHistoryItem {
  primitive_name: string
  primitive_version: string
  as_of_ts: string // ISO timestamp
  decision_state: string
  confidence: "HIGH" | "MEDIUM" | "LOW"
  drivers: { code: string }[]
  computed_at: string // ISO timestamp
}

export interface DecisionHistoryResponse {
  subject_id: string
  items: DecisionHistoryItem[]
}

