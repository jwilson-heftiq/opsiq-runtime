/**
 * TypeScript types for decision pack API responses.
 */

export interface DefaultWorklist {
  title: string
  primitive_name: string
  default_filters: Record<string, any>
  ui_route: string
}

export interface SubjectDefinition {
  subject_type: string
  default_worklist: DefaultWorklist
}

export interface EnabledPackSummary {
  pack_id: string
  pack_version: string
  name: string
  description: string
  tags: string[]
  subjects: SubjectDefinition[]
  primitives: Array<{
    primitive_name: string
    primitive_version: string
  }>
}

export interface DecisionPackDefinition {
  pack_id: string
  pack_version: string
  name: string
  description: string
  status: string
  tags: string[]
  subjects: SubjectDefinition[]
  primitives: Array<{
    primitive_name: string
    primitive_version: string
    canonical_version: string
    kind: string
    depends_on: {
      canonical_inputs: string[]
      primitives: string[]
    }
  }>
  activation?: {
    eventbridge_enabled: boolean
    event_types: string[]
    recommended_targets: string[]
  }
  onboarding_checks?: Array<{
    type: string
    severity: string
    table?: string
    message?: string
  }>
}

export interface CanonicalFreshnessResult {
  table: string
  last_as_of_ts: string | null // ISO timestamp
  hours_since_last_update: number | null
  status: "PASS" | "WARN" | "FAIL"
}

export interface DecisionHealthResult {
  primitive_name: string
  total_decisions: number
  state_counts: Record<string, number>
  unknown_rate: number
  last_computed_at: string | null // ISO timestamp
  status: "PASS" | "WARN" | "FAIL"
}

export interface RollupIntegrityResult {
  check: string
  pass_rate: number
  status: "PASS" | "WARN" | "FAIL"
}

export interface PackReadinessResponse {
  tenant_id: string
  pack_id: string
  pack_version: string
  overall_status: "PASS" | "WARN" | "FAIL"
  canonical_freshness: CanonicalFreshnessResult[]
  decision_health: DecisionHealthResult[]
  rollup_integrity: RollupIntegrityResult[]
  computed_at: string // ISO timestamp
}

