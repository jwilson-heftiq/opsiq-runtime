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

