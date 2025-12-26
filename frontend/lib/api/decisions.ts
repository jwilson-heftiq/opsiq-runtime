/**
 * Client-side API functions for fetching decision data from the FastAPI backend.
 * This module is safe to import in client components.
 */

import {
  DecisionListResponse,
  DecisionBundle,
  DecisionListItem,
  DecisionHistoryResponse,
} from "@/types/decisions"

function getApiBaseUrl(): string {
  const url = process.env.NEXT_PUBLIC_OPSIQ_API_BASE_URL
  if (!url) {
    throw new Error("NEXT_PUBLIC_OPSIQ_API_BASE_URL environment variable is not set")
  }
  return url
}

/**
 * Get access token from localStorage (client-side)
 */
function getAuthTokenClient(): string | null {
  if (typeof window === "undefined") return null
  return localStorage.getItem("access_token")
}

/**
 * Build fetch options with auth header if token is available (client-side)
 */
function buildFetchOptionsClient(): RequestInit {
  const token = getAuthTokenClient()

  const headers: HeadersInit = {
    "Content-Type": "application/json",
  }

  if (token) {
    headers["Authorization"] = `Bearer ${token}`
  }

  return {
    headers,
    cache: "no-store",
  }
}

export interface WorklistParams {
  state?: string[]
  confidence?: string[]
  subject_id?: string
  limit?: number
  cursor?: string
}

/**
 * Client-side version of fetchShopperHealthWorklist
 */
export async function fetchShopperHealthWorklistClient(
  tenantId: string,
  params: WorklistParams = {}
): Promise<DecisionListResponse> {
  const url = new URL(
    `${getApiBaseUrl()}/v1/tenants/${tenantId}/worklists/shopper-health`
  )

  if (params.state && params.state.length > 0) {
    params.state.forEach((s) => url.searchParams.append("state", s))
  }
  if (params.confidence && params.confidence.length > 0) {
    params.confidence.forEach((c) => url.searchParams.append("confidence", c))
  }
  if (params.subject_id) {
    url.searchParams.set("subject_id", params.subject_id)
  }
  if (params.limit) {
    url.searchParams.set("limit", params.limit.toString())
  }
  if (params.cursor) {
    url.searchParams.set("cursor", params.cursor)
  }

  const options = buildFetchOptionsClient()

  const response = await fetch(url.toString(), options)

  if (!response.ok) {
    const errorText = await response.text()
    const error = new Error(
      `Failed to fetch worklist: ${response.status} ${response.statusText}`
    )
    ;(error as any).status = response.status
    ;(error as any).statusText = response.statusText
    ;(error as any).body = errorText
    throw error
  }

  return response.json()
}

export interface DecisionHistoryParams {
  primitive_name?: string[]
  from_ts?: string
  to_ts?: string
  limit?: number
}

/**
 * Client-side function to fetch decision history for a subject
 */
export async function fetchDecisionHistoryClient(
  tenantId: string,
  subjectId: string,
  params: DecisionHistoryParams = {}
): Promise<DecisionHistoryResponse> {
  const url = new URL(
    `${getApiBaseUrl()}/v1/tenants/${tenantId}/subjects/shopper/${subjectId}/decision-history`
  )

  if (params.primitive_name && params.primitive_name.length > 0) {
    params.primitive_name.forEach((pn) => url.searchParams.append("primitive_name", pn))
  }
  if (params.from_ts) {
    url.searchParams.set("from_ts", params.from_ts)
  }
  if (params.to_ts) {
    url.searchParams.set("to_ts", params.to_ts)
  }
  if (params.limit) {
    url.searchParams.set("limit", params.limit.toString())
  }

  const options = buildFetchOptionsClient()

  const response = await fetch(url.toString(), options)

  if (!response.ok) {
    const errorText = await response.text()
    const error = new Error(
      `Failed to fetch decision history: ${response.status} ${response.statusText}`
    )
    ;(error as any).status = response.status
    ;(error as any).statusText = response.statusText
    ;(error as any).body = errorText
    throw error
  }

  return response.json()
}

