/**
 * Server-side API functions for fetching decision data from the FastAPI backend.
 * This module uses next/headers and is only safe to import in Server Components.
 */

import { cookies } from "next/headers"
import {
  DecisionListResponse,
  DecisionBundle,
  DecisionHistoryResponse,
} from "@/types/decisions"
import type { WorklistParams } from "./decisions"

function getApiBaseUrl(): string {
  // For server-side requests in Docker, prefer OPSIQ_API_BASE_URL (can use service name)
  // Fall back to NEXT_PUBLIC_OPSIQ_API_BASE_URL for client-side compatibility
  const url = process.env.OPSIQ_API_BASE_URL || process.env.NEXT_PUBLIC_OPSIQ_API_BASE_URL
  if (!url) {
    throw new Error("OPSIQ_API_BASE_URL or NEXT_PUBLIC_OPSIQ_API_BASE_URL environment variable is not set")
  }
  return url
}

/**
 * Get access token from cookies (server-side) or return null
 */
async function getAuthToken(): Promise<string | null> {
  try {
    const cookieStore = await cookies()
    const token = cookieStore.get("access_token")?.value
    return token || null
  } catch {
    // If cookies() fails, return null
    return null
  }
}

/**
 * Build fetch options with auth header if token is available (server-side)
 */
async function buildFetchOptions(): Promise<RequestInit> {
  const token = await getAuthToken()

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

/**
 * Fetch shopper health worklist decisions (server-side).
 */
export async function fetchShopperHealthWorklist(
  tenantId: string,
  params: WorklistParams = {}
): Promise<DecisionListResponse> {
  const url = new URL(
    `${getApiBaseUrl()}/v1/tenants/${tenantId}/worklists/shopper-health`
  )

  // Add query parameters
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

  const options = await buildFetchOptions()

  let response: Response
  try {
    response = await fetch(url.toString(), options)
  } catch (fetchError: any) {
    const error = new Error(
      `Failed to fetch worklist: ${fetchError.message || "Network error"}`
    )
    ;(error as any).cause = fetchError
    ;(error as any).url = url.toString()
    throw error
  }

  if (!response.ok) {
    const errorText = await response.text()
    const error = new Error(
      `Failed to fetch worklist: ${response.status} ${response.statusText}`
    )
    ;(error as any).status = response.status
    ;(error as any).statusText = response.statusText
    ;(error as any).body = errorText
    ;(error as any).url = url.toString()
    throw error
  }

  return response.json()
}

/**
 * Fetch decision bundle for a subject (server-side).
 */
export async function fetchShopperDecisionBundle(
  tenantId: string,
  subjectId: string,
  asOfTs?: string
): Promise<DecisionBundle> {
  const url = new URL(
    `${getApiBaseUrl()}/v1/tenants/${tenantId}/subjects/shopper/${subjectId}/decision-bundle`
  )

  if (asOfTs) {
    url.searchParams.set("as_of_ts", asOfTs)
  }

  const options = await buildFetchOptions()

  const response = await fetch(url.toString(), options)

  if (!response.ok) {
    const errorText = await response.text()
    const error = new Error(
      `Failed to fetch decision bundle: ${response.status} ${response.statusText}`
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
 * Fetch decision history for a subject (server-side).
 */
export async function fetchDecisionHistory(
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

  const options = await buildFetchOptions()

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
