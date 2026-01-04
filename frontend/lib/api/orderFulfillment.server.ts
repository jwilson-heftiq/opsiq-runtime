/**
 * Server-side API functions for fetching order fulfillment data from the FastAPI backend.
 * This module uses next/headers and is only safe to import in Server Components.
 */

import { cookies } from "next/headers"
import {
  DecisionListResponse,
  DecisionBundle,
} from "@/types/decisions"
import type { WorklistParams } from "./orderFulfillment"

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
 * Fetch order line fulfillment worklist (server-side).
 */
export async function fetchOrderLineWorklist(
  tenantId: string,
  params: WorklistParams = {}
): Promise<DecisionListResponse> {
  const url = new URL(
    `${getApiBaseUrl()}/v1/tenants/${tenantId}/worklists/order-line-fulfillment`
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
 * Fetch order line decision detail (server-side).
 */
export async function fetchOrderLineDecisionDetail(
  tenantId: string,
  subjectId: string,
  asOfTs?: string
): Promise<DecisionBundle> {
  const url = new URL(
    `${getApiBaseUrl()}/v1/tenants/${tenantId}/subjects/order_line/${subjectId}/decision-bundle`
  )

  if (asOfTs) {
    url.searchParams.set("as_of_ts", asOfTs)
  }

  const options = await buildFetchOptions()

  const response = await fetch(url.toString(), options)

  if (!response.ok) {
    const errorText = await response.text()
    const error = new Error(
      `Failed to fetch decision detail: ${response.status} ${response.statusText}`
    )
    ;(error as any).status = response.status
    ;(error as any).statusText = response.statusText
    ;(error as any).body = errorText
    throw error
  }

  return response.json()
}


/**
 * Fetch orders at risk worklist (server-side).
 */
export async function fetchOrdersWorklist(
  tenantId: string,
  params: WorklistParams = {}
): Promise<DecisionListResponse> {
  const url = new URL(
    `${getApiBaseUrl()}/v1/tenants/${tenantId}/worklists/orders-at-risk`
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
 * Fetch order decision detail (server-side).
 */
export async function fetchOrderDecisionDetail(
  tenantId: string,
  orderId: string,
  asOfTs?: string
): Promise<DecisionBundle> {
  const url = new URL(
    `${getApiBaseUrl()}/v1/tenants/${tenantId}/subjects/order/${orderId}/decision-bundle`
  )

  if (asOfTs) {
    url.searchParams.set("as_of_ts", asOfTs)
  }

  const options = await buildFetchOptions()

  const response = await fetch(url.toString(), options)

  if (!response.ok) {
    const errorText = await response.text()
    const error = new Error(
      `Failed to fetch decision detail: ${response.status} ${response.statusText}`
    )
    ;(error as any).status = response.status
    ;(error as any).statusText = response.statusText
    ;(error as any).body = errorText
    throw error
  }

  return response.json()
}

/**
 * Fetch customers impacted worklist (server-side).
 */
export async function fetchCustomersWorklist(
  tenantId: string,
  params: WorklistParams = {}
): Promise<DecisionListResponse> {
  const url = new URL(
    `${getApiBaseUrl()}/v1/tenants/${tenantId}/worklists/customers-impacted`
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
 * Fetch customer decision detail (server-side).
 */
export async function fetchCustomerDecisionDetail(
  tenantId: string,
  customerId: string,
  asOfTs?: string
): Promise<DecisionBundle> {
  const url = new URL(
    `${getApiBaseUrl()}/v1/tenants/${tenantId}/subjects/customer/${customerId}/decision-bundle`
  )

  if (asOfTs) {
    url.searchParams.set("as_of_ts", asOfTs)
  }

  const options = await buildFetchOptions()

  const response = await fetch(url.toString(), options)

  if (!response.ok) {
    const errorText = await response.text()
    const error = new Error(
      `Failed to fetch decision detail: ${response.status} ${response.statusText}`
    )
    ;(error as any).status = response.status
    ;(error as any).statusText = response.statusText
    ;(error as any).body = errorText
    throw error
  }

  return response.json()
}
