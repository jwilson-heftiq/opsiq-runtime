/**
 * Client-side API functions for fetching order fulfillment data from the FastAPI backend.
 * This module is safe to import in client components.
 */

import {
  DecisionListResponse,
  DecisionBundle,
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
 * Client-side function to fetch order line fulfillment worklist
 */
export async function fetchOrderLineWorklist(
  tenantId: string,
  params: WorklistParams = {}
): Promise<DecisionListResponse> {
  const url = new URL(
    `${getApiBaseUrl()}/v1/tenants/${tenantId}/worklists/order-line-fulfillment`
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

/**
 * Client-side function to fetch order line decision detail
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

  const options = buildFetchOptionsClient()

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
 * Client-side function to fetch orders at risk worklist
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

/**
 * Client-side function to fetch order decision detail
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

  const options = buildFetchOptionsClient()

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
 * Client-side function to fetch customers impacted worklist
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

/**
 * Client-side function to fetch customer decision detail
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

  const options = buildFetchOptionsClient()

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
