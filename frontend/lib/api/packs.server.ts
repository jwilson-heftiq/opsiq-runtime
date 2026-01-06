/**
 * Server-side API functions for fetching decision pack data from the FastAPI backend.
 * This module uses next/headers and is only safe to import in Server Components.
 */

import { cookies } from "next/headers"
import {
  EnabledPackSummary,
  DecisionPackDefinition,
  PackReadinessResponse,
} from "@/types/packs"

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
 * Fetch enabled packs for a tenant (server-side).
 */
export async function fetchTenantPacks(tenantId: string): Promise<EnabledPackSummary[]> {
  const url = new URL(
    `${getApiBaseUrl()}/v1/tenants/${tenantId}/decision-packs`
  )

  const options = await buildFetchOptions()

  let response: Response
  try {
    response = await fetch(url.toString(), options)
  } catch (fetchError: any) {
    const error = new Error(
      `Failed to fetch packs: ${fetchError.message || "Network error"}`
    )
    ;(error as any).cause = fetchError
    ;(error as any).url = url.toString()
    throw error
  }

  if (!response.ok) {
    const errorText = await response.text()
    const error = new Error(
      `Failed to fetch packs: ${response.status} ${response.statusText}`
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
 * Fetch pack definition (server-side).
 */
export async function fetchPackDefinition(
  packId: string,
  packVersion: string
): Promise<DecisionPackDefinition> {
  const url = new URL(
    `${getApiBaseUrl()}/v1/decision-packs/${packId}/${packVersion}`
  )

  const options = await buildFetchOptions()

  const response = await fetch(url.toString(), options)

  if (!response.ok) {
    const errorText = await response.text()
    const error = new Error(
      `Failed to fetch pack definition: ${response.status} ${response.statusText}`
    )
    ;(error as any).status = response.status
    ;(error as any).statusText = response.statusText
    ;(error as any).body = errorText
    throw error
  }

  return response.json()
}

/**
 * Fetch pack readiness for a specific pack (server-side).
 */
export async function fetchPackReadiness(
  tenantId: string,
  packId: string
): Promise<PackReadinessResponse> {
  const url = new URL(
    `${getApiBaseUrl()}/v1/tenants/${tenantId}/packs/${packId}/readiness`
  )

  const options = await buildFetchOptions()

  const response = await fetch(url.toString(), options)

  if (!response.ok) {
    const errorText = await response.text()
    const error = new Error(
      `Failed to fetch pack readiness: ${response.status} ${response.statusText}`
    )
    ;(error as any).status = response.status
    ;(error as any).statusText = response.statusText
    ;(error as any).body = errorText
    throw error
  }

  return response.json()
}

/**
 * Fetch readiness for all enabled packs (server-side).
 */
export async function fetchAllPacksReadiness(
  tenantId: string
): Promise<PackReadinessResponse[]> {
  const url = new URL(
    `${getApiBaseUrl()}/v1/tenants/${tenantId}/packs/readiness`
  )

  const options = await buildFetchOptions()

  const response = await fetch(url.toString(), options)

  if (!response.ok) {
    const errorText = await response.text()
    const error = new Error(
      `Failed to fetch packs readiness: ${response.status} ${response.statusText}`
    )
    ;(error as any).status = response.status
    ;(error as any).statusText = response.statusText
    ;(error as any).body = errorText
    throw error
  }

  return response.json()
}

