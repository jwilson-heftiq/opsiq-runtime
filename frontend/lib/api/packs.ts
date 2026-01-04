/**
 * Client-side API functions for fetching decision pack data from the FastAPI backend.
 * This module is safe to import in client components.
 */

import { EnabledPackSummary, DecisionPackDefinition } from "@/types/packs"

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

/**
 * Client-side function to fetch enabled packs for a tenant
 */
export async function fetchTenantPacks(tenantId: string): Promise<EnabledPackSummary[]> {
  const url = new URL(
    `${getApiBaseUrl()}/v1/tenants/${tenantId}/decision-packs`
  )

  const options = buildFetchOptionsClient()

  const response = await fetch(url.toString(), options)

  if (!response.ok) {
    const errorText = await response.text()
    const error = new Error(
      `Failed to fetch packs: ${response.status} ${response.statusText}`
    )
    ;(error as any).status = response.status
    ;(error as any).statusText = response.statusText
    ;(error as any).body = errorText
    throw error
  }

  return response.json()
}

/**
 * Client-side function to fetch a pack definition
 */
export async function fetchPackDefinition(
  packId: string,
  packVersion: string
): Promise<DecisionPackDefinition> {
  const url = new URL(
    `${getApiBaseUrl()}/v1/decision-packs/${packId}/${packVersion}`
  )

  const options = buildFetchOptionsClient()

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

