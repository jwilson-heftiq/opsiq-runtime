/**
 * Client-side API functions for fetching run registry data from the FastAPI backend.
 * This module is safe to import in client components.
 */

import { RunRegistryResponse } from "@/types/runs"

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

export interface RunRegistryParams {
  primitive_name?: string
  status?: string
  from_ts?: string
  to_ts?: string
  limit?: number
  cursor?: string
}

/**
 * Client-side function to fetch run registry for a tenant
 */
export async function fetchRunRegistryClient(
  tenantId: string,
  params: RunRegistryParams = {}
): Promise<RunRegistryResponse> {
  const url = new URL(`${getApiBaseUrl()}/v1/tenants/${tenantId}/runs`)

  if (params.primitive_name) {
    url.searchParams.set("primitive_name", params.primitive_name)
  }
  if (params.status) {
    url.searchParams.set("status", params.status)
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
  if (params.cursor) {
    url.searchParams.set("cursor", params.cursor)
  }

  const options = buildFetchOptionsClient()

  const response = await fetch(url.toString(), options)

  if (!response.ok) {
    const errorText = await response.text()
    const error = new Error(
      `Failed to fetch run registry: ${response.status} ${response.statusText}`
    )
    ;(error as any).status = response.status
    ;(error as any).statusText = response.statusText
    ;(error as any).body = errorText
    throw error
  }

  return response.json()
}

