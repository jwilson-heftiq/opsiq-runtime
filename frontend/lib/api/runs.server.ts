/**
 * Server-side API functions for fetching run registry data from the FastAPI backend.
 * This module uses next/headers and is only safe to import in Server Components.
 */

import { cookies } from "next/headers"
import { RunRegistryResponse } from "@/types/runs"

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

export interface RunRegistryParams {
  primitive_name?: string
  status?: string
  from_ts?: string
  to_ts?: string
  limit?: number
  cursor?: string
}

/**
 * Fetch run registry for a tenant (server-side).
 */
export async function fetchRunRegistry(
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

  const options = await buildFetchOptions()

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

