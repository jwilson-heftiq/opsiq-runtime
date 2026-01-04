import { Suspense } from "react"
import { CustomersWorklistClient } from "./CustomersWorklistClient"
import { fetchCustomersWorklist } from "@/lib/api/orderFulfillment.server"

interface PageProps {
  params: Promise<{
    tenantId: string
  }>
  searchParams: Promise<{
    state?: string | string[]
    subject_id?: string
    cursor?: string
  }>
}

async function WorklistServer({
  tenantId,
  initialState,
  initialSubjectId,
  initialCursor,
}: {
  tenantId: string
  initialState: string[]
  initialSubjectId?: string
  initialCursor?: string
}) {
  try {
    const data = await fetchCustomersWorklist(tenantId, {
      state: initialState,
      subject_id: initialSubjectId,
      cursor: initialCursor,
      limit: 50,
    })

    return (
      <CustomersWorklistClient
        tenantId={tenantId}
        initialData={data}
        initialState={initialState}
        initialSubjectId={initialSubjectId}
      />
    )
  } catch (error: any) {
    console.error("Error loading worklist:", error)
    return (
      <div className="container mx-auto p-6">
        <div className="text-destructive space-y-2">
          <div className="font-semibold">Error loading worklist:</div>
          <div>{error.message || "Unknown error"}</div>
          {error.status && <div>HTTP Status: {error.status}</div>}
          {error.body && (
            <div className="text-sm mt-2 p-2 bg-muted rounded">
              <pre>{error.body}</pre>
            </div>
          )}
          <div className="text-sm text-muted-foreground mt-4">
            Make sure the API is running at {process.env.NEXT_PUBLIC_OPSIQ_API_BASE_URL || "http://localhost:8080"}
          </div>
        </div>
      </div>
    )
  }
}

export default async function CustomersImpactedPage({ params, searchParams }: PageProps) {
  // In Next.js 15+, params is a Promise
  const resolvedParams = await params
  const tenantId = resolvedParams.tenantId

  // In Next.js 15+, searchParams is a Promise
  const resolvedSearchParams = await searchParams
  
  // Parse state filter from searchParams (can be string or string[])
  const stateParam = resolvedSearchParams.state
  const initialState: string[] = Array.isArray(stateParam)
    ? stateParam
    : stateParam
    ? [stateParam]
    : ["HIGH_IMPACT", "MEDIUM_IMPACT"] // Default filter

  const initialSubjectId = resolvedSearchParams.subject_id
  const initialCursor = resolvedSearchParams.cursor

  return (
    <div className="container mx-auto p-6">
      <h1 className="text-3xl font-bold mb-6">Customers Impacted</h1>
      <Suspense fallback={<div>Loading worklist...</div>}>
        <WorklistServer
          tenantId={tenantId}
          initialState={initialState}
          initialSubjectId={initialSubjectId}
          initialCursor={initialCursor}
        />
      </Suspense>
    </div>
  )
}
