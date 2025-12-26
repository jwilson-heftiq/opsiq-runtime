import { Suspense } from "react"
import { fetchRunRegistry } from "@/lib/api/runs.server"
import { RunsTableClient } from "./RunsTableClient"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"

interface PageProps {
  params: Promise<{
    tenantId: string
  }>
  searchParams: Promise<{
    primitive_name?: string
    status?: string
    cursor?: string
  }>
}

async function RunsTableServer({
  tenantId,
  initialPrimitiveName,
  initialStatus,
  initialCursor,
}: {
  tenantId: string
  initialPrimitiveName?: string
  initialStatus?: string
  initialCursor?: string
}) {
  try {
    const data = await fetchRunRegistry(tenantId, {
      primitive_name: initialPrimitiveName,
      status: initialStatus,
      cursor: initialCursor,
      limit: 50,
    })

    return (
      <RunsTableClient
        tenantId={tenantId}
        initialData={data}
        initialPrimitiveName={initialPrimitiveName}
        initialStatus={initialStatus}
      />
    )
  } catch (error: any) {
    return (
      <Card>
        <CardContent className="pt-6">
          <p className="text-destructive">
            Failed to load runs: {error.message || "Unknown error"}
          </p>
        </CardContent>
      </Card>
    )
  }
}

export default async function RunsPage({ params, searchParams }: PageProps) {
  const resolvedParams = await params
  const tenantId = resolvedParams.tenantId

  const resolvedSearchParams = await searchParams
  const initialPrimitiveName = resolvedSearchParams.primitive_name
  const initialStatus = resolvedSearchParams.status
  const initialCursor = resolvedSearchParams.cursor

  return (
    <div className="container mx-auto p-6 space-y-6">
      <div>
        <h1 className="text-3xl font-bold mb-2">Run Registry</h1>
        <p className="text-muted-foreground">
          View runtime execution history for {tenantId}
        </p>
      </div>

      <Suspense fallback={<div>Loading runs...</div>}>
        <RunsTableServer
          tenantId={tenantId}
          initialPrimitiveName={initialPrimitiveName}
          initialStatus={initialStatus}
          initialCursor={initialCursor}
        />
      </Suspense>
    </div>
  )
}

