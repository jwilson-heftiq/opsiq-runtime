import { Suspense } from "react"
import { fetchAllPacksReadiness } from "@/lib/api/packs.server"
import { ReadinessClient } from "./ReadinessClient"
import { Card, CardContent } from "@/components/ui/card"

interface PageProps {
  params: Promise<{
    tenantId: string
  }>
}

async function ReadinessServer({ tenantId }: { tenantId: string }) {
  try {
    const data = await fetchAllPacksReadiness(tenantId)

    return <ReadinessClient readinessData={data} />
  } catch (error: any) {
    return (
      <Card>
        <CardContent className="pt-6">
          <p className="text-destructive">
            Failed to load readiness data: {error.message || "Unknown error"}
          </p>
        </CardContent>
      </Card>
    )
  }
}

export default async function ReadinessPage({ params }: PageProps) {
  const resolvedParams = await params
  const tenantId = resolvedParams.tenantId

  return (
    <div className="container mx-auto p-6 space-y-6">
      <div>
        <h1 className="text-3xl font-bold mb-2">Pack Readiness</h1>
        <p className="text-muted-foreground">
          View readiness metrics for all enabled decision packs for {tenantId}
        </p>
      </div>

      <Suspense fallback={<div>Loading readiness data...</div>}>
        <ReadinessServer tenantId={tenantId} />
      </Suspense>
    </div>
  )
}

