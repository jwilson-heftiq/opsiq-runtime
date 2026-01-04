import { notFound } from "next/navigation"
import Link from "next/link"
import { fetchOrderLineDecisionDetail } from "@/lib/api/orderFulfillment.server"
import { DecisionStateBadge } from "@/components/DecisionStateBadge"
import { EvidenceViewer } from "@/components/EvidenceViewer"
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { format } from "date-fns"
import { DecisionBundle } from "@/types/decisions"

interface PageProps {
  params: Promise<{
    tenantId: string
    subjectId: string
  }>
  searchParams: Promise<{
    as_of_ts?: string
  }>
}

export default async function OrderLineDetailPage({
  params,
  searchParams,
}: PageProps) {
  // In Next.js 15+, params and searchParams are Promises
  const resolvedParams = await params
  const resolvedSearchParams = await searchParams
  const { tenantId, subjectId } = resolvedParams
  const asOfTs = resolvedSearchParams.as_of_ts

  let bundle: DecisionBundle
  try {
    bundle = await fetchOrderLineDecisionDetail(tenantId, subjectId, asOfTs)
  } catch (error: any) {
    if (error.status === 404) {
      notFound()
    }
    throw error
  }

  const { composite, evidence } = bundle

  return (
    <div className="container mx-auto p-6 space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="space-y-1">
          <div className="flex items-center gap-3">
            <Link
              href={`/${tenantId}/order-fulfillment/order-lines`}
              className="text-muted-foreground hover:text-foreground"
            >
              ← Back to Worklist
            </Link>
          </div>
          <h1 className="text-3xl font-bold">Order Line: {subjectId}</h1>
          <DecisionStateBadge state={composite.decision_state as any} className="text-base px-3 py-1" />
        </div>
      </div>

      {/* Decision Summary */}
      <Card>
        <CardHeader>
          <CardTitle>Decision</CardTitle>
          <CardDescription>
            Order Line Fulfillment Risk (v{composite.primitive_version})
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid grid-cols-2 gap-4">
            <div>
              <div className="text-sm font-medium text-muted-foreground">State</div>
              <DecisionStateBadge state={composite.decision_state as any} />
            </div>
            <div>
              <div className="text-sm font-medium text-muted-foreground">Confidence</div>
              <Badge variant="outline">{composite.confidence}</Badge>
            </div>
          </div>

          <div>
            <div className="text-sm font-medium text-muted-foreground mb-2">Drivers</div>
            <div className="flex flex-wrap gap-2">
              {composite.drivers.length > 0 ? (
                composite.drivers.map((driver, idx) => (
                  <Badge key={idx} variant="secondary">
                    {driver}
                  </Badge>
                ))
              ) : (
                <span className="text-sm text-muted-foreground">None</span>
              )}
            </div>
          </div>

          <div>
            <div className="text-sm font-medium text-muted-foreground mb-2">Key Metrics</div>
            <div className="grid grid-cols-2 gap-4">
              {composite.metrics.need_by_date && (
                <div>
                  <span className="text-sm font-medium">Need By Date: </span>
                  <span className="text-sm">{String(composite.metrics.need_by_date)}</span>
                </div>
              )}
              {composite.metrics.open_quantity !== undefined && (
                <div>
                  <span className="text-sm font-medium">Open Quantity: </span>
                  <span className="text-sm">{String(composite.metrics.open_quantity)}</span>
                </div>
              )}
              {composite.metrics.projected_available_quantity !== undefined && (
                <div>
                  <span className="text-sm font-medium">Projected Available: </span>
                  <span className="text-sm">{String(composite.metrics.projected_available_quantity)}</span>
                </div>
              )}
              {composite.metrics.shortage_quantity !== undefined && (
                <div>
                  <span className="text-sm font-medium">Shortage Quantity: </span>
                  <span className="text-sm">{String(composite.metrics.shortage_quantity)}</span>
                </div>
              )}
            </div>
          </div>

          <div className="text-sm text-muted-foreground">
            Computed: {format(new Date(composite.computed_at), "PPpp")}
          </div>
        </CardContent>
      </Card>

      {/* Evidence */}
      <div className="space-y-4">
        <h2 className="text-2xl font-semibold">Evidence</h2>
        {evidence["order_line_fulfillment_risk"] &&
        evidence["order_line_fulfillment_risk"].length > 0 ? (
          evidence["order_line_fulfillment_risk"].map((ev) => (
            <EvidenceViewer key={ev.evidence_id} evidence={ev} />
          ))
        ) : (
          <Card>
            <CardContent className="pt-6">
              <p className="text-muted-foreground">No evidence records found</p>
            </CardContent>
          </Card>
        )}
      </div>
    </div>
  )
}

