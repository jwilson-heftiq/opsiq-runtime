import { notFound } from "next/navigation"
import Link from "next/link"
import { fetchOrderDecisionDetail } from "@/lib/api/orderFulfillment.server"
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
    orderId: string
  }>
  searchParams: Promise<{
    as_of_ts?: string
  }>
}

export default async function OrderDetailPage({
  params,
  searchParams,
}: PageProps) {
  // In Next.js 15+, params and searchParams are Promises
  const resolvedParams = await params
  const resolvedSearchParams = await searchParams
  const { tenantId, orderId } = resolvedParams
  const asOfTs = resolvedSearchParams.as_of_ts

  let bundle: DecisionBundle
  try {
    bundle = await fetchOrderDecisionDetail(tenantId, orderId, asOfTs)
  } catch (error: any) {
    if (error.status === 404) {
      notFound()
    }
    throw error
  }

  const { composite, evidence } = bundle
  const metrics = composite.metrics as any
  const atRiskLineIds = (metrics?.at_risk_line_subject_ids as string[]) || []

  return (
    <div className="container mx-auto p-6 space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="space-y-1">
          <div className="flex items-center gap-3">
            <Link
              href={`/${tenantId}/order-fulfillment/orders`}
              className="text-muted-foreground hover:text-foreground"
            >
              ← Back to Worklist
            </Link>
          </div>
          <h1 className="text-3xl font-bold">Order: {orderId}</h1>
          <DecisionStateBadge state={composite.decision_state as any} className="text-base px-3 py-1" />
        </div>
      </div>

      {/* Decision Summary */}
      <Card>
        <CardHeader>
          <CardTitle>Decision</CardTitle>
          <CardDescription>
            Order Fulfillment Risk (v{composite.primitive_version})
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
            <div className="text-sm font-medium text-muted-foreground mb-2">Order Line Counts</div>
            <div className="grid grid-cols-2 gap-4">
              {metrics?.order_line_count_total !== undefined && (
                <div>
                  <span className="text-sm font-medium">Total: </span>
                  <span className="text-sm">{String(metrics.order_line_count_total)}</span>
                </div>
              )}
              {metrics?.order_line_count_at_risk !== undefined && (
                <div>
                  <span className="text-sm font-medium text-destructive">At Risk: </span>
                  <span className="text-sm text-destructive">{String(metrics.order_line_count_at_risk)}</span>
                </div>
              )}
              {metrics?.order_line_count_unknown !== undefined && (
                <div>
                  <span className="text-sm font-medium text-muted-foreground">Unknown: </span>
                  <span className="text-sm text-muted-foreground">{String(metrics.order_line_count_unknown)}</span>
                </div>
              )}
              {metrics?.order_line_count_not_at_risk !== undefined && (
                <div>
                  <span className="text-sm font-medium">Not At Risk: </span>
                  <span className="text-sm">{String(metrics.order_line_count_not_at_risk)}</span>
                </div>
              )}
            </div>
          </div>

          <div className="text-sm text-muted-foreground">
            Computed: {format(new Date(composite.computed_at), "PPpp")}
          </div>
        </CardContent>
      </Card>

      {/* At-Risk Lines Drilldown */}
      {atRiskLineIds.length > 0 && (
        <Card>
          <CardHeader>
            <CardTitle>At-Risk Order Lines</CardTitle>
            <CardDescription>
              {atRiskLineIds.length} order line{atRiskLineIds.length !== 1 ? "s" : ""} at risk
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div className="space-y-2">
              {atRiskLineIds.map((lineId) => (
                <div key={lineId}>
                  <Link
                    href={`/${tenantId}/order-fulfillment/order-lines/${lineId}`}
                    className="text-primary hover:underline font-mono"
                  >
                    {lineId}
                  </Link>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      )}

      {/* Evidence */}
      <div className="space-y-4">
        <h2 className="text-2xl font-semibold">Evidence</h2>
        {evidence["order_fulfillment_risk"] &&
        evidence["order_fulfillment_risk"].length > 0 ? (
          evidence["order_fulfillment_risk"].map((ev) => (
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
