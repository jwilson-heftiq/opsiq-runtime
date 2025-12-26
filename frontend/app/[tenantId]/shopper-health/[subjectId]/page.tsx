import { notFound } from "next/navigation"
import Link from "next/link"
import { fetchShopperDecisionBundle, fetchDecisionHistory } from "@/lib/api/decisions.server"
import { DecisionStateBadge } from "@/components/DecisionStateBadge"
import { EvidenceViewer } from "@/components/EvidenceViewer"
import { DecisionHistoryTimeline } from "@/components/DecisionHistoryTimeline"
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
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

export default async function ShopperHealthDetailPage({
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
    bundle = await fetchShopperDecisionBundle(tenantId, subjectId, asOfTs)
  } catch (error: any) {
    if (error.status === 404) {
      notFound()
    }
    throw error
  }

  const { composite, components, evidence } = bundle

  // Fetch decision history
  let history
  try {
    history = await fetchDecisionHistory(tenantId, subjectId)
  } catch (error: any) {
    // If history fetch fails, just log and continue (history is optional)
    console.warn("Failed to fetch decision history:", error)
    history = { subject_id: subjectId, items: [] }
  }

  return (
    <div className="container mx-auto p-6 space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="space-y-1">
          <div className="flex items-center gap-3">
            <Link
              href={`/${tenantId}/shopper-health`}
              className="text-muted-foreground hover:text-foreground"
            >
              ← Back to Worklist
            </Link>
          </div>
          <h1 className="text-3xl font-bold">Subject: {subjectId}</h1>
          <DecisionStateBadge state={composite.decision_state} className="text-base px-3 py-1" />
        </div>
      </div>

      {/* Composite Decision Summary */}
      <Card>
        <CardHeader>
          <CardTitle>Composite Decision</CardTitle>
          <CardDescription>
            Shopper Health Classification (v{composite.primitive_version})
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid grid-cols-2 gap-4">
            <div>
              <div className="text-sm font-medium text-muted-foreground">State</div>
              <DecisionStateBadge state={composite.decision_state} />
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
              {composite.metrics.risk_state && (
                <div>
                  <span className="text-sm font-medium">Risk State: </span>
                  <span className="text-sm">{String(composite.metrics.risk_state)}</span>
                </div>
              )}
              {composite.metrics.trend_state && (
                <div>
                  <span className="text-sm font-medium">Trend State: </span>
                  <span className="text-sm">{String(composite.metrics.trend_state)}</span>
                </div>
              )}
            </div>
          </div>

          <div className="text-sm text-muted-foreground">
            Computed: {format(new Date(composite.computed_at), "PPpp")}
          </div>
        </CardContent>
      </Card>

      {/* Tabs for Evidence */}
      <Tabs defaultValue="composite" className="space-y-4">
        <TabsList>
          <TabsTrigger value="composite">Composite Evidence</TabsTrigger>
          <TabsTrigger value="operational_risk">Operational Risk (v1)</TabsTrigger>
          <TabsTrigger value="shopper_frequency_trend">Frequency Trend (v2)</TabsTrigger>
          <TabsTrigger value="history">History</TabsTrigger>
        </TabsList>

        {/* Composite Evidence Tab */}
        <TabsContent value="composite" className="space-y-4">
          {evidence["shopper_health_classification"] &&
          evidence["shopper_health_classification"].length > 0 ? (
            evidence["shopper_health_classification"].map((ev) => (
              <EvidenceViewer key={ev.evidence_id} evidence={ev} />
            ))
          ) : (
            <Card>
              <CardContent className="pt-6">
                <p className="text-muted-foreground">No evidence records found</p>
              </CardContent>
            </Card>
          )}
        </TabsContent>

        {/* Operational Risk Tab */}
        <TabsContent value="operational_risk" className="space-y-4">
          {components["operational_risk"] ? (
            <>
              <Card>
                <CardHeader>
                  <CardTitle>Operational Risk Decision</CardTitle>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div className="grid grid-cols-2 gap-4">
                    <div>
                      <div className="text-sm font-medium text-muted-foreground">State</div>
                      <DecisionStateBadge state={components["operational_risk"].decision_state} />
                    </div>
                    <div>
                      <div className="text-sm font-medium text-muted-foreground">Confidence</div>
                      <Badge variant="outline">{components["operational_risk"].confidence}</Badge>
                    </div>
                  </div>
                  <div>
                    <div className="text-sm font-medium text-muted-foreground mb-2">Drivers</div>
                    <div className="flex flex-wrap gap-2">
                      {components["operational_risk"].drivers.length > 0 ? (
                        components["operational_risk"].drivers.map((driver, idx) => (
                          <Badge key={idx} variant="secondary">
                            {driver}
                          </Badge>
                        ))
                      ) : (
                        <span className="text-sm text-muted-foreground">None</span>
                      )}
                    </div>
                  </div>
                  <div className="text-sm text-muted-foreground">
                    Computed: {format(new Date(components["operational_risk"].computed_at), "PPpp")}
                  </div>
                </CardContent>
              </Card>
              {evidence["operational_risk"] && evidence["operational_risk"].length > 0 ? (
                <div className="space-y-4">
                  <h3 className="text-lg font-semibold">Evidence</h3>
                  {evidence["operational_risk"].map((ev) => (
                    <EvidenceViewer key={ev.evidence_id} evidence={ev} />
                  ))}
                </div>
              ) : (
                <Card>
                  <CardContent className="pt-6">
                    <p className="text-muted-foreground">No evidence records found</p>
                  </CardContent>
                </Card>
              )}
            </>
          ) : (
            <Card>
              <CardContent className="pt-6">
                <p className="text-muted-foreground">No operational risk decision found</p>
              </CardContent>
            </Card>
          )}
        </TabsContent>

        {/* Frequency Trend Tab */}
        <TabsContent value="shopper_frequency_trend" className="space-y-4">
          {components["shopper_frequency_trend"] ? (
            <>
              <Card>
                <CardHeader>
                  <CardTitle>Frequency Trend Decision</CardTitle>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div className="grid grid-cols-2 gap-4">
                    <div>
                      <div className="text-sm font-medium text-muted-foreground">State</div>
                      <DecisionStateBadge state={components["shopper_frequency_trend"].decision_state} />
                    </div>
                    <div>
                      <div className="text-sm font-medium text-muted-foreground">Confidence</div>
                      <Badge variant="outline">{components["shopper_frequency_trend"].confidence}</Badge>
                    </div>
                  </div>
                  <div>
                    <div className="text-sm font-medium text-muted-foreground mb-2">Drivers</div>
                    <div className="flex flex-wrap gap-2">
                      {components["shopper_frequency_trend"].drivers.length > 0 ? (
                        components["shopper_frequency_trend"].drivers.map((driver, idx) => (
                          <Badge key={idx} variant="secondary">
                            {driver}
                          </Badge>
                        ))
                      ) : (
                        <span className="text-sm text-muted-foreground">None</span>
                      )}
                    </div>
                  </div>
                  <div className="text-sm text-muted-foreground">
                    Computed: {format(new Date(components["shopper_frequency_trend"].computed_at), "PPpp")}
                  </div>
                </CardContent>
              </Card>
              {evidence["shopper_frequency_trend"] && evidence["shopper_frequency_trend"].length > 0 ? (
                <div className="space-y-4">
                  <h3 className="text-lg font-semibold">Evidence</h3>
                  {evidence["shopper_frequency_trend"].map((ev) => (
                    <EvidenceViewer key={ev.evidence_id} evidence={ev} />
                  ))}
                </div>
              ) : (
                <Card>
                  <CardContent className="pt-6">
                    <p className="text-muted-foreground">No evidence records found</p>
                  </CardContent>
                </Card>
              )}
            </>
          ) : (
            <Card>
              <CardContent className="pt-6">
                <p className="text-muted-foreground">No frequency trend decision found</p>
              </CardContent>
            </Card>
          )}
        </TabsContent>

        {/* History Tab */}
        <TabsContent value="history" className="space-y-4">
          <DecisionHistoryTimeline items={history.items} />
        </TabsContent>
      </Tabs>
    </div>
  )
}

