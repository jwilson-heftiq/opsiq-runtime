"use client"

import { useState } from "react"
import { format, formatDistanceToNow } from "date-fns"
import { ChevronDown, ChevronRight } from "lucide-react"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { PackReadinessResponse } from "@/types/packs"
import { cn } from "@/lib/utils"

interface ReadinessClientProps {
  readinessData: PackReadinessResponse[]
}

function StatusBadge({ status }: { status: "PASS" | "WARN" | "FAIL" }) {
  const variantMap: Record<"PASS" | "WARN" | "FAIL", "default" | "secondary" | "destructive"> = {
    PASS: "default",
    WARN: "secondary",
    FAIL: "destructive",
  }

  return (
    <Badge variant={variantMap[status]} className="font-medium">
      {status}
    </Badge>
  )
}

export function ReadinessClient({ readinessData }: ReadinessClientProps) {
  const [expandedPacks, setExpandedPacks] = useState<Set<string>>(new Set())

  const togglePackExpansion = (packId: string) => {
    const newExpanded = new Set(expandedPacks)
    if (newExpanded.has(packId)) {
      newExpanded.delete(packId)
    } else {
      newExpanded.add(packId)
    }
    setExpandedPacks(newExpanded)
  }

  return (
    <div className="space-y-6">
      {readinessData.map((pack) => (
        <Card key={`${pack.pack_id}-${pack.pack_version}`}>
          <CardHeader>
            <div className="flex items-center justify-between">
              <div>
                <CardTitle className="text-xl">{pack.pack_id}</CardTitle>
                <p className="text-sm text-muted-foreground mt-1">
                  Version {pack.pack_version}
                </p>
              </div>
              <div className="flex items-center gap-4">
                <StatusBadge status={pack.overall_status} />
                <button
                  onClick={() => togglePackExpansion(pack.pack_id)}
                  className="p-1 hover:bg-accent rounded-md transition-colors"
                  aria-label="Toggle details"
                >
                  {expandedPacks.has(pack.pack_id) ? (
                    <ChevronDown className="h-5 w-5" />
                  ) : (
                    <ChevronRight className="h-5 w-5" />
                  )}
                </button>
              </div>
            </div>
            <div className="text-xs text-muted-foreground mt-2">
              Computed: {format(new Date(pack.computed_at), "PPpp")} (
              {formatDistanceToNow(new Date(pack.computed_at), { addSuffix: true })})
            </div>
          </CardHeader>

          {expandedPacks.has(pack.pack_id) && (
            <CardContent className="space-y-6">
              {/* Canonical Freshness */}
              {pack.canonical_freshness.length > 0 && (
                <div>
                  <h3 className="text-lg font-semibold mb-3">Canonical Freshness</h3>
                  <div className="border rounded-md">
                    <Table>
                      <TableHeader>
                        <TableRow>
                          <TableHead>Table</TableHead>
                          <TableHead>Last Update</TableHead>
                          <TableHead>Hours Since</TableHead>
                          <TableHead>Status</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {pack.canonical_freshness.map((freshness) => (
                          <TableRow key={freshness.table}>
                            <TableCell className="font-mono text-sm">
                              {freshness.table}
                            </TableCell>
                            <TableCell>
                              {freshness.last_as_of_ts ? (
                                <div>
                                  <div className="text-sm">
                                    {format(new Date(freshness.last_as_of_ts), "PPpp")}
                                  </div>
                                  <div className="text-xs text-muted-foreground">
                                    {formatDistanceToNow(
                                      new Date(freshness.last_as_of_ts),
                                      { addSuffix: true }
                                    )}
                                  </div>
                                </div>
                              ) : (
                                <span className="text-muted-foreground">No data</span>
                              )}
                            </TableCell>
                            <TableCell>
                              {freshness.hours_since_last_update !== null ? (
                                <span className="text-sm">
                                  {freshness.hours_since_last_update.toFixed(1)}h
                                </span>
                              ) : (
                                <span className="text-muted-foreground">—</span>
                              )}
                            </TableCell>
                            <TableCell>
                              <StatusBadge status={freshness.status} />
                            </TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </div>
                </div>
              )}

              {/* Decision Health */}
              {pack.decision_health.length > 0 && (
                <div>
                  <h3 className="text-lg font-semibold mb-3">Decision Health</h3>
                  <div className="border rounded-md">
                    <Table>
                      <TableHeader>
                        <TableRow>
                          <TableHead>Primitive</TableHead>
                          <TableHead>Total Decisions</TableHead>
                          <TableHead>State Breakdown</TableHead>
                          <TableHead>Unknown Rate</TableHead>
                          <TableHead>Last Computed</TableHead>
                          <TableHead>Status</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {pack.decision_health.map((health) => (
                          <TableRow key={health.primitive_name}>
                            <TableCell className="font-medium">
                              {health.primitive_name}
                            </TableCell>
                            <TableCell>
                              <span className="text-sm">{health.total_decisions.toLocaleString()}</span>
                            </TableCell>
                            <TableCell>
                              <div className="flex flex-wrap gap-2">
                                {Object.entries(health.state_counts).map(([state, count]) => (
                                  <Badge key={state} variant="outline" className="text-xs">
                                    {state}: {count}
                                  </Badge>
                                ))}
                              </div>
                            </TableCell>
                            <TableCell>
                              <span
                                className={cn(
                                  "text-sm font-medium",
                                  health.unknown_rate >= 0.6
                                    ? "text-destructive"
                                    : health.unknown_rate >= 0.3
                                    ? "text-yellow-600"
                                    : "text-muted-foreground"
                                )}
                              >
                                {(health.unknown_rate * 100).toFixed(1)}%
                              </span>
                            </TableCell>
                            <TableCell>
                              {health.last_computed_at ? (
                                <div>
                                  <div className="text-sm">
                                    {format(new Date(health.last_computed_at), "PPpp")}
                                  </div>
                                  <div className="text-xs text-muted-foreground">
                                    {formatDistanceToNow(new Date(health.last_computed_at), {
                                      addSuffix: true,
                                    })}
                                  </div>
                                </div>
                              ) : (
                                <span className="text-muted-foreground">Never</span>
                              )}
                            </TableCell>
                            <TableCell>
                              <StatusBadge status={health.status} />
                            </TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </div>
                </div>
              )}

              {/* Rollup Integrity */}
              {pack.rollup_integrity.length > 0 && (
                <div>
                  <h3 className="text-lg font-semibold mb-3">Rollup Integrity</h3>
                  <div className="border rounded-md">
                    <Table>
                      <TableHeader>
                        <TableRow>
                          <TableHead>Check</TableHead>
                          <TableHead>Pass Rate</TableHead>
                          <TableHead>Status</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {pack.rollup_integrity.map((integrity) => (
                          <TableRow key={integrity.check}>
                            <TableCell className="font-medium">{integrity.check}</TableCell>
                            <TableCell>
                              <span
                                className={cn(
                                  "text-sm font-medium",
                                  integrity.pass_rate < 0.8
                                    ? "text-destructive"
                                    : integrity.pass_rate < 0.95
                                    ? "text-yellow-600"
                                    : "text-muted-foreground"
                                )}
                              >
                                {(integrity.pass_rate * 100).toFixed(1)}%
                              </span>
                            </TableCell>
                            <TableCell>
                              <StatusBadge status={integrity.status} />
                            </TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </div>
                </div>
              )}
            </CardContent>
          )}
        </Card>
      ))}

      {readinessData.length === 0 && (
        <Card>
          <CardContent className="pt-6">
            <p className="text-center text-muted-foreground">
              No enabled packs found for this tenant.
            </p>
          </CardContent>
        </Card>
      )}
    </div>
  )
}

