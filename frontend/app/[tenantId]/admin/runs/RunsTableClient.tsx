"use client"

import { useState, useTransition } from "react"
import { format, formatDistanceToNow } from "date-fns"
import { AlertCircle } from "lucide-react"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { Input } from "@/components/ui/input"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { RunStatusBadge } from "@/components/RunStatusBadge"
import { RunRegistryResponse, RunRegistryItem } from "@/types/runs"
import { fetchRunRegistryClient } from "@/lib/api/runs"

interface RunsTableClientProps {
  tenantId: string
  initialData: RunRegistryResponse
  initialPrimitiveName?: string
  initialStatus?: string
}

const ALL_STATUSES = ["STARTED", "SUCCESS", "FAILED"]

export function RunsTableClient({
  tenantId,
  initialData,
  initialPrimitiveName: initialPrimitiveNameProp,
  initialStatus: initialStatusProp,
}: RunsTableClientProps) {
  const [primitiveNameFilter, setPrimitiveNameFilter] = useState(initialPrimitiveNameProp || "")
  const [statusFilter, setStatusFilter] = useState<string>(initialStatusProp || "")
  const [data, setData] = useState<RunRegistryResponse>(initialData)
  const [expandedRows, setExpandedRows] = useState<Set<string>>(new Set())
  const [isPending, startTransition] = useTransition()
  const [isLoadingMore, setIsLoadingMore] = useState(false)

  const toggleRowExpansion = (correlationId: string) => {
    const newExpanded = new Set(expandedRows)
    if (newExpanded.has(correlationId)) {
      newExpanded.delete(correlationId)
    } else {
      newExpanded.add(correlationId)
    }
    setExpandedRows(newExpanded)
  }

  const applyFilters = (primitiveName: string, status: string) => {
    startTransition(() => {
      fetchRunRegistryClient(tenantId, {
        primitive_name: primitiveName || undefined,
        status: status || undefined,
        limit: 50,
      })
        .then((newData) => {
          setData(newData)
        })
        .catch((error) => {
          console.error("Failed to fetch runs:", error)
        })
    })
  }

  const handlePrimitiveNameChange = (value: string) => {
    setPrimitiveNameFilter(value)
    applyFilters(value, statusFilter)
  }

  const handleStatusChange = (value: string) => {
    setStatusFilter(value)
    applyFilters(primitiveNameFilter, value)
  }

  const loadMore = () => {
    if (!data.next_cursor || isLoadingMore) return

    setIsLoadingMore(true)
    fetchRunRegistryClient(tenantId, {
      primitive_name: primitiveNameFilter || undefined,
      status: statusFilter || undefined,
      limit: 50,
      cursor: data.next_cursor,
    })
      .then((newData) => {
        setData({
          ...newData,
          items: [...data.items, ...newData.items],
        })
      })
      .catch((error) => {
        console.error("Failed to load more:", error)
      })
      .finally(() => {
        setIsLoadingMore(false)
      })
  }

  const formatDuration = (ms: number | null | undefined): string => {
    if (ms === null || ms === undefined) return "—"
    if (ms < 1000) return `${ms}ms`
    const seconds = Math.floor(ms / 1000)
    if (seconds < 60) return `${seconds}s`
    const minutes = Math.floor(seconds / 60)
    const remainingSeconds = seconds % 60
    return `${minutes}m ${remainingSeconds}s`
  }

  return (
    <div className="space-y-4">
      {/* Filters */}
      <Card>
        <CardHeader>
          <CardTitle>Filters</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
              <label className="text-sm font-medium mb-2 block">Primitive Name</label>
              <Input
                placeholder="Filter by primitive name..."
                value={primitiveNameFilter}
                onChange={(e) => handlePrimitiveNameChange(e.target.value)}
                disabled={isPending}
              />
            </div>
            <div>
              <label className="text-sm font-medium mb-2 block">Status</label>
              <select
                className="flex h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2"
                value={statusFilter}
                onChange={(e) => handleStatusChange(e.target.value)}
                disabled={isPending}
              >
                <option value="">All Statuses</option>
                {ALL_STATUSES.map((status) => (
                  <option key={status} value={status}>
                    {status}
                  </option>
                ))}
              </select>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Table */}
      <Card>
        <CardHeader>
          <CardTitle>Runs</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="border rounded-md">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Primitive</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead>Started At</TableHead>
                  <TableHead>Duration</TableHead>
                  <TableHead>Decision Count</TableHead>
                  <TableHead>Errors</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {data.items.length === 0 ? (
                  <TableRow>
                    <TableCell colSpan={6} className="text-center text-muted-foreground py-8">
                      No runs found
                    </TableCell>
                  </TableRow>
                ) : (
                  data.items.map((item) => (
                    <>
                      <TableRow
                        key={item.correlation_id}
                        className="cursor-pointer"
                        onClick={() => toggleRowExpansion(item.correlation_id)}
                      >
                        <TableCell>
                          <div>
                            <div className="font-medium">{item.primitive_name}</div>
                            <div className="text-xs text-muted-foreground">
                              v{item.primitive_version}
                            </div>
                          </div>
                        </TableCell>
                        <TableCell>
                          <RunStatusBadge status={item.status as any} />
                        </TableCell>
                        <TableCell>
                          <div className="text-sm">
                            <div>{format(new Date(item.started_at), "PPpp")}</div>
                            <div className="text-xs text-muted-foreground">
                              {formatDistanceToNow(new Date(item.started_at), { addSuffix: true })}
                            </div>
                          </div>
                        </TableCell>
                        <TableCell>
                          <span className="text-sm">{formatDuration(item.duration_ms)}</span>
                        </TableCell>
                        <TableCell>
                          <span className="text-sm">
                            {item.decision_count !== null ? item.decision_count : "—"}
                          </span>
                        </TableCell>
                        <TableCell>
                          {item.error_message ? (
                            <AlertCircle className="h-4 w-4 text-destructive" />
                          ) : (
                            <span className="text-muted-foreground">—</span>
                          )}
                        </TableCell>
                      </TableRow>
                      {expandedRows.has(item.correlation_id) && (
                        <TableRow key={`${item.correlation_id}-expanded`}>
                          <TableCell colSpan={6} className="bg-muted/50">
                            <div className="space-y-3 py-4 px-2">
                              <div>
                                <span className="text-sm font-medium">Correlation ID: </span>
                                <code className="text-sm bg-background px-2 py-1 rounded">
                                  {item.correlation_id}
                                </code>
                              </div>
                              {item.error_message && (
                                <div>
                                  <span className="text-sm font-medium text-destructive">
                                    Error Message:
                                  </span>
                                  <pre className="mt-1 text-xs bg-background p-2 rounded overflow-auto">
                                    {item.error_message}
                                  </pre>
                                </div>
                              )}
                              <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
                                {item.input_count !== null && (
                                  <div>
                                    <span className="text-muted-foreground">Input Count: </span>
                                    <span className="font-medium">{item.input_count}</span>
                                  </div>
                                )}
                                {item.decision_count !== null && (
                                  <div>
                                    <span className="text-muted-foreground">Decision Count: </span>
                                    <span className="font-medium">{item.decision_count}</span>
                                  </div>
                                )}
                                {item.at_risk_count !== null && (
                                  <div>
                                    <span className="text-muted-foreground">At Risk: </span>
                                    <span className="font-medium">{item.at_risk_count}</span>
                                  </div>
                                )}
                                {item.unknown_count !== null && (
                                  <div>
                                    <span className="text-muted-foreground">Unknown: </span>
                                    <span className="font-medium">{item.unknown_count}</span>
                                  </div>
                                )}
                              </div>
                              {item.completed_at && (
                                <div className="text-sm text-muted-foreground">
                                  Completed: {format(new Date(item.completed_at), "PPpp")}
                                </div>
                              )}
                            </div>
                          </TableCell>
                        </TableRow>
                      )}
                    </>
                  ))
                )}
              </TableBody>
            </Table>
          </div>

          {/* Load More Button */}
          {data.next_cursor && (
            <div className="mt-4 flex justify-center">
              <Button
                onClick={loadMore}
                disabled={isLoadingMore}
                variant="outline"
              >
                {isLoadingMore ? "Loading..." : "Load More"}
              </Button>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  )
}

