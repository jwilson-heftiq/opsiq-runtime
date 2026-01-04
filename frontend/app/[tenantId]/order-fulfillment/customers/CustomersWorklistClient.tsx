"use client"

import { useState, useTransition } from "react"
import Link from "next/link"
import { formatDistanceToNow } from "date-fns"
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
import { DecisionStateBadge, DecisionState } from "@/components/DecisionStateBadge"
import {
  DecisionListResponse,
} from "@/types/decisions"
import { fetchCustomersWorklist } from "@/lib/api/orderFulfillment"

interface CustomersWorklistClientProps {
  tenantId: string
  initialData: DecisionListResponse
  initialState: string[]
  initialSubjectId?: string
}

const ALL_STATES: DecisionState[] = ["HIGH_IMPACT", "MEDIUM_IMPACT", "LOW_IMPACT", "UNKNOWN"]

export function CustomersWorklistClient({
  tenantId,
  initialData,
  initialState,
  initialSubjectId: initialSubjectIdProp,
}: CustomersWorklistClientProps) {
  const [selectedStates, setSelectedStates] = useState<Set<DecisionState>>(
    new Set(initialState as DecisionState[])
  )
  const [subjectIdFilter, setSubjectIdFilter] = useState(initialSubjectIdProp || "")
  const [data, setData] = useState<DecisionListResponse>(initialData)
  const [isPending, startTransition] = useTransition()
  const [isLoadingMore, setIsLoadingMore] = useState(false)

  const toggleState = (state: DecisionState) => {
    const newStates = new Set(selectedStates)
    if (newStates.has(state)) {
      newStates.delete(state)
    } else {
      newStates.add(state)
    }
    setSelectedStates(newStates)
    applyFilters(newStates, subjectIdFilter)
  }

  const applyFilters = (states: Set<DecisionState>, subjectId: string) => {
    startTransition(() => {
      fetchCustomersWorklist(tenantId, {
        state: Array.from(states),
        subject_id: subjectId || undefined,
        limit: 50,
      })
        .then((newData) => {
          setData(newData)
        })
        .catch((error) => {
          console.error("Failed to fetch worklist:", error)
          // TODO: Show error to user
        })
    })
  }

  const handleSubjectIdChange = (value: string) => {
    setSubjectIdFilter(value)
    applyFilters(selectedStates, value)
  }

  const loadMore = () => {
    if (!data.next_cursor || isLoadingMore) return

    setIsLoadingMore(true)
    fetchCustomersWorklist(tenantId, {
      state: Array.from(selectedStates),
      subject_id: subjectIdFilter || undefined,
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

  return (
    <div className="space-y-4">
      {/* Filters */}
      <div className="space-y-4">
        <div className="flex flex-wrap gap-2">
          <span className="text-sm font-medium self-center">State:</span>
          {ALL_STATES.map((state) => (
            <Button
              key={state}
              variant={selectedStates.has(state) ? "default" : "outline"}
              size="sm"
              onClick={() => toggleState(state)}
              disabled={isPending}
            >
              {state}
            </Button>
          ))}
        </div>
        <div className="flex gap-2 items-center">
          <Input
            placeholder="Search by Customer ID"
            value={subjectIdFilter}
            onChange={(e) => handleSubjectIdChange(e.target.value)}
            className="max-w-xs"
            disabled={isPending}
          />
        </div>
      </div>

      {/* Table */}
      <div className="border rounded-md">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Customer ID</TableHead>
              <TableHead>State</TableHead>
              <TableHead>Order Counts</TableHead>
              <TableHead>Drivers</TableHead>
              <TableHead>Computed At</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {data.items.length === 0 ? (
              <TableRow>
                <TableCell colSpan={5} className="text-center text-muted-foreground py-8">
                  No decisions found
                </TableCell>
              </TableRow>
            ) : (
              data.items.map((item) => {
                const metrics = item.metrics as any
                const total = metrics?.order_count_total ?? 0
                const atRisk = metrics?.order_count_at_risk ?? 0
                const unknown = metrics?.order_count_unknown ?? 0
                
                return (
                  <TableRow key={`${item.subject_id}-${item.computed_at}`}>
                    <TableCell>
                      <Link
                        href={`/${tenantId}/order-fulfillment/customers/${item.subject_id}`}
                        className="text-primary hover:underline font-mono"
                      >
                        {item.subject_id}
                      </Link>
                    </TableCell>
                    <TableCell>
                      <DecisionStateBadge state={item.decision_state as DecisionState} />
                    </TableCell>
                    <TableCell>
                      <div className="text-sm">
                        <div>Total: {total}</div>
                        <div className="text-destructive">At Risk: {atRisk}</div>
                        <div className="text-muted-foreground">Unknown: {unknown}</div>
                      </div>
                    </TableCell>
                    <TableCell>
                      <div className="text-sm">
                        {item.drivers.slice(0, 2).join(", ")}
                        {item.drivers.length > 2 && (
                          <span className="text-muted-foreground">
                            {" "}
                            +{item.drivers.length - 2} more
                          </span>
                        )}
                      </div>
                    </TableCell>
                    <TableCell>
                      <div
                        className="text-sm"
                        title={new Date(item.computed_at).toLocaleString()}
                      >
                        {formatDistanceToNow(new Date(item.computed_at), {
                          addSuffix: true,
                        })}
                      </div>
                    </TableCell>
                  </TableRow>
                )
              })
            )}
          </TableBody>
        </Table>
      </div>

      {/* Pagination */}
      {data.next_cursor && (
        <div className="flex justify-center">
          <Button
            onClick={loadMore}
            disabled={isLoadingMore}
            variant="outline"
          >
            {isLoadingMore ? "Loading..." : "Load More"}
          </Button>
        </div>
      )}
    </div>
  )
}
