"use client"

import { DecisionHistoryItem } from "@/types/decisions"
import { DecisionStateBadge } from "@/components/DecisionStateBadge"
import { Badge } from "@/components/ui/badge"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { format, formatDistanceToNow } from "date-fns"

interface DecisionHistoryTimelineProps {
  items: DecisionHistoryItem[]
}

export function DecisionHistoryTimeline({ items }: DecisionHistoryTimelineProps) {
  if (items.length === 0) {
    return (
      <Card>
        <CardContent className="pt-6">
          <p className="text-muted-foreground">No decision history found</p>
        </CardContent>
      </Card>
    )
  }

  // Group items by primitive_name
  const groupedByPrimitive: Record<string, DecisionHistoryItem[]> = {}
  for (const item of items) {
    if (!groupedByPrimitive[item.primitive_name]) {
      groupedByPrimitive[item.primitive_name] = []
    }
    groupedByPrimitive[item.primitive_name].push(item)
  }

  return (
    <div className="space-y-6">
      {Object.entries(groupedByPrimitive).map(([primitiveName, primitiveItems]) => (
        <div key={primitiveName} className="space-y-4">
          <div className="flex items-center gap-2">
            <h3 className="text-lg font-semibold">{primitiveName}</h3>
            <Badge variant="outline">{primitiveItems.length} entries</Badge>
          </div>
          <div className="space-y-3">
            {primitiveItems.map((item, idx) => (
              <Card key={`${item.as_of_ts}-${idx}`}>
                <CardHeader className="pb-3">
                  <div className="flex items-start justify-between">
                    <div className="space-y-2">
                      <div className="flex items-center gap-2">
                        <Badge variant="secondary">v{item.primitive_version}</Badge>
                        <DecisionStateBadge state={item.decision_state as any} />
                        <Badge variant="outline">{item.confidence}</Badge>
                      </div>
                      <div className="text-sm text-muted-foreground">
                        <span
                          title={format(new Date(item.as_of_ts), "PPpp")}
                          className="cursor-help"
                        >
                          {formatDistanceToNow(new Date(item.as_of_ts), { addSuffix: true })}
                        </span>
                        {" · "}
                        Computed: {format(new Date(item.computed_at), "PPpp")}
                      </div>
                    </div>
                  </div>
                </CardHeader>
                <CardContent className="pt-0">
                  {item.drivers.length > 0 && (
                    <div>
                      <div className="text-sm font-medium text-muted-foreground mb-2">
                        Drivers
                      </div>
                      <div className="flex flex-wrap gap-2">
                        {item.drivers.map((driver, driverIdx) => (
                          <Badge key={driverIdx} variant="secondary">
                            {driver.code}
                          </Badge>
                        ))}
                      </div>
                    </div>
                  )}
                  {item.drivers.length === 0 && (
                    <div className="text-sm text-muted-foreground">No drivers</div>
                  )}
                </CardContent>
              </Card>
            ))}
          </div>
        </div>
      ))}
    </div>
  )
}

