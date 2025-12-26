"use client"

import { useState } from "react"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { EvidenceRecord } from "@/types/decisions"
import { format } from "date-fns"

interface EvidenceViewerProps {
  evidence: EvidenceRecord
}

export function EvidenceViewer({ evidence }: EvidenceViewerProps) {
  const [showRaw, setShowRaw] = useState(false)

  return (
    <Card>
      <CardHeader>
        <div className="flex items-center justify-between">
          <CardTitle className="text-lg">
            {evidence.evidence_id}
          </CardTitle>
          <Button
            variant="outline"
            size="sm"
            onClick={() => setShowRaw(!showRaw)}
          >
            {showRaw ? "Show Formatted" : "Show Raw JSON"}
          </Button>
        </div>
        <div className="text-sm text-muted-foreground space-y-1">
          <div>
            <span className="font-medium">Primitive:</span> {evidence.primitive_name} v{evidence.primitive_version}
          </div>
          <div>
            <span className="font-medium">As of:</span> {format(new Date(evidence.as_of_ts), "PPpp")}
          </div>
          <div>
            <span className="font-medium">Computed:</span> {format(new Date(evidence.computed_at), "PPpp")}
          </div>
        </div>
      </CardHeader>
      <CardContent>
        {showRaw ? (
          <pre className="bg-muted p-4 rounded-md overflow-auto text-xs">
            {JSON.stringify(evidence.evidence, null, 2)}
          </pre>
        ) : (
          <div className="space-y-2">
            {Object.entries(evidence.evidence).map(([key, value]) => (
              <div key={key} className="border-b pb-2 last:border-0">
                <div className="font-medium text-sm">{key}</div>
                <div className="text-sm text-muted-foreground mt-1">
                  {typeof value === "object" ? (
                    <pre className="bg-muted p-2 rounded text-xs">
                      {JSON.stringify(value, null, 2)}
                    </pre>
                  ) : (
                    String(value)
                  )}
                </div>
              </div>
            ))}
          </div>
        )}
      </CardContent>
    </Card>
  )
}

