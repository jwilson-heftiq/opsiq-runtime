"use client"

import { Badge } from "@/components/ui/badge"
import { cn } from "@/lib/utils"

export type RunStatus = "STARTED" | "SUCCESS" | "FAILED"

interface RunStatusBadgeProps {
  status: RunStatus
  className?: string
}

export function RunStatusBadge({ status, className }: RunStatusBadgeProps) {
  const variantMap: Record<RunStatus, "default" | "secondary" | "destructive" | "outline"> = {
    STARTED: "secondary",
    SUCCESS: "default",
    FAILED: "destructive",
  }

  return (
    <Badge variant={variantMap[status]} className={cn(className)}>
      {status}
    </Badge>
  )
}

