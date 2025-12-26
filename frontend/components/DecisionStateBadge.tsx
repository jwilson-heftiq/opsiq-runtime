"use client"

import { Badge } from "@/components/ui/badge"
import { cn } from "@/lib/utils"

export type DecisionState = "URGENT" | "WATCHLIST" | "HEALTHY" | "UNKNOWN"

interface DecisionStateBadgeProps {
  state: DecisionState
  className?: string
}

export function DecisionStateBadge({
  state,
  className,
}: DecisionStateBadgeProps) {
  const variantMap: Record<
    DecisionState,
    "default" | "secondary" | "destructive" | "outline"
  > = {
    URGENT: "destructive",
    WATCHLIST: "secondary",
    HEALTHY: "default",
    UNKNOWN: "outline",
  }

  return (
    <Badge variant={variantMap[state]} className={cn(className)}>
      {state}
    </Badge>
  )
}

