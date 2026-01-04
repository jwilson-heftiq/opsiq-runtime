"use client"

import { Badge } from "@/components/ui/badge"
import { cn } from "@/lib/utils"

export type DecisionState = "URGENT" | "WATCHLIST" | "HEALTHY" | "UNKNOWN" | "AT_RISK" | "NOT_AT_RISK" | "HIGH_IMPACT" | "MEDIUM_IMPACT" | "LOW_IMPACT"

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
    AT_RISK: "destructive",
    NOT_AT_RISK: "default",
    HIGH_IMPACT: "destructive",
    MEDIUM_IMPACT: "secondary",
    LOW_IMPACT: "default",
  }

  return (
    <Badge variant={variantMap[state]} className={cn(className)}>
      {state}
    </Badge>
  )
}

