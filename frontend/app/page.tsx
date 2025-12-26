import Link from "next/link"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"

export default function HomePage() {
  // Example tenant ID - replace with your actual tenant ID or create a tenant selector
  const defaultTenantId = "price_chopper"

  return (
    <div className="container mx-auto p-6 flex items-center justify-center min-h-screen">
      <Card className="max-w-md w-full">
        <CardHeader>
          <CardTitle className="text-2xl">OpsIQ Shopper Health</CardTitle>
          <CardDescription>
            Select a tenant to view shopper health decisions
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="space-y-2">
            <p className="text-sm text-muted-foreground">
              Navigate to a tenant-specific worklist:
            </p>
            <div className="flex gap-2">
              <Link href={`/${defaultTenantId}/shopper-health`} className="flex-1">
                <Button className="w-full">
                  View {defaultTenantId}
                </Button>
              </Link>
            </div>
          </div>
          <div className="text-xs text-muted-foreground pt-4 border-t">
            <p>Or navigate directly to:</p>
            <code className="block mt-1 p-2 bg-muted rounded">
              /[tenantId]/shopper-health
            </code>
          </div>
        </CardContent>
      </Card>
    </div>
  )
}

