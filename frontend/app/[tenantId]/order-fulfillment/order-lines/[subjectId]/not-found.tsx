import Link from "next/link"
import { Button } from "@/components/ui/button"

export default function NotFound() {
  return (
    <div className="container mx-auto p-6 flex flex-col items-center justify-center min-h-[400px] space-y-4">
      <h2 className="text-2xl font-bold">Order Line Not Found</h2>
      <p className="text-muted-foreground">
        The order line you are looking for does not exist or has no decisions.
      </p>
      <Link href="..">
        <Button variant="outline">Back to Worklist</Button>
      </Link>
    </div>
  )
}

