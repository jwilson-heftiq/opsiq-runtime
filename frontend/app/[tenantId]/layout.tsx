import { Suspense } from "react"
import Link from "next/link"
import { fetchTenantPacks } from "@/lib/api/packs.server"
import { EnabledPackSummary } from "@/types/packs"

interface TenantLayoutProps {
  children: React.ReactNode
  params: Promise<{
    tenantId: string
  }>
}

async function Navigation({ tenantId, children }: { tenantId: string; children: React.ReactNode }) {
  let packs: EnabledPackSummary[] = []
  try {
    packs = await fetchTenantPacks(tenantId)
  } catch (error) {
    console.error("Failed to load packs:", error)
    // Continue with empty packs - navigation will be empty
  }

  return (
    <div className="flex h-screen">
      {/* Sidebar Navigation */}
      <aside className="w-64 border-r bg-muted/40 p-4">
        <div className="mb-4">
          <Link href={`/${tenantId}`} className="text-lg font-bold">
            OpsIQ
          </Link>
        </div>
        <nav className="space-y-2">
          {packs.length === 0 ? (
            <div className="text-sm text-muted-foreground">No packs enabled</div>
          ) : (
            packs.map((pack) =>
              pack.subjects.map((subject, idx) => (
                <Link
                  key={`${pack.pack_id}-${subject.subject_type}-${idx}`}
                  href={subject.default_worklist.ui_route}
                  className="block rounded-md px-3 py-2 text-sm font-medium hover:bg-accent"
                >
                  {subject.default_worklist.title}
                </Link>
              ))
            )
          )}
        </nav>
      </aside>
      {/* Main Content */}
      <main className="flex-1 overflow-auto">{children}</main>
    </div>
  )
}

export default async function TenantLayout({ children, params }: TenantLayoutProps) {
  const resolvedParams = await params
  const tenantId = resolvedParams.tenantId

  return (
    <Suspense fallback={<div>Loading navigation...</div>}>
      <Navigation tenantId={tenantId}>{children}</Navigation>
    </Suspense>
  )
}

