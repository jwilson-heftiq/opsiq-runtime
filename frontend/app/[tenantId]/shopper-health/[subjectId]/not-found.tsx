import Link from "next/link"

export default function NotFound() {
  return (
    <div className="container mx-auto p-6 text-center space-y-4">
      <h2 className="text-2xl font-bold">Decision Not Found</h2>
      <p className="text-muted-foreground">
        The requested decision bundle could not be found.
      </p>
    </div>
  )
}

