# OpsIQ Frontend

Next.js frontend application for viewing and managing OpsIQ decision intelligence, including Shopper Health Classification and Order Line Fulfillment Risk.

## Technology Stack

- **Next.js 16** (App Router)
- **React 19**
- **TypeScript**
- **Tailwind CSS**
- **ShadCN UI** components
- **date-fns** for date formatting

## Getting Started

### Prerequisites

- Node.js 20 or higher
- npm or yarn

### Installation

1. Install dependencies:
```bash
npm install
```

2. Create `.env.local` file in the frontend directory:
```env
NEXT_PUBLIC_OPSIQ_API_BASE_URL=http://localhost:8080
```

3. Run the development server:
```bash
npm run dev
```

The application will be available at `http://localhost:3000`.

## Routes

The frontend uses pack-aware navigation that dynamically displays enabled decision packs for each tenant.

### Shopper Health Intelligence
- `/[tenantId]/shopper-health` - Worklist page displaying latest shopper health decisions
- `/[tenantId]/shopper-health/[subjectId]` - Detail page showing decision bundle with evidence

### Order Line Fulfillment Risk
- `/[tenantId]/order-fulfillment/order-lines` - Worklist page displaying order lines at risk
- `/[tenantId]/order-fulfillment/order-lines/[subjectId]` - Detail page showing order line decision and evidence

## Docker

### Development

The frontend can be run in Docker using docker-compose:

```bash
# From the repository root
docker-compose --profile frontend --profile api up
```

The frontend will be available at `http://localhost:3000`.

### Building for Production

```bash
docker build -t opsiq-frontend ./frontend
```

### Environment Variables

- `NEXT_PUBLIC_OPSIQ_API_BASE_URL` - Base URL for the OpsIQ API backend
  - For local development: `http://localhost:8080`
  - For Docker: Set to match your API service configuration
  - Note: This variable is used for both server-side and client-side requests

## Project Structure

```
frontend/
├── app/                          # Next.js App Router pages
│   ├── [tenantId]/
│   │   ├── layout.tsx            # Tenant layout with pack-aware navigation
│   │   ├── shopper-health/
│   │   │   ├── page.tsx          # Worklist page
│   │   │   ├── WorklistClient.tsx # Client component for filters/table
│   │   │   └── [subjectId]/
│   │   │       └── page.tsx      # Detail page
│   │   └── order-fulfillment/
│   │       └── order-lines/
│   │           ├── page.tsx      # Worklist page
│   │           ├── OrderLineWorklistClient.tsx # Client component
│   │           └── [subjectId]/
│   │               └── page.tsx  # Detail page
│   ├── globals.css               # Global styles
│   └── layout.tsx                # Root layout
├── components/
│   ├── ui/                       # ShadCN UI components
│   ├── DecisionStateBadge.tsx    # Badge component for decision states
│   └── EvidenceViewer.tsx        # Evidence display component
├── lib/
│   ├── api/
│   │   ├── decisions.ts          # Shopper health API client functions
│   │   ├── decisions.server.ts   # Server-side API functions
│   │   ├── packs.ts              # Pack API client functions
│   │   ├── packs.server.ts       # Server-side pack API functions
│   │   ├── orderFulfillment.ts   # Order fulfillment API client
│   │   └── orderFulfillment.server.ts # Server-side order fulfillment API
│   └── utils.ts                  # Utility functions
├── types/
│   ├── decisions.ts              # Decision type definitions
│   └── packs.ts                  # Pack type definitions
└── package.json
```

## Features

### Navigation

The frontend uses pack-aware navigation that:
- Dynamically loads enabled packs for each tenant from the backend
- Displays each pack's default worklist as a navigation link
- Automatically adapts when packs are enabled or disabled
- Renders in a sidebar layout for tenant-specific routes

### Shopper Health Worklist Page

- Filter decisions by state (URGENT, WATCHLIST, HEALTHY, UNKNOWN)
- Search by subject ID
- Pagination with "Load more" button
- Default filter shows URGENT + WATCHLIST states
- Displays: Subject ID, State, Drivers, Confidence, Computed At

### Shopper Health Detail Page

- Composite decision summary
- Tabs for different evidence types:
  - Composite Evidence
  - Operational Risk (v1)
  - Frequency Trend (v2)
  - History timeline
- Evidence viewer with formatted and raw JSON views
- Decision metrics and drivers

### Order Line Fulfillment Worklist Page

- Filter decisions by state (AT_RISK, NOT_AT_RISK, UNKNOWN)
- Search by subject ID
- Pagination with "Load more" button
- Default filter shows AT_RISK state
- Displays: Subject ID, State, Drivers, Computed At

### Order Line Fulfillment Detail Page

- Primary decision summary with metrics:
  - Need By Date
  - Open Quantity
  - Projected Available Quantity
  - Shortage Quantity
- Evidence viewer showing order line fulfillment evidence
- Decision drivers and confidence

### Orders at Risk Worklist Page

- Filter decisions by state (AT_RISK, NOT_AT_RISK, UNKNOWN)
- Search by order ID
- Pagination with "Load more" button
- Default filter shows AT_RISK state
- Displays: Order ID, State, Drivers, Computed At

### Order Detail Page

- Primary decision summary with metrics:
  - Order Line Counts (total, at risk, unknown, not at risk)
  - At-risk order line subject IDs
- Drilldown section listing at-risk order lines with links to order line detail pages
- Evidence viewer showing order fulfillment evidence
- Decision drivers and confidence

### Customers Impacted Worklist Page

- Filter decisions by state (HIGH_IMPACT, MEDIUM_IMPACT, LOW_IMPACT, UNKNOWN)
- Search by customer ID
- Pagination with "Load more" button
- Default filter shows HIGH_IMPACT + MEDIUM_IMPACT states
- Displays: Customer ID, State, Drivers, Computed At

### Customer Detail Page

- Primary decision summary with metrics:
  - Order Counts (total, at risk, unknown)
  - At-risk order IDs
- Drilldown section listing at-risk orders with links to order detail pages
- Evidence viewer showing customer impact evidence
- Decision drivers and confidence

## API Integration

The frontend calls the FastAPI backend directly (no Next.js API routes). All API calls:

- Use the base URL from `NEXT_PUBLIC_OPSIQ_API_BASE_URL`
- Include `Authorization: Bearer <token>` header if `access_token` cookie is present (server-side) or available in localStorage (client-side)
- Use `cache: 'no-store'` to ensure fresh data

### API Endpoints Used

**Decision Packs:**
- `GET /v1/tenants/{tenantId}/decision-packs` - Fetch enabled packs for tenant
- `GET /v1/decision-packs/{packId}/{packVersion}` - Fetch pack definition

**Shopper Health:**
- `GET /v1/tenants/{tenantId}/worklists/shopper-health` - Fetch worklist
- `GET /v1/tenants/{tenantId}/subjects/shopper/{subjectId}/decision-bundle` - Fetch decision bundle
- `GET /v1/tenants/{tenantId}/subjects/shopper/{subjectId}/decision-history` - Fetch decision history

**Order Line Fulfillment:**
- `GET /v1/tenants/{tenantId}/worklists/order-line-fulfillment` - Fetch worklist
- `GET /v1/tenants/{tenantId}/subjects/order_line/{subjectId}/decision-bundle` - Fetch decision bundle

**Orders at Risk:**
- `GET /v1/tenants/{tenantId}/worklists/orders-at-risk` - Fetch worklist
- `GET /v1/tenants/{tenantId}/subjects/order/{orderId}/decision-bundle` - Fetch decision bundle

**Customers Impacted:**
- `GET /v1/tenants/{tenantId}/worklists/customers-impacted` - Fetch worklist
- `GET /v1/tenants/{tenantId}/subjects/customer/{customerId}/decision-bundle` - Fetch decision bundle

## Development

### Build

```bash
npm run build
```

### Start Production Server

```bash
npm start
```

### Lint

```bash
npm run lint
```

## Notes

- Server components are used for initial data fetching
- Client components are used for interactive features (filters, pagination)
- All API requests bypass Next.js API routes and call the backend directly
- Authentication tokens are read from cookies (server) or localStorage (client)
- Navigation is pack-aware and dynamically loads based on tenant configuration
- Decision states support both shopper health (URGENT, WATCHLIST, HEALTHY, UNKNOWN) and order fulfillment (AT_RISK, NOT_AT_RISK, UNKNOWN) types

