# OpsIQ Shopper Health Frontend

Next.js frontend application for viewing and managing Shopper Health Classification decisions.

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

- `/[tenantId]/shopper-health` - Worklist page displaying latest shopper health decisions
- `/[tenantId]/shopper-health/[subjectId]` - Detail page showing decision bundle with evidence

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
│   │   └── shopper-health/
│   │       ├── page.tsx          # Worklist page
│   │       ├── WorklistClient.tsx # Client component for filters/table
│   │       └── [subjectId]/
│   │           └── page.tsx      # Detail page
│   ├── globals.css               # Global styles
│   └── layout.tsx                # Root layout
├── components/
│   ├── ui/                       # ShadCN UI components
│   ├── DecisionStateBadge.tsx    # Badge component for decision states
│   └── EvidenceViewer.tsx        # Evidence display component
├── lib/
│   ├── api/
│   │   └── decisions.ts          # API client functions
│   └── utils.ts                  # Utility functions
├── types/
│   └── decisions.ts              # TypeScript type definitions
└── package.json
```

## Features

### Worklist Page

- Filter decisions by state (URGENT, WATCHLIST, HEALTHY, UNKNOWN)
- Search by subject ID
- Pagination with "Load more" button
- Default filter shows URGENT + WATCHLIST states
- Displays: Subject ID, State, Drivers, Confidence, Computed At

### Detail Page

- Composite decision summary
- Tabs for different evidence types:
  - Composite Evidence
  - Operational Risk (v1)
  - Frequency Trend (v2)
- Evidence viewer with formatted and raw JSON views
- Decision metrics and drivers

## API Integration

The frontend calls the FastAPI backend directly (no Next.js API routes). All API calls:

- Use the base URL from `NEXT_PUBLIC_OPSIQ_API_BASE_URL`
- Include `Authorization: Bearer <token>` header if `access_token` cookie is present (server-side) or available in localStorage (client-side)
- Use `cache: 'no-store'` to ensure fresh data

### API Endpoints Used

- `GET /v1/tenants/{tenantId}/worklists/shopper-health` - Fetch worklist
- `GET /v1/tenants/{tenantId}/subjects/shopper/{subjectId}/decision-bundle` - Fetch decision bundle

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

