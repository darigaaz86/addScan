# Address Scanner

A multi-chain blockchain address aggregation service that provides real-time and historical transaction data for Ethereum and EVM-compatible chains.

## Overview

Address Scanner tracks blockchain addresses across multiple chains, fetches historical transactions, and provides real-time updates. It supports portfolio management for aggregating data across multiple addresses.

### Supported Chains

- Ethereum
- Polygon
- Arbitrum
- Optimism
- Base

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              API Server (:8080)                              │
│  REST API for users, portfolios, addresses, transactions                     │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                    ┌─────────────────┼─────────────────┐
                    ▼                 ▼                 ▼
            ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
            │   Postgres   │  │  ClickHouse  │  │    Redis     │
            │  (Metadata)  │  │(Transactions)│  │   (Cache)    │
            └──────────────┘  └──────────────┘  └──────────────┘
                    ▲                 ▲                 ▲
                    │                 │                 │
┌───────────────────┴─────────────────┴─────────────────┴───────────────────┐
│                                                                            │
│  ┌────────────────────────┐          ┌────────────────────────┐           │
│  │      Sync Worker       │          │    Backfill Worker     │           │
│  │  (Real-time updates)   │          │  (Historical data)     │           │
│  │  Polls every 15s       │          │  Etherscan → Alchemy   │           │
│  └────────────────────────┘          └────────────────────────┘           │
│                                                                            │
└────────────────────────────────────────────────────────────────────────────┘
                    │                              │
                    ▼                              ▼
            ┌──────────────┐              ┌──────────────┐
            │   Alchemy    │              │  Etherscan   │
            │   RPC API    │              │     API      │
            └──────────────┘              └──────────────┘
```

### Components

| Component | Description |
|-----------|-------------|
| **API Server** | REST API handling user requests, portfolio management, transaction queries |
| **Sync Worker** | Real-time blockchain monitoring, polls new blocks every 15 seconds |
| **Backfill Worker** | Fetches historical transactions for newly added addresses |
| **Postgres** | Stores metadata: users, portfolios, addresses, backfill jobs |
| **ClickHouse** | Stores all transactions, optimized for time-series queries |
| **Redis** | Caches recent transactions (1000 per address, configurable TTL) |

## Data Flow

### Adding an Address

1. User creates portfolio with addresses via `POST /api/portfolios`
2. Address validated and stored in Postgres
3. Backfill job queued for each chain
4. Backfill worker fetches history:
   - **Etherscan** (primary): Complete data with gas, methodId, funcName
   - **Alchemy** (fallback): If Etherscan fails
5. Transactions stored in ClickHouse
6. Address marked as backfill complete

### Real-Time Sync

1. Sync worker polls each chain every 15 seconds
2. Scans new blocks for tracked addresses
3. Detects:
   - Native transfers (ETH, MATIC, etc.)
   - ERC20 token transfers
   - ERC721 NFT transfers
   - ERC1155 multi-token transfers
4. Stores transactions in ClickHouse
5. Updates Redis cache

### Querying Transactions

1. API receives query with optional filters
2. Simple queries → Redis cache (fast)
3. Complex queries → ClickHouse (filtered)
4. Returns paginated results with metadata

## Database Schema

### Postgres Tables

| Table | Purpose |
|-------|---------|
| `users` | User accounts with tier (free/paid) |
| `portfolios` | Collections of addresses |
| `addresses` | Tracked addresses per chain |
| `backfill_jobs` | Historical sync job queue |
| `portfolio_snapshots` | Daily portfolio snapshots |

### ClickHouse Tables

| Table | Purpose |
|-------|---------|
| `transactions` | All transactions, partitioned by month |

Transaction fields:
- `hash`, `chain`, `address`, `from`, `to`, `value`
- `timestamp`, `block_number`, `status`
- `gas_used`, `gas_price`
- `token_transfers` (JSON array)
- `method_id`, `func_name`, `input`

## User Tiers

| Feature | Free | Paid |
|---------|------|------|
| Backfill Limit | 1,000 transactions | Unlimited |
| API Rate Limit | 1,000 req/day | 100,000 req/day |
| Sync Priority | Normal | High |

## API Endpoints

### Health
- `GET /health` - Service health check

### Users
- `POST /api/users` - Create user
- `GET /api/users/:id` - Get user

### Addresses
- `POST /api/addresses` - Add address for tracking
- `GET /api/addresses/:address` - Get address details
- `GET /api/addresses/:address/transactions` - Get transactions
- `GET /api/addresses/:address/balance` - Get balance
- `DELETE /api/addresses/:address` - Remove tracking

### Portfolios
- `POST /api/portfolios` - Create portfolio
- `GET /api/portfolios/:id` - Get portfolio with aggregated data
- `PUT /api/portfolios/:id` - Update portfolio
- `DELETE /api/portfolios/:id` - Delete portfolio
- `GET /api/portfolios/:id/statistics` - Get statistics
- `GET /api/portfolios/:id/snapshots` - Get daily snapshots

### Search
- `GET /api/search/transaction/:hash` - Search by transaction hash

## Quick Start

See [QUICKSTART.md](QUICKSTART.md) for setup instructions.

## Documentation

Detailed implementation documentation:

- [Backfill System](docs/BACKFILL.md) - Historical transaction fetching
- [Sync Worker](docs/SYNC_WORKER.md) - Real-time blockchain monitoring
- [Query Service](docs/QUERY_SERVICE.md) - Transaction queries and caching
- [Portfolio Service](docs/PORTFOLIO_SERVICE.md) - Portfolio aggregation
- [Rate Limiting](docs/RATE_LIMITING.md) - CU budget management
- [API Specification](docs/api-swagger.yaml) - OpenAPI 3.0 spec

## Configuration

### Environment Variables

```bash
# Server
SERVER_PORT=8080
SERVER_HOST=0.0.0.0

# Postgres
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=address_scanner
POSTGRES_USER=scanner
POSTGRES_PASSWORD=scanner_dev_password

# ClickHouse
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=9000
CLICKHOUSE_DB=address_scanner
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=clickhouse_dev_password

# Redis
REDIS_HOST=localhost
REDIS_PORT=6379

# Chains (Alchemy RPC URLs)
ENABLED_CHAINS=ethereum,polygon,arbitrum,optimism,base
ETHEREUM_RPC_PRIMARY=https://eth-mainnet.g.alchemy.com/v2/YOUR_KEY
POLYGON_RPC_PRIMARY=https://polygon-mainnet.g.alchemy.com/v2/YOUR_KEY

# Etherscan (for complete transaction history)
ETHERSCAN_API_KEY=YOUR_ETHERSCAN_KEY

# Rate Limiting (Alchemy CU budget)
ALCHEMY_CU_PER_SECOND=500
ALCHEMY_RESERVED_CU=300
ALCHEMY_SHARED_CU=200
```

## Rate Limiting

### API Rate Limiting
Per-user rate limits based on tier, enforced via token bucket algorithm.

### RPC Rate Limiting (CU Budget)
Coordinates Alchemy usage between sync worker and backfill:
- **Total Budget**: 500 CU/s
- **Reserved (Sync)**: 300 CU/s - Priority for real-time updates
- **Shared (Backfill)**: 200 CU/s - Best-effort historical fetching
- Backfill pauses when utilization exceeds 90%

## Project Structure

```
├── cmd/
│   ├── server/      # API server entry point
│   ├── worker/      # Sync worker entry point
│   ├── backfill/    # Backfill worker entry point
│   └── migrate/     # Database migration tool
├── internal/
│   ├── adapter/     # Blockchain adapters (Ethereum, Etherscan)
│   ├── api/         # HTTP handlers and middleware
│   ├── config/      # Configuration management
│   ├── job/         # Backfill job processing
│   ├── models/      # Data models
│   ├── ratelimit/   # CU budget tracking
│   ├── service/     # Business logic
│   ├── storage/     # Database repositories
│   ├── types/       # Common type definitions
│   └── worker/      # Sync worker implementation
├── migrations/
│   ├── postgres/    # Postgres migrations
│   └── clickhouse/  # ClickHouse migrations
└── docs/
    └── api-swagger.yaml  # OpenAPI specification
```

## Development

### Build

```bash
make build
```

### Run Tests

```bash
make test
```

### Run with Docker

```bash
docker compose up -d
```

## License

MIT
