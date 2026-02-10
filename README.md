# Address Scanner

A multi-chain blockchain address aggregation service that provides real-time and historical transaction data for Ethereum and EVM-compatible chains.

## Overview

Address Scanner tracks blockchain addresses across multiple chains, fetches historical transactions, and provides real-time updates. It supports portfolio management for aggregating data across multiple addresses.

### Supported Chains

- Ethereum
- Base
- BNB (BSC)

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              API Server (:8080)                              │
│  REST API for users, portfolios, addresses, transactions                     │
│  + Goldsky webhook endpoints (/goldsky/traces, /goldsky/logs)                │
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
│  │  Alchemy RPC polling   │          │  Etherscan/Dune/Alchemy │           │
│  └────────────────────────┘          └────────────────────────┘           │
│                                                                            │
└────────────────────────────────────────────────────────────────────────────┘
         │                                         │
         ▼                                         ▼
  ┌──────────────┐                    ┌────────────────────────┐
  │   Alchemy    │                    │  Etherscan (most chains) │
  │   RPC API    │                    │  Dune Sim (BNB chain)    │
  └──────────────┘                    └────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                    Goldsky (Internal Transactions)                           │
│  Streams traces + logs for ETH/Base/BNB → webhook → ClickHouse              │
│  Catches contract-to-contract calls that RPC polling misses                  │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Components

| Component | Description |
|-----------|-------------|
| **API Server** | REST API handling user requests, portfolio management, transaction queries |
| **Sync Worker** | Real-time blockchain monitoring via Alchemy RPC, polls new blocks every 15 seconds |
| **Backfill Worker** | Fetches historical transactions (Etherscan for most chains, Dune for BNB) |
| **Goldsky** | Streams internal transactions (traces) via webhook for ETH/Base/BNB |
| **Postgres** | Stores metadata: users, portfolios, addresses, backfill jobs, protocols, tokens |
| **ClickHouse** | Stores all transactions and balance snapshots, optimized for time-series queries |
| **Redis** | Caches recent transactions (1000 per address, configurable TTL) |

## Data Flow

### Adding an Address

1. User creates portfolio with addresses via `POST /api/portfolios`
2. Address validated and stored in Postgres
3. Backfill job queued for each chain
4. Backfill worker fetches history:
   - **Etherscan** (primary for most chains): Complete data with gas, methodId, funcName
   - **Dune Sim** (primary for BNB): Etherscan free tier doesn't support BNB
   - **Alchemy** (fallback): If primary sources fail
5. Transactions stored in ClickHouse
6. Address marked as backfill complete

### Real-Time Sync

Two complementary approaches:

**1. RPC Polling (Alchemy)**
- Sync worker polls each chain every 15 seconds
- Scans new blocks for tracked addresses
- Detects: Native transfers, ERC20/721/1155 tokens
- Stores transactions in ClickHouse
- Updates Redis cache

**2. Goldsky Streaming (Internal Transactions)**
- Streams traces and logs for ETH/Base/BNB
- Catches internal transactions RPC polling misses
- Webhook receives data → stores in ClickHouse
- Materialized views merge into unified_timeline

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
| `protocols` | DeFi protocol registry (Aave, Uniswap, etc.) |
| `tokens` | Token metadata registry |
| `protocol_contracts` | Protocol contract addresses |
| `token_patterns` | Token symbol patterns for DeFi detection |
| `token_whitelist` | Whitelisted tokens |

### ClickHouse Tables

| Table | Purpose |
|-------|---------|
| `transactions` | All transactions from RPC/Etherscan, partitioned by month |
| `goldsky_traces` | Internal transactions from Goldsky |
| `goldsky_logs` | Token events from Goldsky |
| `unified_timeline` | Merged view of all transaction sources |
| `balance_snapshots` | Daily balance snapshots for historical tracking |

Transaction fields:
- `hash`, `chain`, `address`, `from`, `to`, `value`
- `timestamp`, `block_number`, `status`
- `gas_used`, `gas_price`, `l1_fee` (for L2 chains)
- `token_transfers` (JSON array)
- `method_id`, `func_name`

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
- `GET /api/addresses/:address/balance` - Get balance (legacy)
- `DELETE /api/addresses/:address` - Remove tracking

### Address Balances (DeBank-aligned)
- `GET /api/addresses/:address/balances` - Get token balances
- `GET /api/addresses/:address/balances/all` - Get balances across all chains
- `GET /api/addresses/:address/protocols` - Get DeFi protocol positions
- `GET /api/addresses/:address/history` - Get balance history
- `GET /api/addresses/:address/defi` - Get DeFi activity summary
- `GET /api/addresses/:address/defi/interactions` - Get detailed DeFi interactions

### Portfolios
- `POST /api/portfolios` - Create portfolio
- `GET /api/portfolios/:id` - Get portfolio
- `PUT /api/portfolios/:id` - Update portfolio
- `DELETE /api/portfolios/:id` - Delete portfolio
- `GET /api/portfolios/:id/statistics` - Get statistics
- `GET /api/portfolios/:id/balances` - Get aggregated balances
- `GET /api/portfolios/:id/protocols` - Get protocol positions
- `GET /api/portfolios/:id/holdings` - Get top holdings
- `GET /api/portfolios/:id/history` - Get balance history

### Search
- `GET /api/search/transaction/:hash` - Search by transaction hash

### Goldsky Webhooks
- `POST /goldsky/traces` - Receive internal transaction traces
- `POST /goldsky/logs` - Receive token event logs

## Quick Start

See [QUICKSTART.md](QUICKSTART.md) for setup instructions.

## Documentation

Detailed implementation documentation:

- [Backfill System](docs/BACKFILL.md) - Historical transaction fetching
- [Sync Worker](docs/SYNC_WORKER.md) - Real-time blockchain monitoring (RPC + Goldsky)
- [Query Service](docs/QUERY_SERVICE.md) - Transaction queries and caching
- [Portfolio Service](docs/PORTFOLIO_SERVICE.md) - Portfolio aggregation
- [Rate Limiting](docs/RATE_LIMITING.md) - CU budget management
- [Goldsky Setup](goldsky/README.md) - Internal transaction streaming
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
ENABLED_CHAINS=ethereum,base,bnb
ETHEREUM_RPC_PRIMARY=https://eth-mainnet.g.alchemy.com/v2/YOUR_KEY
BASE_RPC_PRIMARY=https://base-mainnet.g.alchemy.com/v2/YOUR_KEY
BNB_RPC_PRIMARY=https://bnb-mainnet.g.alchemy.com/v2/YOUR_KEY

# Etherscan (for complete transaction history - most chains)
ETHERSCAN_API_KEY=YOUR_ETHERSCAN_KEY

# Dune Sim (for BNB chain - Etherscan free tier doesn't support BNB)
DUNE_API_KEY=YOUR_DUNE_KEY

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
│   ├── migrate/     # Database migration tool
│   └── snapshot/    # Daily balance snapshot worker
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

### Run Balance Integration Test

After backfill completes, verify balance accuracy against on-chain data:

```bash
go test -v ./internal/storage -run TestBalanceIntegration -tags=integration
```

This checks all 54 addresses for native and ERC20 token balance accuracy across all enabled chains.

### Run with Docker

```bash
docker compose up -d
```

## License

MIT
