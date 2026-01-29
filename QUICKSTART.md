# Address Scanner - Quick Start Guide

Get the Address Scanner running locally in 5 minutes.

## Prerequisites

- Docker & Docker Compose
- Go 1.21+
- Etherscan API Key (free at https://etherscan.io/apis)
- Alchemy API Key (free at https://www.alchemy.com/)

## Setup

### 1. Configure Environment

```bash
cp .env.example .env
```

Edit `.env` with your API keys:

```bash
# Required for transaction history
ETHERSCAN_API_KEY=your_etherscan_key

# Required for blockchain RPC
ETHEREUM_RPC_PRIMARY=https://eth-mainnet.g.alchemy.com/v2/your_alchemy_key
```

Etherscan provides complete transaction data (gas, methodId, funcName).
Alchemy provides real-time blockchain access and fallback for backfill.

### 2. Start Infrastructure Services Only

Start only the database services first (without app services):

```bash
# Clean start (removes all data)
docker compose down -v

# Start only infrastructure (postgres, clickhouse, redis)
docker compose up -d postgres clickhouse redis
```

### 3. Run Migrations

**Important:** Run migrations BEFORE starting the application services to avoid startup errors.

```bash
# Build migrate tool
go build -o migrate ./cmd/migrate

# Run Postgres migrations
./migrate -db postgres -action up

# Run ClickHouse migrations
./migrate -db clickhouse -action up
```

### 4. Start Application Services

Now start the server, worker, and backfill services:

```bash
docker compose up -d
```

### 5. Verify Services

```bash
# Check health
curl http://localhost:8080/health

# Check all services are running
docker compose ps
```

## API Usage

### Create a User

```bash
curl -X POST http://localhost:8080/api/users \
  -H "Content-Type: application/json" \
  -d '{"email": "test@example.com", "tier": "paid"}'
```

Response:
```json
{
  "id": "88376bb2-26a2-442d-898e-10702512b618",
  "email": "test@example.com",
  "tier": "paid",
  "createdAt": "2026-01-27T10:00:41Z",
  "updatedAt": "2026-01-27T10:00:41Z"
}
```

Tiers: `free` (1000 tx limit) or `paid` (unlimited)

### Create a Portfolio

```bash
curl -X POST http://localhost:8080/api/portfolios \
  -H "Content-Type: application/json" \
  -H "X-User-ID: <user-id>" \
  -d '{
    "name": "My Portfolio",
    "addresses": ["0xcfcdF79251b5862607eb138e494445639Fd73790"]
  }'
```

This automatically:
1. Adds the address to the tracking system
2. Creates a backfill job based on user tier
3. Worker starts fetching historical transactions

### Get Portfolio

```bash
curl http://localhost:8080/api/portfolios/<portfolio-id> \
  -H "X-User-ID: <user-id>"
```

### Get Address Transactions

```bash
curl http://localhost:8080/api/addresses/<address>/transactions \
  -H "X-User-ID: <user-id>"
```

## Service Ports

| Service | Port |
|---------|------|
| API Server | 8080 |
| Postgres | 5432 |
| ClickHouse | 9000, 8123 |
| Redis | 6379 |
| Adminer | 5050 |
| ClickHouse UI | 8124 |

## Logs

```bash
# All services
docker compose logs -f

# Specific service
docker compose logs -f server
docker compose logs -f worker
docker compose logs -f backfill
```

## Troubleshooting

### Backfill not working
- Check `ETHERSCAN_API_KEY` is set
- Check backfill logs: `docker compose logs -f backfill`

### No transactions returned
- Wait for backfill to complete (check logs)
- Verify address has transactions on Etherscan

### Connection refused
- Ensure all services are healthy: `docker compose ps`
- Check migrations ran successfully

## Next Steps

- See [README.md](README.md) for full documentation
- See [docs/api-swagger.yaml](docs/api-swagger.yaml) for API specification
