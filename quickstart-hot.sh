#!/usr/bin/env zsh
set -euo pipefail

echo "=== Address Scanner Quick Start (Hot Wallets) ==="
echo "Chains: ethereum, base, bnb"
echo ""

# Well-known active addresses (high-volume hot wallets)
typeset -A HOT_ADDRS
HOT_ADDRS[ethereum]="0x28c6c06298d514db089934071355e5743bf21d60"  # Binance Hot Wallet
HOT_ADDRS[base]="0x3304e22ddaa22bcdc5fca2269b418046ae7b566a"      # Base Bridge
HOT_ADDRS[bnb]="0x3c783c21a0383057d128bae431894a5c19f9cf06"       # Binance Hot Wallet 3

# Check prerequisites
command -v docker >/dev/null 2>&1 || { echo "Error: docker is required"; exit 1; }
command -v go >/dev/null 2>&1 || { echo "Error: go is required"; exit 1; }

# Step 1: Check environment
echo "Step 1: Checking environment..."
if [ ! -f .env ]; then
    echo "Error: .env file not found. Copy .env.example and fill in your keys."
    exit 1
fi
echo "  .env found"

# Step 2: Start infrastructure
echo ""
echo "Step 2: Starting infrastructure services..."
docker compose down -v 2>/dev/null || true
docker compose up -d postgres clickhouse redis
echo "  Waiting for services to be ready..."
sleep 5

# Step 3: Run migrations
echo ""
echo "Step 3: Running migrations..."
go build -o migrate ./cmd/migrate
./migrate -db postgres -action up
./migrate -db clickhouse -action up
echo "  Migrations complete"

# Step 4: Build and start application services
echo ""
echo "Step 4: Building and starting application services..."
docker compose build --no-cache
docker compose up -d
sleep 10

# Step 5: Verify services
echo ""
echo "Step 5: Verifying services..."
if curl -sf http://localhost:8080/health > /dev/null 2>&1; then
    echo "  API Server: OK"
else
    echo "  WARNING: health check failed, services may still be starting"
fi
docker compose ps

# Step 6: Create user and portfolio
echo ""
echo "Step 6: Creating free tier user..."

USER_RESPONSE=$(curl -s -X POST http://localhost:8080/api/users \
  -H "Content-Type: application/json" \
  -d '{"email": "hotwallets@test.local", "tier": "free"}')

USER_ID=$(echo "$USER_RESPONSE" | grep -o '"id":"[^"]*"' | cut -d'"' -f4)

if [ -z "$USER_ID" ]; then
    # Maybe user already exists
    USER_ID=$(docker exec address-scanner-postgres \
        psql -U scanner -d address_scanner -tAc \
        "SELECT id FROM users WHERE email = 'hotwallets@test.local' LIMIT 1" 2>/dev/null)
    if [ -z "$USER_ID" ]; then
        echo "  Error: Failed to create user"
        echo "  Response: $USER_RESPONSE"
        exit 1
    fi
    echo "  Using existing user: $USER_ID"
else
    echo "  Created user: $USER_ID (free tier)"
fi

# Build address list from the 3 chains
ADDR_LIST=""
for chain addr in "${(@kv)HOT_ADDRS}"; do
    echo "  ${chain} -> ${addr}"
    if [ -n "$ADDR_LIST" ]; then
        ADDR_LIST="${ADDR_LIST},\"${addr}\""
    else
        ADDR_LIST="\"${addr}\""
    fi
done

echo ""
echo "  Creating portfolio..."
PORTFOLIO_RESPONSE=$(curl -s -X POST http://localhost:8080/api/portfolios \
  -H "Content-Type: application/json" \
  -H "X-User-ID: ${USER_ID}" \
  -d "{\"name\": \"Hot Wallets\", \"addresses\": [${ADDR_LIST}]}")

PORTFOLIO_ID=$(echo "$PORTFOLIO_RESPONSE" | grep -o '"id":"[^"]*"' | cut -d'"' -f4)

if [ -z "$PORTFOLIO_ID" ]; then
    echo "  WARN: Failed to create portfolio (addresses may already be tracked)"
    echo "  Response: $PORTFOLIO_RESPONSE"
else
    echo "  Created portfolio: $PORTFOLIO_ID"
fi

echo ""
echo "=== All Done ==="
echo "User ID:      $USER_ID"
echo "Portfolio ID:  ${PORTFOLIO_ID:-N/A}"
echo "Addresses:     3 (ethereum, base, bnb)"
echo ""
echo "Check backfill progress:  docker compose logs -f backfill"
echo "Check worker polling:     docker compose logs -f worker"
echo "Monitor live:             ./scripts/monitor-polling.sh"
