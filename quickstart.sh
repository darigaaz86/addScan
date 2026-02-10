#!/bin/bash
set -e

echo "=== Address Scanner Quick Start ==="

# Check prerequisites
command -v docker >/dev/null 2>&1 || { echo "Error: docker is required"; exit 1; }
command -v go >/dev/null 2>&1 || { echo "Error: go is required"; exit 1; }

# Step 1: Check environment
echo ""
echo "Step 1: Checking environment..."
if [ ! -f .env ]; then
    echo "Error: .env file not found"
    exit 1
fi
echo ".env found"

# Step 2: Start infrastructure services
echo ""
echo "Step 2: Starting infrastructure services..."
docker compose down -v 2>/dev/null || true
docker compose up -d postgres clickhouse redis
echo "Waiting for services to be ready..."
sleep 5

# Step 3: Run migrations
echo ""
echo "Step 3: Running migrations..."
go build -o migrate ./cmd/migrate
./migrate -db postgres -action up
./migrate -db clickhouse -action up
echo "Migrations complete"

# Step 4: Build and start application services
echo ""
echo "Step 4: Building and starting application services..."
docker compose build --no-cache
docker compose up -d

# Step 5: Verify services
echo ""
echo "Step 5: Verifying services..."
sleep 10
curl -s http://localhost:8080/health || echo "Warning: health check failed"
echo ""
docker compose ps

echo ""
echo "=== Setup Complete ==="
echo ""
echo "API Server: http://localhost:8080"
echo "ClickHouse UI: http://localhost:8124"
echo "Adminer: http://localhost:5050"

# Step 6: Create user and portfolio
echo ""
echo "Step 6: Creating user and portfolio..."

USER_RESPONSE=$(curl -s -X POST http://localhost:8080/api/users \
  -H "Content-Type: application/json" \
  -d '{"email": "test@example.com", "tier": "paid"}')

USER_ID=$(echo "$USER_RESPONSE" | grep -o '"id":"[^"]*"' | cut -d'"' -f4)

if [ -z "$USER_ID" ]; then
    echo "Error: Failed to create user"
    echo "$USER_RESPONSE"
    exit 1
fi
echo "Created user: $USER_ID"

# Read addresses from file
ADDRESSES=$(cat data/user_addresses.txt | tr '\n' ',' | sed 's/,$//' | sed 's/,/","/g')

PORTFOLIO_RESPONSE=$(curl -s -X POST http://localhost:8080/api/portfolios \
  -H "Content-Type: application/json" \
  -H "X-User-ID: $USER_ID" \
  -d "{\"name\": \"My Portfolio\", \"addresses\": [\"$ADDRESSES\"]}")

PORTFOLIO_ID=$(echo "$PORTFOLIO_RESPONSE" | grep -o '"id":"[^"]*"' | cut -d'"' -f4)

if [ -z "$PORTFOLIO_ID" ]; then
    echo "Error: Failed to create portfolio"
    echo "$PORTFOLIO_RESPONSE"
    exit 1
fi
echo "Created portfolio: $PORTFOLIO_ID with 54 addresses"

echo ""
echo "=== All Done ==="
echo "User ID: $USER_ID"
echo "Portfolio ID: $PORTFOLIO_ID"
echo ""
echo "Check backfill progress: docker compose logs -f backfill"
