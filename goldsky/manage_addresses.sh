#!/bin/bash
# Helper script to manage tracked addresses in Goldsky pipeline
# Usage: ./manage_addresses.sh <command> [address]

POSTGRES_URL="${GOLDSKY_POSTGRES_URL:-postgresql://localhost:5432/goldsky}"

case "$1" in
  add)
    if [ -z "$2" ]; then
      echo "Usage: $0 add <address>"
      exit 1
    fi
    ADDRESS=$(echo "$2" | tr '[:upper:]' '[:lower:]')
    psql "$POSTGRES_URL" -c "INSERT INTO streamling.tracked_addresses (contract_address) VALUES ('$ADDRESS') ON CONFLICT DO NOTHING;"
    echo "Added: $ADDRESS"
    ;;
    
  remove)
    if [ -z "$2" ]; then
      echo "Usage: $0 remove <address>"
      exit 1
    fi
    ADDRESS=$(echo "$2" | tr '[:upper:]' '[:lower:]')
    psql "$POSTGRES_URL" -c "DELETE FROM streamling.tracked_addresses WHERE contract_address = '$ADDRESS';"
    echo "Removed: $ADDRESS"
    ;;
    
  list)
    psql "$POSTGRES_URL" -c "SELECT contract_address FROM streamling.tracked_addresses ORDER BY contract_address;"
    ;;
    
  count)
    psql "$POSTGRES_URL" -c "SELECT COUNT(*) as total FROM streamling.tracked_addresses;"
    ;;
    
  sync)
    # Sync addresses from data/user_addresses.txt
    if [ ! -f "data/user_addresses.txt" ]; then
      echo "Error: data/user_addresses.txt not found"
      exit 1
    fi
    
    echo "Syncing addresses from data/user_addresses.txt..."
    while IFS= read -r addr; do
      if [ -n "$addr" ]; then
        ADDRESS=$(echo "$addr" | tr '[:upper:]' '[:lower:]')
        psql "$POSTGRES_URL" -c "INSERT INTO streamling.tracked_addresses (contract_address) VALUES ('$ADDRESS') ON CONFLICT DO NOTHING;" > /dev/null 2>&1
      fi
    done < "data/user_addresses.txt"
    echo "Sync complete!"
    $0 count
    ;;
    
  *)
    echo "Goldsky Address Manager"
    echo ""
    echo "Usage: $0 <command> [address]"
    echo ""
    echo "Commands:"
    echo "  add <address>    - Add a new address to track"
    echo "  remove <address> - Remove an address from tracking"
    echo "  list             - List all tracked addresses"
    echo "  count            - Count total tracked addresses"
    echo "  sync             - Sync addresses from data/user_addresses.txt"
    echo ""
    echo "Environment:"
    echo "  GOLDSKY_POSTGRES_URL - PostgreSQL connection string"
    ;;
esac
