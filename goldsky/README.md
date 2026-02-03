# Goldsky Pipeline Setup

Real-time blockchain transaction streaming using Goldsky Mirror.

## Overview

This pipeline streams traces (internal transactions) and logs for tracked addresses from multiple chains in real-time via webhooks to your local server.

**Supported Chains:**
- Ethereum
- Base
- BNB (BSC)

## Architecture

```
Blockchain → Goldsky Pipeline → Webhook → Your Server → ClickHouse
                  ↓
           Filter by addresses
           (traces + logs)
```

## Why Goldsky?

RPC polling (Alchemy) misses internal transactions. Goldsky captures:
- Contract-to-contract ETH transfers
- Internal calls within complex transactions
- All traces with from/to addresses

## Prerequisites

1. Goldsky account: https://app.goldsky.com
2. Goldsky CLI + Turbo extension installed
3. ngrok for local development

## Setup

### 1. Install Goldsky CLI

```bash
curl https://goldsky.com | sh
goldsky login
```

### 2. Install Turbo Extension

```bash
curl https://install-turbo.goldsky.com | sh
```

Or manually:
```bash
mkdir -p ~/.goldsky/bin
curl -sL https://install-turbo.goldsky.com/macos/turbo-latest -o ~/.goldsky/bin/turbo
chmod +x ~/.goldsky/bin/turbo
```

### 3. Start Local Services

```bash
docker-compose up -d
```

### 4. Expose Server via ngrok

```bash
ngrok http 8080
```

Copy the forwarding URL (e.g., `https://xxx.ngrok-free.app`)

### 5. Update Pipeline Config

Edit `goldsky/pipeline.yaml` and update the webhook URLs with your ngrok URL.

### 6. Deploy Pipeline

```bash
~/.goldsky/bin/turbo apply goldsky/pipeline.yaml
```

## Pipeline Commands

```bash
# Check status
~/.goldsky/bin/turbo get addr-scanner

# View logs
~/.goldsky/bin/turbo logs addr-scanner

# Inspect live data
~/.goldsky/bin/turbo inspect addr-scanner -n all_traces
~/.goldsky/bin/turbo inspect addr-scanner -n all_logs

# Pause pipeline
~/.goldsky/bin/turbo pause addr-scanner

# Resume pipeline
~/.goldsky/bin/turbo resume addr-scanner

# Delete pipeline
~/.goldsky/bin/turbo delete addr-scanner
```

## Data Flow

### Sources (per chain)
- `ethereum.raw_traces` / `base.raw_traces` / `bsc.raw_traces` - Internal transactions
- `ethereum.raw_logs` / `base.raw_logs` / `bsc.raw_logs` - Token events

### Transforms
- `eth_filtered_traces`, `base_filtered_traces`, `bsc_filtered_traces` - Traces matching tracked addresses
- `eth_filtered_logs`, `base_filtered_logs`, `bsc_filtered_logs` - Logs matching tracked addresses
- `all_traces` - Combined traces from all chains
- `all_logs` - Combined logs from all chains

### Sinks
- Webhook to `/goldsky/traces` and `/goldsky/logs` endpoints

## Database Schema

### goldsky_traces
```sql
CREATE TABLE goldsky_traces (
    id String,
    transaction_hash String,
    block_number UInt64,
    block_timestamp DateTime,
    from_address String,
    to_address String,
    value Decimal(38, 0),
    call_type String,
    gas_used UInt64,
    status UInt8,
    chain String DEFAULT 'ethereum',
    created_at DateTime
)
```

### goldsky_logs
```sql
CREATE TABLE goldsky_logs (
    id String,
    transaction_hash String,
    block_number UInt64,
    block_timestamp DateTime,
    contract_address String,
    event_signature String,
    from_address String,
    to_address String,
    topics String,
    data String,
    log_index UInt32,
    chain String DEFAULT 'ethereum',
    created_at DateTime
)
```

### Materialized Views
Data automatically flows to `unified_timeline` via:
- `goldsky_traces_mv` / `goldsky_traces_to_mv`
- `goldsky_logs_mv` / `goldsky_logs_to_mv`

## Managing Tracked Addresses

Currently addresses are hardcoded in `pipeline.yaml`. To update:

1. Edit the SQL WHERE clauses in `pipeline.yaml`
2. Re-apply: `~/.goldsky/bin/turbo apply goldsky/pipeline.yaml`

For dynamic address management (no redeploy), use Goldsky Dynamic Tables with a cloud Postgres.

## Querying Data

```bash
# Connect to ClickHouse
docker exec -it address-scanner-clickhouse clickhouse-client \
  --password clickhouse_dev_password -d address_scanner

# Count traces by chain
SELECT chain, count() FROM goldsky_traces GROUP BY chain;

# Recent traces for an address
SELECT * FROM goldsky_traces 
WHERE from_address = '0x...' OR to_address = '0x...'
ORDER BY block_number DESC LIMIT 10;

# Query unified timeline
SELECT * FROM unified_timeline 
WHERE address = '0x...' 
ORDER BY block_number DESC LIMIT 10;
```

## Cost Estimation (Free Tier)

- **750 worker-hours/month** - 1 small pipeline = ~730 hours ✓
- **1M records written/month** - Only filtered records count ✓

With 54 low-activity addresses across 3 chains, you should stay within free tier.

## Troubleshooting

### No data flowing
1. Check pipeline status: `~/.goldsky/bin/turbo get addr-scanner`
2. Check logs: `~/.goldsky/bin/turbo logs addr-scanner`
3. Verify ngrok is running and URL is correct
4. Check server logs: `docker-compose logs -f server`

### Webhook errors
- Ensure server is running and accessible via ngrok URL
- Check for 4xx/5xx errors in pipeline logs

### Missing internal transactions
- Traces only capture successful calls (`status = 1`)
- Some contract interactions may not emit traces
