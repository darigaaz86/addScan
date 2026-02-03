# Transaction Sync Architecture

This document describes the transaction synchronization system.

## Overview

The system uses **two complementary approaches** for syncing blockchain transactions:

| Approach | Use Case | Data Coverage |
|----------|----------|---------------|
| **RPC Polling (Primary)** | Real-time sync | Native + token transfers |
| **Goldsky (Supplementary)** | Internal transactions | Traces + logs that RPC misses |

## Data Flow Summary

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         REAL-TIME SYNC                                       │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                    RPC Polling (Alchemy)                             │    │
│  │                    Every 15 seconds                                  │    │
│  │                                                                      │    │
│  │  • Native ETH/token transfers                                        │    │
│  │  • ERC20/721/1155 events                                             │    │
│  │  • Approve calls                                                     │    │
│  │  ❌ Misses internal transactions                                     │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                              │                                               │
│                              ▼                                               │
│                    transactions table                                        │
│                              │                                               │
│                              ▼                                               │
│                    unified_timeline_mv                                       │
│                              │                                               │
│                              ▼                                               │
│                    unified_timeline ◄────────────────────┐                   │
│                                                          │                   │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                    Goldsky Streaming                                 │    │
│  │                    Real-time webhooks                                │    │
│  │                                                                      │    │
│  │  • Internal transactions (traces)                                    │    │
│  │  • Contract-to-contract calls                                        │    │
│  │  • Token events (logs)                                               │    │
│  │  ✅ Catches what RPC misses                                          │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                              │                                               │
│                              ▼                                               │
│                    goldsky_traces / goldsky_logs                             │
│                              │                                               │
│                              ▼                                               │
│                    goldsky_*_mv ─────────────────────────┘                   │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## RPC Polling (Primary Real-Time)

The sync worker polls Alchemy RPC endpoints every 15 seconds to detect new transactions.

### Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    Sync Worker                                           │
│                                                                          │
│  pollLoop() ──► PollChain() ──► ProcessBlock()                          │
│      │              │                │                                   │
│  (every 15s)   eth_blockNumber   eth_getBlockByNumber                   │
│                     │            eth_getLogs                             │
│                     ▼                │                                   │
│              ┌───────────┐           ▼                                   │
│              │  Alchemy  │    Match tracked addresses                    │
│              │    RPC    │           │                                   │
│              └───────────┘           ▼                                   │
│                              Store in ClickHouse                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### What RPC Polling Captures

| Transaction Type | Captured |
|------------------|----------|
| Native ETH transfers | ✅ |
| Token transfers (ERC20/721/1155) | ✅ |
| approve() calls | ✅ |
| Failed transactions | ✅ |
| **Internal transactions** | ❌ |
| **Contract-to-contract calls** | ❌ |

### Configuration

```bash
# Enable RPC polling (in .env)
ENABLED_CHAINS=ethereum,polygon,base

# Poll interval
ETHEREUM_POLL_INTERVAL=15s

# Rate limiting
ALCHEMY_CU_PER_SECOND=500
```

### Running the Worker

```bash
# Start worker container
docker-compose up -d worker

# Or run directly
go run cmd/worker/main.go
```

---

## Goldsky Streaming (Supplementary)

Goldsky Mirror streams Ethereum data in real-time via webhooks, capturing **internal transactions** that RPC polling misses.

### Architecture

```
Ethereum Blockchain
        │
        ▼
┌─────────────────────────────────────────────────────────────┐
│                    Goldsky Pipeline                          │
│                                                              │
│  Sources:                                                    │
│  ├─ ethereum.raw_traces (internal transactions)             │
│  ├─ base.raw_traces                                         │
│  ├─ bsc.raw_traces                                          │
│  └─ *.raw_logs (token events)                               │
│                                                              │
│  Transforms:                                                 │
│  ├─ filtered_traces (by tracked addresses)                  │
│  └─ filtered_logs (by tracked addresses)                    │
│                                                              │
│  Sinks:                                                      │
│  └─ Webhook → Your Server                                   │
└─────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────┐
│                    Your Server                               │
│                                                              │
│  POST /goldsky/traces → GoldskyRepository.InsertTraces()    │
│  POST /goldsky/logs   → GoldskyRepository.InsertLogs()      │
│                                                              │
└─────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────┐
│                    ClickHouse                                │
│                                                              │
│  goldsky_traces ──► goldsky_traces_mv ──┐                   │
│                                          ├──► unified_timeline
│  goldsky_logs ────► goldsky_logs_mv ────┘                   │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

### What Goldsky Captures

| Transaction Type | Goldsky | RPC Polling |
|------------------|---------|-------------|
| Native ETH transfers | ✅ traces | ✅ |
| Token transfers (ERC20/721/1155) | ✅ logs | ✅ |
| **Internal transactions** | ✅ traces | ❌ |
| **Contract-to-contract calls** | ✅ traces | ❌ |
| approve() calls | ✅ logs | ✅ |
| Failed transactions | ✅ (status=0) | ✅ |

### Supported Chains

- Ethereum
- Base
- BNB (BSC)

### Setup

See [goldsky/README.md](../goldsky/README.md) for complete setup instructions.

### Managing Addresses

Currently addresses are hardcoded in `goldsky/pipeline.yaml`. To update:

```bash
# Edit pipeline.yaml with new addresses
vim goldsky/pipeline.yaml

# Re-apply pipeline
~/.goldsky/bin/turbo apply goldsky/pipeline.yaml
```

For dynamic address management without redeploy, use Goldsky Dynamic Tables (requires cloud Postgres).

---

## Comparison

| Feature | RPC Polling | Goldsky |
|---------|-------------|---------|
| Internal transactions | ❌ | ✅ |
| Real-time latency | ~15s | ~5-10s |
| RPC costs | High (Alchemy CU) | None |
| Setup complexity | Low | Medium |
| Multi-chain | Any EVM | Ethereum, Base, BNB |
| Free tier | Alchemy limits | 750 worker-hours/mo |

## Recommendation

**Use both together:**
- **RPC polling** for all chains - reliable, simple
- **Goldsky** for Ethereum/Base/BNB - catches internal transactions

---

## Database Schema

Both approaches write to the same `unified_timeline` table:

```sql
SELECT * FROM unified_timeline 
WHERE address = '0x...' 
ORDER BY block_number DESC 
LIMIT 10;
```

Goldsky data is also available in raw form:

```sql
-- Internal transactions
SELECT * FROM goldsky_traces WHERE from_address = '0x...' LIMIT 10;

-- Token events  
SELECT * FROM goldsky_logs WHERE from_address = '0x...' LIMIT 10;
```
