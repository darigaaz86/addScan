# Sync Worker Implementation

This document describes the detailed implementation of the real-time blockchain synchronization system.

## Overview

The sync worker monitors blockchain networks in real-time, detecting new transactions for tracked addresses. It polls each chain every 15 seconds and stores discovered transactions in ClickHouse while updating the Redis cache.

## Design Decisions

### Why Poll-Based Instead of WebSocket Subscriptions

We use polling (every 15 seconds) instead of WebSocket subscriptions because:

| Approach | Pros | Cons |
|----------|------|------|
| **Polling (our choice)** | Simple, reliable, works with all providers, easy to resume after restart | Slight delay (up to 15s) |
| WebSocket subscriptions | Real-time updates | Connection management complexity, reconnection handling, not all providers support it |

For a portfolio tracker, 15-second delay is acceptable and the simplicity/reliability tradeoff is worth it.

### Block-by-Block Scanning Strategy

For real-time sync, we scan each new block to find transactions involving tracked addresses:

```
Poll cycle:
  1. Get current block number (eth_blockNumber)
  2. Calculate blocks behind, apply batch limit (max 10 per cycle)
  3. For each block from lastProcessed+1 to targetBlock:
     a. Fetch full block with transactions (eth_getBlockByNumber)
     b. Scan all transactions for native transfers
     c. Fetch logs for token transfers (eth_getLogs)
     d. Match against tracked addresses
     e. Store matched transactions
  4. Save progress to Postgres
  5. If still behind, continue catching up in next poll cycle
```

This is different from backfill (which queries by address) because:
- We need to detect NEW transactions as they happen
- Block scanning catches ALL transaction types including approve
- We only scan new blocks (not entire history)

### Batch Limit for Catch-Up

To prevent long-running poll cycles when the worker falls behind (e.g., after restart or downtime), we limit processing per poll cycle.

**Configuration:**

```bash
# Environment variable (default: 30)
SYNC_MAX_BLOCKS_PER_POLL=30

# For Alchemy paid tier, increase for faster catch-up:
SYNC_MAX_BLOCKS_PER_POLL=100
```

**Why 30 blocks (default)?**

Based on Alchemy free tier limits:
- Free tier: **500 CU/s**
- Poll interval: 15 seconds → **7,500 CU available per cycle**
- Reserve ~30% for backfill → **~5,000 CU for sync**
- CU per block: ~100 CU (eth_getBlockByNumber: 20 + eth_getLogs: 60 + overhead)
- Safe limit: 5,000 ÷ 100 = 50, use **30 for safety margin**

**Recommended values by Alchemy tier:**

| Alchemy Tier | CU/s | Recommended `SYNC_MAX_BLOCKS_PER_POLL` |
|--------------|------|----------------------------------------|
| Free | 500 | 30 (default) |
| Growth | 1,000+ | 60-100 |
| Scale | 3,000+ | 150-200 |

**Ethereum Catch-Up Rate (at default 30 blocks):**

- Block time: ~12 seconds
- New blocks per poll cycle: 15s ÷ 12s = ~1.25 blocks
- Net catch-up per cycle: 30 - 1.25 = **~28.75 blocks**
- Catch-up rate: **~115 blocks/minute** or **~6,900 blocks/hour**

| Blocks Behind | Catch-Up Time |
|---------------|---------------|
| 100 | ~1 minute |
| 1,000 | ~9 minutes |
| 10,000 | ~1.5 hours |
| 50,000 (~1 week) | ~7 hours |

This ensures:
- Poll cycles complete quickly (~3-5 seconds)
- Stay within Alchemy CU limits
- Backfill worker still has CU budget available
- Progress is saved incrementally (crash-safe)

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           Sync Worker                                    │
│                                                                          │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │                    Per-Chain Worker Loop                         │    │
│  │                                                                  │    │
│  │   Start() ──► pollLoop() ──► PollChain() ──► ProcessBlock()     │    │
│  │                   │              │                │              │    │
│  │              (every 15s)   GetCurrentBlock()  FetchTransactions │    │
│  │                   │              │                │              │    │
│  │                   ▼              ▼                ▼              │    │
│  │              ┌─────────┐  ┌───────────┐   ┌─────────────┐       │    │
│  │              │ Ticker  │  │  Alchemy  │   │ UpdateAddrs │       │    │
│  │              └─────────┘  │    RPC    │   └─────────────┘       │    │
│  │                          └───────────┘          │               │    │
│  │                                                 ▼               │    │
│  │                                    ┌────────────────────┐       │    │
│  │                                    │  BatchInsert()     │       │    │
│  │                                    │  (ClickHouse)      │       │    │
│  │                                    └────────────────────┘       │    │
│  │                                                 │               │    │
│  │                                                 ▼               │    │
│  │                                    ┌────────────────────┐       │    │
│  │                                    │  UpdateCache()     │       │    │
│  │                                    │  (Redis)           │       │    │
│  │                                    └────────────────────┘       │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│                                                                          │
│  One worker instance per enabled chain (ethereum, polygon, etc.)         │
└─────────────────────────────────────────────────────────────────────────┘
```

## Components

### 1. SyncWorker (Core)

Location: `internal/worker/sync_worker.go`

Each chain has its own SyncWorker instance running independently.

```go
type SyncWorker struct {
    chain              types.ChainID
    chainAdapter       adapter.ChainAdapter
    txRepo             *storage.TransactionRepository
    syncStatusRepo     *storage.SyncStatusRepository
    addressRepo        *storage.AddressRepository
    cache              *storage.RedisCache
    pollInterval       time.Duration        // Default: 15s
    lastBlockProcessed uint64
    running            bool
    tierProvider       *TierAwareAddressProvider
    rateLimitTracker   *ratelimit.CUBudgetTracker
}
```

### 2. SyncWorkerConfig

```go
type SyncWorkerConfig struct {
    Chain             types.ChainID
    ChainAdapter      adapter.ChainAdapter
    TxRepo            *storage.TransactionRepository
    SyncStatusRepo    *storage.SyncStatusRepository
    AddressRepo       *storage.AddressRepository
    PortfolioRepo     *storage.PortfolioRepository
    UserRepo          *storage.UserRepository
    Cache             *storage.RedisCache
    PollInterval      time.Duration         // 10-30 seconds
    UseTierPriority   bool                  // Process paid tier first
    RateLimitTracker  *ratelimit.CUBudgetTracker
    RateLimitRegistry *ratelimit.CUCostRegistry
}
```

### 3. TierAwareAddressProvider

Location: `internal/worker/priority_queue.go`

Provides addresses sorted by user tier (paid users processed first).

```go
type TierAwareAddressProvider struct {
    addressRepo     *storage.AddressRepository
    portfolioRepo   *storage.PortfolioRepository
    userRepo        *storage.UserRepository
    priorityQueue   *PriorityQueue
}

// Returns addresses split by tier
func (tap *TierAwareAddressProvider) GetAddressesByTier(ctx, chain) (paid, free []string, err error)
```

### 4. PriorityQueue

Manages address priority based on user tier.

```go
type AddressPriority struct {
    Address  string
    Tier     types.UserTier
    Priority int  // Paid: 100, Free: 10
}
```

## Data Flow

### 1. Worker Startup

```
main() (cmd/worker/main.go)
    │
    ▼
Load configuration from .env
    │
    ▼
Connect to Postgres, ClickHouse, Redis
    │
    ▼
Initialize rate limiter (optional)
    │
    ▼
Create chain adapters for enabled chains
    │
    ▼
For each chain:
    │
    ├─► Create SyncWorker with config
    │
    └─► syncWorker.Start(ctx)
            │
            ▼
        Load last processed block from Postgres
        (or start from current block if none)
            │
            ▼
        Start pollLoop() goroutine
```

### 2. Poll Loop

```
pollLoop() (runs in goroutine)
    │
    ▼
ticker := time.NewTicker(15 * time.Second)
    │
    ▼
for {
    select {
    case <-ctx.Done():
        return  // Shutdown
    case <-stopCh:
        return  // Stop signal
    case <-ticker.C:
        │
        ▼
        PollChain(ctx)
    }
}
```

### 3. Block Processing

```
PollChain(ctx)
    │
    ▼
currentBlock := chainAdapter.GetCurrentBlock()
    │
    ▼
currentBlock > lastBlockProcessed? ──No──► return 0
    │
   Yes
    │
    ▼
for blockNum := lastBlock+1; blockNum <= currentBlock; blockNum++ {
    │
    ▼
    ProcessBlock(ctx, blockNum)
        │
        ▼
    getTrackedAddresses()
        │
        ├─► If tier priority enabled:
        │       TierAwareAddressProvider.GetAddressesByTier()
        │       Returns: paid addresses first, then free
        │
        └─► Else: addressRepo.List()
        │
        ▼
    chainAdapter.FetchTransactionsForBlock(blockNum, addresses)
        │
        ▼
    UpdateAddresses(ctx, transactions)
        │
        ├─► txRepo.BatchInsert() → ClickHouse (async)
        │
        └─► cache.UpdateTransactions() → Redis
}
    │
    ▼
saveLastProcessedBlock(currentBlock) → Postgres
```

### 4. Transaction Detection

The chain adapter scans blocks for:

| Type | Detection Method |
|------|------------------|
| Native transfers | `eth_getBlockByNumber` - check `from`/`to` |
| ERC20 transfers | `eth_getLogs` - Transfer event topic |
| ERC721 transfers | `eth_getLogs` - Transfer event topic |
| ERC1155 transfers | `eth_getLogs` - TransferSingle/TransferBatch topics |

## Block Scanning Implementation

Location: `internal/adapter/ethereum_adapter.go` - `FetchTransactionsForBlock()`

### Step 1: Build Address Lookup Map

```go
// O(1) lookup for tracked addresses
addressMap := make(map[string]bool)
for _, addr := range addresses {
    addressMap[strings.ToLower(addr)] = true
}
```

### Step 2: Fetch Block with Full Transactions

```go
// RPC: eth_getBlockByNumber (with full tx objects)
block, err := client.BlockByNumber(ctx, blockBig)
```

This returns the block header AND all transaction objects in the block.

### Step 3: Scan Native Transfers

For each transaction in the block:

```go
for _, tx := range block.Transactions() {
    // Check 'to' address
    if tx.To() != nil {
        toAddr := strings.ToLower(tx.To().Hex())
        if addressMap[toAddr] {
            matchedTxMap[tx.Hash()] = true  // Match!
        }
    }

    // Check 'from' address (derive sender)
    sender, _ := ethtypes.Sender(signer, tx)
    fromAddr := strings.ToLower(sender.Hex())
    if addressMap[fromAddr] {
        matchedTxMap[tx.Hash()] = true  // Match!
    }
}
```

This catches:
- ETH transfers TO tracked addresses
- ETH transfers FROM tracked addresses
- Contract calls FROM tracked addresses (including approve!)

### Step 4: Fetch Token Transfer Logs

```go
// Event signatures we look for
transferEventSig := "0xddf252ad..." // ERC20/721 Transfer(from, to, value)
transferSingleSig := "0xc3d58168..." // ERC1155 TransferSingle
transferBatchSig := "0x4a39dc06..."  // ERC1155 TransferBatch

// RPC: eth_getLogs for this block
filterQuery := ethereum.FilterQuery{
    FromBlock: blockBig,
    ToBlock:   blockBig,
    Topics: [][]common.Hash{
        {transferEventSig, transferSingleSig, transferBatchSig},
    },
}
logs, _ := client.FilterLogs(ctx, filterQuery)
```

### Step 5: Match Token Transfers

```go
for _, logEntry := range logs {
    // ERC20/721: topics[1] = from, topics[2] = to
    if logEntry.Topics[0] == transferEventSig {
        from := common.BytesToAddress(logEntry.Topics[1].Bytes()).Hex()
        to := common.BytesToAddress(logEntry.Topics[2].Bytes()).Hex()
        
        if addressMap[from] || addressMap[to] {
            matchedTxMap[logEntry.TxHash] = true  // Match!
        }
    }
    
    // ERC1155: topics[2] = from, topics[3] = to
    if logEntry.Topics[0] == transferSingleSig {
        from := common.BytesToAddress(logEntry.Topics[2].Bytes()).Hex()
        to := common.BytesToAddress(logEntry.Topics[3].Bytes()).Hex()
        
        if addressMap[from] || addressMap[to] {
            matchedTxMap[logEntry.TxHash] = true  // Match!
        }
    }
}
```

### Step 6: Fetch Full Transaction Details

For each matched transaction hash:

```go
for txHash := range matchedTxMap {
    // Get full transaction
    tx, _, _ := client.TransactionByHash(ctx, txHash)
    
    // Get receipt (for status, gas used, logs)
    receipt, _ := client.TransactionReceipt(ctx, txHash)
    
    // Normalize with all data
    normalized := NormalizeTransactionWithReceipt(tx, receipt, block)
    transactions = append(transactions, normalized)
}
```

### RPC Calls Per Block

| Operation | RPC Method | CU Cost | Count |
|-----------|------------|---------|-------|
| Get block | `eth_getBlockByNumber` | 16 | 1 |
| Get logs | `eth_getLogs` | 75 | 1 |
| Get tx by hash | `eth_getTransactionByHash` | 17 | N (matched) |
| Get receipt | `eth_getTransactionReceipt` | 15 | N (matched) |

For a block with 0 matches: ~91 CU
For a block with 5 matches: ~251 CU

### Logging

```
[Adapter:ethereum] RPC Call: BlockByNumber for block 19234568 (with full txs)
[Adapter:ethereum] Block 19234568: scanned 150 txs + 423 logs, found 2 native + 1 token matches
```

## Why Block Scanning Catches Everything

Unlike Alchemy's `getAssetTransfers` API (used in backfill fallback), block scanning catches ALL transactions:

| Transaction Type | Block Scanning | Alchemy getAssetTransfers |
|------------------|----------------|---------------------------|
| ETH transfers | ✅ (from/to check) | ✅ |
| Token transfers | ✅ (Transfer logs) | ✅ |
| **approve()** | ✅ (from check) | ❌ No asset movement |
| **setApprovalForAll()** | ✅ (from check) | ❌ No asset movement |
| **Contract calls (no transfer)** | ✅ (from check) | ❌ No asset movement |
| Failed transactions | ✅ (receipt status) | ❌ Not returned |

The key insight: We check the `from` field of EVERY transaction in the block, not just transfers. Any transaction sent BY a tracked address is captured, regardless of what it does.

```go
// This catches approve, setApprovalForAll, any contract call
sender, _ := ethtypes.Sender(signer, tx)
if addressMap[strings.ToLower(sender.Hex())] {
    matchedTxMap[tx.Hash()] = true  // Caught!
}
```

## Tier-Based Priority

**Important:** Unlike backfill, the sync worker processes ALL transactions for ALL addresses regardless of tier. The tier only affects **processing order**, not data completeness.

When `UseTierPriority` is enabled:

1. Addresses are fetched and sorted by user tier
2. Paid tier addresses are processed first in each block
3. Free tier addresses are processed after

```go
// Priority values (for ordering only)
Paid tier:  100  // Processed first
Free tier:  10   // Processed second
```

This ensures paid users get slightly faster updates when there are many addresses to track, but **all addresses receive the same data**.

| Aspect | Free Tier | Paid Tier |
|--------|-----------|-----------|
| Real-time sync | ✅ Full | ✅ Full |
| All transaction types | ✅ Yes | ✅ Yes |
| Processing order | Second | First |
| **Backfill limit** | 1,000 txs | Unlimited |

The only tier difference that affects data is in **backfill** (historical transactions), not sync.

## Rate Limiting Integration

The sync worker uses `PriorityHigh` for rate limiting:

```go
// Real-time sync gets reserved budget (300 CU/s by default)
rateLimitedClient, _ := ratelimit.NewRateLimitedClient(&ratelimit.RateLimitedClientConfig{
    Client:       underlyingClient,
    Tracker:      rateLimitTracker,
    CostRegistry: rateLimitRegistry,
    Priority:     ratelimit.PriorityHigh,  // Reserved budget
})
```

This guarantees sync worker has priority access to RPC calls over backfill.

## Progress Persistence

Worker progress is saved to Postgres to survive restarts:

```sql
CREATE TABLE worker_progress (
    chain VARCHAR(20) PRIMARY KEY,
    last_processed_block BIGINT NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);
```

On startup:
1. Load `last_processed_block` from `worker_progress`
2. If not found, start from current block
3. Resume processing from saved block

## Cache Updates

After processing transactions, the cache is updated:

```go
// Update Redis cache for each affected address
cache.UpdateTransactions(ctx, address, chain, transactions)
```

Cache configuration:
- Max 1000 transactions per address
- TTL: Configurable via `CACHE_TTL` (default: 20s, slightly longer than poll interval)

## Error Handling

### Block Processing Errors

```go
for blockNum := lastBlock + 1; blockNum <= currentBlock; blockNum++ {
    result, err := w.ProcessBlock(ctx, blockNum)
    if err != nil {
        log.Printf("Failed to process block %d: %v", blockNum, err)
        continue  // Skip to next block, don't fail entire poll
    }
}
```

### Transaction Storage Errors

```go
// Store transactions asynchronously - don't block polling
go func() {
    if err := w.txRepo.BatchInsert(bgCtx, modelTransactions); err != nil {
        log.Printf("Failed to store transactions: %v", err)
    }
}()
```

### Cache Update Errors

```go
if err := w.cache.UpdateTransactions(ctx, address, chain, transactions); err != nil {
    log.Printf("Failed to update cache: %v", err)
    // Non-fatal: cache will be populated on next query
}
```

## Graceful Shutdown

```go
// Signal stop
close(w.stopCh)

// Wait for polling loop to finish
select {
case <-w.doneCh:
    // Stopped gracefully
case <-time.After(30 * time.Second):
    // Timeout
}
```

## Configuration

### Environment Variables

```bash
# Chain RPC endpoints (Alchemy)
ENABLED_CHAINS=ethereum,polygon,arbitrum,optimism,base
ETHEREUM_RPC_PRIMARY=https://eth-mainnet.g.alchemy.com/v2/YOUR_KEY
POLYGON_RPC_PRIMARY=https://polygon-mainnet.g.alchemy.com/v2/YOUR_KEY

# Poll intervals per chain (optional, default 15s)
ETHEREUM_POLL_INTERVAL=15s
POLYGON_POLL_INTERVAL=15s

# Rate limiting (optional)
ALCHEMY_CU_PER_SECOND=500
ALCHEMY_RESERVED_CU=300
```

### Poll Interval Constraints

```go
// Must be between 10-30 seconds
if pollInterval < 10*time.Second || pollInterval > 30*time.Second {
    return nil, fmt.Errorf("poll interval must be between 10 and 30 seconds")
}
```

## Monitoring

### Worker Status

```go
type SyncWorkerStatus struct {
    Chain               types.ChainID
    Running             bool
    LastPollTime        time.Time
    LastBlockProcessed  uint64
    CurrentBlock        uint64
    AddressesTracked    int
    PollIntervalSeconds int
}

// Get status
status := syncWorker.GetStatus()
```

### Logs

```
[SyncWorker] Starting sync worker for chain ethereum with poll interval 15s
[SyncWorker] Chain ethereum initialized at current block 19234567
[SyncWorker] Chain ethereum: processing block 19234568
[SyncWorker] Chain ethereum: block 19234568 - found 3 transactions for 2 addresses
[SyncWorker] Chain ethereum: saving progress at block 19234568
[SyncWorker] Chain ethereum: progress saved successfully
```

### Database Query

```sql
-- Check worker progress
SELECT chain, last_processed_block, updated_at
FROM worker_progress
ORDER BY chain;

-- Check tracked addresses per chain
SELECT chain, COUNT(*) as address_count
FROM addresses
GROUP BY chain;
```

## Performance Considerations

### Block Processing

- Each block is processed sequentially
- Transactions are stored asynchronously (non-blocking)
- Cache updates are synchronous but fast

### Address Scanning

- All tracked addresses are scanned per block
- Tier priority reduces latency for paid users
- Address list is refreshed each poll cycle

### RPC Calls per Poll

| Operation | RPC Method | CU Cost |
|-----------|------------|---------|
| Get current block | `eth_blockNumber` | 10 |
| Get block | `eth_getBlockByNumber` | 16 |
| Get logs | `eth_getLogs` | 75 |

Typical poll: ~100 CU per chain per poll cycle

## Entry Point

Location: `cmd/worker/main.go`

```go
func main() {
    // 1. Load config
    // 2. Connect databases
    // 3. Initialize rate limiter
    // 4. Create chain adapters
    // 5. Create and start sync workers
    // 6. Wait for shutdown signal
    // 7. Stop workers gracefully
}
```
