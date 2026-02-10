# Backfill Implementation

This document describes the detailed implementation of the historical transaction backfill system.

## Overview

The backfill system fetches historical transactions for newly tracked addresses. It uses a multi-source strategy:
1. **Etherscan API** (primary for most chains) - Complete transaction data with gas, methodId, funcName
2. **Dune Sim API** (primary for BNB) - Etherscan free tier doesn't support BNB chain
3. **Alchemy RPC** (fallback) - Used when primary sources fail or are not configured

## Design Decisions

### Why Etherscan as Primary Source

We use Etherscan API as the primary data source because Alchemy's `alchemy_getAssetTransfers` RPC has limitations:

| Transaction Type | Etherscan | Alchemy getAssetTransfers |
|------------------|-----------|---------------------------|
| Normal transfers | ✅ | ✅ |
| Internal transactions | ✅ | ✅ |
| ERC20 transfers | ✅ | ✅ |
| ERC721 transfers | ✅ | ✅ |
| ERC1155 transfers | ✅ | ✅ |
| **Approve transactions** | ✅ | ❌ Missing |
| **Contract calls (no transfer)** | ✅ | ❌ Missing |
| Gas used/price | ✅ | ❌ Not included |
| Method ID | ✅ | ❌ Not included |
| Function name | ✅ | ❌ Not included |

Alchemy's `getAssetTransfers` only returns transactions that involve asset movement. Transactions like `approve()`, `setApprovalForAll()`, or contract interactions without transfers are not captured.

### BNB Chain: Dune Sim API

For BNB chain, we use **Dune Sim API** instead of Etherscan because:
- Etherscan free tier does not support BNB chain
- Dune Sim provides similar activity data via their `/v1/evm/activity/{address}` endpoint

| Data Source | Supported Chains |
|-------------|------------------|
| Etherscan | Ethereum, Polygon, Arbitrum, Optimism, Base |
| Dune Sim | BNB |
| Alchemy (fallback) | All chains |

### Address-Based Fetching Strategy

We fetch historical data **per address** rather than scanning the entire blockchain history:

```
Traditional approach (NOT used):
  Scan block 0 → block N, filter for tracked addresses
  Problem: Extremely slow, high RPC costs, unnecessary data

Our approach:
  For each tracked address:
    Query Etherscan: "Give me all transactions for address 0x..."
    Store results in ClickHouse
  
  Benefits:
    - Minimal RPC/API calls
    - Only fetches relevant data
    - Fast backfill completion
    - Lower Alchemy CU consumption
```

This strategy keeps the system efficient and cost-effective, especially important when tracking many addresses across multiple chains.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           API Server                                     │
│  POST /api/addresses → AddressService.AddAddress()                       │
│                              │                                           │
│                              ▼                                           │
│                    BackfillJobService.Execute()                          │
│                              │                                           │
│                              ▼                                           │
│                    Creates BackfillJobRecord in Postgres                 │
│                    (one job per chain, status: "queued")                 │
└─────────────────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                        Backfill Worker                                   │
│                                                                          │
│  processLoop() ─────► BackfillService.ProcessNextJob()                   │
│       │                        │                                         │
│       │                        ▼                                         │
│       │               GetQueuedJobs(limit=1)                             │
│       │                        │                                         │
│       │                        ▼                                         │
│       │               processBackfill(job)                               │
│       │                        │                                         │
│       │                        ▼                                         │
│       │               fetchHistoricalTransactions()                      │
│       │                   │           │                                  │
│       │                   ▼           ▼                                  │
│       │            Etherscan    OR   Alchemy                             │
│       │                   │           │                                  │
│       │                   └─────┬─────┘                                  │
│       │                         ▼                                        │
│       │               txRepo.BatchInsert() → ClickHouse                  │
│       │                         │                                        │
│       │                         ▼                                        │
│       │               Update job status: "completed"                     │
│       │                                                                  │
│  (every 5s) ◄───────────────────┘                                        │
└─────────────────────────────────────────────────────────────────────────┘
```

## Components

### 1. BackfillJobRecord (Model)

```go
// internal/models/backfill_job.go
type BackfillJobRecord struct {
    JobID               string         // UUID
    Address             string         // Blockchain address (0x...)
    Chain               types.ChainID  // ethereum, polygon, etc.
    Tier                types.UserTier // free or paid
    Status              string         // queued, in_progress, completed, failed
    Priority            int            // Higher = processed first
    TransactionsFetched int64          // Count of fetched transactions
    StartedAt           time.Time
    CompletedAt         *time.Time
    Error               *string        // Error message if failed
    RetryCount          int
}
```

### 2. BackfillJobRepository (Storage)

Location: `internal/storage/backfill_job_repository.go`

Methods:
- `Create(job)` - Insert new job
- `GetByID(jobID)` - Get job by ID
- `Update(job)` - Update job record
- `GetQueuedJobs(limit)` - Get queued jobs ordered by priority
- `ListByAddress(address)` - Get all jobs for an address
- `ListByStatus(status, limit)` - Get jobs by status

### 3. BackfillJobService (API Server)

Location: `internal/job/backfill_job.go`

This service runs in the API server and handles job creation when addresses are added.

```go
// Interface
type BackfillJob interface {
    Execute(ctx, input) (*BackfillResult, error)
    GetProgress(ctx, jobID) (*BackfillProgress, error)
    Cancel(ctx, jobID) (*CancelBackfillResult, error)
}
```

**Execute Flow:**
1. Create one `BackfillJobRecord` per chain
2. Store in Postgres with status "queued"
3. Start async goroutine to execute backfill
4. Return immediately with job IDs

**Async Execution:**
1. Update status to "in_progress"
2. Determine transaction limit based on tier:
   - Free: 1,000 transactions
   - Paid: Unlimited (-1)
3. Fetch transactions via `fetchTransactionsForChain()`
4. Store in ClickHouse via `txRepo.BatchInsert()`
5. Update sync status in addresses table
6. Mark job as "completed"

### 4. BackfillService (Backfill Worker)

Location: `internal/service/backfill_service.go`

This service runs in the dedicated backfill worker process.

**ProcessNextJob Flow:**
1. Get next queued job from Postgres
2. Update status to "in_progress"
3. Call `processBackfill(job)`
4. Update status to "completed" or "failed"

**Rate Limiting Integration:**
- Checks `rateController.ShouldPause()` before fetching
- Waits for CU budget via `rateController.WaitForBudget()`
- Records success/failure for exponential backoff

### 5. EtherscanClient (Data Source)

Location: `internal/adapter/etherscan_client.go`

Fetches ALL transaction types from Etherscan API v2.

**Transaction Types Fetched:**

| Type | API Action | Description |
|------|------------|-------------|
| Normal | `txlist` | External transactions (ETH transfers, contract calls) |
| Internal | `txlistinternal` | Contract-to-contract calls |
| ERC20 | `tokentx` | Token transfers |
| ERC721 | `tokennfttx` | NFT transfers |
| ERC1155 | `token1155tx` | Multi-token transfers |

**FetchAllTransactions Flow:**
```go
func (c *EtherscanClient) FetchAllTransactions(ctx, address, chain) ([]*NormalizedTransaction, error) {
    // 1. Fetch normal transactions
    normalTxs := c.fetchTransactionList(ctx, address, chainID, "txlist")
    
    // 2. Fetch internal transactions
    internalTxs := c.fetchTransactionList(ctx, address, chainID, "txlistinternal")
    
    // 3. Fetch ERC20 token transfers
    erc20Txs := c.fetchTokenTransfers(ctx, address, chainID)
    
    // 4. Fetch ERC721 NFT transfers
    erc721Txs := c.fetchNFTTransfers(ctx, address, chainID)
    
    // 5. Fetch ERC1155 multi-token transfers
    erc1155Txs := c.fetchERC1155Transfers(ctx, address, chainID)
    
    // Combine all - NO deduplication (each transfer is separate record)
    return append(normalTxs, internalTxs, erc20Txs, erc721Txs, erc1155Txs...), nil
}
```

**Chain ID Mapping:**
```go
func (c *EtherscanClient) GetChainID(chain types.ChainID) int {
    switch chain {
    case types.ChainEthereum:  return 1
    case types.ChainPolygon:   return 137
    case types.ChainArbitrum:  return 42161
    case types.ChainOptimism:  return 10
    case types.ChainBase:      return 8453
    }
}
```

**Data Enrichment:**

Etherscan provides fields not available from Alchemy:
- `gasUsed` - Actual gas consumed
- `gasPrice` - Gas price in wei
- `methodId` - First 4 bytes of input (e.g., `0xa9059cbb`)
- `functionName` - Decoded function name (e.g., `transfer(address,uint256)`)
- `isError` / `txreceipt_status` - Transaction success/failure

### 6. Backfill Worker Entry Point

Location: `cmd/backfill/main.go`

**Initialization:**
1. Load configuration from `.env`
2. Connect to Postgres, ClickHouse, Redis
3. Initialize rate limiter (optional)
4. Create chain adapters for enabled chains
5. Create `BackfillService` with Etherscan client
6. Start processing loop

**Processing Loop:**
```go
func processLoop(ctx context.Context, backfillService *service.BackfillService) {
    ticker := time.NewTicker(5 * time.Second)
    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            backfillService.ProcessNextJob(ctx)
        }
    }
}
```

## Data Flow

### 1. Job Creation (API Server)

```
User Request
    │
    ▼
POST /api/addresses
    │
    ▼
AddressService.AddAddress()
    │
    ▼
BackfillJobService.Execute()
    │
    ├─► Create BackfillJobRecord (chain: ethereum, status: queued)
    ├─► Create BackfillJobRecord (chain: polygon, status: queued)
    └─► ... (one per enabled chain)
    │
    ▼
Return { jobId, status: "queued" }
```

### 2. Job Processing (Backfill Worker)

```
processLoop (every 5s)
    │
    ▼
GetQueuedJobs(limit=1)
    │
    ▼
Found job? ──No──► Sleep 5s
    │
   Yes
    │
    ▼
Update status: "in_progress"
    │
    ▼
fetchHistoricalTransactions()
    │
    ├─► BNB chain? ──Yes──► Dune Sim API
    │                           │
    │                           ▼
    │                       FetchAllTransactions()
    │                           │
    │                           ▼
    │                       Return transactions
    │
    └─► Other chains? ──Yes──► Try Etherscan first
            │
            ▼
        FetchAllTransactions()
            │
            ├─► txlist (normal)
            ├─► txlistinternal (internal)
            ├─► tokentx (ERC20)
            ├─► tokennfttx (ERC721)
            └─► token1155tx (ERC1155)
            │
            ▼
        Combine all transactions
            │
            ▼
        If NOTOK response → Retry forever with backoff
    │
    └─► If Etherscan fails, fallback to Alchemy
            │
            ▼
        chainAdapter.FetchTransactionHistory()
    │
    ▼
txRepo.BatchInsert() → ClickHouse
    │
    ▼
Update status: "completed"
```

## Tier-Based Limits

| Tier | Transaction Limit | Priority |
|------|-------------------|----------|
| Free | 1,000 | 5 |
| Paid | Unlimited | 10 |

The limit is applied AFTER fetching:
```go
if limit > 0 && int64(len(transactions)) > limit {
    return transactions[:limit], nil
}
```

## Rate Limiting

The backfill service integrates with the CU budget tracker:

```go
// Check if should pause (>90% utilization)
if s.rateController.ShouldPause(ctx) {
    s.rateController.WaitForBudget(ctx, 150) // Wait for 150 CU
}

// After Alchemy call
if err != nil {
    s.rateController.RecordFailure() // Trigger backoff
} else {
    s.rateController.RecordSuccess() // Reset backoff
}
```

## Error Handling

**Retry Logic (Alchemy fallback):**
```go
const maxRetries = 5
const baseDelay = 1 * time.Second

for attempt := 0; attempt <= maxRetries; attempt++ {
    txs, err := chainAdapter.FetchTransactionHistory(ctx, address, maxCount)
    if err == nil {
        return txs, nil
    }
    
    // Exponential backoff: 1s, 2s, 4s, 8s, 16s (max 60s)
    delay := baseDelay * time.Duration(1<<uint(attempt))
    if delay > 60*time.Second {
        delay = 60 * time.Second
    }
    time.Sleep(delay)
}
```

**Job Failure:**
- Status set to "failed"
- Error message stored in `error` column
- `retry_count` incremented
- Job can be manually retried

## Database Schema

### Postgres: backfill_jobs

```sql
CREATE TABLE backfill_jobs (
    job_id VARCHAR(36) PRIMARY KEY,
    address VARCHAR(42) NOT NULL,
    chain VARCHAR(20) NOT NULL,
    tier VARCHAR(10) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'queued',
    priority INTEGER NOT NULL DEFAULT 0,
    transactions_fetched BIGINT NOT NULL DEFAULT 0,
    started_at TIMESTAMP NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMP,
    error TEXT,
    retry_count INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX idx_backfill_jobs_status ON backfill_jobs(status);
CREATE INDEX idx_backfill_jobs_address ON backfill_jobs(address);
CREATE INDEX idx_backfill_jobs_priority ON backfill_jobs(priority DESC);
```

### ClickHouse: transactions

See `migrations/clickhouse/001_create_transactions_table.sql`

## Configuration

### Environment Variables

```bash
# Etherscan API (required for complete data on most chains)
ETHERSCAN_API_KEY=your_key

# Dune Sim API (required for BNB chain)
DUNE_API_KEY=your_key

# Chain RPC endpoints (Alchemy)
ETHEREUM_RPC_PRIMARY=https://eth-mainnet.g.alchemy.com/v2/your_key
POLYGON_RPC_PRIMARY=https://polygon-mainnet.g.alchemy.com/v2/your_key

# Rate limiting (optional)
ALCHEMY_CU_PER_SECOND=500
ALCHEMY_RESERVED_CU=300
ALCHEMY_SHARED_CU=200
```

## Monitoring

### Logs

```
[BackfillJob] Fetching from Etherscan for 0x... (limit: 1000)
[Etherscan] Fetched 523 total transactions for 0x... (normal: 100, internal: 23, erc20: 350, erc721: 50, erc1155: 0)
[BackfillJob] Stored 523 transactions for address 0x... on chain ethereum
Backfill job abc-123 completed: fetched 523 transactions (tier: free)
```

### Job Status Query

```sql
SELECT job_id, address, chain, status, transactions_fetched, error
FROM backfill_jobs
WHERE address = '0x...'
ORDER BY started_at DESC;
```


## Auto Re-queue Strategy

The backfill system has two levels of retry handling:

### 1. Etherscan NOTOK Retry (Retry Forever)

When Etherscan returns a `NOTOK` response (common on Base chain's flaky free tier), the system retries **forever** with exponential backoff:

```go
// Retry forever since we know it will eventually succeed
baseDelay := 1 * time.Second
maxDelay := 30 * time.Second

for {
    // ... make request ...
    
    // NOTOK with "Free API access" - retry forever for flaky free tier
    if rawResp.Message == "NOTOK" {
        attempt++
        delay := baseDelay * time.Duration(1<<uint(min(attempt, 5))) // Cap at 32s
        if delay > maxDelay {
            delay = maxDelay
        }
        log.Printf("[Etherscan] NOTOK (attempt %d), retrying in %v", attempt, delay)
        time.Sleep(delay)
        continue
    }
}
```

This handles:
- Base chain's flaky free tier access
- Temporary Etherscan API issues
- "Free API access" rate limiting

### 2. Job-Level Auto Re-queue (Background Process)

For jobs that fail at the job level (not Etherscan NOTOK), a background process re-queues them:

| Parameter | Value | Description |
|-----------|-------|-------------|
| Cooldown period | 30 minutes | Wait time after failure before retry |
| Max auto-retries | 3 | Maximum automatic retry attempts |
| Check interval | 15 minutes | How often to check for retriable jobs |

### Eligible Errors (Transient)

Jobs with these error patterns are eligible for auto re-queue:
- `429` - Rate limit exceeded
- `timeout` - Request timeout
- `rate limit` - Rate limiting
- `connection` - Network issues
- `temporary` - Temporary failures

### Permanent Errors (No Auto-Retry)

Jobs with these errors require manual intervention:
- `not found` - Resource doesn't exist
- `invalid address` - Malformed address
- `unsupported` - Unsupported chain/feature
- `unauthorized` - API key issues

### Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                    Auto Re-queue Check (every 15 min)           │
│                                                                 │
│  SELECT failed jobs WHERE:                                      │
│    - retry_count < 3                                            │
│    - completed_at < NOW() - 30 minutes                          │
│    - error LIKE '%429%' OR '%timeout%' OR '%rate limit%'...     │
│                                                                 │
│  For each eligible job:                                         │
│    - SET status = 'queued'                                      │
│    - INCREMENT retry_count                                      │
│    - CLEAR completed_at                                         │
│                                                                 │
│  Jobs with retry_count >= 3 OR permanent errors:                │
│    - Stay as 'failed'                                           │
│    - Require manual retry via API                               │
└─────────────────────────────────────────────────────────────────┘
```

### Manual Retry API

For jobs that exceed auto-retry limits or have permanent errors:

```bash
# Re-queue all failed jobs for a specific chain
curl -X POST http://localhost:8080/api/admin/backfill/requeue?chain=bnb

# Re-queue a specific job
curl -X POST http://localhost:8080/api/admin/backfill/requeue/{jobId}

# Get job statistics
curl http://localhost:8080/api/admin/backfill/stats
```

### Cleanup

Old completed and failed jobs are automatically cleaned up:

| Job Status | Retention | Action |
|------------|-----------|--------|
| Completed | 7 days | Deleted |
| Failed (auto-retry exhausted) | 30 days | Deleted |

```sql
-- Manual cleanup query
DELETE FROM backfill_jobs 
WHERE status = 'completed' AND completed_at < NOW() - INTERVAL '7 days';

DELETE FROM backfill_jobs 
WHERE status = 'failed' AND completed_at < NOW() - INTERVAL '30 days';
```
