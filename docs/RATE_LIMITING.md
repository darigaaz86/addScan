# Rate Limiting Implementation

This document describes the detailed implementation of the CU (Compute Unit) budget rate limiting system for Alchemy RPC calls.

## Overview

The rate limiting system coordinates Alchemy API usage between the Sync Worker (real-time) and Backfill Worker (historical) using Redis for cross-service coordination. It implements a sliding window rate limiter with separate budget pools.

## Multi-Account RPC Pool

For free tier users, a single Alchemy account (500 CU/s) may not be sufficient. The RPC Pool allows using multiple free accounts with automatic failover on rate limiting.

### Configuration

```bash
# Single account (default)
ETHEREUM_RPC_PRIMARY=https://eth-mainnet.g.alchemy.com/v2/key1

# Multiple accounts (comma-separated)
ETHEREUM_RPC_URLS=https://eth-mainnet.g.alchemy.com/v2/key1,https://eth-mainnet.g.alchemy.com/v2/key2,https://eth-mainnet.g.alchemy.com/v2/key3
```

### Strategy: Sticky with Failover

```
Account 1 ──► use until 429 ──► Account 2 ──► use until 429 ──► Account 3
                                                                    │
              ◄── cooldown expires, try to return to Account 1 ─────┘
```

- Stick to current account until it returns HTTP 429 (rate limited)
- On 429, immediately switch to next account
- After cooldown period (default: 60s), try to return to primary account

### Usage

```go
// Create pool from comma-separated URLs
provider, err := adapter.NewPooledRPCProviderFromURLs(os.Getenv("ETHEREUM_RPC_URLS"))

// Or create manually
pool, err := adapter.NewRPCPool(&adapter.RPCPoolConfig{
    Endpoints: []string{
        "https://eth-mainnet.g.alchemy.com/v2/key1",
        "https://eth-mainnet.g.alchemy.com/v2/key2",
        "https://eth-mainnet.g.alchemy.com/v2/key3",
    },
    CooldownTime: 60 * time.Second,
})
provider := adapter.NewPooledRPCProvider(pool)

// Use with adapter
adapter, err := adapter.NewEthereumAdapter(chainID, provider)
```

### Capacity with Multiple Accounts

| Free Accounts | Effective CU/s | Monthly CU |
|---------------|----------------|------------|
| 1 | 500 | ~1.3B |
| 2 | 1,000 | ~2.6B |
| 3 | 1,500 | ~3.9B |
| 5 | 2,500 | ~6.5B |

With 3 free accounts + batched polling, you can comfortably run Ethereum + Base + BNB.

## Architecture

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         Rate Limiting System                             │
│                                                                          │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │                    CU Budget Tracker (Redis)                     │    │
│  │                                                                  │    │
│  │   Total Budget: 500 CU/s                                         │    │
│  │   ┌─────────────────────┬─────────────────────┐                 │    │
│  │   │   Reserved Pool     │    Shared Pool      │                 │    │
│  │   │   300 CU/s          │    200 CU/s         │                 │    │
│  │   │   (PriorityHigh)    │    (PriorityLow)    │                 │    │
│  │   └─────────────────────┴─────────────────────┘                 │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│                    │                         │                           │
│                    ▼                         ▼                           │
│  ┌─────────────────────────┐   ┌─────────────────────────┐              │
│  │      Sync Worker        │   │    Backfill Worker      │              │
│  │   (Real-time sync)      │   │   (Historical data)     │              │
│  │                         │   │                         │              │
│  │   Uses: Reserved Pool   │   │   Uses: Shared Pool     │              │
│  │   Priority: High        │   │   Priority: Low         │              │
│  │   Guaranteed budget     │   │   Best-effort           │              │
│  └─────────────────────────┘   └─────────────────────────┘              │
└─────────────────────────────────────────────────────────────────────────┘
```

## Components

### 1. CUBudgetTracker

Location: `internal/ratelimit/budget_tracker.go`

Central coordinator for CU consumption across services.

```go
type CUBudgetTracker struct {
    redis          redis.Cmdable
    totalBudget    int           // Default: 500 CU/s
    reservedBudget int           // Default: 300 CU/s
    sharedBudget   int           // Default: 200 CU/s
    windowSize     time.Duration // Default: 1 second
    keyTTL         time.Duration // Default: 2 seconds
}
```

### 2. BackfillRateController

Location: `internal/ratelimit/rate_controller.go`

Manages backfill request pacing with exponential backoff.

```go
type BackfillRateController struct {
    tracker          *CUBudgetTracker
    baseDelay        time.Duration // Default: 100ms
    maxDelay         time.Duration // Default: 10s
    currentDelay     time.Duration
    consecutiveFails int
}
```

### 3. CUCostRegistry

Location: `internal/ratelimit/cost_registry.go`

Maps RPC methods to their CU costs.

```go
// Common method costs
const (
    CostEthBlockNumber          = 10
    CostEthGetBlockByNumber     = 16
    CostEthGetLogs              = 75
    CostAlchemyGetAssetTransfers = 150
)
```

## Budget Allocation

### Priority Levels

| Priority | Pool | Budget | Use Case |
|----------|------|--------|----------|
| High | Reserved | 300 CU/s | Real-time sync |
| Low | Shared | 200 CU/s | Backfill operations |

### Budget Consumption Flow

```
TryConsume(cu, priority)
    │
    ▼
Get current window timestamp
    │
    ▼
Determine pool (reserved/shared)
    │
    ▼
Execute Lua script (atomic):
    ├─► Check total budget
    ├─► Check pool budget
    └─► Increment counters if allowed
    │
    ▼
Return (allowed, waitTime)
```

### Lua Script (Atomic Check-and-Increment)

```lua
-- Get current values
local totalUsed = redis.call('GET', totalKey) or '0'
local poolUsed = redis.call('GET', poolKey) or '0'

-- Check budgets
if totalUsed + cu > totalBudget then
    return {0, totalUsed, poolUsed}  -- Denied
end
if poolUsed + cu > poolBudget then
    return {0, totalUsed, poolUsed}  -- Denied
end

-- Atomically increment
redis.call('INCRBY', totalKey, cu)
redis.call('INCRBY', poolKey, cu)
return {1, totalUsed + cu, poolUsed + cu}  -- Allowed
```

## Sliding Window

The system uses a 1-second sliding window aligned to second boundaries:

```go
func (t *CUBudgetTracker) getWindowTimestamp() int64 {
    now := time.Now()
    windowStart := now.Truncate(t.windowSize)
    return windowStart.UnixMilli()
}
```

### Redis Keys

| Key Pattern | Purpose |
|-------------|---------|
| `cu:total:{timestamp}` | Total CU consumed in window |
| `cu:reserved:{timestamp}` | Reserved pool consumption |
| `cu:shared:{timestamp}` | Shared pool consumption |
| `cu:method:{method}:{timestamp}` | Per-method tracking |

## Backfill Rate Controller

### Exponential Backoff

When budget is exhausted, backfill uses exponential backoff:

```
Attempt 1: 100ms delay
Attempt 2: 200ms delay
Attempt 3: 400ms delay
Attempt 4: 800ms delay
...
Max: 10s delay
```

### WaitForBudget Flow

```go
func (c *BackfillRateController) WaitForBudget(ctx context.Context, requiredCU int) error {
    for {
        // Try to consume budget
        allowed, waitTime := c.tracker.TryConsume(ctx, requiredCU, PriorityLow)
        if allowed {
            c.RecordSuccess()  // Reset backoff
            return nil
        }

        // Budget not available
        c.RecordFailure()  // Increase backoff

        // Wait before retry
        delay := max(waitTime, c.currentDelay)
        select {
        case <-ctx.Done():
            return ErrContextCancelled
        case <-time.After(delay):
            // Retry
        }
    }
}
```

### Graceful Degradation

Backfill pauses when utilization exceeds 90%:

```go
func (c *BackfillRateController) ShouldPause(ctx context.Context) bool {
    utilization, _ := c.tracker.TotalUtilization(ctx)
    return utilization >= 90
}
```

## Thresholds

| Threshold | Percentage | Action |
|-----------|------------|--------|
| Warning | 80% | Log warning |
| Pause | 90% | Backfill pauses |
| Critical | 100% | All requests denied |

## Usage Statistics

```go
type CUUsageStats struct {
    TotalUsed      int       // CU consumed in current window
    ReservedUsed   int       // Reserved pool consumption
    SharedUsed     int       // Shared pool consumption
    TotalBudget    int       // Configured total budget
    ReservedBudget int       // Configured reserved budget
    SharedBudget   int       // Configured shared budget
    WindowStart    time.Time // Current window start
}

// Get current usage
stats, _ := tracker.GetUsage(ctx)
```

## Configuration

### Environment Variables

```bash
# Total CU budget per second
ALCHEMY_CU_PER_SECOND=500

# Reserved budget for real-time sync
ALCHEMY_RESERVED_CU=300

# Shared budget for backfill (calculated: total - reserved)
ALCHEMY_SHARED_CU=200

# Sliding window size in milliseconds
ALCHEMY_WINDOW_SIZE_MS=1000

# Warning threshold percentage
ALCHEMY_WARNING_THRESHOLD=80

# Pause threshold percentage
ALCHEMY_PAUSE_THRESHOLD=90

# Default CU cost for unknown methods
ALCHEMY_DEFAULT_METHOD_COST=20
```

### Configuration Validation

```go
func (c *RateLimitConfig) Validate() error {
    // TotalCUPerSecond must be positive
    // ReservedCU + SharedCU cannot exceed TotalCUPerSecond
    // WarningThreshold must be <= PauseThreshold
    // All thresholds must be 0-100
}
```

## Integration

### Sync Worker Integration

```go
// Create rate-limited client with high priority
rateLimitedClient, _ := ratelimit.NewRateLimitedClient(&ratelimit.RateLimitedClientConfig{
    Client:       underlyingClient,
    Tracker:      rateLimitTracker,
    CostRegistry: rateLimitRegistry,
    Priority:     ratelimit.PriorityHigh,  // Uses reserved pool
})

// Set on adapter
ethAdapter.SetRateLimiter(rateLimitedClient)
```

### Backfill Worker Integration

```go
// Create backfill rate controller
controller, _ := ratelimit.NewBackfillRateController(&ratelimit.BackfillRateControllerConfig{
    Tracker: tracker,
})

// Before making RPC call
if controller.ShouldPause(ctx) {
    log.Printf("Pausing due to high utilization")
    controller.WaitForBudget(ctx, 150)  // Wait for 150 CU
}

// Make RPC call
txs, err := chainAdapter.FetchTransactionHistory(ctx, address, limit)
if err != nil {
    controller.RecordFailure()  // Increase backoff
} else {
    controller.RecordSuccess()  // Reset backoff
}
```

## RPC Method Costs

| Method | CU Cost |
|--------|---------|
| `eth_blockNumber` | 10 |
| `eth_getBlockByNumber` | 16 |
| `eth_getLogs` | 75 |
| `eth_getTransactionByHash` | 15 |
| `eth_getTransactionReceipt` | 15 |
| `alchemy_getAssetTransfers` | 300 (150 per call × 2 calls for in/out) |

## Sync Worker CU Usage

### Per-Block Mode (default)
Each block requires multiple RPC calls:
- `eth_blockNumber`: 10 CU
- `eth_getBlockByNumber`: 16 CU  
- `eth_getLogs`: 75 CU
- Per matched tx: `eth_getTransactionByHash` (17) + `eth_getTransactionReceipt` (15) = 32 CU

**Total per block (0 matches): ~101 CU**

### Batched Mode (recommended for fast chains)
Uses single `eth_getLogs` call for multiple blocks:
- `eth_blockNumber`: 10 CU
- `eth_getLogs` (covers entire range): 75 CU
- Per matched tx: 32 CU

**Total per poll cycle (0 matches): ~85 CU regardless of block count**

### Monthly CU Comparison

| Chain | Block Time | Per-Block Mode | Batched Mode | Savings |
|-------|------------|----------------|--------------|---------|
| Ethereum | 12s | ~22M CU | ~13M CU | 40% |
| Base | 2s | ~131M CU | ~13M CU | **90%** |
| BNB | 0.45s | ~585M CU | ~13M CU | **98%** |

Enable batched mode with `UseBatchedPolling: true` in SyncWorkerConfig.

## Monitoring

### Logs

```
[Backfill] Rate controller initialized for CU budget management
[Backfill] Pausing due to high CU utilization (>90%), waiting for budget...
[Backfill] Resuming after budget became available
[SyncWorker] Chain ethereum: rate limiting enabled with PriorityHigh
```

### Metrics

```go
// Get utilization
utilization, _ := tracker.TotalUtilization(ctx)
fmt.Printf("Current utilization: %.1f%%\n", utilization)

// Check thresholds
isWarning, _ := tracker.IsWarningThreshold(ctx)
isPause, _ := tracker.IsPauseThreshold(ctx)

// Get available budget
available, _ := tracker.AvailableBudget(ctx, ratelimit.PriorityLow)
fmt.Printf("Available shared budget: %d CU\n", available)
```

## Error Handling

### Redis Errors

On Redis errors, the system denies requests to be safe:

```go
result, err := script.Run(ctx, t.redis, keys, args...)
if err != nil {
    // Deny request, return wait time
    return false, t.calculateWaitTime(windowTS)
}
```

### Context Cancellation

```go
var ErrContextCancelled = errors.New("context cancelled while waiting for budget")

// WaitForBudget respects context cancellation
select {
case <-ctx.Done():
    return ErrContextCancelled
case <-time.After(delay):
    // Continue retry
}
```

## Best Practices

1. **Always use appropriate priority**: Sync = High, Backfill = Low
2. **Check ShouldPause before batch operations**: Prevents overwhelming the system
3. **Record success/failure**: Enables proper backoff behavior
4. **Monitor utilization**: Set up alerts for warning threshold
5. **Configure based on Alchemy plan**: Adjust total budget to match your plan
