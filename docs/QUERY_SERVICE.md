# Query Service Implementation

This document describes the detailed implementation of the transaction query system.

## Overview

The Query Service handles transaction queries with filtering, sorting, and pagination. It implements a two-tier caching strategy: Redis for recent transactions and ClickHouse for complex queries.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           Query Service                                  │
│                                                                          │
│  Query(input) ──► shouldUseCache()                                       │
│                        │                                                 │
│           ┌────────────┴────────────┐                                    │
│           │                         │                                    │
│           ▼                         ▼                                    │
│    Simple Query?              Complex Query?                             │
│    (no filters)               (has filters)                              │
│           │                         │                                    │
│           ▼                         ▼                                    │
│    queryFromCache()          queryFromClickHouse()                       │
│           │                         │                                    │
│           ▼                         │                                    │
│    Cache Hit? ──No──────────────────┤                                    │
│           │                         │                                    │
│          Yes                        │                                    │
│           │                         │                                    │
│           ▼                         ▼                                    │
│    Return cached data         Query ClickHouse                           │
│                                     │                                    │
│                                     ▼                                    │
│                              Populate cache (async)                      │
│                                     │                                    │
│                                     ▼                                    │
│                              Return results                              │
└─────────────────────────────────────────────────────────────────────────┘
```

## Components

### 1. QueryService

Location: `internal/service/query_service.go`

```go
type QueryService struct {
    transactionRepo     TransactionQuerier
    unifiedTimelineRepo *storage.UnifiedTimelineRepository
    cacheService        *storage.CacheService
    cacheWindowSize     int              // Default: 1000
    perfMonitor         *PerformanceMonitor
}
```

### 2. QueryInput

```go
type QueryInput struct {
    Address   string              // Required: blockchain address
    Chains    []types.ChainID     // Optional: filter by chains
    DateFrom  *time.Time          // Optional: start date
    DateTo    *time.Time          // Optional: end date
    MinValue  *float64            // Optional: minimum value
    MaxValue  *float64            // Optional: maximum value
    Status    *types.TransactionStatus // Optional: success/failed
    SortBy    string              // timestamp, value, block_number
    SortOrder string              // asc, desc
    Limit     int                 // Default: 50, Max: 1000
    Offset    int                 // Default: 0
}
```

### 3. QueryResult

```go
type QueryResult struct {
    Address      string
    Transactions []*types.NormalizedTransaction
    Pagination   PaginationInfo
    Cached       bool        // Whether result came from cache
    QueryTimeMs  int64       // Query execution time
}

type PaginationInfo struct {
    Total   int64
    Limit   int
    Offset  int
    HasMore bool
}
```

## Query Routing

The service routes queries based on complexity:

| Query Type | Route | Reason |
|------------|-------|--------|
| Simple (no filters, offset < 1000) | Redis Cache | Sub-100ms response |
| Complex (has filters) | ClickHouse | Full filtering capability |
| Large offset (>= 1000) | ClickHouse | Beyond cache window |

### Decision Logic

```go
func (s *QueryService) shouldUseCache(input *QueryInput) bool {
    hasComplexFilters := input.DateFrom != nil ||
        input.DateTo != nil ||
        input.MinValue != nil ||
        input.MaxValue != nil ||
        input.Status != nil ||
        len(input.Chains) > 0

    if hasComplexFilters {
        return false  // Go to ClickHouse
    }

    if input.Offset >= s.cacheWindowSize {
        return false  // Beyond cache window
    }

    return true  // Try cache first
}
```

## Caching Strategy

### Cache Structure

```go
type CachedTransactionWindow struct {
    Address      string
    Transactions []*models.Transaction
    CachedAt     time.Time
    TotalCount   int64
}
```

### Cache Configuration

| Parameter | Value | Description |
|-----------|-------|-------------|
| Window Size | 1000 | Max transactions cached per address |
| TTL | Configurable (`CACHE_TTL`, default: 20s) | Slightly longer than sync poll interval |
| Key Format | `tx:{address}` | Per-address caching |

### Cache Population

On cache miss for simple queries:
1. Query ClickHouse for results
2. Return results immediately
3. Asynchronously populate cache for future requests

```go
if s.shouldPopulateCache(input) {
    go func() {
        cacheCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
        defer cancel()
        s.PopulateCache(cacheCtx, input.Address)
    }()
}
```

## Progressive Loading

For large result sets, the service implements progressive loading:

1. First request returns 50 transactions immediately
2. Client can request more via `QueryNextPage()`
3. Pagination info indicates if more data is available

```go
// First request
result, _ := queryService.Query(ctx, &QueryInput{
    Address: "0x...",
    Limit:   100,  // Requested 100
})
// Returns 50 immediately (progressive loading)

// Get next page
nextResult, _ := queryService.QueryNextPage(ctx, result, 50)
```

## Transaction Hash Search

The `SearchByHash` method searches for transactions by hash:

```go
func (s *QueryService) SearchByHash(ctx context.Context, hash string) (*types.NormalizedTransaction, error)
```

- Uses ClickHouse bloom filter index for fast lookup
- Target: < 200ms response time
- Returns `TRANSACTION_NOT_FOUND` error if not found

## Filtering Capabilities

### Date Range Filtering

```go
input := &QueryInput{
    Address:  "0x...",
    DateFrom: &startDate,
    DateTo:   &endDate,
}
```

### Value Filtering

```go
input := &QueryInput{
    Address:  "0x...",
    MinValue: &minVal,  // e.g., 1.0 ETH
    MaxValue: &maxVal,  // e.g., 100.0 ETH
}
```

### Chain Filtering

```go
input := &QueryInput{
    Address: "0x...",
    Chains:  []types.ChainID{types.ChainEthereum, types.ChainPolygon},
}
```

### Status Filtering

```go
input := &QueryInput{
    Address: "0x...",
    Status:  &types.StatusSuccess,
}
```

## Sorting

Supported sort fields:
- `timestamp` (default, descending)
- `value`
- `block_number`

Sort orders:
- `desc` (default)
- `asc`

## Performance Monitoring

The service includes built-in performance monitoring:

```go
type PerformanceStats struct {
    TotalQueries      int64
    CachedQueries     int64
    ClickHouseQueries int64
    AvgResponseTimeMs float64
    P95ResponseTimeMs float64
}

// Get stats
stats := queryService.GetPerformanceStats()

// Check if meeting SLA
check := queryService.CheckPerformance()
// check.CachedQueriesUnder100ms: true/false
```

## Error Handling

### Validation Errors

```go
// Invalid address format
"invalid address format: address must be 42 characters starting with 0x"

// Invalid limit
"limit cannot exceed 1000"

// Invalid date range
"dateFrom must be before dateTo"
```

### Not Found Errors

```go
&types.ServiceError{
    Code:    "TRANSACTION_NOT_FOUND",
    Message: "Transaction with hash 0x... not found",
    Details: map[string]interface{}{"hash": hash},
}
```

## ClickHouse Query Optimization

The service uses optimized queries for different scenarios:

### Recent Transactions

Uses `recent_transactions_mv` materialized view for simple queries.

### Multi-Chain Queries

Uses `unified_timeline` view for cross-chain queries.

### Index Usage

- `address` - Primary filter
- `timestamp` - Date range filtering
- `hash` - Bloom filter for hash search

## Configuration

No specific environment variables - uses database connections from main config.

## Usage Example

```go
// Create service
queryService := service.NewQueryService(txRepo, timelineRepo, cacheService)

// Simple query (uses cache)
result, err := queryService.Query(ctx, &service.QueryInput{
    Address: "0x742d35Cc6634C0532925a3b844Bc9e7595f1E3B4",
    Limit:   50,
})

// Complex query (uses ClickHouse)
result, err := queryService.Query(ctx, &service.QueryInput{
    Address:  "0x742d35Cc6634C0532925a3b844Bc9e7595f1E3B4",
    Chains:   []types.ChainID{types.ChainEthereum},
    DateFrom: &startDate,
    MinValue: &minValue,
    SortBy:   "value",
    SortOrder: "desc",
    Limit:    100,
})

// Search by hash
tx, err := queryService.SearchByHash(ctx, "0xabc123...")
```

## Performance Targets

| Operation | Target | Actual |
|-----------|--------|--------|
| Cached query | < 100ms | ~10-50ms |
| ClickHouse simple | < 200ms | ~50-150ms |
| ClickHouse complex | < 500ms | ~100-300ms |
| Hash search | < 200ms | ~50-100ms |
