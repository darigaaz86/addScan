# Portfolio Service Implementation

This document describes the detailed implementation of the portfolio management and aggregation system.

## Overview

The Portfolio Service manages collections of blockchain addresses, providing aggregated views of balances, transactions, and statistics across multiple addresses and chains.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         Portfolio Service                                │
│                                                                          │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │                    Portfolio Operations                          │    │
│  │                                                                  │    │
│  │   CreatePortfolio() ──► Validate addresses                       │    │
│  │                         Auto-add missing addresses               │    │
│  │                         Store in Postgres                        │    │
│  │                                                                  │    │
│  │   GetPortfolio() ──► Fetch portfolio                             │    │
│  │                      Aggregate balances (all chains)             │    │
│  │                      Merge transaction timeline                  │    │
│  │                      Calculate statistics                        │    │
│  │                                                                  │    │
│  │   UpdatePortfolio() ──► Validate changes                         │    │
│  │                         Update Postgres                          │    │
│  │                         Trigger async aggregation refresh        │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│                                                                          │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │                    Aggregation Engine                            │    │
│  │                                                                  │    │
│  │   aggregateBalance() ──► Query each chain adapter                │    │
│  │                          Sum native balances                     │    │
│  │                          Merge token balances                    │    │
│  │                                                                  │    │
│  │   mergeTimeline() ──► Query transactions for all addresses       │    │
│  │                       Sort by timestamp (descending)             │    │
│  │                       Return unified view                        │    │
│  │                                                                  │    │
│  │   calculateStatistics() ──► Transaction count                    │    │
│  │                             Total volume                         │    │
│  │                             Top counterparties                   │    │
│  │                             Token holdings                       │    │
│  │                             Chain distribution                   │    │
│  └─────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────┘
```

## Components

### 1. PortfolioService

Location: `internal/service/portfolio_service.go`

```go
type PortfolioService struct {
    portfolioRepo   PortfolioRepository
    addressRepo     AddressRepository
    transactionRepo TransactionRepository
    chainAdapters   map[types.ChainID]adapter.ChainAdapter
    addressService  *AddressService
}
```

### 2. Data Models

#### Portfolio

```go
type Portfolio struct {
    ID          string
    UserID      string
    Name        string
    Description *string
    Addresses   []string
    CreatedAt   time.Time
    UpdatedAt   time.Time
}
```

#### PortfolioView (Aggregated)

```go
type PortfolioView struct {
    ID                string
    Name              string
    Description       *string
    Addresses         []string
    TotalBalance      types.MultiChainBalance
    TransactionCount  int64
    TotalVolume       string
    TopCounterparties []types.Counterparty
    TokenHoldings     []types.TokenHolding
    UnifiedTimeline   []*types.NormalizedTransaction
    LastUpdated       time.Time
}
```

#### PortfolioStatistics

```go
type PortfolioStatistics struct {
    PortfolioID             string
    TotalBalance            types.MultiChainBalance
    TransactionCount        int64
    TotalVolume             string
    AverageTransactionValue string
    TopCounterparties       []types.Counterparty
    TokenHoldings           []types.TokenHolding
    ChainDistribution       []ChainDistribution
    ActivityByDay           []ActivityByDay
}
```

## Operations

### Create Portfolio

```go
func (s *PortfolioService) CreatePortfolio(ctx context.Context, input *CreatePortfolioInput) (*Portfolio, error)
```

Flow:
1. Validate input (userID, name, addresses required)
2. For each address:
   - Check if already tracked
   - If not, auto-add via AddressService (triggers backfill)
3. Create portfolio record in Postgres
4. Return portfolio

```go
input := &CreatePortfolioInput{
    UserID:      "user-123",
    Name:        "My DeFi Portfolio",
    Addresses:   []string{"0xabc...", "0xdef..."},
    Description: &description,
}
portfolio, err := portfolioService.CreatePortfolio(ctx, input)
```

### Get Portfolio (with Aggregation)

```go
func (s *PortfolioService) GetPortfolio(ctx context.Context, portfolioID, userID string) (*PortfolioView, error)
```

Flow:
1. Fetch portfolio from Postgres
2. Verify user ownership
3. Aggregate balances across all chains
4. Merge transaction timeline (most recent 50)
5. Calculate statistics
6. Return aggregated view

### Update Portfolio

```go
func (s *PortfolioService) UpdatePortfolio(ctx context.Context, input *UpdatePortfolioInput) (*Portfolio, error)
```

Flow:
1. Fetch and verify ownership
2. Validate new addresses exist in system
3. Update portfolio record
4. Trigger async aggregation refresh (30s timeout)
5. Return updated portfolio

### Delete Portfolio

```go
func (s *PortfolioService) DeletePortfolio(ctx context.Context, portfolioID, userID string) (*DeletePortfolioResult, error)
```

Important: Deleting a portfolio does NOT:
- Remove individual addresses from tracking
- Delete historical transaction data
- Remove snapshots

## Aggregation Engine

### Balance Aggregation

```go
func (s *PortfolioService) aggregateBalance(ctx context.Context, addresses []string) (types.MultiChainBalance, error)
```

For each address on each chain:
1. Query chain adapter for balance
2. Sum native balances per chain
3. Merge token balances (aggregate by token address)

Result structure:
```go
type MultiChainBalance struct {
    Chains        []ChainBalance
    TotalValueUSD *string  // Optional USD value
}

type ChainBalance struct {
    Chain         types.ChainID
    NativeBalance string
    TokenBalances []TokenBalance
}
```

### Timeline Merging

```go
func (s *PortfolioService) mergeTimeline(ctx context.Context, addresses []string, limit int) ([]*types.NormalizedTransaction, error)
```

1. Query transactions for all addresses (limit * address count)
2. Sort by timestamp descending
3. Return top N transactions

### Statistics Calculation

```go
func (s *PortfolioService) calculateStatistics(ctx context.Context, addresses []string) (*portfolioStats, error)
```

Calculates:
- **Transaction Count**: Total transactions across all addresses
- **Total Volume**: Sum of all transaction values
- **Top Counterparties**: Most frequent transaction partners
- **Token Holdings**: Aggregated token positions
- **Chain Distribution**: Transaction count per chain
- **Activity by Day**: Daily transaction metrics

#### Top Counterparties Algorithm

```go
func (s *PortfolioService) identifyTopCounterparties(transactions []*models.Transaction, portfolioAddresses []string, limit int) []types.Counterparty
```

1. Create set of portfolio addresses
2. For each transaction:
   - Identify counterparty (address NOT in portfolio)
   - Increment transaction count
   - Sum volume
3. Sort by transaction count
4. Return top N

## Snapshots

Portfolio snapshots capture daily state for historical analysis.

```go
type PortfolioSnapshot struct {
    PortfolioID       string
    SnapshotDate      time.Time
    TotalBalance      types.MultiChainBalance
    TransactionCount  int64
    TotalVolume       string
    TopCounterparties []types.Counterparty
    TokenHoldings     []types.TokenHolding
    CreatedAt         time.Time
}
```

Snapshot retrieval is delegated to SnapshotService for full functionality.

## Error Handling

### Validation Errors

```go
&types.ServiceError{
    Code:    "INVALID_INPUT",
    Message: "userId is required",
}

&types.ServiceError{
    Code:    "INVALID_INPUT",
    Message: "at least one address is required",
}
```

### Not Found Errors

```go
&types.ServiceError{
    Code:    "PORTFOLIO_NOT_FOUND",
    Message: "portfolio not found or access denied: {id}",
    Details: map[string]interface{}{"portfolioId": id},
}
```

### Address Not Tracked

```go
&types.ServiceError{
    Code:    "ADDRESS_NOT_TRACKED",
    Message: "address 0x... is not tracked in the system",
    Details: map[string]interface{}{"address": addr},
}
```

## Async Operations

Portfolio updates trigger async aggregation refresh:

```go
go func() {
    updateCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()

    s.UpdateAggregations(updateCtx, portfolio.ID)
    s.RefreshPortfolioCache(updateCtx, portfolio.ID)
}()
```

Target: Updates complete within 30 seconds.

## Database Schema

### Postgres: portfolios

```sql
CREATE TABLE portfolios (
    id VARCHAR(36) PRIMARY KEY,
    user_id VARCHAR(36) NOT NULL REFERENCES users(id),
    name VARCHAR(255) NOT NULL,
    description TEXT,
    addresses TEXT[] NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_portfolios_user_id ON portfolios(user_id);
```

### Postgres: portfolio_snapshots

```sql
CREATE TABLE portfolio_snapshots (
    id VARCHAR(36) PRIMARY KEY,
    portfolio_id VARCHAR(36) NOT NULL REFERENCES portfolios(id),
    snapshot_date DATE NOT NULL,
    total_balance JSONB NOT NULL,
    transaction_count BIGINT NOT NULL,
    total_volume VARCHAR(78) NOT NULL,
    top_counterparties JSONB,
    token_holdings JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE(portfolio_id, snapshot_date)
);
```

## Usage Examples

### Create Portfolio

```go
portfolio, err := portfolioService.CreatePortfolio(ctx, &CreatePortfolioInput{
    UserID:    "user-123",
    Name:      "Trading Wallets",
    Addresses: []string{
        "0x742d35Cc6634C0532925a3b844Bc9e7595f1E3B4",
        "0x8ba1f109551bD432803012645Ac136ddd64DBA72",
    },
})
```

### Get Aggregated View

```go
view, err := portfolioService.GetPortfolio(ctx, "portfolio-id", "user-123")

fmt.Printf("Total Balance: %v\n", view.TotalBalance)
fmt.Printf("Transaction Count: %d\n", view.TransactionCount)
fmt.Printf("Top Counterparties: %v\n", view.TopCounterparties)
```

### Get Statistics

```go
stats, err := portfolioService.GetStatistics(ctx, "portfolio-id", "user-123")

fmt.Printf("Chain Distribution:\n")
for _, dist := range stats.ChainDistribution {
    fmt.Printf("  %s: %d txs (%.1f%%)\n", dist.Chain, dist.TransactionCount, dist.Percentage)
}
```

### Update Portfolio

```go
newName := "Updated Portfolio Name"
portfolio, err := portfolioService.UpdatePortfolio(ctx, &UpdatePortfolioInput{
    PortfolioID: "portfolio-id",
    UserID:      "user-123",
    Name:        &newName,
    Addresses:   []string{"0xnew...", "0xaddress..."},
})
```

## Performance Considerations

### Balance Aggregation

- Queries each chain adapter sequentially
- Errors for individual chains don't fail the entire operation
- Consider caching for frequently accessed portfolios

### Timeline Merging

- Fetches `limit * address_count` transactions
- Sorts in memory
- For large portfolios, consider pagination

### Statistics Calculation

- Limited to 10,000 transactions for calculation
- Counterparty identification is O(n) where n = transaction count
- Token holdings aggregation uses map for O(1) lookup
