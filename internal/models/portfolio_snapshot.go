package models

import (
	"time"

	"github.com/address-scanner/internal/types"
)

// PortfolioSnapshot represents a daily snapshot of portfolio state
type PortfolioSnapshot struct {
	PortfolioID       string                  `json:"portfolioId" db:"portfolio_id"`
	SnapshotDate      time.Time               `json:"snapshotDate" db:"snapshot_date"`
	TotalBalance      types.MultiChainBalance `json:"totalBalance" db:"total_balance"`
	TransactionCount  int64                   `json:"transactionCount" db:"transaction_count"`
	TotalVolume       string                  `json:"totalVolume" db:"total_volume"`
	TopCounterparties []types.Counterparty    `json:"topCounterparties" db:"top_counterparties"`
	TokenHoldings     []types.TokenHolding    `json:"tokenHoldings" db:"token_holdings"`
	CreatedAt         time.Time               `json:"createdAt" db:"created_at"`
}
