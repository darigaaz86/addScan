package storage

import (
	"fmt"

	"github.com/address-scanner/internal/types"
)

// QueryOptimizer provides query optimization helpers
type QueryOptimizer struct {
	clickhouse *ClickHouseDB
}

// NewQueryOptimizer creates a new query optimizer
func NewQueryOptimizer(clickhouse *ClickHouseDB) *QueryOptimizer {
	return &QueryOptimizer{
		clickhouse: clickhouse,
	}
}

// UseRecentTransactionsView returns a query for recent transactions
func (qo *QueryOptimizer) UseRecentTransactionsView(address string, limit int) string {
	return fmt.Sprintf(`
		SELECT
			hash,
			chain,
			from,
			to,
			value,
			timestamp,
			block_number,
			status,
			gas_used,
			gas_price,
			token_transfers,
			method_id,
			input
		FROM transactions
		WHERE address = '%s'
		ORDER BY timestamp DESC
		LIMIT %d
	`, address, limit)
}

// UseUnifiedTimelineView returns a query for multi-chain timeline
func (qo *QueryOptimizer) UseUnifiedTimelineView(address string, chains []types.ChainID, limit int, offset int) string {
	query := fmt.Sprintf(`
		SELECT
			hash,
			chain,
			from,
			to,
			value,
			timestamp,
			block_number,
			status,
			gas_used,
			gas_price,
			token_transfers,
			method_id,
			input
		FROM unified_timeline
		WHERE address = '%s'
	`, address)

	// Add chain filter if specified
	if len(chains) > 0 {
		chainList := ""
		for i, chain := range chains {
			if i > 0 {
				chainList += ", "
			}
			chainList += fmt.Sprintf("'%s'", chain)
		}
		query += fmt.Sprintf(" AND chain IN (%s)", chainList)
	}

	query += fmt.Sprintf(`
		ORDER BY timestamp DESC
		LIMIT %d OFFSET %d
	`, limit, offset)

	return query
}
