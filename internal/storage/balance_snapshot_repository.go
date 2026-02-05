package storage

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/address-scanner/internal/types"
)

// BalanceSnapshot represents a point-in-time balance for an address/token
type BalanceSnapshot struct {
	Address       string    `json:"address"`
	Chain         string    `json:"chain"`
	TokenAddress  string    `json:"tokenAddress"`
	TokenSymbol   string    `json:"tokenSymbol"`
	TokenDecimals int       `json:"tokenDecimals"`
	Date          time.Time `json:"date"`
	BalanceRaw    string    `json:"balanceRaw"`
	ProtocolID    string    `json:"protocolId"`
	PositionType  string    `json:"positionType"`
}

// NativeBalanceSnapshot represents a point-in-time native balance
type NativeBalanceSnapshot struct {
	Address    string    `json:"address"`
	Chain      string    `json:"chain"`
	Date       time.Time `json:"date"`
	BalanceRaw string    `json:"balanceRaw"`
	TxCountIn  uint64    `json:"txCountIn"`
	TxCountOut uint64    `json:"txCountOut"`
}

// PortfolioDailySummary represents aggregated daily portfolio data
type PortfolioDailySummary struct {
	PortfolioID    string    `json:"portfolioId"`
	Date           time.Time `json:"date"`
	AddressCount   int       `json:"addressCount"`
	ChainCount     int       `json:"chainCount"`
	TokenCount     int       `json:"tokenCount"`
	TotalNativeETH string    `json:"totalNativeEth"`
	SnapshotData   string    `json:"snapshotData"` // JSON
}

// BalanceSnapshotRepository handles balance snapshot data access
type BalanceSnapshotRepository struct {
	db *ClickHouseDB
}

// NewBalanceSnapshotRepository creates a new balance snapshot repository
func NewBalanceSnapshotRepository(db *ClickHouseDB) *BalanceSnapshotRepository {
	return &BalanceSnapshotRepository{db: db}
}

// CreateNativeSnapshot creates a native balance snapshot for an address
func (r *BalanceSnapshotRepository) CreateNativeSnapshot(ctx context.Context, snapshot *NativeBalanceSnapshot) error {
	// Normalize chain ID at insert time
	chain := string(types.NormalizeChainID(snapshot.Chain))

	query := `
		INSERT INTO native_balance_snapshots (address, chain, date, balance_raw, tx_count_in, tx_count_out)
		VALUES (?, ?, ?, ?, ?, ?)
	`
	return r.db.Conn().Exec(ctx, query,
		strings.ToLower(snapshot.Address),
		chain,
		snapshot.Date,
		snapshot.BalanceRaw,
		snapshot.TxCountIn,
		snapshot.TxCountOut,
	)
}

// CreateTokenSnapshot creates a token balance snapshot
func (r *BalanceSnapshotRepository) CreateTokenSnapshot(ctx context.Context, snapshot *BalanceSnapshot) error {
	// Normalize chain ID at insert time
	chain := string(types.NormalizeChainID(snapshot.Chain))

	query := `
		INSERT INTO balance_snapshots (address, chain, token_address, token_symbol, token_decimals, date, balance_raw, protocol_id, position_type)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`
	return r.db.Conn().Exec(ctx, query,
		strings.ToLower(snapshot.Address),
		chain,
		strings.ToLower(snapshot.TokenAddress),
		snapshot.TokenSymbol,
		snapshot.TokenDecimals,
		snapshot.Date,
		snapshot.BalanceRaw,
		snapshot.ProtocolID,
		snapshot.PositionType,
	)
}

// CreateBatchNativeSnapshots creates multiple native balance snapshots efficiently
func (r *BalanceSnapshotRepository) CreateBatchNativeSnapshots(ctx context.Context, snapshots []NativeBalanceSnapshot) error {
	if len(snapshots) == 0 {
		return nil
	}

	batch, err := r.db.Conn().PrepareBatch(ctx, `
		INSERT INTO native_balance_snapshots (address, chain, date, balance_raw, tx_count_in, tx_count_out)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	for _, s := range snapshots {
		// Normalize chain ID at insert time
		chain := string(types.NormalizeChainID(s.Chain))
		if err := batch.Append(strings.ToLower(s.Address), chain, s.Date, s.BalanceRaw, s.TxCountIn, s.TxCountOut); err != nil {
			return fmt.Errorf("failed to append to batch: %w", err)
		}
	}

	return batch.Send()
}

// CreateBatchTokenSnapshots creates multiple token balance snapshots efficiently
func (r *BalanceSnapshotRepository) CreateBatchTokenSnapshots(ctx context.Context, snapshots []BalanceSnapshot) error {
	if len(snapshots) == 0 {
		return nil
	}

	batch, err := r.db.Conn().PrepareBatch(ctx, `
		INSERT INTO balance_snapshots (address, chain, token_address, token_symbol, token_decimals, date, balance_raw, protocol_id, position_type)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	for _, s := range snapshots {
		// Normalize chain ID at insert time
		chain := string(types.NormalizeChainID(s.Chain))
		if err := batch.Append(strings.ToLower(s.Address), chain, strings.ToLower(s.TokenAddress), s.TokenSymbol, s.TokenDecimals, s.Date, s.BalanceRaw, s.ProtocolID, s.PositionType); err != nil {
			return fmt.Errorf("failed to append to batch: %w", err)
		}
	}

	return batch.Send()
}

// GetNativeBalanceHistory gets native balance history for an address
func (r *BalanceSnapshotRepository) GetNativeBalanceHistory(ctx context.Context, address, chain string, from, to time.Time) ([]NativeBalanceSnapshot, error) {
	query := `
		SELECT address, chain, date, balance_raw, tx_count_in, tx_count_out
		FROM native_balance_snapshots
		WHERE address = ? AND chain = ? AND date >= ? AND date <= ?
		ORDER BY date ASC
	`

	rows, err := r.db.Conn().Query(ctx, query, address, chain, from, to)
	if err != nil {
		return nil, fmt.Errorf("failed to query native balance history: %w", err)
	}
	defer rows.Close()

	var snapshots []NativeBalanceSnapshot
	for rows.Next() {
		var s NativeBalanceSnapshot
		if err := rows.Scan(&s.Address, &s.Chain, &s.Date, &s.BalanceRaw, &s.TxCountIn, &s.TxCountOut); err != nil {
			return nil, fmt.Errorf("failed to scan snapshot: %w", err)
		}
		snapshots = append(snapshots, s)
	}

	return snapshots, nil
}

// GetTokenBalanceHistory gets token balance history for an address
func (r *BalanceSnapshotRepository) GetTokenBalanceHistory(ctx context.Context, address, chain, tokenAddress string, from, to time.Time) ([]BalanceSnapshot, error) {
	query := `
		SELECT address, chain, token_address, token_symbol, token_decimals, date, balance_raw, protocol_id, position_type
		FROM balance_snapshots
		WHERE address = ? AND chain = ? AND token_address = ? AND date >= ? AND date <= ?
		ORDER BY date ASC
	`

	rows, err := r.db.Conn().Query(ctx, query, address, chain, tokenAddress, from, to)
	if err != nil {
		return nil, fmt.Errorf("failed to query token balance history: %w", err)
	}
	defer rows.Close()

	var snapshots []BalanceSnapshot
	for rows.Next() {
		var s BalanceSnapshot
		if err := rows.Scan(&s.Address, &s.Chain, &s.TokenAddress, &s.TokenSymbol, &s.TokenDecimals, &s.Date, &s.BalanceRaw, &s.ProtocolID, &s.PositionType); err != nil {
			return nil, fmt.Errorf("failed to scan snapshot: %w", err)
		}
		snapshots = append(snapshots, s)
	}

	return snapshots, nil
}

// GetAllBalancesAtDate gets all balances for an address at a specific date
func (r *BalanceSnapshotRepository) GetAllBalancesAtDate(ctx context.Context, address string, date time.Time) ([]BalanceSnapshot, error) {
	query := `
		SELECT address, chain, token_address, token_symbol, token_decimals, date, balance_raw, protocol_id, position_type
		FROM balance_snapshots
		WHERE address = ? AND date = ?
		ORDER BY chain, token_address
	`

	rows, err := r.db.Conn().Query(ctx, query, address, date)
	if err != nil {
		return nil, fmt.Errorf("failed to query balances at date: %w", err)
	}
	defer rows.Close()

	var snapshots []BalanceSnapshot
	for rows.Next() {
		var s BalanceSnapshot
		if err := rows.Scan(&s.Address, &s.Chain, &s.TokenAddress, &s.TokenSymbol, &s.TokenDecimals, &s.Date, &s.BalanceRaw, &s.ProtocolID, &s.PositionType); err != nil {
			return nil, fmt.Errorf("failed to scan snapshot: %w", err)
		}
		snapshots = append(snapshots, s)
	}

	return snapshots, nil
}

// GetPortfolioBalancesAtDate gets all balances for multiple addresses at a date
func (r *BalanceSnapshotRepository) GetPortfolioBalancesAtDate(ctx context.Context, addresses []string, date time.Time) (map[string][]BalanceSnapshot, error) {
	if len(addresses) == 0 {
		return make(map[string][]BalanceSnapshot), nil
	}

	// Build placeholders
	placeholders := ""
	args := make([]any, 0, len(addresses)+1)
	for i, addr := range addresses {
		if i > 0 {
			placeholders += ","
		}
		placeholders += "?"
		args = append(args, addr)
	}
	args = append(args, date)

	query := `
		SELECT address, chain, token_address, token_symbol, token_decimals, date, balance_raw, protocol_id, position_type
		FROM balance_snapshots
		WHERE address IN (` + placeholders + `) AND date = ?
		ORDER BY address, chain, token_address
	`

	rows, err := r.db.Conn().Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query portfolio balances: %w", err)
	}
	defer rows.Close()

	result := make(map[string][]BalanceSnapshot)
	for rows.Next() {
		var s BalanceSnapshot
		if err := rows.Scan(&s.Address, &s.Chain, &s.TokenAddress, &s.TokenSymbol, &s.TokenDecimals, &s.Date, &s.BalanceRaw, &s.ProtocolID, &s.PositionType); err != nil {
			return nil, fmt.Errorf("failed to scan snapshot: %w", err)
		}
		result[s.Address] = append(result[s.Address], s)
	}

	return result, nil
}

// SavePortfolioDailySummary saves a portfolio daily summary
func (r *BalanceSnapshotRepository) SavePortfolioDailySummary(ctx context.Context, summary *PortfolioDailySummary) error {
	query := `
		INSERT INTO portfolio_daily_summary (portfolio_id, date, address_count, chain_count, token_count, total_native_eth, snapshot_data)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`
	return r.db.Conn().Exec(ctx, query,
		summary.PortfolioID,
		summary.Date,
		summary.AddressCount,
		summary.ChainCount,
		summary.TokenCount,
		summary.TotalNativeETH,
		summary.SnapshotData,
	)
}

// GetPortfolioDailyHistory gets portfolio daily summaries for a date range
func (r *BalanceSnapshotRepository) GetPortfolioDailyHistory(ctx context.Context, portfolioID string, from, to time.Time) ([]PortfolioDailySummary, error) {
	query := `
		SELECT portfolio_id, date, address_count, chain_count, token_count, total_native_eth, snapshot_data
		FROM portfolio_daily_summary
		WHERE portfolio_id = ? AND date >= ? AND date <= ?
		ORDER BY date ASC
	`

	rows, err := r.db.Conn().Query(ctx, query, portfolioID, from, to)
	if err != nil {
		return nil, fmt.Errorf("failed to query portfolio history: %w", err)
	}
	defer rows.Close()

	var summaries []PortfolioDailySummary
	for rows.Next() {
		var s PortfolioDailySummary
		if err := rows.Scan(&s.PortfolioID, &s.Date, &s.AddressCount, &s.ChainCount, &s.TokenCount, &s.TotalNativeETH, &s.SnapshotData); err != nil {
			return nil, fmt.Errorf("failed to scan summary: %w", err)
		}
		summaries = append(summaries, s)
	}

	return summaries, nil
}

// GetLatestSnapshotDate gets the most recent snapshot date for an address
func (r *BalanceSnapshotRepository) GetLatestSnapshotDate(ctx context.Context, address string) (time.Time, error) {
	query := `SELECT max(date) FROM native_balance_snapshots WHERE address = ?`

	var maxDate time.Time
	row := r.db.Conn().QueryRow(ctx, query, address)
	if err := row.Scan(&maxDate); err != nil {
		return time.Time{}, err
	}

	return maxDate, nil
}

// CreateSnapshotFromCurrentBalances creates snapshots from current aggregated balances
func (r *BalanceSnapshotRepository) CreateSnapshotFromCurrentBalances(ctx context.Context, addresses []string, chains []types.ChainID, date time.Time) error {
	if len(addresses) == 0 {
		return nil
	}

	// Build placeholders for addresses
	placeholders := ""
	args := make([]any, 0, len(addresses))
	for i, addr := range addresses {
		if i > 0 {
			placeholders += ","
		}
		placeholders += "?"
		args = append(args, addr)
	}

	// Insert native balance snapshots from aggregation table
	nativeQuery := `
		INSERT INTO native_balance_snapshots (address, chain, date, balance_raw, tx_count_in, tx_count_out)
		SELECT 
			address,
			chain,
			toDate(?) as date,
			toString(sum(balance_in) - sum(balance_out) - sum(gas_spent)) as balance_raw,
			sum(tx_count_in) as tx_count_in,
			sum(tx_count_out) as tx_count_out
		FROM native_balances_agg
		WHERE address IN (` + placeholders + `)
		GROUP BY address, chain
		HAVING sum(balance_in) - sum(balance_out) - sum(gas_spent) != 0
	`

	nativeArgs := append([]any{date}, args...)
	if err := r.db.Conn().Exec(ctx, nativeQuery, nativeArgs...); err != nil {
		return fmt.Errorf("failed to create native snapshots: %w", err)
	}

	// Insert token balance snapshots from aggregation table
	tokenQuery := `
		INSERT INTO balance_snapshots (address, chain, token_address, token_symbol, token_decimals, date, balance_raw, protocol_id, position_type)
		SELECT 
			address,
			chain,
			token_address,
			any(token_symbol) as token_symbol,
			any(token_decimals) as token_decimals,
			toDate(?) as date,
			toString(sum(balance_in) - sum(balance_out)) as balance_raw,
			'' as protocol_id,
			'wallet' as position_type
		FROM token_balances_agg
		WHERE address IN (` + placeholders + `)
		GROUP BY address, chain, token_address
		HAVING sum(balance_in) - sum(balance_out) > 0
	`

	tokenArgs := append([]any{date}, args...)
	if err := r.db.Conn().Exec(ctx, tokenQuery, tokenArgs...); err != nil {
		return fmt.Errorf("failed to create token snapshots: %w", err)
	}

	return nil
}

// SnapshotSummary represents a summary of snapshots for a date
type SnapshotSummary struct {
	Date         time.Time `json:"date"`
	AddressCount int       `json:"addressCount"`
	NativeCount  int       `json:"nativeCount"`
	TokenCount   int       `json:"tokenCount"`
}

// GetSnapshotSummary gets a summary of snapshots for a date range
func (r *BalanceSnapshotRepository) GetSnapshotSummary(ctx context.Context, from, to time.Time) ([]SnapshotSummary, error) {
	query := `
		SELECT 
			date,
			uniqExact(address) as address_count,
			count() as native_count
		FROM native_balance_snapshots
		WHERE date >= ? AND date <= ?
		GROUP BY date
		ORDER BY date ASC
	`

	rows, err := r.db.Conn().Query(ctx, query, from, to)
	if err != nil {
		return nil, fmt.Errorf("failed to query snapshot summary: %w", err)
	}
	defer rows.Close()

	summaryMap := make(map[time.Time]*SnapshotSummary)
	for rows.Next() {
		var date time.Time
		var addressCount, nativeCount int
		if err := rows.Scan(&date, &addressCount, &nativeCount); err != nil {
			return nil, fmt.Errorf("failed to scan summary: %w", err)
		}
		summaryMap[date] = &SnapshotSummary{
			Date:         date,
			AddressCount: addressCount,
			NativeCount:  nativeCount,
		}
	}

	// Get token counts
	tokenQuery := `
		SELECT date, count() as token_count
		FROM balance_snapshots
		WHERE date >= ? AND date <= ?
		GROUP BY date
	`

	rows2, err := r.db.Conn().Query(ctx, tokenQuery, from, to)
	if err != nil {
		return nil, fmt.Errorf("failed to query token summary: %w", err)
	}
	defer rows2.Close()

	for rows2.Next() {
		var date time.Time
		var tokenCount int
		if err := rows2.Scan(&date, &tokenCount); err != nil {
			continue
		}
		if s, ok := summaryMap[date]; ok {
			s.TokenCount = tokenCount
		}
	}

	// Convert to slice
	var summaries []SnapshotSummary
	for _, s := range summaryMap {
		summaries = append(summaries, *s)
	}

	return summaries, nil
}
