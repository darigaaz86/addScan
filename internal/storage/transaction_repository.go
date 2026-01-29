package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/address-scanner/internal/models"
	"github.com/address-scanner/internal/types"
)

// TransactionRepository handles transaction data persistence in ClickHouse
type TransactionRepository struct {
	db        *ClickHouseDB
	optimizer *QueryOptimizer
}

// NewTransactionRepository creates a new transaction repository
func NewTransactionRepository(db *ClickHouseDB) *TransactionRepository {
	return &TransactionRepository{
		db:        db,
		optimizer: NewQueryOptimizer(db),
	}
}

// Insert inserts a single transaction
func (r *TransactionRepository) Insert(ctx context.Context, tx *models.Transaction) error {
	// Validate and normalize address
	if err := ValidateAddress(tx.Address); err != nil {
		return err
	}
	tx.Address = strings.ToLower(tx.Address)
	tx.From = strings.ToLower(tx.From)
	tx.To = strings.ToLower(tx.To)

	// Serialize token transfers to JSON
	tokenTransfersJSON, err := json.Marshal(tx.TokenTransfers)
	if err != nil {
		return fmt.Errorf("failed to marshal token transfers: %w", err)
	}

	query := `
		INSERT INTO transactions (
			hash, chain, address, from, to, value, asset, category, direction, timestamp, block_number,
			status, gas_used, gas_price, token_transfers, method_id, func_name, input
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`

	err = r.db.Conn().Exec(ctx, query,
		tx.Hash,
		string(tx.Chain),
		tx.Address,
		tx.From,
		tx.To,
		tx.Value,
		tx.Asset,
		tx.Category,
		string(tx.Direction),
		tx.Timestamp,
		tx.BlockNumber,
		tx.Status,
		tx.GasUsed,
		tx.GasPrice,
		string(tokenTransfersJSON),
		tx.MethodID,
		tx.FuncName,
		tx.Input,
	)

	if err != nil {
		return fmt.Errorf("failed to insert transaction: %w", err)
	}

	return nil
}

// BatchInsert inserts multiple transactions in a batch
func (r *TransactionRepository) BatchInsert(ctx context.Context, transactions []*models.Transaction) error {
	if len(transactions) == 0 {
		return nil
	}

	batch, err := r.db.Conn().PrepareBatch(ctx, `
		INSERT INTO transactions (
			hash, chain, address, from, to, value, asset, category, direction, timestamp, block_number,
			status, gas_used, gas_price, token_transfers, method_id, func_name, input
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	for _, tx := range transactions {
		// Validate and normalize addresses
		if err := ValidateAddress(tx.Address); err != nil {
			return fmt.Errorf("invalid address %s: %w", tx.Address, err)
		}
		tx.Address = strings.ToLower(tx.Address)
		tx.From = strings.ToLower(tx.From)
		tx.To = strings.ToLower(tx.To)

		// Serialize token transfers to JSON
		var tokenTransfersJSON []byte
		var err error
		if len(tx.TokenTransfers) == 0 {
			tokenTransfersJSON = []byte("[]")
		} else {
			tokenTransfersJSON, err = json.Marshal(tx.TokenTransfers)
			if err != nil {
				return fmt.Errorf("failed to marshal token transfers for tx %s: %w", tx.Hash, err)
			}
		}

		err = batch.Append(
			tx.Hash,
			string(tx.Chain),
			tx.Address,
			tx.From,
			tx.To,
			tx.Value,
			tx.Asset,
			tx.Category,
			string(tx.Direction),
			tx.Timestamp,
			tx.BlockNumber,
			tx.Status,
			tx.GasUsed,
			tx.GasPrice,
			string(tokenTransfersJSON),
			tx.MethodID,
			tx.FuncName,
			tx.Input,
		)
		if err != nil {
			return fmt.Errorf("failed to append transaction %s to batch: %w", tx.Hash, err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send batch: %w", err)
	}

	return nil
}

// GetByHash retrieves a transaction by hash
func (r *TransactionRepository) GetByHash(ctx context.Context, hash string) (*models.Transaction, error) {
	query := `
		SELECT hash, chain, address, from, to, value, direction, timestamp, block_number,
			   status, gas_used, gas_price, token_transfers, method_id, input
		FROM transactions
		WHERE hash = ?
		LIMIT 1
	`

	var tx models.Transaction
	var tokenTransfersJSON string
	var chainStr string
	var directionStr string

	row := r.db.Conn().QueryRow(ctx, query, hash)
	err := row.Scan(
		&tx.Hash,
		&chainStr,
		&tx.Address,
		&tx.From,
		&tx.To,
		&tx.Value,
		&directionStr,
		&tx.Timestamp,
		&tx.BlockNumber,
		&tx.Status,
		&tx.GasUsed,
		&tx.GasPrice,
		&tokenTransfersJSON,
		&tx.MethodID,
		&tx.Input,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to get transaction by hash: %w", err)
	}

	tx.Chain = types.ChainID(chainStr)
	tx.Direction = types.TransactionDirection(directionStr)

	// Deserialize token transfers
	if tokenTransfersJSON != "" && tokenTransfersJSON != "[]" {
		if err := json.Unmarshal([]byte(tokenTransfersJSON), &tx.TokenTransfers); err != nil {
			return nil, fmt.Errorf("failed to unmarshal token transfers: %w", err)
		}
	}

	return &tx, nil
}

// GetByAddress retrieves transactions for an address with filters
func (r *TransactionRepository) GetByAddress(ctx context.Context, address string, filters *TransactionFilters) ([]*models.Transaction, error) {
	// Validate and normalize address
	if err := ValidateAddress(address); err != nil {
		return nil, err
	}
	address = strings.ToLower(address)

	query := `
		SELECT hash, chain, address, from, to, value, direction, timestamp, block_number,
			   status, gas_used, gas_price, token_transfers, method_id, input
		FROM transactions
		WHERE address = ?
	`
	args := []any{address}

	// Apply filters
	if filters != nil {
		if len(filters.Chains) > 0 {
			chainStrs := make([]string, len(filters.Chains))
			for i, chain := range filters.Chains {
				chainStrs[i] = string(chain)
			}
			query += " AND chain IN (" + strings.Repeat("?,", len(chainStrs)-1) + "?)"
			for _, chain := range chainStrs {
				args = append(args, chain)
			}
		}

		if filters.Direction != nil {
			query += " AND direction = ?"
			args = append(args, string(*filters.Direction))
		}

		if filters.DateFrom != nil {
			query += " AND timestamp >= ?"
			args = append(args, *filters.DateFrom)
		}

		if filters.DateTo != nil {
			query += " AND timestamp <= ?"
			args = append(args, *filters.DateTo)
		}

		if filters.MinValue != nil {
			query += " AND toFloat64OrZero(value) >= ?"
			args = append(args, *filters.MinValue)
		}

		if filters.MaxValue != nil {
			query += " AND toFloat64OrZero(value) <= ?"
			args = append(args, *filters.MaxValue)
		}

		if filters.Status != nil {
			query += " AND status = ?"
			args = append(args, string(*filters.Status))
		}
	}

	// Add ordering
	orderBy := "timestamp DESC"
	if filters != nil && filters.SortBy != "" {
		switch filters.SortBy {
		case "timestamp":
			orderBy = "timestamp"
		case "value":
			orderBy = "toFloat64OrZero(value)"
		case "block_number":
			orderBy = "block_number"
		}

		if filters.SortOrder == "asc" {
			orderBy += " ASC"
		} else {
			orderBy += " DESC"
		}
	}
	query += " ORDER BY " + orderBy

	// Add pagination
	if filters != nil {
		if filters.Limit > 0 {
			query += " LIMIT ?"
			args = append(args, filters.Limit)
		}

		if filters.Offset > 0 {
			query += " OFFSET ?"
			args = append(args, filters.Offset)
		}
	}

	rows, err := r.db.Conn().Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query transactions: %w", err)
	}
	defer rows.Close()

	var transactions []*models.Transaction
	for rows.Next() {
		var tx models.Transaction
		var tokenTransfersJSON string
		var chainStr string
		var directionStr string

		err := rows.Scan(
			&tx.Hash,
			&chainStr,
			&tx.Address,
			&tx.From,
			&tx.To,
			&tx.Value,
			&directionStr,
			&tx.Timestamp,
			&tx.BlockNumber,
			&tx.Status,
			&tx.GasUsed,
			&tx.GasPrice,
			&tokenTransfersJSON,
			&tx.MethodID,
			&tx.Input,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan transaction: %w", err)
		}

		tx.Chain = types.ChainID(chainStr)
		tx.Direction = types.TransactionDirection(directionStr)

		// Deserialize token transfers
		if tokenTransfersJSON != "" && tokenTransfersJSON != "[]" {
			if err := json.Unmarshal([]byte(tokenTransfersJSON), &tx.TokenTransfers); err != nil {
				return nil, fmt.Errorf("failed to unmarshal token transfers: %w", err)
			}
		}

		transactions = append(transactions, &tx)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating transactions: %w", err)
	}

	return transactions, nil
}

// GetByAddresses retrieves transactions for multiple addresses (for portfolio queries)
func (r *TransactionRepository) GetByAddresses(ctx context.Context, addresses []string, filters *TransactionFilters) ([]*models.Transaction, error) {
	if len(addresses) == 0 {
		return []*models.Transaction{}, nil
	}

	// Validate and normalize addresses
	normalizedAddresses := make([]string, len(addresses))
	for i, addr := range addresses {
		if err := ValidateAddress(addr); err != nil {
			return nil, fmt.Errorf("invalid address at index %d: %w", i, err)
		}
		normalizedAddresses[i] = strings.ToLower(addr)
	}

	query := `
		SELECT hash, chain, address, from, to, value, direction, timestamp, block_number,
			   status, gas_used, gas_price, token_transfers, method_id, input
		FROM transactions
		WHERE address IN (` + strings.Repeat("?,", len(normalizedAddresses)-1) + `?)
	`
	args := make([]any, len(normalizedAddresses))
	for i, addr := range normalizedAddresses {
		args[i] = addr
	}

	// Apply filters (same as GetByAddress)
	if filters != nil {
		if len(filters.Chains) > 0 {
			chainStrs := make([]string, len(filters.Chains))
			for i, chain := range filters.Chains {
				chainStrs[i] = string(chain)
			}
			query += " AND chain IN (" + strings.Repeat("?,", len(chainStrs)-1) + "?)"
			for _, chain := range chainStrs {
				args = append(args, chain)
			}
		}

		if filters.Direction != nil {
			query += " AND direction = ?"
			args = append(args, string(*filters.Direction))
		}

		if filters.DateFrom != nil {
			query += " AND timestamp >= ?"
			args = append(args, *filters.DateFrom)
		}

		if filters.DateTo != nil {
			query += " AND timestamp <= ?"
			args = append(args, *filters.DateTo)
		}

		if filters.MinValue != nil {
			query += " AND toFloat64OrZero(value) >= ?"
			args = append(args, *filters.MinValue)
		}

		if filters.MaxValue != nil {
			query += " AND toFloat64OrZero(value) <= ?"
			args = append(args, *filters.MaxValue)
		}

		if filters.Status != nil {
			query += " AND status = ?"
			args = append(args, string(*filters.Status))
		}
	}

	// Add ordering
	orderBy := "timestamp DESC"
	if filters != nil && filters.SortBy != "" {
		switch filters.SortBy {
		case "timestamp":
			orderBy = "timestamp"
		case "value":
			orderBy = "toFloat64OrZero(value)"
		case "block_number":
			orderBy = "block_number"
		}

		if filters.SortOrder == "asc" {
			orderBy += " ASC"
		} else {
			orderBy += " DESC"
		}
	}
	query += " ORDER BY " + orderBy

	// Add pagination
	if filters != nil {
		if filters.Limit > 0 {
			query += " LIMIT ?"
			args = append(args, filters.Limit)
		}

		if filters.Offset > 0 {
			query += " OFFSET ?"
			args = append(args, filters.Offset)
		}
	}

	rows, err := r.db.Conn().Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query transactions: %w", err)
	}
	defer rows.Close()

	var transactions []*models.Transaction
	for rows.Next() {
		var tx models.Transaction
		var tokenTransfersJSON string
		var chainStr string
		var directionStr string

		err := rows.Scan(
			&tx.Hash,
			&chainStr,
			&tx.Address,
			&tx.From,
			&tx.To,
			&tx.Value,
			&directionStr,
			&tx.Timestamp,
			&tx.BlockNumber,
			&tx.Status,
			&tx.GasUsed,
			&tx.GasPrice,
			&tokenTransfersJSON,
			&tx.MethodID,
			&tx.Input,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan transaction: %w", err)
		}

		tx.Chain = types.ChainID(chainStr)
		tx.Direction = types.TransactionDirection(directionStr)

		// Deserialize token transfers
		if tokenTransfersJSON != "" && tokenTransfersJSON != "[]" {
			if err := json.Unmarshal([]byte(tokenTransfersJSON), &tx.TokenTransfers); err != nil {
				return nil, fmt.Errorf("failed to unmarshal token transfers: %w", err)
			}
		}

		transactions = append(transactions, &tx)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating transactions: %w", err)
	}

	return transactions, nil
}

// CountByAddress counts transactions for an address with filters
func (r *TransactionRepository) CountByAddress(ctx context.Context, address string, filters *TransactionFilters) (int64, error) {
	// Validate and normalize address
	if err := ValidateAddress(address); err != nil {
		return 0, err
	}
	address = strings.ToLower(address)

	query := `SELECT COUNT(*) FROM transactions WHERE address = ?`
	args := []any{address}

	// Apply filters
	if filters != nil {
		if len(filters.Chains) > 0 {
			chainStrs := make([]string, len(filters.Chains))
			for i, chain := range filters.Chains {
				chainStrs[i] = string(chain)
			}
			query += " AND chain IN (" + strings.Repeat("?,", len(chainStrs)-1) + "?)"
			for _, chain := range chainStrs {
				args = append(args, chain)
			}
		}

		if filters.DateFrom != nil {
			query += " AND timestamp >= ?"
			args = append(args, *filters.DateFrom)
		}

		if filters.DateTo != nil {
			query += " AND timestamp <= ?"
			args = append(args, *filters.DateTo)
		}

		if filters.MinValue != nil {
			query += " AND toFloat64OrZero(value) >= ?"
			args = append(args, *filters.MinValue)
		}

		if filters.MaxValue != nil {
			query += " AND toFloat64OrZero(value) <= ?"
			args = append(args, *filters.MaxValue)
		}

		if filters.Status != nil {
			query += " AND status = ?"
			args = append(args, string(*filters.Status))
		}
	}

	var count uint64
	row := r.db.Conn().QueryRow(ctx, query, args...)
	if err := row.Scan(&count); err != nil {
		return 0, fmt.Errorf("failed to count transactions: %w", err)
	}

	return int64(count), nil
}

// CountByAddressAndChain counts transactions for an address on a specific chain
// Used for per-chain address classification (Requirement 10.3)
func (r *TransactionRepository) CountByAddressAndChain(ctx context.Context, address string, chain types.ChainID) (int64, error) {
	// Validate and normalize address
	if err := ValidateAddress(address); err != nil {
		return 0, err
	}
	address = strings.ToLower(address)

	query := `SELECT COUNT(*) FROM transactions WHERE address = ? AND chain = ?`

	var count uint64
	row := r.db.Conn().QueryRow(ctx, query, address, string(chain))
	if err := row.Scan(&count); err != nil {
		return 0, fmt.Errorf("failed to count transactions for chain %s: %w", chain, err)
	}

	return int64(count), nil
}

// GetLatestByAddress retrieves the most recent N transactions for an address
func (r *TransactionRepository) GetLatestByAddress(ctx context.Context, address string, limit int) ([]*models.Transaction, error) {
	filters := &TransactionFilters{
		Limit:     limit,
		SortBy:    "timestamp",
		SortOrder: "desc",
	}
	return r.GetByAddress(ctx, address, filters)
}

// DeleteByAddress deletes all transactions for an address (use with caution)
func (r *TransactionRepository) DeleteByAddress(ctx context.Context, address string) error {
	// Validate and normalize address
	if err := ValidateAddress(address); err != nil {
		return err
	}
	address = strings.ToLower(address)

	query := `ALTER TABLE transactions DELETE WHERE address = ?`

	err := r.db.Conn().Exec(ctx, query, address)
	if err != nil {
		return fmt.Errorf("failed to delete transactions: %w", err)
	}

	return nil
}

// TransactionFilters defines filters for querying transactions
type TransactionFilters struct {
	Chains    []types.ChainID
	Direction *types.TransactionDirection // Filter by in/out
	DateFrom  *time.Time
	DateTo    *time.Time
	MinValue  *float64
	MaxValue  *float64
	Status    *types.TransactionStatus
	SortBy    string // timestamp, value, block_number
	SortOrder string // asc, desc
	Limit     int
	Offset    int
}

// GetOptimizer returns the query optimizer
// Requirement 7.4: Provide access to query optimization features
func (r *TransactionRepository) GetOptimizer() *QueryOptimizer {
	return r.optimizer
}

// GetByAddressOptimized retrieves transactions using optimized queries
// Requirement 7.4: Use materialized views for aggregations, optimize indexes
func (r *TransactionRepository) GetByAddressOptimized(ctx context.Context, address string, filters *TransactionFilters) ([]*models.Transaction, error) {
	// Validate and normalize address
	if err := ValidateAddress(address); err != nil {
		return nil, err
	}
	address = strings.ToLower(address)

	// For simple recent queries, use the optimized recent_transactions_mv
	if filters != nil && filters.Limit <= 1000 && filters.Offset == 0 &&
		filters.DateFrom == nil && filters.DateTo == nil &&
		filters.MinValue == nil && filters.MaxValue == nil &&
		len(filters.Chains) == 0 {

		// Use optimized view for recent transactions
		query := r.optimizer.UseRecentTransactionsView(address, filters.Limit)

		rows, err := r.db.Conn().Query(ctx, query)
		if err != nil {
			// Fall back to regular query if optimized view fails
			return r.GetByAddress(ctx, address, filters)
		}
		defer rows.Close()

		var transactions []*models.Transaction
		for rows.Next() {
			var tx models.Transaction
			var tokenTransfersJSON string
			var chainStr string

			err := rows.Scan(
				&tx.Hash,
				&chainStr,
				&tx.From,
				&tx.To,
				&tx.Value,
				&tx.Timestamp,
				&tx.BlockNumber,
				&tx.Status,
				&tx.GasUsed,
				&tx.GasPrice,
				&tokenTransfersJSON,
				&tx.MethodID,
				&tx.Input,
			)
			if err != nil {
				return nil, fmt.Errorf("failed to scan transaction: %w", err)
			}

			// Set address and chain
			tx.Address = address
			tx.Chain = types.ChainID(chainStr)

			// Deserialize token transfers
			if tokenTransfersJSON != "" && tokenTransfersJSON != "[]" {
				if err := json.Unmarshal([]byte(tokenTransfersJSON), &tx.TokenTransfers); err != nil {
					return nil, fmt.Errorf("failed to unmarshal token transfers: %w", err)
				}
			}

			transactions = append(transactions, &tx)
		}

		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("error iterating transactions: %w", err)
		}

		return transactions, nil
	}

	// For multi-chain queries, use unified_timeline
	if filters != nil && len(filters.Chains) > 0 {
		query := r.optimizer.UseUnifiedTimelineView(address, filters.Chains, filters.Limit, filters.Offset)

		rows, err := r.db.Conn().Query(ctx, query)
		if err != nil {
			// Fall back to regular query if optimized view fails
			return r.GetByAddress(ctx, address, filters)
		}
		defer rows.Close()

		var transactions []*models.Transaction
		for rows.Next() {
			var tx models.Transaction
			var tokenTransfersJSON string
			var chainStr string

			err := rows.Scan(
				&tx.Hash,
				&chainStr,
				&tx.From,
				&tx.To,
				&tx.Value,
				&tx.Timestamp,
				&tx.BlockNumber,
				&tx.Status,
				&tx.GasUsed,
				&tx.GasPrice,
				&tokenTransfersJSON,
				&tx.MethodID,
				&tx.Input,
			)
			if err != nil {
				return nil, fmt.Errorf("failed to scan transaction: %w", err)
			}

			// Set address and chain
			tx.Address = address
			tx.Chain = types.ChainID(chainStr)

			// Deserialize token transfers
			if tokenTransfersJSON != "" && tokenTransfersJSON != "[]" {
				if err := json.Unmarshal([]byte(tokenTransfersJSON), &tx.TokenTransfers); err != nil {
					return nil, fmt.Errorf("failed to unmarshal token transfers: %w", err)
				}
			}

			transactions = append(transactions, &tx)
		}

		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("error iterating transactions: %w", err)
		}

		return transactions, nil
	}

	// For other queries, use regular query with potential caching
	return r.GetByAddress(ctx, address, filters)
}
