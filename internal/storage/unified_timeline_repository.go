package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/address-scanner/internal/models"
)

// UnifiedTimelineRepository handles pre-computed unified timeline storage in ClickHouse
// Requirement 3.2: Store unified timeline in ClickHouse
type UnifiedTimelineRepository struct {
	db *ClickHouseDB
}

// NewUnifiedTimelineRepository creates a new unified timeline repository
func NewUnifiedTimelineRepository(db *ClickHouseDB) *UnifiedTimelineRepository {
	return &UnifiedTimelineRepository{db: db}
}

// GetByAddress retrieves pre-computed unified timeline for an address
// This is faster than querying the transactions table directly as data is pre-sorted
func (r *UnifiedTimelineRepository) GetByAddress(ctx context.Context, address string, filters *TransactionFilters) ([]*models.Transaction, error) {
	// Validate and normalize address
	if err := ValidateAddress(address); err != nil {
		return nil, err
	}
	address = strings.ToLower(address)

	query := `
		SELECT hash, chain, address, from, to, value, timestamp, block_number,
			   status, gas_used, gas_price, token_transfers, method_id, input
		FROM unified_timeline
		WHERE address = ?
	`
	args := []interface{}{address}

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

	// The unified_timeline table is already ordered by (address, timestamp DESC, hash)
	// So we don't need to add ORDER BY unless we want different sorting
	// For now, we'll use the natural order (timestamp DESC)

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
		return nil, fmt.Errorf("failed to query unified timeline: %w", err)
	}
	defer rows.Close()

	var transactions []*models.Transaction
	for rows.Next() {
		var tx models.Transaction
		var tokenTransfersJSON string

		err := rows.Scan(
			&tx.Hash,
			&tx.Chain,
			&tx.Address,
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

// GetByAddresses retrieves pre-computed unified timeline for multiple addresses
// Used for portfolio queries
func (r *UnifiedTimelineRepository) GetByAddresses(ctx context.Context, addresses []string, filters *TransactionFilters) ([]*models.Transaction, error) {
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
		SELECT hash, chain, address, from, to, value, timestamp, block_number,
			   status, gas_used, gas_price, token_transfers, method_id, input
		FROM unified_timeline
		WHERE address IN (` + strings.Repeat("?,", len(normalizedAddresses)-1) + `?)
	`
	args := make([]interface{}, len(normalizedAddresses))
	for i, addr := range normalizedAddresses {
		args[i] = addr
	}

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

	// Add explicit ordering for multi-address queries
	query += " ORDER BY timestamp DESC, hash DESC"

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
		return nil, fmt.Errorf("failed to query unified timeline: %w", err)
	}
	defer rows.Close()

	var transactions []*models.Transaction
	for rows.Next() {
		var tx models.Transaction
		var tokenTransfersJSON string

		err := rows.Scan(
			&tx.Hash,
			&tx.Chain,
			&tx.Address,
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

// CountByAddress counts transactions in unified timeline for an address
func (r *UnifiedTimelineRepository) CountByAddress(ctx context.Context, address string, filters *TransactionFilters) (int64, error) {
	// Validate and normalize address
	if err := ValidateAddress(address); err != nil {
		return 0, err
	}
	address = strings.ToLower(address)

	query := `SELECT COUNT(*) FROM unified_timeline WHERE address = ?`
	args := []interface{}{address}

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

	var count int64
	row := r.db.Conn().QueryRow(ctx, query, args...)
	if err := row.Scan(&count); err != nil {
		return 0, fmt.Errorf("failed to count transactions: %w", err)
	}

	return count, nil
}

// RefreshForAddress manually refreshes the unified timeline for an address
// This is typically not needed as the materialized view handles updates automatically
// But can be useful for maintenance or recovery scenarios
func (r *UnifiedTimelineRepository) RefreshForAddress(ctx context.Context, address string) error {
	// Validate and normalize address
	if err := ValidateAddress(address); err != nil {
		return err
	}
	address = strings.ToLower(address)

	// Delete existing entries for this address
	deleteQuery := `ALTER TABLE unified_timeline DELETE WHERE address = ?`
	if err := r.db.Conn().Exec(ctx, deleteQuery, address); err != nil {
		return fmt.Errorf("failed to delete existing timeline entries: %w", err)
	}

	// Wait a moment for the delete to propagate
	time.Sleep(100 * time.Millisecond)

	// Insert fresh data from transactions table
	insertQuery := `
		INSERT INTO unified_timeline
		SELECT
			address,
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
			input,
			now() as created_at
		FROM transactions
		WHERE address = ?
	`
	if err := r.db.Conn().Exec(ctx, insertQuery, address); err != nil {
		return fmt.Errorf("failed to refresh timeline: %w", err)
	}

	return nil
}

// GetLatestTimestamp returns the most recent transaction timestamp for an address
// Used to determine if timeline needs updating
func (r *UnifiedTimelineRepository) GetLatestTimestamp(ctx context.Context, address string) (*time.Time, error) {
	// Validate and normalize address
	if err := ValidateAddress(address); err != nil {
		return nil, err
	}
	address = strings.ToLower(address)

	query := `
		SELECT MAX(timestamp) as latest
		FROM unified_timeline
		WHERE address = ?
	`

	var latest *time.Time
	row := r.db.Conn().QueryRow(ctx, query, address)
	if err := row.Scan(&latest); err != nil {
		return nil, fmt.Errorf("failed to get latest timestamp: %w", err)
	}

	return latest, nil
}
