package storage

import (
	"context"
	"time"

	"github.com/address-scanner/internal/models"
)

// UnifiedTimelineRepository handles unified timeline queries
// With the new schema, this delegates to the main transactions table
type UnifiedTimelineRepository struct {
	db   *ClickHouseDB
	repo *TransactionRepository
}

// NewUnifiedTimelineRepository creates a new unified timeline repository
func NewUnifiedTimelineRepository(db *ClickHouseDB) *UnifiedTimelineRepository {
	return &UnifiedTimelineRepository{
		db:   db,
		repo: NewTransactionRepository(db),
	}
}

// GetByAddress retrieves transactions for an address
func (r *UnifiedTimelineRepository) GetByAddress(ctx context.Context, address string, filters *TransactionFilters) ([]*models.Transaction, error) {
	return r.repo.GetByAddress(ctx, address, filters)
}

// GetByAddresses retrieves transactions for multiple addresses
func (r *UnifiedTimelineRepository) GetByAddresses(ctx context.Context, addresses []string, filters *TransactionFilters) ([]*models.Transaction, error) {
	return r.repo.GetByAddresses(ctx, addresses, filters)
}

// CountByAddress counts transactions for an address
func (r *UnifiedTimelineRepository) CountByAddress(ctx context.Context, address string, filters *TransactionFilters) (int64, error) {
	return r.repo.CountByAddress(ctx, address, filters)
}

// RefreshForAddress is a no-op with the new schema
func (r *UnifiedTimelineRepository) RefreshForAddress(ctx context.Context, address string) error {
	return nil
}

// GetLatestTimestamp returns the most recent transaction timestamp for an address
func (r *UnifiedTimelineRepository) GetLatestTimestamp(ctx context.Context, address string) (*time.Time, error) {
	txs, err := r.repo.GetByAddress(ctx, address, &TransactionFilters{Limit: 1})
	if err != nil {
		return nil, err
	}
	if len(txs) == 0 {
		return nil, nil
	}
	return &txs[0].Timestamp, nil
}
