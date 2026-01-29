package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/address-scanner/internal/models"
	"github.com/address-scanner/internal/types"
	"github.com/jackc/pgx/v5"
)

// SyncStatusRepository handles sync status data persistence
type SyncStatusRepository struct {
	db *PostgresDB
}

// NewSyncStatusRepository creates a new sync status repository
func NewSyncStatusRepository(db *PostgresDB) *SyncStatusRepository {
	return &SyncStatusRepository{db: db}
}

// DB returns the underlying database connection for raw queries
func (r *SyncStatusRepository) DB() *PostgresDB {
	return r.db
}

// Create creates a new sync status record
func (r *SyncStatusRepository) Create(ctx context.Context, status *models.SyncStatus) error {
	// Validate address format
	if err := ValidateAddress(status.Address); err != nil {
		return err
	}
	status.Address = strings.ToLower(status.Address)

	query := `
		INSERT INTO sync_status (
			address, chain, last_synced_block, current_block,
			backfill_complete, backfill_progress, last_sync_at,
			sync_errors, next_sync_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`

	_, err := r.db.Pool().Exec(ctx, query,
		status.Address,
		status.Chain,
		status.LastSyncedBlock,
		status.CurrentBlock,
		status.BackfillComplete,
		status.BackfillProgress,
		status.LastSyncAt,
		status.SyncErrors,
		status.NextSyncAt,
	)

	if err != nil {
		return fmt.Errorf("failed to create sync status: %w", err)
	}

	return nil
}

// Get retrieves sync status for an address and chain
func (r *SyncStatusRepository) Get(ctx context.Context, address string, chain types.ChainID) (*models.SyncStatus, error) {
	// Validate and normalize address
	if err := ValidateAddress(address); err != nil {
		return nil, err
	}
	address = strings.ToLower(address)

	query := `
		SELECT address, chain, last_synced_block, current_block,
			   backfill_complete, backfill_progress, last_sync_at,
			   sync_errors, next_sync_at
		FROM sync_status
		WHERE address = $1 AND chain = $2
	`

	var status models.SyncStatus

	err := r.db.Pool().QueryRow(ctx, query, address, chain).Scan(
		&status.Address,
		&status.Chain,
		&status.LastSyncedBlock,
		&status.CurrentBlock,
		&status.BackfillComplete,
		&status.BackfillProgress,
		&status.LastSyncAt,
		&status.SyncErrors,
		&status.NextSyncAt,
	)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("sync status not found for address %s on chain %s", address, chain)
		}
		return nil, fmt.Errorf("failed to get sync status: %w", err)
	}

	return &status, nil
}

// Update updates an existing sync status record
func (r *SyncStatusRepository) Update(ctx context.Context, status *models.SyncStatus) error {
	// Validate and normalize address
	if err := ValidateAddress(status.Address); err != nil {
		return err
	}
	status.Address = strings.ToLower(status.Address)

	query := `
		UPDATE sync_status
		SET last_synced_block = $3, current_block = $4,
			backfill_complete = $5, backfill_progress = $6,
			last_sync_at = $7, sync_errors = $8, next_sync_at = $9
		WHERE address = $1 AND chain = $2
	`

	result, err := r.db.Pool().Exec(ctx, query,
		status.Address,
		status.Chain,
		status.LastSyncedBlock,
		status.CurrentBlock,
		status.BackfillComplete,
		status.BackfillProgress,
		status.LastSyncAt,
		status.SyncErrors,
		status.NextSyncAt,
	)

	if err != nil {
		return fmt.Errorf("failed to update sync status: %w", err)
	}

	if result.RowsAffected() == 0 {
		return fmt.Errorf("sync status not found for address %s on chain %s", status.Address, status.Chain)
	}

	return nil
}

// Upsert creates or updates a sync status record
func (r *SyncStatusRepository) Upsert(ctx context.Context, status *models.SyncStatus) error {
	// Validate and normalize address
	if err := ValidateAddress(status.Address); err != nil {
		return err
	}
	status.Address = strings.ToLower(status.Address)

	query := `
		INSERT INTO sync_status (
			address, chain, last_synced_block, current_block,
			backfill_complete, backfill_progress, last_sync_at,
			sync_errors, next_sync_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (address, chain)
		DO UPDATE SET
			last_synced_block = EXCLUDED.last_synced_block,
			current_block = EXCLUDED.current_block,
			backfill_complete = EXCLUDED.backfill_complete,
			backfill_progress = EXCLUDED.backfill_progress,
			last_sync_at = EXCLUDED.last_sync_at,
			sync_errors = EXCLUDED.sync_errors,
			next_sync_at = EXCLUDED.next_sync_at
	`

	_, err := r.db.Pool().Exec(ctx, query,
		status.Address,
		status.Chain,
		status.LastSyncedBlock,
		status.CurrentBlock,
		status.BackfillComplete,
		status.BackfillProgress,
		status.LastSyncAt,
		status.SyncErrors,
		status.NextSyncAt,
	)

	if err != nil {
		return fmt.Errorf("failed to upsert sync status: %w", err)
	}

	return nil
}

// UpdateLastSyncedBlock updates the last synced block for an address
func (r *SyncStatusRepository) UpdateLastSyncedBlock(ctx context.Context, address string, chain types.ChainID, blockNumber uint64) error {
	// Validate and normalize address
	if err := ValidateAddress(address); err != nil {
		return err
	}
	address = strings.ToLower(address)

	// Try to update existing record
	query := `
		UPDATE sync_status
		SET last_synced_block = $3, current_block = $3, last_sync_at = $4
		WHERE address = $1 AND chain = $2
	`

	result, err := r.db.Pool().Exec(ctx, query, address, chain, blockNumber, time.Now())
	if err != nil {
		return fmt.Errorf("failed to update last synced block: %w", err)
	}

	// If no rows affected, create new record
	if result.RowsAffected() == 0 {
		status := &models.SyncStatus{
			Address:          address,
			Chain:            chain,
			LastSyncedBlock:  blockNumber,
			CurrentBlock:     blockNumber,
			BackfillComplete: false,
			LastSyncAt:       time.Now(),
		}
		return r.Create(ctx, status)
	}

	return nil
}

// MarkBackfillComplete marks backfill as complete for an address
func (r *SyncStatusRepository) MarkBackfillComplete(ctx context.Context, address string, chain types.ChainID) error {
	// Validate and normalize address
	if err := ValidateAddress(address); err != nil {
		return err
	}
	address = strings.ToLower(address)

	query := `
		UPDATE sync_status
		SET backfill_complete = true, backfill_progress = 100
		WHERE address = $1 AND chain = $2
	`

	result, err := r.db.Pool().Exec(ctx, query, address, chain)
	if err != nil {
		return fmt.Errorf("failed to mark backfill complete: %w", err)
	}

	if result.RowsAffected() == 0 {
		return fmt.Errorf("sync status not found for address %s on chain %s", address, chain)
	}

	return nil
}

// UpdateBackfillProgress updates backfill progress for an address
func (r *SyncStatusRepository) UpdateBackfillProgress(ctx context.Context, address string, chain types.ChainID, progress int) error {
	// Validate and normalize address
	if err := ValidateAddress(address); err != nil {
		return err
	}
	address = strings.ToLower(address)

	// Validate progress range
	if progress < 0 || progress > 100 {
		return fmt.Errorf("backfill progress must be between 0 and 100, got %d", progress)
	}

	query := `
		UPDATE sync_status
		SET backfill_progress = $3
		WHERE address = $1 AND chain = $2
	`

	result, err := r.db.Pool().Exec(ctx, query, address, chain, progress)
	if err != nil {
		return fmt.Errorf("failed to update backfill progress: %w", err)
	}

	if result.RowsAffected() == 0 {
		return fmt.Errorf("sync status not found for address %s on chain %s", address, chain)
	}

	return nil
}

// IncrementSyncErrors increments the sync error count for an address
func (r *SyncStatusRepository) IncrementSyncErrors(ctx context.Context, address string, chain types.ChainID) error {
	// Validate and normalize address
	if err := ValidateAddress(address); err != nil {
		return err
	}
	address = strings.ToLower(address)

	query := `
		UPDATE sync_status
		SET sync_errors = sync_errors + 1
		WHERE address = $1 AND chain = $2
	`

	result, err := r.db.Pool().Exec(ctx, query, address, chain)
	if err != nil {
		return fmt.Errorf("failed to increment sync errors: %w", err)
	}

	if result.RowsAffected() == 0 {
		return fmt.Errorf("sync status not found for address %s on chain %s", address, chain)
	}

	return nil
}

// ResetSyncErrors resets the sync error count for an address
func (r *SyncStatusRepository) ResetSyncErrors(ctx context.Context, address string, chain types.ChainID) error {
	// Validate and normalize address
	if err := ValidateAddress(address); err != nil {
		return err
	}
	address = strings.ToLower(address)

	query := `
		UPDATE sync_status
		SET sync_errors = 0
		WHERE address = $1 AND chain = $2
	`

	result, err := r.db.Pool().Exec(ctx, query, address, chain)
	if err != nil {
		return fmt.Errorf("failed to reset sync errors: %w", err)
	}

	if result.RowsAffected() == 0 {
		return fmt.Errorf("sync status not found for address %s on chain %s", address, chain)
	}

	return nil
}

// ListByChain retrieves all sync statuses for a specific chain
func (r *SyncStatusRepository) ListByChain(ctx context.Context, chain types.ChainID) ([]*models.SyncStatus, error) {
	query := `
		SELECT address, chain, last_synced_block, current_block,
			   backfill_complete, backfill_progress, last_sync_at,
			   sync_errors, next_sync_at
		FROM sync_status
		WHERE chain = $1
		ORDER BY last_sync_at DESC
	`

	rows, err := r.db.Pool().Query(ctx, query, chain)
	if err != nil {
		return nil, fmt.Errorf("failed to list sync statuses: %w", err)
	}
	defer rows.Close()

	var statuses []*models.SyncStatus
	for rows.Next() {
		var status models.SyncStatus

		err := rows.Scan(
			&status.Address,
			&status.Chain,
			&status.LastSyncedBlock,
			&status.CurrentBlock,
			&status.BackfillComplete,
			&status.BackfillProgress,
			&status.LastSyncAt,
			&status.SyncErrors,
			&status.NextSyncAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan sync status: %w", err)
		}

		statuses = append(statuses, &status)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating sync statuses: %w", err)
	}

	return statuses, nil
}

// ListByAddress retrieves all sync statuses for a specific address across all chains
func (r *SyncStatusRepository) ListByAddress(ctx context.Context, address string) ([]*models.SyncStatus, error) {
	// Validate and normalize address
	if err := ValidateAddress(address); err != nil {
		return nil, err
	}
	address = strings.ToLower(address)

	query := `
		SELECT address, chain, last_synced_block, current_block,
			   backfill_complete, backfill_progress, last_sync_at,
			   sync_errors, next_sync_at
		FROM sync_status
		WHERE address = $1
		ORDER BY chain
	`

	rows, err := r.db.Pool().Query(ctx, query, address)
	if err != nil {
		return nil, fmt.Errorf("failed to list sync statuses: %w", err)
	}
	defer rows.Close()

	var statuses []*models.SyncStatus
	for rows.Next() {
		var status models.SyncStatus

		err := rows.Scan(
			&status.Address,
			&status.Chain,
			&status.LastSyncedBlock,
			&status.CurrentBlock,
			&status.BackfillComplete,
			&status.BackfillProgress,
			&status.LastSyncAt,
			&status.SyncErrors,
			&status.NextSyncAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan sync status: %w", err)
		}

		statuses = append(statuses, &status)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating sync statuses: %w", err)
	}

	return statuses, nil
}

// Delete deletes a sync status record
func (r *SyncStatusRepository) Delete(ctx context.Context, address string, chain types.ChainID) error {
	// Validate and normalize address
	if err := ValidateAddress(address); err != nil {
		return err
	}
	address = strings.ToLower(address)

	query := `DELETE FROM sync_status WHERE address = $1 AND chain = $2`

	result, err := r.db.Pool().Exec(ctx, query, address, chain)
	if err != nil {
		return fmt.Errorf("failed to delete sync status: %w", err)
	}

	if result.RowsAffected() == 0 {
		return fmt.Errorf("sync status not found for address %s on chain %s", address, chain)
	}

	return nil
}
