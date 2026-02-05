package storage

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/address-scanner/internal/models"
	"github.com/address-scanner/internal/types"
	"github.com/jackc/pgx/v5"
)

// Ethereum address regex pattern (0x followed by 40 hexadecimal characters)
var ethereumAddressRegex = regexp.MustCompile(`^0x[a-fA-F0-9]{40}$`)

// AddressRepository handles address data persistence
type AddressRepository struct {
	db *PostgresDB
}

// NewAddressRepository creates a new address repository
func NewAddressRepository(db *PostgresDB) *AddressRepository {
	return &AddressRepository{db: db}
}

// ValidateAddress validates an Ethereum address format
func ValidateAddress(address string) error {
	if !ethereumAddressRegex.MatchString(address) {
		return &types.ServiceError{
			Code:    "INVALID_ADDRESS_FORMAT",
			Message: fmt.Sprintf("invalid address format: %s (must be 0x followed by 40 hexadecimal characters)", address),
			Details: map[string]any{
				"address": address,
				"format":  "0x[a-fA-F0-9]{40}",
			},
		}
	}
	return nil
}

// Create creates a new address record for a specific chain
func (r *AddressRepository) Create(ctx context.Context, address *models.Address) error {
	if err := ValidateAddress(address.Address); err != nil {
		return err
	}
	address.Address = strings.ToLower(address.Address)

	if address.Classification == "" {
		address.Classification = types.ClassificationNormal
	}
	if address.TotalVolume == "" {
		address.TotalVolume = "0"
	}
	if address.BackfillTier == "" {
		address.BackfillTier = types.TierFree
	}

	query := `
		INSERT INTO addresses (
			address, chain, classification, transaction_count,
			first_seen, last_activity, total_volume, labels,
			backfill_complete, backfill_tier, backfill_tx_count, last_synced_block, last_sync_at, sync_errors
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
	`

	_, err := r.db.Pool().Exec(ctx, query,
		address.Address,
		address.Chain,
		address.Classification,
		address.TransactionCount,
		address.FirstSeen,
		address.LastActivity,
		address.TotalVolume,
		address.Labels,
		address.BackfillComplete,
		address.BackfillTier,
		address.BackfillTxCount,
		address.LastSyncedBlock,
		address.LastSyncAt,
		address.SyncErrors,
	)

	if err != nil {
		return fmt.Errorf("failed to create address: %w", err)
	}
	return nil
}

// Get retrieves an address by address string and chain
func (r *AddressRepository) Get(ctx context.Context, address string, chain types.ChainID) (*models.Address, error) {
	if err := ValidateAddress(address); err != nil {
		return nil, err
	}
	address = strings.ToLower(address)

	query := `
		SELECT address, chain, classification, transaction_count,
			   first_seen, last_activity, total_volume, labels,
			   backfill_complete, backfill_tier, backfill_tx_count, last_synced_block, last_sync_at, sync_errors
		FROM addresses
		WHERE address = $1 AND chain = $2
	`

	var addr models.Address
	err := r.db.Pool().QueryRow(ctx, query, address, chain).Scan(
		&addr.Address,
		&addr.Chain,
		&addr.Classification,
		&addr.TransactionCount,
		&addr.FirstSeen,
		&addr.LastActivity,
		&addr.TotalVolume,
		&addr.Labels,
		&addr.BackfillComplete,
		&addr.BackfillTier,
		&addr.BackfillTxCount,
		&addr.LastSyncedBlock,
		&addr.LastSyncAt,
		&addr.SyncErrors,
	)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil // Not found, return nil without error
		}
		return nil, fmt.Errorf("failed to get address: %w", err)
	}
	return &addr, nil
}

// GetByAddress retrieves all chain records for an address
func (r *AddressRepository) GetByAddress(ctx context.Context, address string) ([]*models.Address, error) {
	if err := ValidateAddress(address); err != nil {
		return nil, err
	}
	address = strings.ToLower(address)

	query := `
		SELECT address, chain, classification, transaction_count,
			   first_seen, last_activity, total_volume, labels,
			   backfill_complete, backfill_tier, backfill_tx_count, last_synced_block, last_sync_at, sync_errors
		FROM addresses
		WHERE address = $1
		ORDER BY chain
	`

	rows, err := r.db.Pool().Query(ctx, query, address)
	if err != nil {
		return nil, fmt.Errorf("failed to get addresses: %w", err)
	}
	defer rows.Close()

	var addresses []*models.Address
	for rows.Next() {
		var addr models.Address
		err := rows.Scan(
			&addr.Address,
			&addr.Chain,
			&addr.Classification,
			&addr.TransactionCount,
			&addr.FirstSeen,
			&addr.LastActivity,
			&addr.TotalVolume,
			&addr.Labels,
			&addr.BackfillComplete,
			&addr.BackfillTier,
			&addr.BackfillTxCount,
			&addr.LastSyncedBlock,
			&addr.LastSyncAt,
			&addr.SyncErrors,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan address: %w", err)
		}
		addresses = append(addresses, &addr)
	}
	return addresses, rows.Err()
}

// Update updates an existing address record
func (r *AddressRepository) Update(ctx context.Context, address *models.Address) error {
	if err := ValidateAddress(address.Address); err != nil {
		return err
	}
	address.Address = strings.ToLower(address.Address)

	query := `
		UPDATE addresses
		SET classification = $3, transaction_count = $4,
			first_seen = $5, last_activity = $6, total_volume = $7, labels = $8,
			backfill_complete = $9, backfill_tier = $10, backfill_tx_count = $11,
			last_synced_block = $12, last_sync_at = $13, sync_errors = $14
		WHERE address = $1 AND chain = $2
	`

	result, err := r.db.Pool().Exec(ctx, query,
		address.Address,
		address.Chain,
		address.Classification,
		address.TransactionCount,
		address.FirstSeen,
		address.LastActivity,
		address.TotalVolume,
		address.Labels,
		address.BackfillComplete,
		address.BackfillTier,
		address.BackfillTxCount,
		address.LastSyncedBlock,
		address.LastSyncAt,
		address.SyncErrors,
	)

	if err != nil {
		return fmt.Errorf("failed to update address: %w", err)
	}
	if result.RowsAffected() == 0 {
		return fmt.Errorf("address not found: %s on chain %s", address.Address, address.Chain)
	}
	return nil
}

// Upsert creates or updates an address record
func (r *AddressRepository) Upsert(ctx context.Context, address *models.Address) error {
	if err := ValidateAddress(address.Address); err != nil {
		return err
	}
	address.Address = strings.ToLower(address.Address)

	if address.Classification == "" {
		address.Classification = types.ClassificationNormal
	}
	if address.TotalVolume == "" {
		address.TotalVolume = "0"
	}
	if address.BackfillTier == "" {
		address.BackfillTier = types.TierFree
	}

	query := `
		INSERT INTO addresses (
			address, chain, classification, transaction_count,
			first_seen, last_activity, total_volume, labels,
			backfill_complete, backfill_tier, backfill_tx_count, last_synced_block, last_sync_at, sync_errors
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
		ON CONFLICT (address, chain)
		DO UPDATE SET
			classification = EXCLUDED.classification,
			transaction_count = EXCLUDED.transaction_count,
			last_activity = EXCLUDED.last_activity,
			total_volume = EXCLUDED.total_volume,
			labels = EXCLUDED.labels,
			backfill_complete = EXCLUDED.backfill_complete,
			backfill_tier = EXCLUDED.backfill_tier,
			backfill_tx_count = EXCLUDED.backfill_tx_count,
			last_synced_block = EXCLUDED.last_synced_block,
			last_sync_at = EXCLUDED.last_sync_at,
			sync_errors = EXCLUDED.sync_errors
	`

	_, err := r.db.Pool().Exec(ctx, query,
		address.Address,
		address.Chain,
		address.Classification,
		address.TransactionCount,
		address.FirstSeen,
		address.LastActivity,
		address.TotalVolume,
		address.Labels,
		address.BackfillComplete,
		address.BackfillTier,
		address.BackfillTxCount,
		address.LastSyncedBlock,
		address.LastSyncAt,
		address.SyncErrors,
	)

	if err != nil {
		return fmt.Errorf("failed to upsert address: %w", err)
	}
	return nil
}

// Delete deletes an address record for a specific chain
func (r *AddressRepository) Delete(ctx context.Context, address string, chain types.ChainID) error {
	if err := ValidateAddress(address); err != nil {
		return err
	}
	address = strings.ToLower(address)

	query := `DELETE FROM addresses WHERE address = $1 AND chain = $2`
	result, err := r.db.Pool().Exec(ctx, query, address, chain)
	if err != nil {
		return fmt.Errorf("failed to delete address: %w", err)
	}
	if result.RowsAffected() == 0 {
		return fmt.Errorf("address not found: %s on chain %s", address, chain)
	}
	return nil
}

// Exists checks if an address exists (on any chain)
func (r *AddressRepository) Exists(ctx context.Context, address string) (bool, error) {
	if err := ValidateAddress(address); err != nil {
		return false, err
	}
	address = strings.ToLower(address)

	var exists bool
	query := `SELECT EXISTS(SELECT 1 FROM addresses WHERE address = $1)`
	err := r.db.Pool().QueryRow(ctx, query, address).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check address existence: %w", err)
	}
	return exists, nil
}

// ExistsOnChain checks if an address exists on a specific chain
func (r *AddressRepository) ExistsOnChain(ctx context.Context, address string, chain types.ChainID) (bool, error) {
	if err := ValidateAddress(address); err != nil {
		return false, err
	}
	address = strings.ToLower(address)

	var exists bool
	query := `SELECT EXISTS(SELECT 1 FROM addresses WHERE address = $1 AND chain = $2)`
	err := r.db.Pool().QueryRow(ctx, query, address, chain).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check address existence: %w", err)
	}
	return exists, nil
}

// List retrieves addresses with optional filters and pagination
func (r *AddressRepository) List(ctx context.Context, filters *AddressFilters) ([]*models.Address, error) {
	query := `
		SELECT address, chain, classification, transaction_count,
			   first_seen, last_activity, total_volume, labels,
			   backfill_complete, backfill_tier, backfill_tx_count, last_synced_block, last_sync_at, sync_errors
		FROM addresses
		WHERE 1=1
	`
	args := []any{}
	argPos := 1

	if filters != nil {
		if filters.Chain != nil {
			query += fmt.Sprintf(" AND chain = $%d", argPos)
			args = append(args, string(*filters.Chain))
			argPos++
		}
		if filters.Classification != nil {
			query += fmt.Sprintf(" AND classification = $%d", argPos)
			args = append(args, *filters.Classification)
			argPos++
		}
		if filters.BackfillComplete != nil {
			query += fmt.Sprintf(" AND backfill_complete = $%d", argPos)
			args = append(args, *filters.BackfillComplete)
			argPos++
		}
	}

	query += " ORDER BY last_activity DESC"

	if filters != nil {
		if filters.Limit > 0 {
			query += fmt.Sprintf(" LIMIT $%d", argPos)
			args = append(args, filters.Limit)
			argPos++
		}
		if filters.Offset > 0 {
			query += fmt.Sprintf(" OFFSET $%d", argPos)
			args = append(args, filters.Offset)
		}
	}

	rows, err := r.db.Pool().Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to list addresses: %w", err)
	}
	defer rows.Close()

	var addresses []*models.Address
	for rows.Next() {
		var addr models.Address
		err := rows.Scan(
			&addr.Address,
			&addr.Chain,
			&addr.Classification,
			&addr.TransactionCount,
			&addr.FirstSeen,
			&addr.LastActivity,
			&addr.TotalVolume,
			&addr.Labels,
			&addr.BackfillComplete,
			&addr.BackfillTier,
			&addr.BackfillTxCount,
			&addr.LastSyncedBlock,
			&addr.LastSyncAt,
			&addr.SyncErrors,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan address: %w", err)
		}
		addresses = append(addresses, &addr)
	}
	return addresses, rows.Err()
}

// ListAll retrieves all tracked addresses (for snapshot worker)
func (r *AddressRepository) ListAll(ctx context.Context) ([]*models.Address, error) {
	return r.List(ctx, nil)
}

// MarkBackfillComplete marks backfill as complete for an address on a chain
func (r *AddressRepository) MarkBackfillComplete(ctx context.Context, address string, chain types.ChainID, tier types.UserTier, txCount int) error {
	if err := ValidateAddress(address); err != nil {
		return err
	}
	address = strings.ToLower(address)

	query := `
		UPDATE addresses
		SET backfill_complete = true, backfill_tier = $3, backfill_tx_count = $4
		WHERE address = $1 AND chain = $2
	`
	result, err := r.db.Pool().Exec(ctx, query, address, chain, tier, txCount)
	if err != nil {
		return fmt.Errorf("failed to mark backfill complete: %w", err)
	}
	if result.RowsAffected() == 0 {
		return fmt.Errorf("address not found: %s on chain %s", address, chain)
	}
	return nil
}

// NeedsRebackfill checks if an address needs re-backfill due to tier upgrade
func (r *AddressRepository) NeedsRebackfill(ctx context.Context, address string, chain types.ChainID, currentTier types.UserTier) (bool, error) {
	addr, err := r.Get(ctx, address, chain)
	if err != nil {
		return false, err
	}
	if addr == nil {
		return false, nil // Address doesn't exist
	}

	// If backfill was done with free tier and user is now paid, needs re-backfill
	if addr.BackfillComplete && addr.BackfillTier == types.TierFree && currentTier == types.TierPaid {
		return true, nil
	}
	return false, nil
}

// UpdateLastSyncedBlock updates the last synced block for an address
func (r *AddressRepository) UpdateLastSyncedBlock(ctx context.Context, address string, chain types.ChainID, blockNumber uint64) error {
	if err := ValidateAddress(address); err != nil {
		return err
	}
	address = strings.ToLower(address)

	now := time.Now()
	query := `
		UPDATE addresses
		SET last_synced_block = $3, last_sync_at = $4, last_activity = $4
		WHERE address = $1 AND chain = $2
	`
	result, err := r.db.Pool().Exec(ctx, query, address, chain, blockNumber, now)
	if err != nil {
		return fmt.Errorf("failed to update last synced block: %w", err)
	}

	// If no rows affected, the address is not tracked - this is not an error
	// We should NOT auto-create addresses here as that would pollute the tracking list
	if result.RowsAffected() == 0 {
		return fmt.Errorf("address not tracked: %s on chain %s", address, chain)
	}
	return nil
}

// UpdateClassification updates an address classification
func (r *AddressRepository) UpdateClassification(ctx context.Context, address string, chain types.ChainID, classification types.AddressClassification) error {
	if err := ValidateAddress(address); err != nil {
		return err
	}
	address = strings.ToLower(address)

	query := `UPDATE addresses SET classification = $3 WHERE address = $1 AND chain = $2`
	result, err := r.db.Pool().Exec(ctx, query, address, chain, classification)
	if err != nil {
		return fmt.Errorf("failed to update classification: %w", err)
	}
	if result.RowsAffected() == 0 {
		return fmt.Errorf("address not found: %s on chain %s", address, chain)
	}
	return nil
}

// IncrementSyncErrors increments the sync error count
func (r *AddressRepository) IncrementSyncErrors(ctx context.Context, address string, chain types.ChainID) error {
	if err := ValidateAddress(address); err != nil {
		return err
	}
	address = strings.ToLower(address)

	query := `UPDATE addresses SET sync_errors = sync_errors + 1 WHERE address = $1 AND chain = $2`
	_, err := r.db.Pool().Exec(ctx, query, address, chain)
	return err
}

// AddressFilters defines filters for listing addresses
type AddressFilters struct {
	Chain            *types.ChainID
	Classification   *types.AddressClassification
	BackfillComplete *bool
	Limit            int
	Offset           int
}

// UpdateSyncStatus updates the sync status fields for an address on a chain
func (r *AddressRepository) UpdateSyncStatus(ctx context.Context, address string, chain types.ChainID, lastSyncedBlock uint64, backfillComplete bool) error {
	if err := ValidateAddress(address); err != nil {
		return err
	}
	address = strings.ToLower(address)

	now := time.Now()
	query := `
		UPDATE addresses
		SET last_synced_block = $3, last_sync_at = $4, backfill_complete = $5, last_activity = $4
		WHERE address = $1 AND chain = $2
	`
	result, err := r.db.Pool().Exec(ctx, query, address, chain, lastSyncedBlock, now, backfillComplete)
	if err != nil {
		return fmt.Errorf("failed to update sync status: %w", err)
	}
	if result.RowsAffected() == 0 {
		return fmt.Errorf("address not found: %s on chain %s", address, chain)
	}
	return nil
}
