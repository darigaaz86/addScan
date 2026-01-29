package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/address-scanner/internal/models"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// SnapshotRepository handles portfolio snapshot storage operations
type SnapshotRepository struct {
	pool *pgxpool.Pool
}

// NewSnapshotRepository creates a new snapshot repository
func NewSnapshotRepository(pool *pgxpool.Pool) *SnapshotRepository {
	return &SnapshotRepository{
		pool: pool,
	}
}

// Create stores a new portfolio snapshot
// Requirements: 16.2, 16.3
func (r *SnapshotRepository) Create(ctx context.Context, snapshot *models.PortfolioSnapshot) error {
	// Serialize JSON fields
	totalBalanceJSON, err := json.Marshal(snapshot.TotalBalance)
	if err != nil {
		return fmt.Errorf("failed to marshal total balance: %w", err)
	}

	topCounterpartiesJSON, err := json.Marshal(snapshot.TopCounterparties)
	if err != nil {
		return fmt.Errorf("failed to marshal top counterparties: %w", err)
	}

	tokenHoldingsJSON, err := json.Marshal(snapshot.TokenHoldings)
	if err != nil {
		return fmt.Errorf("failed to marshal token holdings: %w", err)
	}

	query := `
		INSERT INTO portfolio_snapshots (
			portfolio_id,
			snapshot_date,
			total_balance,
			transaction_count,
			total_volume,
			top_counterparties,
			token_holdings,
			created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (portfolio_id, snapshot_date) 
		DO UPDATE SET
			total_balance = EXCLUDED.total_balance,
			transaction_count = EXCLUDED.transaction_count,
			total_volume = EXCLUDED.total_volume,
			top_counterparties = EXCLUDED.top_counterparties,
			token_holdings = EXCLUDED.token_holdings,
			created_at = EXCLUDED.created_at
	`

	_, err = r.pool.Exec(
		ctx,
		query,
		snapshot.PortfolioID,
		snapshot.SnapshotDate,
		totalBalanceJSON,
		snapshot.TransactionCount,
		snapshot.TotalVolume,
		topCounterpartiesJSON,
		tokenHoldingsJSON,
		snapshot.CreatedAt,
	)

	if err != nil {
		return fmt.Errorf("failed to insert snapshot: %w", err)
	}

	return nil
}

// GetByPortfolioAndDateRange retrieves snapshots for a portfolio within a date range
// Requirements: 16.4
// Returns snapshots in chronological order
func (r *SnapshotRepository) GetByPortfolioAndDateRange(ctx context.Context, portfolioID string, from, to time.Time) ([]*models.PortfolioSnapshot, error) {
	query := `
		SELECT 
			portfolio_id,
			snapshot_date,
			total_balance,
			transaction_count,
			total_volume,
			top_counterparties,
			token_holdings,
			created_at
		FROM portfolio_snapshots
		WHERE portfolio_id = $1
			AND snapshot_date >= $2
			AND snapshot_date <= $3
		ORDER BY snapshot_date ASC
	`

	rows, err := r.pool.Query(ctx, query, portfolioID, from, to)
	if err != nil {
		return nil, fmt.Errorf("failed to query snapshots: %w", err)
	}
	defer rows.Close()

	var snapshots []*models.PortfolioSnapshot

	for rows.Next() {
		var snapshot models.PortfolioSnapshot
		var totalBalanceJSON []byte
		var topCounterpartiesJSON []byte
		var tokenHoldingsJSON []byte

		err := rows.Scan(
			&snapshot.PortfolioID,
			&snapshot.SnapshotDate,
			&totalBalanceJSON,
			&snapshot.TransactionCount,
			&snapshot.TotalVolume,
			&topCounterpartiesJSON,
			&tokenHoldingsJSON,
			&snapshot.CreatedAt,
		)

		if err != nil {
			return nil, fmt.Errorf("failed to scan snapshot row: %w", err)
		}

		// Deserialize JSON fields
		if err := json.Unmarshal(totalBalanceJSON, &snapshot.TotalBalance); err != nil {
			return nil, fmt.Errorf("failed to unmarshal total balance: %w", err)
		}

		if err := json.Unmarshal(topCounterpartiesJSON, &snapshot.TopCounterparties); err != nil {
			return nil, fmt.Errorf("failed to unmarshal top counterparties: %w", err)
		}

		if err := json.Unmarshal(tokenHoldingsJSON, &snapshot.TokenHoldings); err != nil {
			return nil, fmt.Errorf("failed to unmarshal token holdings: %w", err)
		}

		snapshots = append(snapshots, &snapshot)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating snapshot rows: %w", err)
	}

	return snapshots, nil
}

// DeleteOldSnapshots deletes snapshots older than the retention period
// Requirements: 16.6
func (r *SnapshotRepository) DeleteOldSnapshots(ctx context.Context, portfolioID string, retentionDays int) error {
	if retentionDays < 0 {
		// Negative retention means unlimited (no deletion)
		return nil
	}

	cutoffDate := time.Now().UTC().AddDate(0, 0, -retentionDays)

	query := `
		DELETE FROM portfolio_snapshots
		WHERE portfolio_id = $1
			AND snapshot_date < $2
	`

	result, err := r.pool.Exec(ctx, query, portfolioID, cutoffDate)
	if err != nil {
		return fmt.Errorf("failed to delete old snapshots: %w", err)
	}

	rowsAffected := result.RowsAffected()

	if rowsAffected > 0 {
		fmt.Printf("Deleted %d old snapshots for portfolio %s (older than %s)\n", rowsAffected, portfolioID, cutoffDate.Format("2006-01-02"))
	}

	return nil
}

// GetAllActivePortfolios retrieves all portfolio IDs that should have snapshots
// Requirements: 16.1, 16.5
func (r *SnapshotRepository) GetAllActivePortfolios(ctx context.Context) ([]string, error) {
	query := `
		SELECT DISTINCT id
		FROM portfolios
		ORDER BY id
	`

	rows, err := r.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query active portfolios: %w", err)
	}
	defer rows.Close()

	var portfolioIDs []string

	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("failed to scan portfolio ID: %w", err)
		}
		portfolioIDs = append(portfolioIDs, id)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating portfolio rows: %w", err)
	}

	return portfolioIDs, nil
}

// GetLatestSnapshot retrieves the most recent snapshot for a portfolio
func (r *SnapshotRepository) GetLatestSnapshot(ctx context.Context, portfolioID string) (*models.PortfolioSnapshot, error) {
	query := `
		SELECT 
			portfolio_id,
			snapshot_date,
			total_balance,
			transaction_count,
			total_volume,
			top_counterparties,
			token_holdings,
			created_at
		FROM portfolio_snapshots
		WHERE portfolio_id = $1
		ORDER BY snapshot_date DESC
		LIMIT 1
	`

	var snapshot models.PortfolioSnapshot
	var totalBalanceJSON []byte
	var topCounterpartiesJSON []byte
	var tokenHoldingsJSON []byte

	err := r.pool.QueryRow(ctx, query, portfolioID).Scan(
		&snapshot.PortfolioID,
		&snapshot.SnapshotDate,
		&totalBalanceJSON,
		&snapshot.TransactionCount,
		&snapshot.TotalVolume,
		&topCounterpartiesJSON,
		&tokenHoldingsJSON,
		&snapshot.CreatedAt,
	)

	if err == pgx.ErrNoRows {
		return nil, nil // No snapshot found
	}

	if err != nil {
		return nil, fmt.Errorf("failed to query latest snapshot: %w", err)
	}

	// Deserialize JSON fields
	if err := json.Unmarshal(totalBalanceJSON, &snapshot.TotalBalance); err != nil {
		return nil, fmt.Errorf("failed to unmarshal total balance: %w", err)
	}

	if err := json.Unmarshal(topCounterpartiesJSON, &snapshot.TopCounterparties); err != nil {
		return nil, fmt.Errorf("failed to unmarshal top counterparties: %w", err)
	}

	if err := json.Unmarshal(tokenHoldingsJSON, &snapshot.TokenHoldings); err != nil {
		return nil, fmt.Errorf("failed to unmarshal token holdings: %w", err)
	}

	return &snapshot, nil
}

// CountSnapshots returns the number of snapshots for a portfolio
func (r *SnapshotRepository) CountSnapshots(ctx context.Context, portfolioID string) (int64, error) {
	query := `
		SELECT COUNT(*)
		FROM portfolio_snapshots
		WHERE portfolio_id = $1
	`

	var count int64
	err := r.pool.QueryRow(ctx, query, portfolioID).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count snapshots: %w", err)
	}

	return count, nil
}
