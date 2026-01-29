package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/address-scanner/internal/models"
	"github.com/jackc/pgx/v5"
)

// BackfillJobRepository handles backfill job data persistence
type BackfillJobRepository struct {
	db *PostgresDB
}

// NewBackfillJobRepository creates a new backfill job repository
func NewBackfillJobRepository(db *PostgresDB) *BackfillJobRepository {
	return &BackfillJobRepository{db: db}
}

// Create creates a new backfill job record
func (r *BackfillJobRepository) Create(ctx context.Context, job *models.BackfillJobRecord) error {
	query := `
		INSERT INTO backfill_jobs (
			job_id, address, chain, tier, status, priority,
			transactions_fetched, started_at, completed_at, error, retry_count
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
	`

	_, err := r.db.Pool().Exec(ctx, query,
		job.JobID,
		job.Address,
		job.Chain,
		job.Tier,
		job.Status,
		job.Priority,
		job.TransactionsFetched,
		job.StartedAt,
		job.CompletedAt,
		job.Error,
		job.RetryCount,
	)

	if err != nil {
		return fmt.Errorf("failed to create backfill job: %w", err)
	}

	return nil
}

// GetByID retrieves a backfill job by ID
func (r *BackfillJobRepository) GetByID(ctx context.Context, jobID string) (*models.BackfillJobRecord, error) {
	query := `
		SELECT job_id, address, chain, tier, status, priority,
			   transactions_fetched, started_at, completed_at, error, retry_count
		FROM backfill_jobs
		WHERE job_id = $1
	`

	var job models.BackfillJobRecord
	var completedAt *time.Time
	var errorMsg *string

	err := r.db.Pool().QueryRow(ctx, query, jobID).Scan(
		&job.JobID,
		&job.Address,
		&job.Chain,
		&job.Tier,
		&job.Status,
		&job.Priority,
		&job.TransactionsFetched,
		&job.StartedAt,
		&completedAt,
		&errorMsg,
		&job.RetryCount,
	)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("backfill job not found: %s", jobID)
		}
		return nil, fmt.Errorf("failed to get backfill job: %w", err)
	}

	job.CompletedAt = completedAt
	job.Error = errorMsg

	return &job, nil
}

// Update updates an existing backfill job
func (r *BackfillJobRepository) Update(ctx context.Context, job *models.BackfillJobRecord) error {
	query := `
		UPDATE backfill_jobs
		SET address = $2, chain = $3, tier = $4, status = $5, priority = $6,
			transactions_fetched = $7, started_at = $8, completed_at = $9,
			error = $10, retry_count = $11
		WHERE job_id = $1
	`

	result, err := r.db.Pool().Exec(ctx, query,
		job.JobID,
		job.Address,
		job.Chain,
		job.Tier,
		job.Status,
		job.Priority,
		job.TransactionsFetched,
		job.StartedAt,
		job.CompletedAt,
		job.Error,
		job.RetryCount,
	)

	if err != nil {
		return fmt.Errorf("failed to update backfill job: %w", err)
	}

	if result.RowsAffected() == 0 {
		return fmt.Errorf("backfill job not found: %s", job.JobID)
	}

	return nil
}

// Delete deletes a backfill job
func (r *BackfillJobRepository) Delete(ctx context.Context, jobID string) error {
	query := `DELETE FROM backfill_jobs WHERE job_id = $1`

	result, err := r.db.Pool().Exec(ctx, query, jobID)
	if err != nil {
		return fmt.Errorf("failed to delete backfill job: %w", err)
	}

	if result.RowsAffected() == 0 {
		return fmt.Errorf("backfill job not found: %s", jobID)
	}

	return nil
}

// ListByStatus retrieves backfill jobs by status
func (r *BackfillJobRepository) ListByStatus(ctx context.Context, status string, limit int) ([]*models.BackfillJobRecord, error) {
	query := `
		SELECT job_id, address, chain, tier, status, priority,
			   transactions_fetched, started_at, completed_at, error, retry_count
		FROM backfill_jobs
		WHERE status = $1
		ORDER BY priority DESC, started_at ASC
		LIMIT $2
	`

	rows, err := r.db.Pool().Query(ctx, query, status, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to list backfill jobs: %w", err)
	}
	defer rows.Close()

	var jobs []*models.BackfillJobRecord
	for rows.Next() {
		var job models.BackfillJobRecord
		var completedAt *time.Time
		var errorMsg *string

		err := rows.Scan(
			&job.JobID,
			&job.Address,
			&job.Chain,
			&job.Tier,
			&job.Status,
			&job.Priority,
			&job.TransactionsFetched,
			&job.StartedAt,
			&completedAt,
			&errorMsg,
			&job.RetryCount,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan backfill job: %w", err)
		}

		job.CompletedAt = completedAt
		job.Error = errorMsg

		jobs = append(jobs, &job)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating backfill jobs: %w", err)
	}

	return jobs, nil
}

// ListByAddress retrieves backfill jobs for a specific address
func (r *BackfillJobRepository) ListByAddress(ctx context.Context, address string) ([]*models.BackfillJobRecord, error) {
	query := `
		SELECT job_id, address, chain, tier, status, priority,
			   transactions_fetched, started_at, completed_at, error, retry_count
		FROM backfill_jobs
		WHERE address = $1
		ORDER BY started_at DESC
	`

	rows, err := r.db.Pool().Query(ctx, query, address)
	if err != nil {
		return nil, fmt.Errorf("failed to list backfill jobs by address: %w", err)
	}
	defer rows.Close()

	var jobs []*models.BackfillJobRecord
	for rows.Next() {
		var job models.BackfillJobRecord
		var completedAt *time.Time
		var errorMsg *string

		err := rows.Scan(
			&job.JobID,
			&job.Address,
			&job.Chain,
			&job.Tier,
			&job.Status,
			&job.Priority,
			&job.TransactionsFetched,
			&job.StartedAt,
			&completedAt,
			&errorMsg,
			&job.RetryCount,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan backfill job: %w", err)
		}

		job.CompletedAt = completedAt
		job.Error = errorMsg

		jobs = append(jobs, &job)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating backfill jobs: %w", err)
	}

	return jobs, nil
}

// GetQueuedJobs retrieves queued jobs ordered by priority
func (r *BackfillJobRepository) GetQueuedJobs(ctx context.Context, limit int) ([]*models.BackfillJobRecord, error) {
	return r.ListByStatus(ctx, "queued", limit)
}

// UpdateStatus updates the status of a backfill job
func (r *BackfillJobRepository) UpdateStatus(ctx context.Context, jobID string, status string) error {
	query := `
		UPDATE backfill_jobs
		SET status = $2
		WHERE job_id = $1
	`

	result, err := r.db.Pool().Exec(ctx, query, jobID, status)
	if err != nil {
		return fmt.Errorf("failed to update backfill job status: %w", err)
	}

	if result.RowsAffected() == 0 {
		return fmt.Errorf("backfill job not found: %s", jobID)
	}

	return nil
}
