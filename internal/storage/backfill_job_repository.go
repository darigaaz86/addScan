package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/address-scanner/internal/models"
	"github.com/address-scanner/internal/types"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
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

// BatchCreate creates multiple backfill job records in a single transaction
func (r *BackfillJobRepository) BatchCreate(ctx context.Context, jobs []*models.BackfillJobRecord) error {
	if len(jobs) == 0 {
		return nil
	}

	tx, err := r.db.Pool().Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	query := `
		INSERT INTO backfill_jobs (
			job_id, address, chain, tier, status, priority,
			transactions_fetched, started_at, completed_at, error, retry_count
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
	`

	for _, job := range jobs {
		_, err := tx.Exec(ctx, query,
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
			return fmt.Errorf("failed to insert backfill job %s: %w", job.JobID, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
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

// GetQueuedJobsForChain retrieves queued jobs for a specific chain ordered by priority
func (r *BackfillJobRepository) GetQueuedJobsForChain(ctx context.Context, chain types.ChainID, limit int) ([]*models.BackfillJobRecord, error) {
	query := `
		SELECT job_id, address, chain, tier, status, priority,
			   transactions_fetched, started_at, completed_at, error, retry_count
		FROM backfill_jobs
		WHERE status = 'queued' AND chain = $1
		ORDER BY priority DESC, started_at ASC
		LIMIT $2
	`

	rows, err := r.db.Pool().Query(ctx, query, string(chain), limit)
	if err != nil {
		return nil, fmt.Errorf("failed to list queued jobs for chain %s: %w", chain, err)
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

// RequeueFailedJobs re-queues failed jobs for retry
// Optionally filter by chain. Pass empty string to re-queue all failed jobs.
// Returns the number of jobs re-queued.
func (r *BackfillJobRepository) RequeueFailedJobs(ctx context.Context, chain string) (int64, error) {
	var query string
	var result pgconn.CommandTag
	var err error

	if chain != "" {
		query = `
			UPDATE backfill_jobs
			SET status = 'queued', error = NULL, retry_count = 0, completed_at = NULL
			WHERE status = 'failed' AND chain = $1
		`
		result, err = r.db.Pool().Exec(ctx, query, chain)
	} else {
		query = `
			UPDATE backfill_jobs
			SET status = 'queued', error = NULL, retry_count = 0, completed_at = NULL
			WHERE status = 'failed'
		`
		result, err = r.db.Pool().Exec(ctx, query)
	}

	if err != nil {
		return 0, fmt.Errorf("failed to re-queue failed jobs: %w", err)
	}

	return result.RowsAffected(), nil
}

// RequeueTransientFailures re-queues jobs that failed with transient errors
// Only re-queues jobs that:
// - Have retry_count < maxRetries
// - Failed more than cooldown duration ago
// - Have transient error patterns (429, timeout, rate limit, connection)
// Returns the number of jobs re-queued.
func (r *BackfillJobRepository) RequeueTransientFailures(ctx context.Context, maxRetries int, cooldown time.Duration) (int64, error) {
	cutoff := time.Now().Add(-cooldown)

	query := `
		UPDATE backfill_jobs
		SET status = 'queued', completed_at = NULL
		WHERE status = 'failed'
		  AND retry_count < $1
		  AND completed_at < $2
		  AND (
		    error ILIKE '%429%'
		    OR error ILIKE '%timeout%'
		    OR error ILIKE '%rate limit%'
		    OR error ILIKE '%connection%'
		    OR error ILIKE '%temporary%'
		    OR error ILIKE '%too many requests%'
		  )
		  AND NOT (
		    error ILIKE '%not found%'
		    OR error ILIKE '%invalid address%'
		    OR error ILIKE '%unsupported%'
		    OR error ILIKE '%unauthorized%'
		  )
	`

	result, err := r.db.Pool().Exec(ctx, query, maxRetries, cutoff)
	if err != nil {
		return 0, fmt.Errorf("failed to re-queue transient failures: %w", err)
	}

	return result.RowsAffected(), nil
}

// DeleteOldFailedJobs deletes failed jobs older than the specified duration
// Returns the number of jobs deleted.
func (r *BackfillJobRepository) DeleteOldFailedJobs(ctx context.Context, olderThan time.Duration) (int64, error) {
	cutoff := time.Now().Add(-olderThan)

	query := `
		DELETE FROM backfill_jobs
		WHERE status = 'failed' AND completed_at < $1
	`

	result, err := r.db.Pool().Exec(ctx, query, cutoff)
	if err != nil {
		return 0, fmt.Errorf("failed to delete old failed jobs: %w", err)
	}

	return result.RowsAffected(), nil
}

// DeleteCompletedJobs deletes completed jobs older than the specified duration
// Returns the number of jobs deleted.
func (r *BackfillJobRepository) DeleteCompletedJobs(ctx context.Context, olderThan time.Duration) (int64, error) {
	cutoff := time.Now().Add(-olderThan)

	query := `
		DELETE FROM backfill_jobs
		WHERE status = 'completed' AND completed_at < $1
	`

	result, err := r.db.Pool().Exec(ctx, query, cutoff)
	if err != nil {
		return 0, fmt.Errorf("failed to delete old completed jobs: %w", err)
	}

	return result.RowsAffected(), nil
}

// GetJobStats returns job counts by status
func (r *BackfillJobRepository) GetJobStats(ctx context.Context) (map[string]int64, error) {
	query := `
		SELECT status, COUNT(*) as count
		FROM backfill_jobs
		GROUP BY status
	`

	rows, err := r.db.Pool().Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get job stats: %w", err)
	}
	defer rows.Close()

	stats := make(map[string]int64)
	for rows.Next() {
		var status string
		var count int64
		if err := rows.Scan(&status, &count); err != nil {
			return nil, fmt.Errorf("failed to scan job stats: %w", err)
		}
		stats[status] = count
	}

	return stats, rows.Err()
}

// ResetStaleInProgressJobs resets jobs stuck in "in_progress" status
// Jobs are considered stale if they've been in_progress for longer than the threshold
// This handles cases where the worker crashed while processing jobs
func (r *BackfillJobRepository) ResetStaleInProgressJobs(ctx context.Context, staleThreshold time.Duration) (int64, error) {
	cutoff := time.Now().Add(-staleThreshold)

	query := `
		UPDATE backfill_jobs
		SET status = 'queued'
		WHERE status = 'in_progress'
		  AND started_at < $1
	`

	result, err := r.db.Pool().Exec(ctx, query, cutoff)
	if err != nil {
		return 0, fmt.Errorf("failed to reset stale in_progress jobs: %w", err)
	}

	return result.RowsAffected(), nil
}
