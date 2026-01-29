package models

import (
	"time"

	"github.com/address-scanner/internal/types"
)

// BackfillJobRecord represents a backfill job in the database (one per address per chain)
type BackfillJobRecord struct {
	JobID               string         `json:"jobId" db:"job_id"`
	Address             string         `json:"address" db:"address"`
	Chain               types.ChainID  `json:"chain" db:"chain"`
	Tier                types.UserTier `json:"tier" db:"tier"`
	Status              string         `json:"status" db:"status"` // queued, in_progress, completed, failed
	Priority            int            `json:"priority" db:"priority"`
	TransactionsFetched int64          `json:"transactionsFetched" db:"transactions_fetched"`
	StartedAt           time.Time      `json:"startedAt" db:"started_at"`
	CompletedAt         *time.Time     `json:"completedAt,omitempty" db:"completed_at"`
	Error               *string        `json:"error,omitempty" db:"error"`
	RetryCount          int            `json:"retryCount" db:"retry_count"`
}
