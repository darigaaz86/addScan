package models

import (
	"time"

	"github.com/address-scanner/internal/types"
)

// SyncStatus represents the synchronization status for an address on a specific chain
type SyncStatus struct {
	Address          string         `json:"address" db:"address"`
	Chain            types.ChainID  `json:"chain" db:"chain"`
	LastSyncedBlock  uint64         `json:"lastSyncedBlock" db:"last_synced_block"`
	CurrentBlock     uint64         `json:"currentBlock" db:"current_block"`
	BackfillComplete bool           `json:"backfillComplete" db:"backfill_complete"`
	BackfillProgress *int           `json:"backfillProgress,omitempty" db:"backfill_progress"`
	LastSyncAt       time.Time      `json:"lastSyncAt" db:"last_sync_at"`
	SyncErrors       *int           `json:"syncErrors,omitempty" db:"sync_errors"`
	NextSyncAt       *time.Time     `json:"nextSyncAt,omitempty" db:"next_sync_at"`
}
