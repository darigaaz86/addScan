package models

import (
	"time"

	"github.com/address-scanner/internal/types"
)

// Address represents a blockchain address being tracked on a specific chain
// One row per address per chain (composite primary key: address + chain)
type Address struct {
	Address          string                      `json:"address" db:"address"`
	Chain            types.ChainID               `json:"chain" db:"chain"`
	Classification   types.AddressClassification `json:"classification" db:"classification"`
	TransactionCount int64                       `json:"transactionCount" db:"transaction_count"`
	FirstSeen        time.Time                   `json:"firstSeen" db:"first_seen"`
	LastActivity     time.Time                   `json:"lastActivity" db:"last_activity"`
	TotalVolume      string                      `json:"totalVolume" db:"total_volume"`
	Labels           []string                    `json:"labels,omitempty" db:"labels"`
	// Sync/backfill status fields
	BackfillComplete bool           `json:"backfillComplete" db:"backfill_complete"`
	BackfillTier     types.UserTier `json:"backfillTier" db:"backfill_tier"`        // Tier used for backfill (free/paid)
	BackfillTxCount  int            `json:"backfillTxCount" db:"backfill_tx_count"` // Number of transactions backfilled
	LastSyncedBlock  uint64         `json:"lastSyncedBlock" db:"last_synced_block"`
	LastSyncAt       *time.Time     `json:"lastSyncAt,omitempty" db:"last_sync_at"`
	SyncErrors       int            `json:"syncErrors" db:"sync_errors"`
}
