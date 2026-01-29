-- Create addresses table (one row per address per chain)
CREATE TABLE IF NOT EXISTS addresses (
  address VARCHAR(66) NOT NULL,
  chain VARCHAR(50) NOT NULL,
  classification VARCHAR(10) NOT NULL DEFAULT 'normal' CHECK (classification IN ('normal', 'super')),
  transaction_count BIGINT NOT NULL DEFAULT 0,
  first_seen TIMESTAMP NOT NULL DEFAULT NOW(),
  last_activity TIMESTAMP NOT NULL DEFAULT NOW(),
  total_volume NUMERIC NOT NULL DEFAULT 0,
  labels TEXT[],
  -- Sync/backfill status fields
  backfill_complete BOOLEAN NOT NULL DEFAULT FALSE,
  backfill_tier VARCHAR(10) DEFAULT 'free' CHECK (backfill_tier IN ('free', 'paid')),
  backfill_tx_count INTEGER DEFAULT 0,
  last_synced_block BIGINT NOT NULL DEFAULT 0,
  last_sync_at TIMESTAMP,
  sync_errors INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (address, chain)
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_addresses_chain ON addresses(chain);
CREATE INDEX IF NOT EXISTS idx_addresses_classification ON addresses(classification);
CREATE INDEX IF NOT EXISTS idx_addresses_last_activity ON addresses(last_activity DESC);
CREATE INDEX IF NOT EXISTS idx_addresses_backfill ON addresses(backfill_complete, address);
CREATE INDEX IF NOT EXISTS idx_addresses_last_sync ON addresses(last_sync_at) WHERE last_sync_at IS NOT NULL;
