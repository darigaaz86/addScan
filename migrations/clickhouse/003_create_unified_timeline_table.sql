-- Create unified timeline table for pre-computed multi-chain timelines
-- This table stores merged and sorted transactions for faster retrieval
-- Requirement 3.2: Store unified timeline in ClickHouse

CREATE TABLE IF NOT EXISTS unified_timeline (
  address String,
  hash String,
  chain String,
  from String,
  to String,
  value String,
  timestamp DateTime,
  block_number UInt64,
  status Enum8('success' = 1, 'failed' = 2),
  gas_used Nullable(String),
  gas_price Nullable(String),
  token_transfers String DEFAULT '[]',
  method_id Nullable(String),
  input Nullable(String),
  created_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(created_at)
PARTITION BY toYYYYMM(timestamp)
ORDER BY (timestamp, address, hash)
SETTINGS index_granularity = 8192;

-- Create index for address lookups
ALTER TABLE unified_timeline ADD INDEX IF NOT EXISTS idx_unified_address address TYPE bloom_filter GRANULARITY 1;

-- Create index for chain filtering
ALTER TABLE unified_timeline ADD INDEX IF NOT EXISTS idx_unified_chain chain TYPE set(10) GRANULARITY 1;

-- Create index for timestamp range queries
ALTER TABLE unified_timeline ADD INDEX IF NOT EXISTS idx_unified_timestamp timestamp TYPE minmax GRANULARITY 1;

-- Create materialized view to automatically populate unified_timeline from transactions
-- This ensures the unified timeline is updated whenever new transactions are inserted
CREATE MATERIALIZED VIEW IF NOT EXISTS unified_timeline_mv TO unified_timeline AS
SELECT
  address,
  hash,
  chain,
  from,
  to,
  value,
  timestamp,
  block_number,
  status,
  gas_used,
  gas_price,
  token_transfers,
  method_id,
  input,
  now() as created_at
FROM transactions;

