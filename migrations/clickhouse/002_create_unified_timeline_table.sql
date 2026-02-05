-- Create unified timeline table for pre-computed multi-chain timelines
-- This table stores merged and sorted transactions for faster retrieval
-- Requirement 3.2: Store unified timeline in ClickHouse
-- 
-- Note: With the new schema, the transactions table already contains all transfer info
-- This table provides a denormalized view optimized for timeline queries

CREATE TABLE IF NOT EXISTS unified_timeline (
  address String,
  tx_hash String,
  log_index UInt32,
  chain String,
  tx_from String,
  tx_to String,
  transfer_type Enum8('native' = 1, 'erc20' = 2, 'erc721' = 3, 'erc1155' = 4),
  transfer_from String,
  transfer_to String,
  value String,
  direction Enum8('in' = 1, 'out' = 2),
  token_address String DEFAULT '',
  token_symbol String DEFAULT '',
  token_decimals UInt8 DEFAULT 18,
  token_id String DEFAULT '',
  timestamp DateTime,
  block_number UInt64,
  status String,
  gas_used String DEFAULT '',
  gas_price String DEFAULT '',
  method_id String DEFAULT '',
  func_name String DEFAULT '',
  created_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(created_at)
PARTITION BY toYYYYMM(timestamp)
ORDER BY (timestamp, address, tx_hash, log_index)
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
  tx_hash,
  log_index,
  chain,
  tx_from,
  tx_to,
  transfer_type,
  transfer_from,
  transfer_to,
  value,
  direction,
  token_address,
  token_symbol,
  token_decimals,
  token_id,
  timestamp,
  block_number,
  status,
  gas_used,
  gas_price,
  method_id,
  func_name,
  now() as created_at
FROM transactions;
