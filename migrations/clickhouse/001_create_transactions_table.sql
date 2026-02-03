-- Create transactions table
CREATE TABLE IF NOT EXISTS transactions (
  hash String,
  chain String,
  address String,
  from String,
  to String,
  value String,
  asset Nullable(String),
  category Nullable(String),
  direction Enum8('in' = 1, 'out' = 2),
  timestamp DateTime,
  block_number UInt64,
  status Enum8('success' = 1, 'failed' = 2),
  gas_used Nullable(String),
  gas_price Nullable(String),
  token_transfers String DEFAULT '[]',
  method_id Nullable(String),
  func_name Nullable(String),
  input Nullable(String)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (address, timestamp, hash)
SETTINGS index_granularity = 8192;

-- Create bloom filter index for transaction hash lookups
ALTER TABLE transactions ADD INDEX IF NOT EXISTS idx_tx_hash hash TYPE bloom_filter GRANULARITY 1;

-- Create set index for chain filtering
ALTER TABLE transactions ADD INDEX IF NOT EXISTS idx_tx_chain chain TYPE set(10) GRANULARITY 1;

-- Create index for block number
ALTER TABLE transactions ADD INDEX IF NOT EXISTS idx_tx_block block_number TYPE minmax GRANULARITY 1;

-- Create index for asset filtering
ALTER TABLE transactions ADD INDEX IF NOT EXISTS idx_tx_asset asset TYPE bloom_filter GRANULARITY 1;

-- Create index for category filtering
ALTER TABLE transactions ADD INDEX IF NOT EXISTS idx_tx_category category TYPE set(10) GRANULARITY 1;
