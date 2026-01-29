-- Query Performance Optimization
-- Requirement 7.4: Use materialized views for aggregations, optimize indexes, add query result caching

-- Create query result cache table
-- This table caches frequently accessed query results for faster retrieval
CREATE TABLE IF NOT EXISTS query_result_cache (
  query_hash String,
  address String,
  result_data String, -- JSON serialized result
  filters_hash String,
  created_at DateTime DEFAULT now(),
  expires_at DateTime
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(created_at)
ORDER BY (query_hash, created_at)
TTL expires_at
SETTINGS index_granularity = 8192;

-- Create index for query hash lookups
ALTER TABLE query_result_cache ADD INDEX IF NOT EXISTS idx_query_hash query_hash TYPE bloom_filter GRANULARITY 1;

-- Create index for address lookups
ALTER TABLE query_result_cache ADD INDEX IF NOT EXISTS idx_query_address address TYPE bloom_filter GRANULARITY 1;

-- Optimize transactions table with additional indexes for common query patterns
-- Add index for value range queries
ALTER TABLE transactions ADD INDEX IF NOT EXISTS idx_tx_value value TYPE minmax GRANULARITY 4;

-- Add index for timestamp range queries (more granular)
ALTER TABLE transactions ADD INDEX IF NOT EXISTS idx_tx_timestamp timestamp TYPE minmax GRANULARITY 1;

-- Add index for from/to address lookups
ALTER TABLE transactions ADD INDEX IF NOT EXISTS idx_tx_from from TYPE bloom_filter GRANULARITY 2;
ALTER TABLE transactions ADD INDEX IF NOT EXISTS idx_tx_to to TYPE bloom_filter GRANULARITY 2;

-- Create aggregated statistics table for portfolio queries
-- This table pre-computes portfolio-level statistics for faster retrieval
CREATE TABLE IF NOT EXISTS portfolio_stats_cache (
  portfolio_id String,
  address String,
  total_transactions UInt64,
  total_volume Float64,
  first_transaction DateTime,
  last_transaction DateTime,
  updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (portfolio_id, address)
SETTINGS index_granularity = 8192;

-- Create index for portfolio lookups
ALTER TABLE portfolio_stats_cache ADD INDEX IF NOT EXISTS idx_portfolio_id portfolio_id TYPE bloom_filter GRANULARITY 1;
