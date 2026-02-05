-- Migration 004: Query performance optimization
-- Note: query_result_cache and portfolio_stats_cache tables were removed as they are 
-- not required per PORTFOLIO_BALANCE_PNL.md spec.
-- Keeping only the index optimizations for transactions table.

-- Optimize transactions table with additional indexes for common query patterns
ALTER TABLE transactions ADD INDEX IF NOT EXISTS idx_tx_value value TYPE minmax GRANULARITY 4;
ALTER TABLE transactions ADD INDEX IF NOT EXISTS idx_tx_timestamp timestamp TYPE minmax GRANULARITY 1;
ALTER TABLE transactions ADD INDEX IF NOT EXISTS idx_tx_from tx_from TYPE bloom_filter GRANULARITY 2;
ALTER TABLE transactions ADD INDEX IF NOT EXISTS idx_tx_to tx_to TYPE bloom_filter GRANULARITY 2;
ALTER TABLE transactions ADD INDEX IF NOT EXISTS idx_transfer_from transfer_from TYPE bloom_filter GRANULARITY 2;
ALTER TABLE transactions ADD INDEX IF NOT EXISTS idx_transfer_to transfer_to TYPE bloom_filter GRANULARITY 2;
