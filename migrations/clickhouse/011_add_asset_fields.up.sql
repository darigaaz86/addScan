-- Add asset and category fields to transactions table
-- These fields represent the primary asset being transferred in the transaction

ALTER TABLE transactions 
  ADD COLUMN asset Nullable(String) AFTER value;

ALTER TABLE transactions 
  ADD COLUMN category Nullable(String) AFTER asset;

-- Add index for asset filtering
ALTER TABLE transactions ADD INDEX IF NOT EXISTS idx_tx_asset asset TYPE bloom_filter GRANULARITY 1;

-- Add index for category filtering  
ALTER TABLE transactions ADD INDEX IF NOT EXISTS idx_tx_category category TYPE set(10) GRANULARITY 1;