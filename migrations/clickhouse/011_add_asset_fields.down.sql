-- Remove asset and category fields from transactions table

ALTER TABLE transactions DROP INDEX IF EXISTS idx_tx_asset;
ALTER TABLE transactions DROP INDEX IF EXISTS idx_tx_category;

ALTER TABLE transactions DROP COLUMN IF EXISTS asset;
ALTER TABLE transactions DROP COLUMN IF EXISTS category;