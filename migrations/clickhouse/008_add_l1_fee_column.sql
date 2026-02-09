-- Add l1_fee column for L2 chains (Base, Optimism, Arbitrum)
-- L2 chains have separate L1 data fees that must be tracked for accurate balance calculation

ALTER TABLE transactions ADD COLUMN IF NOT EXISTS l1_fee String DEFAULT '';
