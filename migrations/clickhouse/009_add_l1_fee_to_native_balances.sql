-- Add l1_fee_spent to native_balances_agg for L2 chains (Base, Optimism, Arbitrum)
-- The outgoing MV only tracked gas_used * gas_price but missed the separate L1 data fee

USE address_scanner;

-- Step 1: Add l1_fee_spent column to the aggregation table
ALTER TABLE native_balances_agg ADD COLUMN IF NOT EXISTS l1_fee_spent Decimal(76,0) DEFAULT 0;

-- Step 2: Update SummingMergeTree to include l1_fee_spent
-- SummingMergeTree automatically sums all numeric columns not in the ORDER BY key,
-- so adding the column is sufficient — no engine change needed.

-- Step 3: Drop and recreate the outgoing MV to include l1_fee
DROP VIEW IF EXISTS native_balances_tx_out_mv;

CREATE MATERIALIZED VIEW native_balances_tx_out_mv
TO native_balances_agg
AS SELECT
    address,
    chain,
    toDecimal256(0, 0) as balance_in,
    toDecimal256(value, 0) as balance_out,
    toDecimal256(toUInt256OrZero(gas_used) * toUInt256OrZero(gas_price), 0) as gas_spent,
    toDecimal256(toUInt256OrZero(l1_fee), 0) as l1_fee_spent,
    0 as tx_count_in,
    1 as tx_count_out,
    timestamp as first_seen,
    timestamp as last_seen
FROM transactions
WHERE status = 'success' 
  AND direction = 'out'
  AND transfer_type = 'native';

-- Step 4: Drop and recreate the final view to subtract l1_fee_spent
DROP VIEW IF EXISTS native_balances_final;

CREATE VIEW native_balances_final AS
SELECT
    address,
    chain,
    balance_in - balance_out - gas_spent - l1_fee_spent as balance,
    balance_in,
    balance_out,
    gas_spent,
    l1_fee_spent,
    tx_count_in,
    tx_count_out,
    min(first_seen) as first_seen,
    max(last_seen) as last_seen
FROM native_balances_agg
GROUP BY address, chain, balance_in, balance_out, gas_spent, l1_fee_spent, tx_count_in, tx_count_out;

-- Step 5: Backfill l1_fee_spent from existing transactions
-- This inserts correction rows that the SummingMergeTree will merge
INSERT INTO native_balances_agg (address, chain, balance_in, balance_out, gas_spent, l1_fee_spent, tx_count_in, tx_count_out, first_seen, last_seen)
SELECT
    address,
    chain,
    toDecimal256(0, 0) as balance_in,
    toDecimal256(0, 0) as balance_out,
    toDecimal256(0, 0) as gas_spent,
    toDecimal256(toUInt256OrZero(l1_fee), 0) as l1_fee_spent,
    0 as tx_count_in,
    0 as tx_count_out,
    timestamp as first_seen,
    timestamp as last_seen
FROM transactions
WHERE status = 'success' 
  AND direction = 'out'
  AND transfer_type = 'native'
  AND l1_fee != ''
  AND l1_fee != '0';
