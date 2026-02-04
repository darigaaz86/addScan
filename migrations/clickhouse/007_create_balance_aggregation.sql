-- Balance aggregation views for calculating address balances from transaction history
-- This allows paid tier users to get balances without RPC calls

-- Native balance aggregation by address and chain
-- Calculates balance as: sum(incoming) - sum(outgoing) - sum(gas_fees)
CREATE TABLE IF NOT EXISTS address_balances (
    address String,
    chain String,
    native_balance_wei String,  -- Stored as string for precision (uint256)
    total_in_wei String,
    total_out_wei String,
    total_gas_wei String,
    transaction_count UInt64,
    last_updated DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(last_updated)
ORDER BY (address, chain);

-- Materialized view to aggregate native balances
-- Note: ClickHouse doesn't support uint256 natively, so we use String and aggregate in app
CREATE MATERIALIZED VIEW IF NOT EXISTS address_balances_mv
TO address_balances
AS SELECT
    address,
    chain,
    -- For native balance, we need to calculate in application layer due to uint256
    -- Here we just store the aggregated counts and let app do the math
    '' as native_balance_wei,
    '' as total_in_wei,
    '' as total_out_wei,
    '' as total_gas_wei,
    count() as transaction_count,
    max(timestamp) as last_updated
FROM transactions
WHERE status = 'success'
GROUP BY address, chain;

-- Token balance aggregation table
-- Tracks ERC20/ERC721 token balances per address/chain/token
CREATE TABLE IF NOT EXISTS address_token_balances (
    address String,
    chain String,
    token_address String,
    token_symbol String,
    token_decimals UInt8,
    balance_raw String,  -- Raw balance as string for precision
    total_in_raw String,
    total_out_raw String,
    transfer_count UInt64,
    last_updated DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(last_updated)
ORDER BY (address, chain, token_address);

-- Summary view for quick balance lookups
-- This provides transaction counts and volume summaries per address/chain
CREATE TABLE IF NOT EXISTS address_balance_summary (
    address String,
    chain String,
    tx_count_in UInt64,
    tx_count_out UInt64,
    first_tx_time DateTime,
    last_tx_time DateTime,
    last_block UInt64
) ENGINE = ReplacingMergeTree(last_tx_time)
ORDER BY (address, chain);

-- Materialized view for balance summary
CREATE MATERIALIZED VIEW IF NOT EXISTS address_balance_summary_mv
TO address_balance_summary
AS SELECT
    address,
    chain,
    countIf(direction = 'in') as tx_count_in,
    countIf(direction = 'out') as tx_count_out,
    min(timestamp) as first_tx_time,
    max(timestamp) as last_tx_time,
    max(block_number) as last_block
FROM transactions
WHERE status = 'success'
GROUP BY address, chain;
