-- Real-time balance aggregation using materialized views
-- Uses Decimal(76,0) to handle uint256 values (max 10^76)
-- SummingMergeTree auto-aggregates balance changes

USE address_scanner;

-- Native token balances (ETH/BNB) aggregated from all sources
CREATE TABLE IF NOT EXISTS native_balances_agg (
    address String,
    chain String,
    balance_in Decimal(76,0) DEFAULT 0,
    balance_out Decimal(76,0) DEFAULT 0,
    gas_spent Decimal(76,0) DEFAULT 0,
    tx_count_in UInt64 DEFAULT 0,
    tx_count_out UInt64 DEFAULT 0,
    first_seen DateTime DEFAULT now(),
    last_seen DateTime DEFAULT now()
) ENGINE = SummingMergeTree((balance_in, balance_out, gas_spent, tx_count_in, tx_count_out))
ORDER BY (address, chain);

-- MV: Native balance from transactions (incoming)
-- Only count native transfers (not token transfers)
CREATE MATERIALIZED VIEW IF NOT EXISTS native_balances_tx_in_mv
TO native_balances_agg
AS SELECT
    address,
    chain,
    toDecimal256(value, 0) as balance_in,
    toDecimal256(0, 0) as balance_out,
    toDecimal256(0, 0) as gas_spent,
    1 as tx_count_in,
    0 as tx_count_out,
    timestamp as first_seen,
    timestamp as last_seen
FROM transactions
WHERE status = 'success' 
  AND direction = 'in'
  AND transfer_type = 'native';

-- MV: Native balance from transactions (outgoing + gas)
-- Only count native transfers and include gas for outgoing
CREATE MATERIALIZED VIEW IF NOT EXISTS native_balances_tx_out_mv
TO native_balances_agg
AS SELECT
    address,
    chain,
    toDecimal256(0, 0) as balance_in,
    toDecimal256(value, 0) as balance_out,
    toDecimal256(toUInt256OrZero(gas_used) * toUInt256OrZero(gas_price), 0) as gas_spent,
    0 as tx_count_in,
    1 as tx_count_out,
    timestamp as first_seen,
    timestamp as last_seen
FROM transactions
WHERE status = 'success' 
  AND direction = 'out'
  AND transfer_type = 'native';

-- MV: Native balance from Goldsky traces (internal tx incoming)
CREATE MATERIALIZED VIEW IF NOT EXISTS native_balances_trace_in_mv
TO native_balances_agg
AS SELECT
    lower(to_address) as address,
    chain,
    value as balance_in,
    toDecimal256(0, 0) as balance_out,
    toDecimal256(0, 0) as gas_spent,
    1 as tx_count_in,
    0 as tx_count_out,
    block_timestamp as first_seen,
    block_timestamp as last_seen
FROM goldsky_traces
WHERE status = 1 AND value > 0;

-- MV: Native balance from Goldsky traces (internal tx outgoing)
CREATE MATERIALIZED VIEW IF NOT EXISTS native_balances_trace_out_mv
TO native_balances_agg
AS SELECT
    lower(from_address) as address,
    chain,
    toDecimal256(0, 0) as balance_in,
    value as balance_out,
    toDecimal256(0, 0) as gas_spent,
    0 as tx_count_in,
    1 as tx_count_out,
    block_timestamp as first_seen,
    block_timestamp as last_seen
FROM goldsky_traces
WHERE status = 1 AND value > 0;

-- Token balances aggregated from transactions table
CREATE TABLE IF NOT EXISTS token_balances_agg (
    address String,
    chain String,
    token_address String,
    token_symbol String,
    token_decimals UInt8 DEFAULT 18,
    balance_in Decimal(76,0) DEFAULT 0,
    balance_out Decimal(76,0) DEFAULT 0,
    transfer_count UInt64 DEFAULT 0,
    first_seen DateTime DEFAULT now(),
    last_seen DateTime DEFAULT now()
) ENGINE = SummingMergeTree((balance_in, balance_out, transfer_count))
ORDER BY (address, chain, token_address);

-- MV: Token balance from transactions (incoming)
CREATE MATERIALIZED VIEW IF NOT EXISTS token_balances_tx_in_mv
TO token_balances_agg
AS SELECT
    address,
    chain,
    token_address,
    token_symbol,
    token_decimals,
    toDecimal256(value, 0) as balance_in,
    toDecimal256(0, 0) as balance_out,
    1 as transfer_count,
    timestamp as first_seen,
    timestamp as last_seen
FROM transactions
WHERE status = 'success' 
  AND direction = 'in'
  AND transfer_type IN ('erc20', 'erc721', 'erc1155')
  AND token_address != ''
  AND is_spam = 0;

-- MV: Token balance from transactions (outgoing)
CREATE MATERIALIZED VIEW IF NOT EXISTS token_balances_tx_out_mv
TO token_balances_agg
AS SELECT
    address,
    chain,
    token_address,
    token_symbol,
    token_decimals,
    toDecimal256(0, 0) as balance_in,
    toDecimal256(value, 0) as balance_out,
    1 as transfer_count,
    timestamp as first_seen,
    timestamp as last_seen
FROM transactions
WHERE status = 'success' 
  AND direction = 'out'
  AND transfer_type IN ('erc20', 'erc721', 'erc1155')
  AND token_address != ''
  AND is_spam = 0;

-- MV: Token balance from Goldsky logs (ERC20 Transfer incoming)
CREATE MATERIALIZED VIEW IF NOT EXISTS token_balances_log_in_mv
TO token_balances_agg
AS SELECT
    lower(to_address) as address,
    chain,
    lower(contract_address) as token_address,
    '' as token_symbol,
    18 as token_decimals,
    toDecimal256(amount, 0) as balance_in,
    toDecimal256(0, 0) as balance_out,
    1 as transfer_count,
    block_timestamp as first_seen,
    block_timestamp as last_seen
FROM goldsky_logs
WHERE startsWith(event_signature, '0xddf252ad') 
  AND amount != '' 
  AND amount != '0'
  AND to_address != '';

-- MV: Token balance from Goldsky logs (ERC20 Transfer outgoing)
CREATE MATERIALIZED VIEW IF NOT EXISTS token_balances_log_out_mv
TO token_balances_agg
AS SELECT
    lower(from_address) as address,
    chain,
    lower(contract_address) as token_address,
    '' as token_symbol,
    18 as token_decimals,
    toDecimal256(0, 0) as balance_in,
    toDecimal256(amount, 0) as balance_out,
    1 as transfer_count,
    block_timestamp as first_seen,
    block_timestamp as last_seen
FROM goldsky_logs
WHERE startsWith(event_signature, '0xddf252ad') 
  AND amount != '' 
  AND amount != '0'
  AND from_address != '';

-- Convenience view: Final native balances (balance = in - out - gas)
CREATE VIEW IF NOT EXISTS native_balances_final AS
SELECT
    address,
    chain,
    balance_in - balance_out - gas_spent as balance,
    balance_in,
    balance_out,
    gas_spent,
    tx_count_in,
    tx_count_out,
    min(first_seen) as first_seen,
    max(last_seen) as last_seen
FROM native_balances_agg
GROUP BY address, chain, balance_in, balance_out, gas_spent, tx_count_in, tx_count_out;

-- Backfill native balances from existing transactions (for fresh installs)
INSERT INTO native_balances_agg
SELECT
    address,
    chain,
    toDecimal256(value, 0) as balance_in,
    toDecimal256(0, 0) as balance_out,
    toDecimal256(0, 0) as gas_spent,
    1 as tx_count_in,
    0 as tx_count_out,
    timestamp as first_seen,
    timestamp as last_seen
FROM transactions
WHERE status = 'success' 
  AND direction = 'in'
  AND transfer_type = 'native';

INSERT INTO native_balances_agg
SELECT
    address,
    chain,
    toDecimal256(0, 0) as balance_in,
    toDecimal256(value, 0) as balance_out,
    toDecimal256(toUInt256OrZero(gas_used) * toUInt256OrZero(gas_price), 0) as gas_spent,
    0 as tx_count_in,
    1 as tx_count_out,
    timestamp as first_seen,
    timestamp as last_seen
FROM transactions
WHERE status = 'success' 
  AND direction = 'out'
  AND transfer_type = 'native';

-- Backfill token balances from existing transactions (for fresh installs)
INSERT INTO token_balances_agg
SELECT
    address,
    chain,
    token_address,
    token_symbol,
    token_decimals,
    toDecimal256(value, 0) as balance_in,
    toDecimal256(0, 0) as balance_out,
    1 as transfer_count,
    timestamp as first_seen,
    timestamp as last_seen
FROM transactions
WHERE status = 'success' 
  AND direction = 'in'
  AND transfer_type IN ('erc20', 'erc721', 'erc1155')
  AND token_address != ''
  AND is_spam = 0;

INSERT INTO token_balances_agg
SELECT
    address,
    chain,
    token_address,
    token_symbol,
    token_decimals,
    toDecimal256(0, 0) as balance_in,
    toDecimal256(value, 0) as balance_out,
    1 as transfer_count,
    timestamp as first_seen,
    timestamp as last_seen
FROM transactions
WHERE status = 'success' 
  AND direction = 'out'
  AND transfer_type IN ('erc20', 'erc721', 'erc1155')
  AND token_address != ''
  AND is_spam = 0;

-- Convenience view: Final token balances (balance = in - out)
CREATE VIEW IF NOT EXISTS token_balances_final AS
SELECT
    address,
    chain,
    token_address,
    any(token_symbol) as token_symbol,
    any(token_decimals) as token_decimals,
    sum(balance_in) - sum(balance_out) as balance,
    sum(balance_in) as total_in,
    sum(balance_out) as total_out,
    sum(transfer_count) as transfer_count,
    min(first_seen) as first_seen,
    max(last_seen) as last_seen
FROM token_balances_agg
GROUP BY address, chain, token_address
HAVING balance > 0;
