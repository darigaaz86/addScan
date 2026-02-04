-- Goldsky traces table (internal transactions)
CREATE TABLE IF NOT EXISTS goldsky_traces (
    id String,
    transaction_hash String,
    block_number UInt64,
    block_timestamp DateTime,
    from_address String,
    to_address String,
    value Decimal(38, 0),
    call_type String,
    gas_used UInt64,
    status UInt8,
    chain String DEFAULT 'ethereum',
    created_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(created_at)
ORDER BY (chain, block_number, transaction_hash, id)
PARTITION BY toYYYYMM(block_timestamp);

-- Goldsky logs table (token events)
-- The 'amount' column stores decoded values from event data (big-endian hex -> decimal string)
-- Supported events:
--   - ERC20 Transfer (0xddf252ad): data = uint256 amount
--   - Uniswap V3 Swap (0xc42079f9): data = int256 amount0 + int256 amount1 (store positive value)
--   - Uniswap V2 Swap (0xd78ad95f): data = amount0In + amount1In + amount0Out + amount1Out
-- Decoding is done at insert time by the webhook handler (handlers_goldsky.go)
CREATE TABLE IF NOT EXISTS goldsky_logs (
    id String,
    transaction_hash String,
    block_number UInt64,
    block_timestamp DateTime,
    contract_address String,
    event_signature String,
    from_address String,
    to_address String,
    amount String DEFAULT '',  -- Decoded amount (uint256 as decimal string)
    topics String,
    data String,
    log_index UInt32,
    chain String DEFAULT 'ethereum',
    created_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(created_at)
ORDER BY (chain, block_number, transaction_hash, id)
PARTITION BY toYYYYMM(block_timestamp);

-- Index for address lookups
ALTER TABLE goldsky_traces ADD INDEX idx_from_address from_address TYPE bloom_filter GRANULARITY 4;
ALTER TABLE goldsky_traces ADD INDEX idx_to_address to_address TYPE bloom_filter GRANULARITY 4;
ALTER TABLE goldsky_logs ADD INDEX idx_contract_address contract_address TYPE bloom_filter GRANULARITY 4;
ALTER TABLE goldsky_logs ADD INDEX idx_from_address from_address TYPE bloom_filter GRANULARITY 4;
ALTER TABLE goldsky_logs ADD INDEX idx_to_address to_address TYPE bloom_filter GRANULARITY 4;
