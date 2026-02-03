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
CREATE TABLE IF NOT EXISTS goldsky_logs (
    id String,
    transaction_hash String,
    block_number UInt64,
    block_timestamp DateTime,
    contract_address String,
    event_signature String,
    from_address String,
    to_address String,
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
