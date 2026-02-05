-- Create transactions table (unified transfers schema)
-- One row per transfer (native or token), multiple rows per tx hash when needed

CREATE TABLE IF NOT EXISTS transactions (
    -- Identity
    tx_hash String,
    log_index UInt32 DEFAULT 0,           -- 0 for native, 1+ for token transfers
    chain String,
    address String,                        -- The tracked wallet address (lowercase)
    
    -- Transaction level (who initiated the tx)
    tx_from String,
    tx_to String,
    
    -- Transfer level (actual value movement)
    transfer_type Enum8('native' = 1, 'erc20' = 2, 'erc721' = 3, 'erc1155' = 4),
    transfer_from String,                  -- Who sent the value
    transfer_to String,                    -- Who received the value
    value String,                          -- Wei for native, token amount for tokens
    direction Enum8('in' = 1, 'out' = 2),  -- Relative to tracked address
    
    -- Token info (empty for native)
    token_address String DEFAULT '',
    token_symbol String DEFAULT '',
    token_decimals UInt8 DEFAULT 18,
    token_id String DEFAULT '',            -- For NFTs (ERC721/ERC1155)
    
    -- Tx metadata
    block_number UInt64,
    timestamp DateTime,
    status Enum8('success' = 1, 'failed' = 2),
    gas_used String DEFAULT '',
    gas_price String DEFAULT '',
    method_id String DEFAULT '',
    func_name String DEFAULT '',
    
    -- Spam detection
    is_spam UInt8 DEFAULT 0                -- 1 = spam token, 0 = legitimate
    
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (address, chain, timestamp, tx_hash, log_index);

-- Indexes
ALTER TABLE transactions ADD INDEX IF NOT EXISTS idx_tx_hash tx_hash TYPE bloom_filter GRANULARITY 1;
ALTER TABLE transactions ADD INDEX IF NOT EXISTS idx_chain chain TYPE set(10) GRANULARITY 1;
ALTER TABLE transactions ADD INDEX IF NOT EXISTS idx_block block_number TYPE minmax GRANULARITY 1;
ALTER TABLE transactions ADD INDEX IF NOT EXISTS idx_token token_address TYPE bloom_filter GRANULARITY 1;
ALTER TABLE transactions ADD INDEX IF NOT EXISTS idx_type transfer_type TYPE set(4) GRANULARITY 1;
ALTER TABLE transactions ADD INDEX IF NOT EXISTS idx_is_spam is_spam TYPE set(2) GRANULARITY 1;
