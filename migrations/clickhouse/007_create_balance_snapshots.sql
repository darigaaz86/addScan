-- Daily balance snapshots for historical tracking
-- Used for P&L calculations and balance history charts

USE address_scanner;

-- Balance snapshots table - stores daily token balances per address
CREATE TABLE IF NOT EXISTS balance_snapshots (
    address String,
    chain String,
    token_address String,
    token_symbol String,
    token_decimals UInt8 DEFAULT 18,
    date Date,
    balance_raw String,                         -- Raw uint256 as string
    protocol_id String DEFAULT '',              -- Protocol slug or empty for wallet tokens
    position_type String DEFAULT 'wallet',      -- wallet, supplied, borrowed, staked, lp, reward
    created_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(created_at)
ORDER BY (address, chain, token_address, date)
PARTITION BY toYYYYMM(date);

-- Native balance snapshots - separate for efficiency
CREATE TABLE IF NOT EXISTS native_balance_snapshots (
    address String,
    chain String,
    date Date,
    balance_raw String,                         -- Raw uint256 as string (wei)
    tx_count_in UInt64 DEFAULT 0,
    tx_count_out UInt64 DEFAULT 0,
    created_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(created_at)
ORDER BY (address, chain, date)
PARTITION BY toYYYYMM(date);

-- Portfolio daily summary - aggregated view for quick lookups
CREATE TABLE IF NOT EXISTS portfolio_daily_summary (
    portfolio_id String,
    date Date,
    address_count UInt32,
    chain_count UInt32,
    token_count UInt32,
    total_native_eth String,                    -- Total ETH equivalent (for sorting)
    snapshot_data String,                       -- JSON with full snapshot details
    created_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(created_at)
ORDER BY (portfolio_id, date)
PARTITION BY toYYYYMM(date);

-- Index for faster date range queries
ALTER TABLE balance_snapshots ADD INDEX idx_date date TYPE minmax GRANULARITY 1;
ALTER TABLE native_balance_snapshots ADD INDEX idx_date date TYPE minmax GRANULARITY 1;
