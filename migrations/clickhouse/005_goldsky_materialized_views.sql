-- Materialized view for Goldsky traces -> unified_timeline
-- Transforms internal transactions into unified format
-- Supports multi-chain: ethereum, base, bnb
CREATE MATERIALIZED VIEW IF NOT EXISTS goldsky_traces_mv TO unified_timeline AS
SELECT
    from_address AS address,
    transaction_hash AS tx_hash,
    0 AS log_index,
    chain AS chain,
    from_address AS tx_from,
    to_address AS tx_to,
    'native' AS transfer_type,
    from_address AS transfer_from,
    to_address AS transfer_to,
    toString(value) AS value,
    'out' AS direction,
    '' AS token_address,
    '' AS token_symbol,
    18 AS token_decimals,
    '' AS token_id,
    block_timestamp AS timestamp,
    block_number,
    CASE WHEN status = 1 THEN 'success' ELSE 'failed' END AS status,
    toString(gas_used) AS gas_used,
    '' AS gas_price,
    '' AS method_id,
    '' AS func_name,
    now() AS created_at
FROM goldsky_traces
WHERE from_address != '';

-- Also track the to_address side of traces
CREATE MATERIALIZED VIEW IF NOT EXISTS goldsky_traces_to_mv TO unified_timeline AS
SELECT
    to_address AS address,
    transaction_hash AS tx_hash,
    0 AS log_index,
    chain AS chain,
    from_address AS tx_from,
    to_address AS tx_to,
    'native' AS transfer_type,
    from_address AS transfer_from,
    to_address AS transfer_to,
    toString(value) AS value,
    'in' AS direction,
    '' AS token_address,
    '' AS token_symbol,
    18 AS token_decimals,
    '' AS token_id,
    block_timestamp AS timestamp,
    block_number,
    CASE WHEN status = 1 THEN 'success' ELSE 'failed' END AS status,
    toString(gas_used) AS gas_used,
    '' AS gas_price,
    '' AS method_id,
    '' AS func_name,
    now() AS created_at
FROM goldsky_traces
WHERE to_address != '' AND to_address != from_address;

-- Materialized view for Goldsky logs -> unified_timeline
-- Transforms token events into unified format
CREATE MATERIALIZED VIEW IF NOT EXISTS goldsky_logs_mv TO unified_timeline AS
SELECT
    from_address AS address,
    transaction_hash AS tx_hash,
    log_index AS log_index,
    chain AS chain,
    from_address AS tx_from,
    to_address AS tx_to,
    'erc20' AS transfer_type,
    from_address AS transfer_from,
    to_address AS transfer_to,
    amount AS value,
    'out' AS direction,
    contract_address AS token_address,
    '' AS token_symbol,
    18 AS token_decimals,
    '' AS token_id,
    block_timestamp AS timestamp,
    block_number,
    'success' AS status,
    '' AS gas_used,
    '' AS gas_price,
    substring(event_signature, 1, 10) AS method_id,
    '' AS func_name,
    now() AS created_at
FROM goldsky_logs
WHERE from_address != '';

-- Also track the to_address side of logs
CREATE MATERIALIZED VIEW IF NOT EXISTS goldsky_logs_to_mv TO unified_timeline AS
SELECT
    to_address AS address,
    transaction_hash AS tx_hash,
    log_index AS log_index,
    chain AS chain,
    from_address AS tx_from,
    to_address AS tx_to,
    'erc20' AS transfer_type,
    from_address AS transfer_from,
    to_address AS transfer_to,
    amount AS value,
    'in' AS direction,
    contract_address AS token_address,
    '' AS token_symbol,
    18 AS token_decimals,
    '' AS token_id,
    block_timestamp AS timestamp,
    block_number,
    'success' AS status,
    '' AS gas_used,
    '' AS gas_price,
    substring(event_signature, 1, 10) AS method_id,
    '' AS func_name,
    now() AS created_at
FROM goldsky_logs
WHERE to_address != '' AND to_address != from_address;
