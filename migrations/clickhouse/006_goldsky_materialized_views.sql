-- Materialized view for Goldsky traces -> unified_timeline
-- Transforms internal transactions into unified format
-- Supports multi-chain: ethereum, base, bnb
CREATE MATERIALIZED VIEW IF NOT EXISTS goldsky_traces_mv TO unified_timeline AS
SELECT
    from_address AS address,
    transaction_hash AS hash,
    chain AS chain,
    from_address AS `from`,
    to_address AS `to`,
    value AS value,
    block_timestamp AS timestamp,
    block_number,
    CASE WHEN status = 1 THEN 'success' ELSE 'failed' END AS status,
    toString(gas_used) AS gas_used,
    CAST(NULL AS Nullable(String)) AS gas_price,
    '[]' AS token_transfers,
    CAST(NULL AS Nullable(String)) AS method_id,
    CAST(NULL AS Nullable(String)) AS input,
    now() AS created_at
FROM goldsky_traces
WHERE from_address != '';

-- Also track the to_address side of traces
CREATE MATERIALIZED VIEW IF NOT EXISTS goldsky_traces_to_mv TO unified_timeline AS
SELECT
    to_address AS address,
    transaction_hash AS hash,
    chain AS chain,
    from_address AS `from`,
    to_address AS `to`,
    value AS value,
    block_timestamp AS timestamp,
    block_number,
    CASE WHEN status = 1 THEN 'success' ELSE 'failed' END AS status,
    toString(gas_used) AS gas_used,
    CAST(NULL AS Nullable(String)) AS gas_price,
    '[]' AS token_transfers,
    CAST(NULL AS Nullable(String)) AS method_id,
    CAST(NULL AS Nullable(String)) AS input,
    now() AS created_at
FROM goldsky_traces
WHERE to_address != '' AND to_address != from_address;

-- Materialized view for Goldsky logs -> unified_timeline
-- Transforms token events into unified format
CREATE MATERIALIZED VIEW IF NOT EXISTS goldsky_logs_mv TO unified_timeline AS
SELECT
    from_address AS address,
    transaction_hash AS hash,
    chain AS chain,
    from_address AS `from`,
    to_address AS `to`,
    data AS value,
    block_timestamp AS timestamp,
    block_number,
    'success' AS status,
    CAST(NULL AS Nullable(String)) AS gas_used,
    CAST(NULL AS Nullable(String)) AS gas_price,
    concat('[{"contract":"', contract_address, '","from":"', from_address, '","to":"', to_address, '","event":"', substring(event_signature, 1, 10), '"}]') AS token_transfers,
    substring(event_signature, 1, 10) AS method_id,
    CAST(NULL AS Nullable(String)) AS input,
    now() AS created_at
FROM goldsky_logs
WHERE from_address != '';

-- Also track the to_address side of logs
CREATE MATERIALIZED VIEW IF NOT EXISTS goldsky_logs_to_mv TO unified_timeline AS
SELECT
    to_address AS address,
    transaction_hash AS hash,
    chain AS chain,
    from_address AS `from`,
    to_address AS `to`,
    data AS value,
    block_timestamp AS timestamp,
    block_number,
    'success' AS status,
    CAST(NULL AS Nullable(String)) AS gas_used,
    CAST(NULL AS Nullable(String)) AS gas_price,
    concat('[{"contract":"', contract_address, '","from":"', from_address, '","to":"', to_address, '","event":"', substring(event_signature, 1, 10), '"}]') AS token_transfers,
    substring(event_signature, 1, 10) AS method_id,
    CAST(NULL AS Nullable(String)) AS input,
    now() AS created_at
FROM goldsky_logs
WHERE to_address != '' AND to_address != from_address;
