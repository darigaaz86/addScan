-- Create materialized view for address statistics
CREATE MATERIALIZED VIEW IF NOT EXISTS address_stats_mv
ENGINE = SummingMergeTree()
PARTITION BY month
ORDER BY (address, chain, month)
AS SELECT
  address,
  chain,
  count() as transaction_count,
  sum(toFloat64OrZero(value)) as total_volume,
  toYYYYMM(timestamp) as month
FROM transactions
GROUP BY address, chain, month;

-- Create materialized view for counterparty analysis
CREATE MATERIALIZED VIEW IF NOT EXISTS counterparty_stats_mv
ENGINE = SummingMergeTree()
PARTITION BY month
ORDER BY (address, counterparty, month)
AS SELECT
  address,
  if(from = address, to, from) as counterparty,
  count() as transaction_count,
  sum(toFloat64OrZero(value)) as total_volume,
  toYYYYMM(timestamp) as month
FROM transactions
GROUP BY address, counterparty, month;

-- Create materialized view for daily activity
CREATE MATERIALIZED VIEW IF NOT EXISTS daily_activity_mv
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (address, date)
AS SELECT
  address,
  toDate(timestamp) as date,
  count() as transaction_count,
  sum(toFloat64OrZero(value)) as total_volume
FROM transactions
GROUP BY address, date;
