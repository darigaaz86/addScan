-- Create portfolio_snapshots table
CREATE TABLE IF NOT EXISTS portfolio_snapshots (
  portfolio_id UUID REFERENCES portfolios(id) ON DELETE CASCADE,
  snapshot_date DATE NOT NULL,
  total_balance JSONB NOT NULL,
  transaction_count BIGINT NOT NULL,
  total_volume NUMERIC NOT NULL,
  top_counterparties JSONB NOT NULL,
  token_holdings JSONB NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
  PRIMARY KEY (portfolio_id, snapshot_date)
);

-- Create index on snapshot_date for date range queries
CREATE INDEX IF NOT EXISTS idx_portfolio_snapshots_date ON portfolio_snapshots(snapshot_date DESC);

-- Create index on portfolio_id and snapshot_date for portfolio history queries
CREATE INDEX IF NOT EXISTS idx_portfolio_snapshots_portfolio_date ON portfolio_snapshots(portfolio_id, snapshot_date DESC);
