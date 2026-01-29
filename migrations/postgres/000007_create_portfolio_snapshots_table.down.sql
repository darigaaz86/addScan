-- Drop portfolio_snapshots table
DROP INDEX IF EXISTS idx_portfolio_snapshots_portfolio_date;
DROP INDEX IF EXISTS idx_portfolio_snapshots_date;
DROP TABLE IF EXISTS portfolio_snapshots;
