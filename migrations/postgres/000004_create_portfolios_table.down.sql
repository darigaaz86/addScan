-- Drop portfolios table
DROP INDEX IF EXISTS idx_portfolios_created_at;
DROP INDEX IF EXISTS idx_portfolios_user_id;
DROP TABLE IF EXISTS portfolios;
