-- Drop backfill_jobs table
DROP INDEX IF EXISTS idx_backfill_jobs_created_at;
DROP INDEX IF EXISTS idx_backfill_jobs_address;
DROP INDEX IF EXISTS idx_backfill_jobs_status_priority;
DROP TABLE IF EXISTS backfill_jobs;
