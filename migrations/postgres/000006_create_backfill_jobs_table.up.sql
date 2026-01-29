-- Create backfill_jobs table (one job per address per chain)
CREATE TABLE IF NOT EXISTS backfill_jobs (
  job_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  address VARCHAR(66) NOT NULL,
  chain VARCHAR(50) NOT NULL,
  tier VARCHAR(10) NOT NULL CHECK (tier IN ('free', 'paid')),
  status VARCHAR(20) NOT NULL CHECK (status IN ('queued', 'in_progress', 'completed', 'failed')),
  priority INTEGER NOT NULL DEFAULT 0,
  transactions_fetched BIGINT NOT NULL DEFAULT 0,
  started_at TIMESTAMP NOT NULL DEFAULT NOW(),
  completed_at TIMESTAMP,
  error TEXT,
  retry_count INTEGER NOT NULL DEFAULT 0,
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Create index on status and priority for job queue processing
CREATE INDEX IF NOT EXISTS idx_backfill_jobs_status_priority ON backfill_jobs(status, priority DESC) WHERE status IN ('queued', 'in_progress');

-- Create index on address for address-specific queries
CREATE INDEX IF NOT EXISTS idx_backfill_jobs_address ON backfill_jobs(address);

-- Create index on created_at for sorting
CREATE INDEX IF NOT EXISTS idx_backfill_jobs_created_at ON backfill_jobs(created_at DESC);
