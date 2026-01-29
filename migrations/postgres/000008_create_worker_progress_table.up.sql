-- Create worker_progress table to track last processed block per chain
CREATE TABLE IF NOT EXISTS worker_progress (
  chain VARCHAR(50) PRIMARY KEY,
  last_processed_block BIGINT NOT NULL DEFAULT 0,
  updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Create index on updated_at for monitoring
CREATE INDEX IF NOT EXISTS idx_worker_progress_updated_at ON worker_progress(updated_at);
