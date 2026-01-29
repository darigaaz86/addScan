-- Create users table
CREATE TABLE IF NOT EXISTS users (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  email VARCHAR(255) UNIQUE NOT NULL,
  tier VARCHAR(10) NOT NULL CHECK (tier IN ('free', 'paid')),
  settings JSONB,
  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Create index on tier for filtering
CREATE INDEX IF NOT EXISTS idx_users_tier ON users(tier);

-- Create index on email for lookups
CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
