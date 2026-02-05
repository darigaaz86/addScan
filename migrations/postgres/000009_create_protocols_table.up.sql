-- Protocol registry for DeFi protocols
CREATE TABLE IF NOT EXISTS protocols (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    slug VARCHAR(50) UNIQUE NOT NULL,
    name VARCHAR(100) NOT NULL,
    category VARCHAR(50),
    site_url TEXT,
    logo_url TEXT,
    has_supported_portfolio BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Index for category lookups
CREATE INDEX idx_protocols_category ON protocols(category);

-- Seed major DeFi protocols
INSERT INTO protocols (slug, name, category, site_url, has_supported_portfolio) VALUES
-- Lending
('aave2', 'Aave V2', 'lending', 'https://app.aave.com', true),
('aave3', 'Aave V3', 'lending', 'https://app.aave.com', true),
('compound2', 'Compound V2', 'lending', 'https://app.compound.finance', true),
('compound3', 'Compound V3', 'lending', 'https://app.compound.finance', true),
('spark', 'Spark', 'lending', 'https://spark.fi', true),
('morpho', 'Morpho', 'lending', 'https://morpho.org', true),

-- DEX
('uniswap2', 'Uniswap V2', 'dex', 'https://app.uniswap.org', true),
('uniswap3', 'Uniswap V3', 'dex', 'https://app.uniswap.org', true),
('curve', 'Curve', 'dex', 'https://curve.fi', true),
('balancer', 'Balancer', 'dex', 'https://balancer.fi', true),
('sushiswap', 'SushiSwap', 'dex', 'https://sushi.com', true),
('pancakeswap', 'PancakeSwap', 'dex', 'https://pancakeswap.finance', true),

-- Staking
('lido', 'Lido', 'staking', 'https://lido.fi', true),
('rocketpool', 'Rocket Pool', 'staking', 'https://rocketpool.net', true),
('frax', 'Frax', 'staking', 'https://frax.finance', true),
('eigenlayer', 'EigenLayer', 'staking', 'https://eigenlayer.xyz', true),

-- Yield
('convex', 'Convex', 'yield', 'https://convexfinance.com', true),
('yearn', 'Yearn', 'yield', 'https://yearn.fi', true),
('beefy', 'Beefy', 'yield', 'https://beefy.com', true),
('pendle', 'Pendle', 'yield', 'https://pendle.finance', true),

-- Bridge
('stargate', 'Stargate', 'bridge', 'https://stargate.finance', true),
('across', 'Across', 'bridge', 'https://across.to', true),
('hop', 'Hop', 'bridge', 'https://hop.exchange', true),

-- Derivatives
('gmx', 'GMX', 'derivatives', 'https://gmx.io', true),
('hyperliquid', 'Hyperliquid', 'derivatives', 'https://hyperliquid.xyz', true),

-- Other
('ens', 'ENS', 'other', 'https://ens.domains', false),
('safe', 'Safe', 'other', 'https://safe.global', false)
ON CONFLICT (slug) DO NOTHING;
