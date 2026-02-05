-- Token pattern rules for auto-detecting DeFi tokens
CREATE TABLE IF NOT EXISTS token_patterns (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    protocol_id UUID REFERENCES protocols(id) ON DELETE CASCADE,
    pattern_type VARCHAR(20) NOT NULL,
    pattern VARCHAR(100) NOT NULL,
    position_type VARCHAR(50) NOT NULL,
    priority INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Index for pattern lookups
CREATE INDEX idx_token_patterns_protocol ON token_patterns(protocol_id);
CREATE INDEX idx_token_patterns_type ON token_patterns(pattern_type);

-- Seed pattern rules
-- Aave tokens (aToken = supplied, variableDebtToken = borrowed)
INSERT INTO token_patterns (protocol_id, pattern_type, pattern, position_type, priority)
SELECT p.id, r.pattern_type, r.pattern, r.position_type, r.priority
FROM protocols p
CROSS JOIN (VALUES
    ('prefix', 'aEth', 'supplied', 10),
    ('prefix', 'aArb', 'supplied', 10),
    ('prefix', 'aOpt', 'supplied', 10),
    ('prefix', 'aBas', 'supplied', 10),
    ('prefix', 'aPol', 'supplied', 10),
    ('prefix', 'a', 'supplied', 1),
    ('prefix', 'variableDebt', 'borrowed', 10),
    ('prefix', 'stableDebt', 'borrowed', 10)
) AS r(pattern_type, pattern, position_type, priority)
WHERE p.slug IN ('aave2', 'aave3')
ON CONFLICT DO NOTHING;

-- Compound tokens (cToken = supplied)
INSERT INTO token_patterns (protocol_id, pattern_type, pattern, position_type, priority)
SELECT p.id, r.pattern_type, r.pattern, r.position_type, r.priority
FROM protocols p
CROSS JOIN (VALUES
    ('prefix', 'c', 'supplied', 1)
) AS r(pattern_type, pattern, position_type, priority)
WHERE p.slug IN ('compound2', 'compound3')
ON CONFLICT DO NOTHING;

-- Lido tokens
INSERT INTO token_patterns (protocol_id, pattern_type, pattern, position_type, priority)
SELECT p.id, r.pattern_type, r.pattern, r.position_type, r.priority
FROM protocols p
CROSS JOIN (VALUES
    ('exact', 'stETH', 'staked', 10),
    ('exact', 'wstETH', 'staked', 10)
) AS r(pattern_type, pattern, position_type, priority)
WHERE p.slug = 'lido'
ON CONFLICT DO NOTHING;

-- Rocket Pool tokens
INSERT INTO token_patterns (protocol_id, pattern_type, pattern, position_type, priority)
SELECT p.id, r.pattern_type, r.pattern, r.position_type, r.priority
FROM protocols p
CROSS JOIN (VALUES
    ('exact', 'rETH', 'staked', 10)
) AS r(pattern_type, pattern, position_type, priority)
WHERE p.slug = 'rocketpool'
ON CONFLICT DO NOTHING;

-- Frax tokens
INSERT INTO token_patterns (protocol_id, pattern_type, pattern, position_type, priority)
SELECT p.id, r.pattern_type, r.pattern, r.position_type, r.priority
FROM protocols p
CROSS JOIN (VALUES
    ('exact', 'sfrxETH', 'staked', 10),
    ('exact', 'frxETH', 'staked', 10)
) AS r(pattern_type, pattern, position_type, priority)
WHERE p.slug = 'frax'
ON CONFLICT DO NOTHING;

-- Curve LP tokens
INSERT INTO token_patterns (protocol_id, pattern_type, pattern, position_type, priority)
SELECT p.id, r.pattern_type, r.pattern, r.position_type, r.priority
FROM protocols p
CROSS JOIN (VALUES
    ('prefix', 'crv', 'lp', 5),
    ('suffix', 'CRV', 'lp', 5),
    ('contains', '3Crv', 'lp', 10)
) AS r(pattern_type, pattern, position_type, priority)
WHERE p.slug = 'curve'
ON CONFLICT DO NOTHING;

-- Uniswap LP tokens
INSERT INTO token_patterns (protocol_id, pattern_type, pattern, position_type, priority)
SELECT p.id, r.pattern_type, r.pattern, r.position_type, r.priority
FROM protocols p
CROSS JOIN (VALUES
    ('prefix', 'UNI-V2', 'lp', 10)
) AS r(pattern_type, pattern, position_type, priority)
WHERE p.slug = 'uniswap2'
ON CONFLICT DO NOTHING;

-- Convex tokens
INSERT INTO token_patterns (protocol_id, pattern_type, pattern, position_type, priority)
SELECT p.id, r.pattern_type, r.pattern, r.position_type, r.priority
FROM protocols p
CROSS JOIN (VALUES
    ('prefix', 'cvx', 'staked', 5),
    ('exact', 'CVX', 'wallet', 1)
) AS r(pattern_type, pattern, position_type, priority)
WHERE p.slug = 'convex'
ON CONFLICT DO NOTHING;

-- Yearn tokens
INSERT INTO token_patterns (protocol_id, pattern_type, pattern, position_type, priority)
SELECT p.id, r.pattern_type, r.pattern, r.position_type, r.priority
FROM protocols p
CROSS JOIN (VALUES
    ('prefix', 'yv', 'supplied', 10),
    ('prefix', 'yvCurve', 'supplied', 10)
) AS r(pattern_type, pattern, position_type, priority)
WHERE p.slug = 'yearn'
ON CONFLICT DO NOTHING;
