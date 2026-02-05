-- Protocol contract mappings
CREATE TABLE IF NOT EXISTS protocol_contracts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    protocol_id UUID REFERENCES protocols(id) ON DELETE CASCADE,
    contract_address VARCHAR(42) NOT NULL,
    chain VARCHAR(20) NOT NULL,
    contract_type VARCHAR(50),
    position_type VARCHAR(50),
    description TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(contract_address, chain)
);

-- Indexes
CREATE INDEX idx_protocol_contracts_protocol ON protocol_contracts(protocol_id);
CREATE INDEX idx_protocol_contracts_chain ON protocol_contracts(chain);
CREATE INDEX idx_protocol_contracts_type ON protocol_contracts(contract_type);

-- Seed Aave V3 contracts (Ethereum)
INSERT INTO protocol_contracts (protocol_id, contract_address, chain, contract_type, position_type, description)
SELECT p.id, c.contract_address, c.chain, c.contract_type, c.position_type, c.description
FROM protocols p
CROSS JOIN (VALUES
    ('0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2', 'ethereum', 'pool', 'supplied', 'Aave V3 Pool'),
    ('0x64b761d848206f447fe2dd461b0c635ec39ebb27', 'ethereum', 'pool', 'supplied', 'Aave V3 Pool (alt)')
) AS c(contract_address, chain, contract_type, position_type, description)
WHERE p.slug = 'aave3'
ON CONFLICT (contract_address, chain) DO NOTHING;

-- Seed Compound V3 contracts (Ethereum)
INSERT INTO protocol_contracts (protocol_id, contract_address, chain, contract_type, position_type, description)
SELECT p.id, c.contract_address, c.chain, c.contract_type, c.position_type, c.description
FROM protocols p
CROSS JOIN (VALUES
    ('0xc3d688b66703497daa19211eedff47f25384cdc3', 'ethereum', 'pool', 'supplied', 'Compound V3 USDC'),
    ('0xa17581a9e3356d9a858b789d68b4d866e593ae94', 'ethereum', 'pool', 'supplied', 'Compound V3 WETH')
) AS c(contract_address, chain, contract_type, position_type, description)
WHERE p.slug = 'compound3'
ON CONFLICT (contract_address, chain) DO NOTHING;

-- Seed Uniswap V3 contracts
INSERT INTO protocol_contracts (protocol_id, contract_address, chain, contract_type, position_type, description)
SELECT p.id, c.contract_address, c.chain, c.contract_type, c.position_type, c.description
FROM protocols p
CROSS JOIN (VALUES
    ('0x1f98431c8ad98523631ae4a59f267346ea31f984', 'ethereum', 'factory', NULL, 'Uniswap V3 Factory'),
    ('0xe592427a0aece92de3edee1f18e0157c05861564', 'ethereum', 'router', NULL, 'Uniswap V3 Router'),
    ('0xc36442b4a4522e871399cd717abdd847ab11fe88', 'ethereum', 'nft', 'lp', 'Uniswap V3 Positions NFT'),
    ('0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45', 'ethereum', 'router', NULL, 'Uniswap V3 Router 2'),
    ('0x3fc91a3afd70395cd496c647d5a6cc9d4b2b7fad', 'ethereum', 'router', NULL, 'Uniswap Universal Router')
) AS c(contract_address, chain, contract_type, position_type, description)
WHERE p.slug = 'uniswap3'
ON CONFLICT (contract_address, chain) DO NOTHING;

-- Seed Lido contracts
INSERT INTO protocol_contracts (protocol_id, contract_address, chain, contract_type, position_type, description)
SELECT p.id, c.contract_address, c.chain, c.contract_type, c.position_type, c.description
FROM protocols p
CROSS JOIN (VALUES
    ('0xae7ab96520de3a18e5e111b5eaab095312d7fe84', 'ethereum', 'token', 'staked', 'stETH Token'),
    ('0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0', 'ethereum', 'token', 'staked', 'wstETH Token')
) AS c(contract_address, chain, contract_type, position_type, description)
WHERE p.slug = 'lido'
ON CONFLICT (contract_address, chain) DO NOTHING;

-- Seed Curve contracts
INSERT INTO protocol_contracts (protocol_id, contract_address, chain, contract_type, position_type, description)
SELECT p.id, c.contract_address, c.chain, c.contract_type, c.position_type, c.description
FROM protocols p
CROSS JOIN (VALUES
    ('0xbebc44782c7db0a1a60cb6fe97d0b483032ff1c7', 'ethereum', 'pool', 'lp', 'Curve 3pool'),
    ('0xdc24316b9ae028f1497c275eb9192a3ea0f67022', 'ethereum', 'pool', 'lp', 'Curve stETH/ETH'),
    ('0xd51a44d3fae010294c616388b506acda1bfaae46', 'ethereum', 'pool', 'lp', 'Curve Tricrypto2')
) AS c(contract_address, chain, contract_type, position_type, description)
WHERE p.slug = 'curve'
ON CONFLICT (contract_address, chain) DO NOTHING;
