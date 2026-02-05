-- Token registry for known tokens
CREATE TABLE IF NOT EXISTS tokens (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    contract_address VARCHAR(42) NOT NULL,
    chain VARCHAR(20) NOT NULL,
    symbol VARCHAR(20),
    name VARCHAR(100),
    decimals INT DEFAULT 18,
    logo_url TEXT,
    is_native BOOLEAN DEFAULT FALSE,
    is_stablecoin BOOLEAN DEFAULT FALSE,
    protocol_id UUID REFERENCES protocols(id),
    underlying_token VARCHAR(42),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(contract_address, chain)
);

-- Indexes
CREATE INDEX idx_tokens_chain ON tokens(chain);
CREATE INDEX idx_tokens_symbol ON tokens(symbol);
CREATE INDEX idx_tokens_protocol ON tokens(protocol_id);

-- Seed native tokens (use 0x0 as placeholder address)
INSERT INTO tokens (contract_address, chain, symbol, name, decimals, is_native) VALUES
('0x0000000000000000000000000000000000000000', 'ethereum', 'ETH', 'Ethereum', 18, true),
('0x0000000000000000000000000000000000000000', 'base', 'ETH', 'Ethereum', 18, true),
('0x0000000000000000000000000000000000000000', 'bnb', 'BNB', 'BNB', 18, true)
ON CONFLICT (contract_address, chain) DO NOTHING;

-- Seed major Ethereum tokens
INSERT INTO tokens (contract_address, chain, symbol, name, decimals, is_stablecoin) VALUES
-- Stablecoins
('0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48', 'ethereum', 'USDC', 'USD Coin', 6, true),
('0xdac17f958d2ee523a2206206994597c13d831ec7', 'ethereum', 'USDT', 'Tether USD', 6, true),
('0x6b175474e89094c44da98b954eedeac495271d0f', 'ethereum', 'DAI', 'Dai Stablecoin', 18, true),
('0x4fabb145d64652a948d72533023f6e7a623c7c53', 'ethereum', 'BUSD', 'Binance USD', 18, true),
('0x853d955acef822db058eb8505911ed77f175b99e', 'ethereum', 'FRAX', 'Frax', 18, true),

-- Wrapped tokens
('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2', 'ethereum', 'WETH', 'Wrapped Ether', 18, false),
('0x2260fac5e5542a773aa44fbcfedf7c193bc2c599', 'ethereum', 'WBTC', 'Wrapped BTC', 8, false),

-- Major tokens
('0x1f9840a85d5af5bf1d1762f925bdaddc4201f984', 'ethereum', 'UNI', 'Uniswap', 18, false),
('0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9', 'ethereum', 'AAVE', 'Aave', 18, false),
('0xc00e94cb662c3520282e6f5717214004a7f26888', 'ethereum', 'COMP', 'Compound', 18, false),
('0xd533a949740bb3306d119cc777fa900ba034cd52', 'ethereum', 'CRV', 'Curve DAO Token', 18, false),
('0x514910771af9ca656af840dff83e8264ecf986ca', 'ethereum', 'LINK', 'Chainlink', 18, false),
('0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2', 'ethereum', 'MKR', 'Maker', 18, false),

-- Liquid staking tokens
('0xae7ab96520de3a18e5e111b5eaab095312d7fe84', 'ethereum', 'stETH', 'Lido Staked ETH', 18, false),
('0xae78736cd615f374d3085123a210448e74fc6393', 'ethereum', 'rETH', 'Rocket Pool ETH', 18, false),
('0xac3e018457b222d93114458476f3e3416abbe38f', 'ethereum', 'sfrxETH', 'Staked Frax Ether', 18, false)
ON CONFLICT (contract_address, chain) DO NOTHING;

-- Seed Base tokens
INSERT INTO tokens (contract_address, chain, symbol, name, decimals, is_stablecoin) VALUES
('0x833589fcd6edb6e08f4c7c32d4f71b54bda02913', 'base', 'USDC', 'USD Coin', 6, true),
('0x4200000000000000000000000000000000000006', 'base', 'WETH', 'Wrapped Ether', 18, false)
ON CONFLICT (contract_address, chain) DO NOTHING;

-- Seed BNB Chain tokens
INSERT INTO tokens (contract_address, chain, symbol, name, decimals, is_stablecoin) VALUES
('0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d', 'bnb', 'USDC', 'USD Coin', 18, true),
('0x55d398326f99059ff775485246999027b3197955', 'bnb', 'USDT', 'Tether USD', 18, true),
('0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c', 'bnb', 'WBNB', 'Wrapped BNB', 18, false),
('0x2170ed0880ac9a755fd29b2688956bd959f933f8', 'bnb', 'ETH', 'Ethereum Token', 18, false)
ON CONFLICT (contract_address, chain) DO NOTHING;
