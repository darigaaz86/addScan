-- Token whitelist table for verified token contracts
CREATE TABLE IF NOT EXISTS token_whitelist (
    id SERIAL PRIMARY KEY,
    chain VARCHAR(50) NOT NULL,
    token_address VARCHAR(66) NOT NULL,
    symbol VARCHAR(50) NOT NULL,
    name VARCHAR(255),
    decimals INTEGER DEFAULT 18,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(chain, token_address)
);

-- Index for fast lookups
CREATE INDEX IF NOT EXISTS idx_token_whitelist_chain_address ON token_whitelist(chain, token_address);
CREATE INDEX IF NOT EXISTS idx_token_whitelist_active ON token_whitelist(is_active) WHERE is_active = true;

-- Insert major Ethereum tokens
INSERT INTO token_whitelist (chain, token_address, symbol, name, decimals) VALUES
-- Stablecoins
('ethereum', '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48', 'USDC', 'USD Coin', 6),
('ethereum', '0xdac17f958d2ee523a2206206994597c13d831ec7', 'USDT', 'Tether USD', 6),
('ethereum', '0x6b175474e89094c44da98b954eedeac495271d0f', 'DAI', 'Dai Stablecoin', 18),
('ethereum', '0x4fabb145d64652a948d72533023f6e7a623c7c53', 'BUSD', 'Binance USD', 18),
('ethereum', '0x853d955acef822db058eb8505911ed77f175b99e', 'FRAX', 'Frax', 18),
('ethereum', '0x5f98805a4e8be255a32880fdec7f6728c6568ba0', 'LUSD', 'Liquity USD', 18),
-- Wrapped tokens
('ethereum', '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2', 'WETH', 'Wrapped Ether', 18),
('ethereum', '0x2260fac5e5542a773aa44fbcfedf7c193bc2c599', 'WBTC', 'Wrapped BTC', 8),
-- LST tokens
('ethereum', '0xae78736cd615f374d3085123a210448e74fc6393', 'rETH', 'Rocket Pool ETH', 18),
('ethereum', '0xae7ab96520de3a18e5e111b5eaab095312d7fe84', 'stETH', 'Lido Staked ETH', 18),
('ethereum', '0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0', 'wstETH', 'Wrapped stETH', 18),
('ethereum', '0xbe9895146f7af43049ca1c1ae358b0541ea49704', 'cbETH', 'Coinbase Wrapped Staked ETH', 18),
-- DeFi tokens
('ethereum', '0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9', 'AAVE', 'Aave', 18),
('ethereum', '0x1f9840a85d5af5bf1d1762f925bdaddc4201f984', 'UNI', 'Uniswap', 18),
('ethereum', '0xc00e94cb662c3520282e6f5717214004a7f26888', 'COMP', 'Compound', 18),
('ethereum', '0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2', 'MKR', 'Maker', 18),
('ethereum', '0xd533a949740bb3306d119cc777fa900ba034cd52', 'CRV', 'Curve DAO Token', 18),
('ethereum', '0x514910771af9ca656af840dff83e8264ecf986ca', 'LINK', 'Chainlink', 18),
-- Other major tokens
('ethereum', '0x95ad61b0a150d79219dcf64e1e6cc01f0b64c4ce', 'SHIB', 'Shiba Inu', 18),
('ethereum', '0x6982508145454ce325ddbe47a25d4ec3d2311933', 'PEPE', 'Pepe', 18),

-- Base chain tokens
('base', '0x833589fcd6edb6e08f4c7c32d4f71b54bda02913', 'USDC', 'USD Coin', 6),
('base', '0x4200000000000000000000000000000000000006', 'WETH', 'Wrapped Ether', 18),
('base', '0x50c5725949a6f0c72e6c4a641f24049a917db0cb', 'DAI', 'Dai Stablecoin', 18),
('base', '0x2ae3f1ec7f1f5012cfeab0185bfc7aa3cf0dec22', 'cbETH', 'Coinbase Wrapped Staked ETH', 18),
('base', '0xc1cba3fcea344f92d9239c08c0568f6f2f0ee452', 'wstETH', 'Wrapped stETH', 18),
('base', '0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca', 'USDbC', 'USD Base Coin', 6),

-- BNB chain tokens
('bnb', '0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d', 'USDC', 'USD Coin', 18),
('bnb', '0x55d398326f99059ff775485246999027b3197955', 'USDT', 'Tether USD', 18),
('bnb', '0x1af3f329e8be154074d8769d1ffa4ee058b1dbc3', 'DAI', 'Dai Stablecoin', 18),
('bnb', '0xe9e7cea3dedca5984780bafc599bd69add087d56', 'BUSD', 'Binance USD', 18),
('bnb', '0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c', 'WBNB', 'Wrapped BNB', 18),
('bnb', '0x2170ed0880ac9a755fd29b2688956bd959f933f8', 'ETH', 'Ethereum Token', 18),
('bnb', '0x7130d2a12b9bcbfae4f2634d864a1ee1ce3ead9c', 'BTCB', 'Bitcoin BEP2', 18)
ON CONFLICT (chain, token_address) DO NOTHING;
