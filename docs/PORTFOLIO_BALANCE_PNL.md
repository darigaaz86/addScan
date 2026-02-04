# Portfolio Balance System

## Overview

Multi-layer system for tracking portfolio token balances across multiple addresses and chains, inspired by DeBank's data model.

**Note:** Fiat/USD pricing will be handled by a separate service. This system focuses on token quantities and position tracking.

---

## Data Model (DeBank-aligned)

### Token
```json
{
  "id": "0xdac17f958d2ee523a2206206994597c13d831ec7",
  "chain": "eth",
  "symbol": "USDT",
  "name": "Tether USD",
  "decimals": 6,
  "logo_url": "...",
  "protocol_id": "",       // Empty = wallet token, filled = DeFi protocol token
  "is_core": true,         // Major token
  "is_wallet": true,       // Can be held in wallet
  "amount": 5.512815,      // Human-readable amount
  "raw_amount": 5512815    // Raw uint256 amount
}
```

### Protocol
```json
{
  "id": "uniswap3",
  "chain": "eth",
  "name": "Uniswap V3",
  "site_url": "https://app.uniswap.org",
  "logo_url": "...",
  "category": "dex",       // lending, dex, yield, bridge, staking
  "has_supported_portfolio": true
}
```

### Protocol Position
```json
{
  "protocol_id": "aave3",
  "chain": "eth",
  "name": "Lending",       // Position type: Lending, Staking, Farming, LP, Deposit
  "detail_types": ["lending"],
  "detail": {
    "supply_token_list": [...],   // Supplied/deposited tokens
    "borrow_token_list": [...],   // Borrowed tokens (debt)
    "reward_token_list": [...]    // Pending rewards
  }
}
```

### Position Types
| Type | Description | Examples |
|------|-------------|----------|
| `wallet` | Regular wallet holding | ETH, USDC in wallet |
| `supplied` | Deposited/supplied to protocol | Aave supply, Compound supply |
| `borrowed` | Borrowed from protocol (debt) | Aave borrow |
| `staked` | Staked tokens | Lido stETH, staking pools |
| `farming` | Yield farming positions | LP staking for rewards |
| `lp` | Liquidity provider positions | Uniswap LP, Curve LP |
| `locked` | Locked/vesting tokens | veTokens, vesting |
| `reward` | Pending/claimable rewards | Unclaimed farming rewards |

---

## Requirements

### Layer 1: Base Token Balances
- Per address, per chain, per token balance
- Track `raw_amount` (uint256) and `amount` (human-readable)
- Include `protocol_id` to identify DeFi positions

### Layer 2: Protocol Positions
- Group tokens by protocol
- Separate supply/borrow/reward lists
- Track position types (lending, staking, LP, etc.)

### Layer 3: Aggregations
- **By Chain**: All positions on one chain
- **By Token**: Same token across all chains
- **By Protocol**: All positions in one protocol
- **Top Holdings**: Ranked by quantity

### Layer 4: Daily Snapshots
- Historical balance snapshots (token quantities)
- Track position changes over time

---

## Database Schema

### Postgres Tables (Metadata)

#### tokens
```sql
CREATE TABLE tokens (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  contract_address VARCHAR(42) NOT NULL,
  chain VARCHAR(20) NOT NULL,
  symbol VARCHAR(20),
  name VARCHAR(100),
  decimals INT DEFAULT 18,
  logo_url TEXT,
  coingecko_id VARCHAR(100),
  is_core BOOLEAN DEFAULT FALSE,
  is_wallet BOOLEAN DEFAULT TRUE,
  protocol_id UUID REFERENCES protocols(id),  -- NULL = wallet token
  underlying_token VARCHAR(42),               -- For wrapped/receipt tokens
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW(),
  UNIQUE(contract_address, chain)
);
```

#### protocols
```sql
CREATE TABLE protocols (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  slug VARCHAR(50) UNIQUE NOT NULL,           -- e.g., "aave3", "uniswap3"
  name VARCHAR(100) NOT NULL,
  category VARCHAR(50),                       -- lending, dex, yield, bridge, staking
  site_url TEXT,
  logo_url TEXT,
  has_supported_portfolio BOOLEAN DEFAULT FALSE,
  created_at TIMESTAMP DEFAULT NOW()
);
```

#### protocol_contracts
```sql
CREATE TABLE protocol_contracts (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  protocol_id UUID REFERENCES protocols(id),
  contract_address VARCHAR(42) NOT NULL,
  chain VARCHAR(20) NOT NULL,
  contract_type VARCHAR(50),                  -- pool, vault, router, staking, token
  position_type VARCHAR(50),                  -- supplied, borrowed, staked, lp, reward
  description TEXT,
  created_at TIMESTAMP DEFAULT NOW(),
  UNIQUE(contract_address, chain)
);
```

#### token_patterns
```sql
CREATE TABLE token_patterns (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  protocol_id UUID REFERENCES protocols(id),
  pattern_type VARCHAR(20),                   -- prefix, suffix, contains
  pattern VARCHAR(100),
  position_type VARCHAR(50),
  created_at TIMESTAMP DEFAULT NOW()
);
-- Examples:
-- protocol=Aave, pattern_type=prefix, pattern='a', position_type=supplied
-- protocol=Compound, pattern_type=prefix, pattern='c', position_type=supplied
-- protocol=Lido, pattern_type=prefix, pattern='st', position_type=staked
```

### ClickHouse Tables (Time-series)

#### balance_snapshots
```sql
CREATE TABLE IF NOT EXISTS balance_snapshots (
  address String,
  chain String,
  token_address String,
  token_symbol String,
  token_decimals UInt8,
  date Date,
  balance_raw String,                         -- Raw uint256 as string
  protocol_id String DEFAULT '',              -- Protocol slug or empty
  position_type String DEFAULT 'wallet',      -- wallet, supplied, borrowed, etc.
  created_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(created_at)
ORDER BY (address, chain, token_address, date)
PARTITION BY toYYYYMM(date);
```

#### position_snapshots (for DeFi positions with multiple tokens)
```sql
CREATE TABLE IF NOT EXISTS position_snapshots (
  address String,
  chain String,
  protocol_id String,
  position_type String,                       -- lending, staking, lp, farming
  position_name String,                       -- "Lending", "USDC-ETH LP", etc.
  date Date,
  supply_tokens String,                       -- JSON array of {token, amount}
  borrow_tokens String,                       -- JSON array of {token, amount}
  reward_tokens String,                       -- JSON array of {token, amount}
  created_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(created_at)
ORDER BY (address, chain, protocol_id, position_type, date)
PARTITION BY toYYYYMM(date);
```

---

## Tasks

### Task 1: Protocol & Token Registry (Postgres)
**Priority: High | Estimate: 4h**

#### 1.1 Create Migrations
- [ ] `000009_create_protocols_table.up.sql`
- [ ] `000010_create_tokens_table.up.sql`
- [ ] `000011_create_protocol_contracts_table.up.sql`
- [ ] `000012_create_token_patterns_table.up.sql`

#### 1.2 Seed Data
- [ ] Seed major protocols:
  - Lending: Aave V2/V3, Compound V2/V3, Spark
  - DEX: Uniswap V2/V3, Curve, Balancer, SushiSwap
  - Staking: Lido, Rocket Pool, Frax
  - Yield: Convex, Yearn
  - Bridge: Stargate, Across
- [ ] Seed top 100 tokens per chain
- [ ] Seed protocol contracts (pools, vaults, staking)
- [ ] Seed token patterns (aToken, cToken, stToken)

#### 1.3 Repository Layer
- [ ] Create `internal/storage/protocol_repository.go`
- [ ] Create `internal/storage/token_registry_repository.go`
- [ ] Methods:
  - `GetProtocol(slug)`, `GetProtocolByContract(address, chain)`
  - `GetToken(address, chain)`, `GetTokensByProtocol(protocolId)`
  - `DetectProtocol(tokenSymbol)` - pattern matching

---

### Task 2: Balance Calculation Service
**Priority: High | Estimate: 4h**

#### 2.1 Update Balance Repository
- [ ] Refactor `internal/storage/balance_repository.go`
- [ ] Return structured token balances with protocol info
- [ ] Methods:
  - `GetAddressBalances(address, chain)` → `[]TokenBalance`
  - `GetAddressAllChains(address)` → `map[chain][]TokenBalance`
  - `GetPortfolioBalances(addresses)` → aggregated

#### 2.2 Token Balance Structure
```go
type TokenBalance struct {
    TokenAddress  string
    Chain         string
    Symbol        string
    Name          string
    Decimals      int
    RawAmount     string    // uint256 as string
    Amount        string    // human-readable
    ProtocolID    string    // empty = wallet
    PositionType  string    // wallet, supplied, borrowed, etc.
    LogoURL       string
}
```

#### 2.3 Protocol Position Service
- [ ] Create `internal/service/position_service.go`
- [ ] Group balances by protocol
- [ ] Separate supply/borrow/reward tokens
- [ ] Methods:
  - `GetProtocolPositions(address, chain)` → `[]ProtocolPosition`
  - `GetAllProtocolPositions(address)` → `map[chain][]ProtocolPosition`

---

### Task 3: Daily Snapshots
**Priority: High | Estimate: 4h**

#### 3.1 Snapshot Tables (ClickHouse)
- [ ] Create migration `008_create_balance_snapshots.sql`
- [ ] Create migration `009_create_position_snapshots.sql`

#### 3.2 Snapshot Repository
- [ ] Create `internal/storage/snapshot_repository.go`
- [ ] Methods:
  - `CreateBalanceSnapshot(address, chain, date, balances)`
  - `CreatePositionSnapshot(address, chain, date, positions)`
  - `GetBalanceHistory(address, chain, token, fromDate, toDate)`
  - `GetPositionHistory(address, chain, protocol, fromDate, toDate)`

#### 3.3 Snapshot Worker
- [ ] Create `cmd/snapshot/main.go` or add to existing worker
- [ ] Daily job at 00:00 UTC
- [ ] Calculate balances for all tracked addresses
- [ ] Store snapshots in ClickHouse

#### 3.4 Historical Backfill
- [ ] Backfill snapshots from transaction history
- [ ] Calculate daily balances for past dates

---

### Task 4: Aggregation APIs
**Priority: Medium | Estimate: 3h**

#### 4.1 Balance Endpoints
- [ ] `GET /api/portfolios/:id/balances` - all balances
- [ ] `GET /api/portfolios/:id/balances?chain=ethereum` - filter by chain
- [ ] `GET /api/addresses/:address/balances` - single address

#### 4.2 Protocol Position Endpoints
- [ ] `GET /api/portfolios/:id/protocols` - all protocol positions
- [ ] `GET /api/portfolios/:id/protocols/:slug` - specific protocol
- [ ] `GET /api/addresses/:address/protocols` - single address

#### 4.3 Aggregation Endpoints
- [ ] `GET /api/portfolios/:id/balances/by-chain` - grouped by chain
- [ ] `GET /api/portfolios/:id/balances/by-token` - grouped by token
- [ ] `GET /api/portfolios/:id/holdings?limit=10` - top holdings

#### 4.4 History Endpoints
- [ ] `GET /api/portfolios/:id/history?from=&to=` - balance history
- [ ] `GET /api/portfolios/:id/history/daily` - daily snapshots

---

### Task 5: DeFi Detection Enhancement
**Priority: Medium | Estimate: 3h**

#### 5.1 Contract-based Detection
- [ ] Match token contract against `protocol_contracts` table
- [ ] Return protocol info and position type

#### 5.2 Pattern-based Detection
- [ ] Match token symbol against `token_patterns` table
- [ ] Fallback when contract not in registry

#### 5.3 Event-based Detection
- [ ] Use Goldsky events to identify DeFi interactions
- [ ] Aave Supply/Withdraw events
- [ ] Uniswap Swap events
- [ ] Compound Mint/Redeem events

---

## Data Flow

```
Transactions (ClickHouse) + Goldsky Data
        ↓
Balance Calculation (balance_repository.go)
        ↓
Token Registry Lookup (Postgres)
        ↓
Protocol Detection (contract + pattern matching)
        ↓
Position Grouping (supply/borrow/reward)
        ↓
Daily Snapshot (balance_snapshots table)
        ↓
Aggregation APIs (by chain, by token, by protocol)
```

---

## API Response Examples

### GET /api/portfolios/:id/balances
```json
{
  "address_count": 3,
  "chain_count": 2,
  "balances": [
    {
      "token_address": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
      "chain": "ethereum",
      "symbol": "USDC",
      "name": "USD Coin",
      "decimals": 6,
      "raw_amount": "1000000000",
      "amount": "1000.00",
      "protocol_id": "",
      "position_type": "wallet",
      "logo_url": "..."
    },
    {
      "token_address": "0x98c23e9d8f34fefb1b7bd6a91b7ff122f4e16f5c",
      "chain": "ethereum",
      "symbol": "aUSDC",
      "name": "Aave USDC",
      "decimals": 6,
      "raw_amount": "500000000",
      "amount": "500.00",
      "protocol_id": "aave3",
      "position_type": "supplied",
      "logo_url": "..."
    }
  ]
}
```

### GET /api/portfolios/:id/protocols
```json
{
  "protocols": [
    {
      "id": "aave3",
      "name": "Aave V3",
      "chain": "ethereum",
      "category": "lending",
      "logo_url": "...",
      "positions": [
        {
          "name": "Lending",
          "position_type": "lending",
          "supply_tokens": [
            {"symbol": "USDC", "amount": "500.00", "raw_amount": "500000000"}
          ],
          "borrow_tokens": [
            {"symbol": "ETH", "amount": "0.1", "raw_amount": "100000000000000000"}
          ],
          "reward_tokens": []
        }
      ]
    }
  ]
}
```

---

## Notes

- All token balances stored as raw uint256 strings for precision
- Protocol detection uses hybrid: contract mapping → pattern matching → event detection
- Snapshots generated daily at 00:00 UTC
- Fiat/USD pricing handled by separate service
- DeBank-aligned data model for future compatibility
