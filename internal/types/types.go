// Package types provides common type definitions for the address scanner system.
package types

import "time"

// UserTier represents the service tier level
type UserTier string

const (
	// TierFree represents the free service tier with limited features
	TierFree UserTier = "free"
	// TierPaid represents the paid service tier with full features
	TierPaid UserTier = "paid"
)

// AddressClassification represents address activity level
type AddressClassification string

const (
	// ClassificationNormal represents addresses with standard transaction volume
	ClassificationNormal AddressClassification = "normal"
	// ClassificationSuper represents addresses with high transaction volume
	ClassificationSuper AddressClassification = "super"
)

// TransactionStatus represents transaction execution status
type TransactionStatus string

const (
	// StatusSuccess represents a successful transaction
	StatusSuccess TransactionStatus = "success"
	// StatusFailed represents a failed transaction
	StatusFailed TransactionStatus = "failed"
)

// TransactionDirection represents whether a transaction is incoming or outgoing
type TransactionDirection string

const (
	// DirectionIn represents an incoming transaction (address is recipient)
	DirectionIn TransactionDirection = "in"
	// DirectionOut represents an outgoing transaction (address is sender)
	DirectionOut TransactionDirection = "out"
)

// ChainID represents supported blockchain networks
type ChainID string

const (
	// ChainEthereum represents the Ethereum mainnet
	ChainEthereum ChainID = "ethereum"
	// ChainPolygon represents the Polygon network
	ChainPolygon ChainID = "polygon"
	// ChainArbitrum represents the Arbitrum network
	ChainArbitrum ChainID = "arbitrum"
	// ChainOptimism represents the Optimism network
	ChainOptimism ChainID = "optimism"
	// ChainBase represents the Base network
	ChainBase ChainID = "base"
	// ChainBNB represents the BNB Chain (BSC)
	ChainBNB ChainID = "bnb"
)

// BackfillStatus represents the status of a backfill job
type BackfillStatus string

const (
	// BackfillStatusQueued represents a job waiting to be processed
	BackfillStatusQueued BackfillStatus = "queued"
	// BackfillStatusInProgress represents a job currently being processed
	BackfillStatusInProgress BackfillStatus = "in_progress"
	// BackfillStatusCompleted represents a successfully completed job
	BackfillStatusCompleted BackfillStatus = "completed"
	// BackfillStatusFailed represents a failed job
	BackfillStatusFailed BackfillStatus = "failed"
)

// ServiceError represents a structured error response
type ServiceError struct {
	Code    string                 `json:"code"`
	Message string                 `json:"message"`
	Details map[string]interface{} `json:"details,omitempty"`
}

func (e *ServiceError) Error() string {
	return e.Message
}

// TokenTransfer represents a token transfer within a transaction
type TokenTransfer struct {
	Token    string  `json:"token"`              // Token contract address
	From     string  `json:"from"`               // Sender address
	To       string  `json:"to"`                 // Recipient address
	Value    string  `json:"value"`              // Transfer amount (as string for big numbers)
	Symbol   *string `json:"symbol,omitempty"`   // Token symbol (e.g., "USDC")
	Decimals *int    `json:"decimals,omitempty"` // Token decimals
}

// TokenBalance represents balance for a specific token
type TokenBalance struct {
	Token    string  `json:"token"`              // Token contract address
	Symbol   string  `json:"symbol"`             // Token symbol
	Balance  string  `json:"balance"`            // Token balance
	Decimals int     `json:"decimals"`           // Token decimals
	ValueUSD *string `json:"valueUsd,omitempty"` // USD value if available
}

// ChainBalance represents balance for a single chain
type ChainBalance struct {
	Chain         ChainID        `json:"chain"`
	NativeBalance string         `json:"nativeBalance"` // Native token balance (ETH, MATIC, etc.)
	TokenBalances []TokenBalance `json:"tokenBalances"`
}

// MultiChainBalance represents aggregated balance across chains
type MultiChainBalance struct {
	Chains        []ChainBalance `json:"chains"`
	TotalValueUSD *string        `json:"totalValueUsd,omitempty"` // Total portfolio value in USD
}

// Counterparty represents a transaction counterparty
type Counterparty struct {
	Address          string  `json:"address"`
	TransactionCount int64   `json:"transactionCount"`
	TotalVolume      string  `json:"totalVolume"`
	Label            *string `json:"label,omitempty"` // Known label (e.g., "Uniswap Router")
}

// TokenHolding represents token holdings across chains
type TokenHolding struct {
	Token    string    `json:"token"`
	Symbol   string    `json:"symbol"`
	Balance  string    `json:"balance"`
	Decimals int       `json:"decimals"`
	Chains   []ChainID `json:"chains"` // Chains where this token is held
	ValueUSD *string   `json:"valueUsd,omitempty"`
}

// NormalizedTransaction represents a transaction in common format across all chains
type NormalizedTransaction struct {
	Hash           string               `json:"hash"`
	Chain          ChainID              `json:"chain"`
	From           string               `json:"from"`
	To             string               `json:"to"`
	Value          string               `json:"value"`
	Asset          *string              `json:"asset,omitempty"`    // Primary asset symbol (e.g., "USDT", "ETH")
	Category       *string              `json:"category,omitempty"` // Transaction category (e.g., "erc20", "external")
	Direction      TransactionDirection `json:"direction"`          // "in" or "out" relative to tracked address
	Timestamp      int64                `json:"timestamp"`          // Unix timestamp
	BlockNumber    uint64               `json:"blockNumber"`
	Status         TransactionStatus    `json:"status"`
	GasUsed        *string              `json:"gasUsed,omitempty"`
	GasPrice       *string              `json:"gasPrice,omitempty"`
	TokenTransfers []TokenTransfer      `json:"tokenTransfers,omitempty"`
	MethodID       *string              `json:"methodId,omitempty"`
	FuncName       *string              `json:"funcName,omitempty"` // Function name (e.g., "approve", "transfer", "withdraw")
	Input          *string              `json:"input,omitempty"`
}

// SyncStatus represents sync status for an address on a chain
type SyncStatus struct {
	Chain            ChainID   `json:"chain"`
	LastSyncedBlock  uint64    `json:"lastSyncedBlock"`
	BackfillComplete bool      `json:"backfillComplete"`
	LastSyncAt       time.Time `json:"lastSyncAt"`
}
