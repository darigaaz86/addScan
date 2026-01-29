package adapter

import (
	"context"
	"fmt"

	"github.com/address-scanner/internal/types"
)

// ChainAdapter defines the interface for blockchain-specific adapters
type ChainAdapter interface {
	// NormalizeTransaction converts blockchain-specific transaction to common format
	// Returns error if transaction format is invalid
	NormalizeTransaction(rawTx interface{}) (*types.NormalizedTransaction, error)

	// FetchTransactions retrieves transactions for an address from data provider
	// fromBlock and toBlock are optional - if nil, fetches from genesis/latest
	// Returns error if provider request fails
	FetchTransactions(ctx context.Context, address string, fromBlock *uint64, toBlock *uint64) ([]*types.NormalizedTransaction, error)

	// FetchTransactionHistory retrieves historical transactions for an address using provider's optimized API
	// This is much more efficient than block scanning for backfill operations
	// maxCount limits the number of transactions returned (for tier-based limits)
	// Returns error if provider request fails
	FetchTransactionHistory(ctx context.Context, address string, maxCount int) ([]*types.NormalizedTransaction, error)

	// FetchTransactionsForBlock retrieves transactions for tracked addresses in a specific block
	// This is optimized for sync workers using eth_getLogs with address filters
	// Returns error if provider request fails
	FetchTransactionsForBlock(ctx context.Context, blockNum uint64, addresses []string) ([]*types.NormalizedTransaction, error)

	// GetCurrentBlock returns the current block number for the chain
	// Returns error if provider request fails
	GetCurrentBlock(ctx context.Context) (uint64, error)

	// GetBlockByTimestamp returns the block number closest to the given timestamp
	// Used for backfill operations to convert time ranges to block ranges
	// Returns error if provider request fails
	GetBlockByTimestamp(ctx context.Context, timestamp int64) (uint64, error)

	// GetBalance retrieves address balance on this chain
	// Returns error if provider request fails
	GetBalance(ctx context.Context, address string) (*types.ChainBalance, error)

	// ValidateAddress checks if address format is valid for this chain
	ValidateAddress(address string) bool

	// GetChainID returns the chain identifier
	GetChainID() types.ChainID
}

// Common error types for chain adapters

var (
	// ErrInvalidTransaction indicates the transaction format is invalid
	ErrInvalidTransaction = fmt.Errorf("invalid transaction format")

	// ErrInvalidAddress indicates the address format is invalid
	ErrInvalidAddress = fmt.Errorf("invalid address format")

	// ErrProviderUnavailable indicates the data provider is unavailable
	ErrProviderUnavailable = fmt.Errorf("data provider unavailable")

	// ErrProviderRateLimit indicates the provider rate limit was exceeded
	ErrProviderRateLimit = fmt.Errorf("provider rate limit exceeded")

	// ErrProviderTimeout indicates the provider request timed out
	ErrProviderTimeout = fmt.Errorf("provider request timeout")

	// ErrBlockNotFound indicates the requested block was not found
	ErrBlockNotFound = fmt.Errorf("block not found")

	// ErrTransactionNotFound indicates the requested transaction was not found
	ErrTransactionNotFound = fmt.Errorf("transaction not found")

	// ErrInvalidBlockRange indicates an invalid block range was specified
	ErrInvalidBlockRange = fmt.Errorf("invalid block range")
)

// AdapterError wraps errors with additional context
type AdapterError struct {
	Chain   types.ChainID
	Op      string // Operation that failed (e.g., "FetchTransactions", "GetBalance")
	Err     error
	Details map[string]interface{}
}

func (e *AdapterError) Error() string {
	if len(e.Details) > 0 {
		return fmt.Sprintf("chain adapter error [%s:%s]: %v (details: %+v)", e.Chain, e.Op, e.Err, e.Details)
	}
	return fmt.Sprintf("chain adapter error [%s:%s]: %v", e.Chain, e.Op, e.Err)
}

func (e *AdapterError) Unwrap() error {
	return e.Err
}

// NewAdapterError creates a new AdapterError
func NewAdapterError(chain types.ChainID, op string, err error, details map[string]interface{}) *AdapterError {
	return &AdapterError{
		Chain:   chain,
		Op:      op,
		Err:     err,
		Details: details,
	}
}
