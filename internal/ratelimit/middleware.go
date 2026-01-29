// Package ratelimit provides CU (Compute Unit) rate limiting for Alchemy RPC calls.
package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

// Default middleware configuration values.
const (
	DefaultMaxWait = 30 * time.Second // Default max time to wait for budget
)

// ErrMaxWaitExceeded is returned when the maximum wait time for budget is exceeded.
var ErrMaxWaitExceeded = errors.New("maximum wait time exceeded waiting for rate limit budget")

// EthClient defines the interface for Ethereum client operations that we rate limit.
// This interface allows for easier testing and mocking.
type EthClient interface {
	BlockNumber(ctx context.Context) (uint64, error)
	BlockByNumber(ctx context.Context, number *big.Int) (*types.Block, error)
	FilterLogs(ctx context.Context, q ethereum.FilterQuery) ([]types.Log, error)
	TransactionByHash(ctx context.Context, hash common.Hash) (*types.Transaction, bool, error)
	TransactionReceipt(ctx context.Context, hash common.Hash) (*types.Receipt, error)
}

// RateLimitedClient wraps an RPC client with rate limiting.
// It intercepts all outgoing RPC calls and enforces CU budget limits
// before allowing the call to proceed.
type RateLimitedClient struct {
	underlying   EthClient
	tracker      *CUBudgetTracker
	costRegistry *CUCostRegistry
	priority     Priority
	maxWait      time.Duration
	logger       *log.Logger
}

// RateLimitedClientConfig holds configuration for the rate-limited client.
type RateLimitedClientConfig struct {
	// Client is the underlying Ethereum client to wrap.
	// Required - middleware cannot function without an underlying client.
	Client EthClient

	// Tracker is the CU budget tracker for rate limiting.
	// Required - middleware cannot function without a tracker.
	Tracker *CUBudgetTracker

	// CostRegistry is the registry for looking up RPC method costs.
	// Required - middleware cannot function without a cost registry.
	CostRegistry *CUCostRegistry

	// Priority is the priority level for this client's requests.
	// PriorityHigh for real-time sync, PriorityLow for backfill.
	Priority Priority

	// MaxWait is the maximum time to wait for budget availability.
	// Default: 30s. If budget is not available within this time,
	// the call returns ErrMaxWaitExceeded.
	MaxWait time.Duration

	// Logger is an optional logger for rate limit events.
	// If nil, a default logger writing to stdout is used.
	Logger *log.Logger
}

// Validate checks if the configuration is valid.
func (c *RateLimitedClientConfig) Validate() error {
	if c.Client == nil {
		return errors.New("underlying client is required")
	}
	if c.Tracker == nil {
		return errors.New("budget tracker is required")
	}
	if c.CostRegistry == nil {
		return errors.New("cost registry is required")
	}
	return nil
}

// Ensure ethclient.Client implements EthClient interface
var _ EthClient = (*ethclient.Client)(nil)

// NewRateLimitedClient creates a rate-limited RPC client.
// Returns an error if the configuration is invalid.
func NewRateLimitedClient(cfg *RateLimitedClientConfig) (*RateLimitedClient, error) {
	if cfg == nil {
		return nil, errors.New("configuration is required")
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	maxWait := cfg.MaxWait
	if maxWait == 0 {
		maxWait = DefaultMaxWait
	}

	logger := cfg.Logger
	if logger == nil {
		logger = log.Default()
	}

	return &RateLimitedClient{
		underlying:   cfg.Client,
		tracker:      cfg.Tracker,
		costRegistry: cfg.CostRegistry,
		priority:     cfg.Priority,
		maxWait:      maxWait,
		logger:       logger,
	}, nil
}

// waitForBudget waits until budget is available or context/maxWait is exceeded.
// It returns nil if budget was acquired, or an error if waiting failed.
func (c *RateLimitedClient) waitForBudget(ctx context.Context, method string, cu int) error {
	startTime := time.Now()
	deadline := startTime.Add(c.maxWait)

	for {
		// Check context cancellation
		select {
		case <-ctx.Done():
			c.logger.Printf("[RateLimit] %s: context cancelled while waiting for budget (priority=%s, cu=%d)",
				method, c.priority, cu)
			return ctx.Err()
		default:
		}

		// Try to consume budget
		allowed, waitTime := c.tracker.TryConsume(ctx, cu, c.priority)
		if allowed {
			// Record method-specific usage for monitoring
			if err := c.tracker.RecordMethodUsage(ctx, method, cu); err != nil {
				// Log but don't fail - method tracking is for monitoring only
				c.logger.Printf("[RateLimit] %s: failed to record method usage: %v", method, err)
			}
			return nil
		}

		// Check if we've exceeded max wait time
		if time.Now().Add(waitTime).After(deadline) {
			c.logger.Printf("[RateLimit] %s: max wait time exceeded (priority=%s, cu=%d, waited=%v)",
				method, c.priority, cu, time.Since(startTime))
			return ErrMaxWaitExceeded
		}

		// Log rate limit event
		c.logger.Printf("[RateLimit] %s: waiting for budget (priority=%s, cu=%d, wait=%v)",
			method, c.priority, cu, waitTime)

		// Wait for the suggested time or context cancellation
		select {
		case <-ctx.Done():
			c.logger.Printf("[RateLimit] %s: context cancelled while waiting (priority=%s, cu=%d)",
				method, c.priority, cu)
			return ctx.Err()
		case <-time.After(waitTime):
			// Continue to retry
		}
	}
}

// BlockNumber wraps eth_blockNumber with rate limiting.
// Requirement 5.1: Intercepts outgoing RPC call
// Requirement 5.2: Checks available budget before proceeding
// Requirement 5.3: Allows call and records consumption if budget available
// Requirement 5.4: Blocks call until budget available if exhausted
// Requirement 5.5: Supports priority levels
// Requirement 5.6: Logs rate limit events
func (c *RateLimitedClient) BlockNumber(ctx context.Context) (uint64, error) {
	method := MethodEthBlockNumber
	cu := c.costRegistry.GetCost(method)

	if err := c.waitForBudget(ctx, method, cu); err != nil {
		return 0, fmt.Errorf("rate limit: %w", err)
	}

	return c.underlying.BlockNumber(ctx)
}

// BlockByNumber wraps eth_getBlockByNumber with rate limiting.
// Requirement 5.1: Intercepts outgoing RPC call
// Requirement 5.2: Checks available budget before proceeding
// Requirement 5.3: Allows call and records consumption if budget available
// Requirement 5.4: Blocks call until budget available if exhausted
// Requirement 5.5: Supports priority levels
// Requirement 5.6: Logs rate limit events
func (c *RateLimitedClient) BlockByNumber(ctx context.Context, number *big.Int) (*types.Block, error) {
	method := MethodEthGetBlockByNumber
	cu := c.costRegistry.GetCost(method)

	if err := c.waitForBudget(ctx, method, cu); err != nil {
		return nil, fmt.Errorf("rate limit: %w", err)
	}

	return c.underlying.BlockByNumber(ctx, number)
}

// FilterLogs wraps eth_getLogs with rate limiting.
// Requirement 5.1: Intercepts outgoing RPC call
// Requirement 5.2: Checks available budget before proceeding
// Requirement 5.3: Allows call and records consumption if budget available
// Requirement 5.4: Blocks call until budget available if exhausted
// Requirement 5.5: Supports priority levels
// Requirement 5.6: Logs rate limit events
func (c *RateLimitedClient) FilterLogs(ctx context.Context, q ethereum.FilterQuery) ([]types.Log, error) {
	method := MethodEthGetLogs
	cu := c.costRegistry.GetCost(method)

	if err := c.waitForBudget(ctx, method, cu); err != nil {
		return nil, fmt.Errorf("rate limit: %w", err)
	}

	return c.underlying.FilterLogs(ctx, q)
}

// TransactionByHash wraps eth_getTransactionByHash with rate limiting.
// Requirement 5.1: Intercepts outgoing RPC call
// Requirement 5.2: Checks available budget before proceeding
// Requirement 5.3: Allows call and records consumption if budget available
// Requirement 5.4: Blocks call until budget available if exhausted
// Requirement 5.5: Supports priority levels
// Requirement 5.6: Logs rate limit events
func (c *RateLimitedClient) TransactionByHash(ctx context.Context, hash common.Hash) (*types.Transaction, bool, error) {
	method := MethodEthGetTransactionByHash
	cu := c.costRegistry.GetCost(method)

	if err := c.waitForBudget(ctx, method, cu); err != nil {
		return nil, false, fmt.Errorf("rate limit: %w", err)
	}

	return c.underlying.TransactionByHash(ctx, hash)
}

// TransactionReceipt wraps eth_getTransactionReceipt with rate limiting.
// Requirement 5.1: Intercepts outgoing RPC call
// Requirement 5.2: Checks available budget before proceeding
// Requirement 5.3: Allows call and records consumption if budget available
// Requirement 5.4: Blocks call until budget available if exhausted
// Requirement 5.5: Supports priority levels
// Requirement 5.6: Logs rate limit events
func (c *RateLimitedClient) TransactionReceipt(ctx context.Context, hash common.Hash) (*types.Receipt, error) {
	method := MethodEthGetTransactionReceipt
	cu := c.costRegistry.GetCost(method)

	if err := c.waitForBudget(ctx, method, cu); err != nil {
		return nil, fmt.Errorf("rate limit: %w", err)
	}

	return c.underlying.TransactionReceipt(ctx, hash)
}

// GetPriority returns the priority level of this client.
func (c *RateLimitedClient) GetPriority() Priority {
	return c.priority
}

// GetMaxWait returns the maximum wait time for budget availability.
func (c *RateLimitedClient) GetMaxWait() time.Duration {
	return c.maxWait
}

// Underlying returns the underlying EthClient.
// This can be used for operations that don't need rate limiting
// or for accessing methods not wrapped by RateLimitedClient.
func (c *RateLimitedClient) Underlying() EthClient {
	return c.underlying
}
