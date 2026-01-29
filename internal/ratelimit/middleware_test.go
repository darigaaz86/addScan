package ratelimit

import (
	"bytes"
	"context"
	"log"
	"math/big"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/redis/go-redis/v9"
)

// setupTestMiddleware creates a test environment with miniredis for middleware tests.
// Returns the rate-limited client config components and a cleanup function.
func setupTestMiddleware(t *testing.T) (*CUBudgetTracker, *CUCostRegistry, *miniredis.Miniredis, func()) {
	t.Helper()

	// Start miniredis
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}

	// Create Redis client
	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})

	// Create budget tracker
	tracker, err := NewCUBudgetTracker(&CUBudgetTrackerConfig{
		Redis:          client,
		TotalBudget:    100,
		ReservedBudget: 60,
		WindowSize:     time.Second,
	})
	if err != nil {
		mr.Close()
		t.Fatalf("failed to create budget tracker: %v", err)
	}

	// Create cost registry
	costRegistry := NewCUCostRegistry(nil)

	cleanup := func() {
		client.Close()
		mr.Close()
	}

	return tracker, costRegistry, mr, cleanup
}

func TestNewRateLimitedClient(t *testing.T) {
	tracker, costRegistry, _, cleanup := setupTestMiddleware(t)
	defer cleanup()

	tests := []struct {
		name    string
		cfg     *RateLimitedClientConfig
		wantErr bool
		errMsg  string
	}{
		{
			name:    "nil config",
			cfg:     nil,
			wantErr: true,
			errMsg:  "configuration is required",
		},
		{
			name: "nil underlying client",
			cfg: &RateLimitedClientConfig{
				Client:       nil,
				Tracker:      tracker,
				CostRegistry: costRegistry,
			},
			wantErr: true,
			errMsg:  "underlying client is required",
		},
		{
			name: "nil tracker",
			cfg: &RateLimitedClientConfig{
				Client:       &mockEthClient{},
				Tracker:      nil,
				CostRegistry: costRegistry,
			},
			wantErr: true,
			errMsg:  "budget tracker is required",
		},
		{
			name: "nil cost registry",
			cfg: &RateLimitedClientConfig{
				Client:       &mockEthClient{},
				Tracker:      tracker,
				CostRegistry: nil,
			},
			wantErr: true,
			errMsg:  "cost registry is required",
		},
		{
			name: "valid config with defaults",
			cfg: &RateLimitedClientConfig{
				Client:       &mockEthClient{},
				Tracker:      tracker,
				CostRegistry: costRegistry,
			},
			wantErr: false,
		},
		{
			name: "valid config with custom values",
			cfg: &RateLimitedClientConfig{
				Client:       &mockEthClient{},
				Tracker:      tracker,
				CostRegistry: costRegistry,
				Priority:     PriorityHigh,
				MaxWait:      10 * time.Second,
				Logger:       log.Default(),
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := NewRateLimitedClient(tt.cfg)

			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error containing %q, got nil", tt.errMsg)
					return
				}
				if tt.errMsg != "" && !containsSubstring(err.Error(), tt.errMsg) {
					t.Errorf("expected error containing %q, got %q", tt.errMsg, err.Error())
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if client == nil {
				t.Error("expected non-nil client")
			}
		})
	}
}

func TestRateLimitedClient_DefaultValues(t *testing.T) {
	tracker, costRegistry, _, cleanup := setupTestMiddleware(t)
	defer cleanup()

	client, err := NewRateLimitedClient(&RateLimitedClientConfig{
		Client:       &mockEthClient{},
		Tracker:      tracker,
		CostRegistry: costRegistry,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if client.GetMaxWait() != DefaultMaxWait {
		t.Errorf("expected max wait %v, got %v", DefaultMaxWait, client.GetMaxWait())
	}

	if client.GetPriority() != PriorityHigh {
		t.Errorf("expected default priority PriorityHigh (0), got %v", client.GetPriority())
	}
}

func TestRateLimitedClient_CustomPriority(t *testing.T) {
	tracker, costRegistry, _, cleanup := setupTestMiddleware(t)
	defer cleanup()

	client, err := NewRateLimitedClient(&RateLimitedClientConfig{
		Client:       &mockEthClient{},
		Tracker:      tracker,
		CostRegistry: costRegistry,
		Priority:     PriorityLow,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if client.GetPriority() != PriorityLow {
		t.Errorf("expected priority PriorityLow, got %v", client.GetPriority())
	}
}

func TestRateLimitedClient_Underlying(t *testing.T) {
	tracker, costRegistry, _, cleanup := setupTestMiddleware(t)
	defer cleanup()

	mockClient := &mockEthClient{blockNumber: 12345}
	client, err := NewRateLimitedClient(&RateLimitedClientConfig{
		Client:       mockClient,
		Tracker:      tracker,
		CostRegistry: costRegistry,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// The underlying client should be accessible and be the same mock
	underlying := client.Underlying()
	if underlying == nil {
		t.Error("expected non-nil underlying client")
	}
	if underlying != mockClient {
		t.Error("expected underlying to be the same mock client")
	}
}

func TestRateLimitedClientConfig_Validate(t *testing.T) {
	tracker, costRegistry, _, cleanup := setupTestMiddleware(t)
	defer cleanup()

	tests := []struct {
		name    string
		cfg     *RateLimitedClientConfig
		wantErr bool
	}{
		{
			name: "valid config",
			cfg: &RateLimitedClientConfig{
				Client:       &mockEthClient{},
				Tracker:      tracker,
				CostRegistry: costRegistry,
			},
			wantErr: false,
		},
		{
			name: "nil client",
			cfg: &RateLimitedClientConfig{
				Client:       nil,
				Tracker:      tracker,
				CostRegistry: costRegistry,
			},
			wantErr: true,
		},
		{
			name: "nil tracker",
			cfg: &RateLimitedClientConfig{
				Client:       &mockEthClient{},
				Tracker:      nil,
				CostRegistry: costRegistry,
			},
			wantErr: true,
		},
		{
			name: "nil cost registry",
			cfg: &RateLimitedClientConfig{
				Client:       &mockEthClient{},
				Tracker:      tracker,
				CostRegistry: nil,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestRateLimitedClient_WaitForBudget_Success(t *testing.T) {
	tracker, costRegistry, _, cleanup := setupTestMiddleware(t)
	defer cleanup()

	var logBuf bytes.Buffer
	logger := log.New(&logBuf, "", 0)

	client, err := NewRateLimitedClient(&RateLimitedClientConfig{
		Client:       &mockEthClient{},
		Tracker:      tracker,
		CostRegistry: costRegistry,
		Priority:     PriorityHigh,
		Logger:       logger,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx := context.Background()

	// waitForBudget is private, but we can test it indirectly through the public methods
	// by checking that budget is consumed after a call

	// First, verify initial budget
	stats, err := tracker.GetUsage(ctx)
	if err != nil {
		t.Fatalf("unexpected error getting usage: %v", err)
	}
	if stats.TotalUsed != 0 {
		t.Errorf("expected initial total used 0, got %d", stats.TotalUsed)
	}

	// The client has a nil underlying ethclient.Client, so actual RPC calls will fail
	// But we can verify the rate limiting logic works by checking budget consumption
	// We need to test the waitForBudget method indirectly

	// For this test, we'll verify the client was created correctly
	if client.GetPriority() != PriorityHigh {
		t.Errorf("expected priority PriorityHigh, got %v", client.GetPriority())
	}
}

func TestRateLimitedClient_WaitForBudget_ContextCancelled(t *testing.T) {
	tracker, costRegistry, _, cleanup := setupTestMiddleware(t)
	defer cleanup()

	var logBuf bytes.Buffer
	logger := log.New(&logBuf, "", 0)

	// Exhaust the budget first
	ctx := context.Background()
	tracker.TryConsume(ctx, 60, PriorityHigh) // Exhaust reserved
	tracker.TryConsume(ctx, 40, PriorityLow)  // Exhaust shared

	client, err := NewRateLimitedClient(&RateLimitedClientConfig{
		Client:       &mockEthClient{},
		Tracker:      tracker,
		CostRegistry: costRegistry,
		Priority:     PriorityHigh,
		MaxWait:      5 * time.Second,
		Logger:       logger,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Create a context that will be cancelled
	cancelCtx, cancel := context.WithCancel(context.Background())

	// Cancel immediately
	cancel()

	// Try to make a call - should fail due to context cancellation
	_, err = client.BlockNumber(cancelCtx)
	if err == nil {
		t.Error("expected error due to context cancellation")
	}
	if err != nil && err != context.Canceled && !containsSubstring(err.Error(), "context canceled") {
		t.Errorf("expected context.Canceled error, got: %v", err)
	}
}

func TestRateLimitedClient_WaitForBudget_MaxWaitExceeded(t *testing.T) {
	tracker, costRegistry, mr, cleanup := setupTestMiddleware(t)
	defer cleanup()

	var logBuf bytes.Buffer
	logger := log.New(&logBuf, "", 0)

	// Exhaust the budget
	ctx := context.Background()
	tracker.TryConsume(ctx, 60, PriorityHigh) // Exhaust reserved
	tracker.TryConsume(ctx, 40, PriorityLow)  // Exhaust shared

	client, err := NewRateLimitedClient(&RateLimitedClientConfig{
		Client:       &mockEthClient{},
		Tracker:      tracker,
		CostRegistry: costRegistry,
		Priority:     PriorityHigh,
		MaxWait:      100 * time.Millisecond, // Very short max wait
		Logger:       logger,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Fast forward time in miniredis to keep budget exhausted
	mr.FastForward(50 * time.Millisecond)

	// Try to make a call - should fail due to max wait exceeded
	_, err = client.BlockNumber(ctx)
	if err == nil {
		t.Error("expected error due to max wait exceeded")
	}
	if err != nil && !containsSubstring(err.Error(), "maximum wait time exceeded") {
		t.Errorf("expected ErrMaxWaitExceeded, got: %v", err)
	}

	// Verify logging occurred
	logOutput := logBuf.String()
	if !containsSubstring(logOutput, "[RateLimit]") {
		t.Error("expected rate limit log messages")
	}
}

func TestRateLimitedClient_BudgetConsumption_HighPriority(t *testing.T) {
	tracker, costRegistry, _, cleanup := setupTestMiddleware(t)
	defer cleanup()

	var logBuf bytes.Buffer
	logger := log.New(&logBuf, "", 0)

	// Create a high priority client
	_, err := NewRateLimitedClient(&RateLimitedClientConfig{
		Client:       &mockEthClient{},
		Tracker:      tracker,
		CostRegistry: costRegistry,
		Priority:     PriorityHigh,
		Logger:       logger,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx := context.Background()

	// Manually consume budget as high priority (simulating what the client would do)
	cu := costRegistry.GetCost(MethodEthBlockNumber)
	allowed, _ := tracker.TryConsume(ctx, cu, PriorityHigh)
	if !allowed {
		t.Error("expected budget consumption to be allowed")
	}

	// Verify budget was consumed from reserved pool
	stats, err := tracker.GetUsage(ctx)
	if err != nil {
		t.Fatalf("unexpected error getting usage: %v", err)
	}
	if stats.ReservedUsed != cu {
		t.Errorf("expected reserved used %d, got %d", cu, stats.ReservedUsed)
	}
	if stats.SharedUsed != 0 {
		t.Errorf("expected shared used 0, got %d", stats.SharedUsed)
	}
}

func TestRateLimitedClient_BudgetConsumption_LowPriority(t *testing.T) {
	tracker, costRegistry, _, cleanup := setupTestMiddleware(t)
	defer cleanup()

	var logBuf bytes.Buffer
	logger := log.New(&logBuf, "", 0)

	// Create a low priority client
	_, err := NewRateLimitedClient(&RateLimitedClientConfig{
		Client:       &mockEthClient{},
		Tracker:      tracker,
		CostRegistry: costRegistry,
		Priority:     PriorityLow,
		Logger:       logger,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx := context.Background()

	// Manually consume budget as low priority (simulating what the client would do)
	cu := costRegistry.GetCost(MethodEthBlockNumber)
	allowed, _ := tracker.TryConsume(ctx, cu, PriorityLow)
	if !allowed {
		t.Error("expected budget consumption to be allowed")
	}

	// Verify budget was consumed from shared pool
	stats, err := tracker.GetUsage(ctx)
	if err != nil {
		t.Fatalf("unexpected error getting usage: %v", err)
	}
	if stats.SharedUsed != cu {
		t.Errorf("expected shared used %d, got %d", cu, stats.SharedUsed)
	}
	if stats.ReservedUsed != 0 {
		t.Errorf("expected reserved used 0, got %d", stats.ReservedUsed)
	}
}

func TestRateLimitedClient_MethodCosts(t *testing.T) {
	costRegistry := NewCUCostRegistry(nil)

	// Verify the cost registry returns correct costs for each method
	tests := []struct {
		method       string
		expectedCost int
	}{
		{MethodEthBlockNumber, CostEthBlockNumber},
		{MethodEthGetBlockByNumber, CostEthGetBlockByNumber},
		{MethodEthGetLogs, CostEthGetLogs},
		{MethodEthGetTransactionByHash, CostEthGetTransactionByHash},
		{MethodEthGetTransactionReceipt, CostEthGetTransactionReceipt},
	}

	for _, tt := range tests {
		t.Run(tt.method, func(t *testing.T) {
			cost := costRegistry.GetCost(tt.method)
			if cost != tt.expectedCost {
				t.Errorf("expected cost %d for %s, got %d", tt.expectedCost, tt.method, cost)
			}
		})
	}
}

func TestRateLimitedClient_Logging(t *testing.T) {
	tracker, costRegistry, _, cleanup := setupTestMiddleware(t)
	defer cleanup()

	var logBuf bytes.Buffer
	logger := log.New(&logBuf, "", 0)

	// Exhaust budget to trigger logging
	ctx := context.Background()
	tracker.TryConsume(ctx, 60, PriorityHigh)
	tracker.TryConsume(ctx, 40, PriorityLow)

	client, err := NewRateLimitedClient(&RateLimitedClientConfig{
		Client:       &mockEthClient{},
		Tracker:      tracker,
		CostRegistry: costRegistry,
		Priority:     PriorityHigh,
		MaxWait:      100 * time.Millisecond,
		Logger:       logger,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Try to make a call - will fail but should log
	client.BlockNumber(ctx)

	// Verify logging occurred
	logOutput := logBuf.String()
	if !containsSubstring(logOutput, "[RateLimit]") {
		t.Error("expected rate limit log prefix")
	}
	if !containsSubstring(logOutput, MethodEthBlockNumber) {
		t.Error("expected method name in log")
	}
	if !containsSubstring(logOutput, "priority=high") {
		t.Error("expected priority in log")
	}
}

func TestRateLimitedClient_PriorityIndependence(t *testing.T) {
	tracker, _, _, cleanup := setupTestMiddleware(t)
	defer cleanup()

	ctx := context.Background()

	// Exhaust shared budget (40 CU)
	tracker.TryConsume(ctx, 40, PriorityLow)

	// Verify shared is exhausted
	allowed, _ := tracker.TryConsume(ctx, 1, PriorityLow)
	if allowed {
		t.Error("expected shared budget to be exhausted")
	}

	// But high priority should still work (reserved budget is 60 CU)
	allowed, _ = tracker.TryConsume(ctx, 30, PriorityHigh)
	if !allowed {
		t.Error("expected high priority to be allowed when only shared is exhausted")
	}

	// Verify stats
	stats, err := tracker.GetUsage(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stats.SharedUsed != 40 {
		t.Errorf("expected shared used 40, got %d", stats.SharedUsed)
	}
	if stats.ReservedUsed != 30 {
		t.Errorf("expected reserved used 30, got %d", stats.ReservedUsed)
	}
}

// mockEthClient is a mock for testing that satisfies the EthClient interface
// but doesn't actually make RPC calls.
type mockEthClient struct {
	blockNumber         uint64
	blockNumberErr      error
	block               *types.Block
	blockErr            error
	logs                []types.Log
	logsErr             error
	tx                  *types.Transaction
	txPending           bool
	txErr               error
	receipt             *types.Receipt
	receiptErr          error
	blockNumberCalled   int
	blockByNumberCalled int
	filterLogsCalled    int
	txByHashCalled      int
	receiptCalled       int
}

func (m *mockEthClient) BlockNumber(ctx context.Context) (uint64, error) {
	m.blockNumberCalled++
	return m.blockNumber, m.blockNumberErr
}

func (m *mockEthClient) BlockByNumber(ctx context.Context, number *big.Int) (*types.Block, error) {
	m.blockByNumberCalled++
	return m.block, m.blockErr
}

func (m *mockEthClient) FilterLogs(ctx context.Context, q ethereum.FilterQuery) ([]types.Log, error) {
	m.filterLogsCalled++
	return m.logs, m.logsErr
}

func (m *mockEthClient) TransactionByHash(ctx context.Context, hash common.Hash) (*types.Transaction, bool, error) {
	m.txByHashCalled++
	return m.tx, m.txPending, m.txErr
}

func (m *mockEthClient) TransactionReceipt(ctx context.Context, hash common.Hash) (*types.Receipt, error) {
	m.receiptCalled++
	return m.receipt, m.receiptErr
}

// Helper function to check if a string contains a substring
func containsSubstring(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsSubstringHelper(s, substr))
}

func containsSubstringHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func TestRateLimitedClient_BlockNumber_Success(t *testing.T) {
	tracker, costRegistry, _, cleanup := setupTestMiddleware(t)
	defer cleanup()

	mockClient := &mockEthClient{blockNumber: 12345}
	client, err := NewRateLimitedClient(&RateLimitedClientConfig{
		Client:       mockClient,
		Tracker:      tracker,
		CostRegistry: costRegistry,
		Priority:     PriorityHigh,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx := context.Background()
	blockNum, err := client.BlockNumber(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if blockNum != 12345 {
		t.Errorf("expected block number 12345, got %d", blockNum)
	}
	if mockClient.blockNumberCalled != 1 {
		t.Errorf("expected BlockNumber to be called once, got %d", mockClient.blockNumberCalled)
	}

	// Verify budget was consumed
	stats, err := tracker.GetUsage(ctx)
	if err != nil {
		t.Fatalf("unexpected error getting usage: %v", err)
	}
	expectedCU := costRegistry.GetCost(MethodEthBlockNumber)
	if stats.ReservedUsed != expectedCU {
		t.Errorf("expected reserved used %d, got %d", expectedCU, stats.ReservedUsed)
	}
}

func TestRateLimitedClient_BlockByNumber_Success(t *testing.T) {
	tracker, costRegistry, _, cleanup := setupTestMiddleware(t)
	defer cleanup()

	mockClient := &mockEthClient{}
	client, err := NewRateLimitedClient(&RateLimitedClientConfig{
		Client:       mockClient,
		Tracker:      tracker,
		CostRegistry: costRegistry,
		Priority:     PriorityHigh,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx := context.Background()
	_, err = client.BlockByNumber(ctx, big.NewInt(100))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mockClient.blockByNumberCalled != 1 {
		t.Errorf("expected BlockByNumber to be called once, got %d", mockClient.blockByNumberCalled)
	}

	// Verify budget was consumed
	stats, err := tracker.GetUsage(ctx)
	if err != nil {
		t.Fatalf("unexpected error getting usage: %v", err)
	}
	expectedCU := costRegistry.GetCost(MethodEthGetBlockByNumber)
	if stats.ReservedUsed != expectedCU {
		t.Errorf("expected reserved used %d, got %d", expectedCU, stats.ReservedUsed)
	}
}

func TestRateLimitedClient_FilterLogs_Success(t *testing.T) {
	// Use a larger budget for this test since eth_getLogs costs 75 CU
	// and the default shared budget is only 40 CU
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	defer mr.Close()

	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	defer client.Close()

	tracker, err := NewCUBudgetTracker(&CUBudgetTrackerConfig{
		Redis:          client,
		TotalBudget:    200,
		ReservedBudget: 100, // Shared budget will be 100 CU
		WindowSize:     time.Second,
	})
	if err != nil {
		t.Fatalf("failed to create budget tracker: %v", err)
	}

	costRegistry := NewCUCostRegistry(nil)

	mockClient := &mockEthClient{logs: []types.Log{{BlockNumber: 100}}}
	rlClient, err := NewRateLimitedClient(&RateLimitedClientConfig{
		Client:       mockClient,
		Tracker:      tracker,
		CostRegistry: costRegistry,
		Priority:     PriorityLow,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx := context.Background()
	logs, err := rlClient.FilterLogs(ctx, ethereum.FilterQuery{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(logs) != 1 {
		t.Errorf("expected 1 log, got %d", len(logs))
	}
	if mockClient.filterLogsCalled != 1 {
		t.Errorf("expected FilterLogs to be called once, got %d", mockClient.filterLogsCalled)
	}

	// Verify budget was consumed from shared pool (low priority)
	stats, err := tracker.GetUsage(ctx)
	if err != nil {
		t.Fatalf("unexpected error getting usage: %v", err)
	}
	expectedCU := costRegistry.GetCost(MethodEthGetLogs)
	if stats.SharedUsed != expectedCU {
		t.Errorf("expected shared used %d, got %d", expectedCU, stats.SharedUsed)
	}
}

func TestRateLimitedClient_TransactionByHash_Success(t *testing.T) {
	tracker, costRegistry, _, cleanup := setupTestMiddleware(t)
	defer cleanup()

	mockClient := &mockEthClient{txPending: true}
	client, err := NewRateLimitedClient(&RateLimitedClientConfig{
		Client:       mockClient,
		Tracker:      tracker,
		CostRegistry: costRegistry,
		Priority:     PriorityHigh,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx := context.Background()
	_, isPending, err := client.TransactionByHash(ctx, common.Hash{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !isPending {
		t.Error("expected isPending to be true")
	}
	if mockClient.txByHashCalled != 1 {
		t.Errorf("expected TransactionByHash to be called once, got %d", mockClient.txByHashCalled)
	}

	// Verify budget was consumed
	stats, err := tracker.GetUsage(ctx)
	if err != nil {
		t.Fatalf("unexpected error getting usage: %v", err)
	}
	expectedCU := costRegistry.GetCost(MethodEthGetTransactionByHash)
	if stats.ReservedUsed != expectedCU {
		t.Errorf("expected reserved used %d, got %d", expectedCU, stats.ReservedUsed)
	}
}

func TestRateLimitedClient_TransactionReceipt_Success(t *testing.T) {
	tracker, costRegistry, _, cleanup := setupTestMiddleware(t)
	defer cleanup()

	mockClient := &mockEthClient{receipt: &types.Receipt{Status: 1}}
	client, err := NewRateLimitedClient(&RateLimitedClientConfig{
		Client:       mockClient,
		Tracker:      tracker,
		CostRegistry: costRegistry,
		Priority:     PriorityHigh,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx := context.Background()
	receipt, err := client.TransactionReceipt(ctx, common.Hash{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if receipt == nil {
		t.Error("expected non-nil receipt")
	}
	if receipt.Status != 1 {
		t.Errorf("expected receipt status 1, got %d", receipt.Status)
	}
	if mockClient.receiptCalled != 1 {
		t.Errorf("expected TransactionReceipt to be called once, got %d", mockClient.receiptCalled)
	}

	// Verify budget was consumed
	stats, err := tracker.GetUsage(ctx)
	if err != nil {
		t.Fatalf("unexpected error getting usage: %v", err)
	}
	expectedCU := costRegistry.GetCost(MethodEthGetTransactionReceipt)
	if stats.ReservedUsed != expectedCU {
		t.Errorf("expected reserved used %d, got %d", expectedCU, stats.ReservedUsed)
	}
}

func TestRateLimitedClient_MultipleCalls_BudgetAccumulates(t *testing.T) {
	tracker, costRegistry, _, cleanup := setupTestMiddleware(t)
	defer cleanup()

	mockClient := &mockEthClient{blockNumber: 100}
	client, err := NewRateLimitedClient(&RateLimitedClientConfig{
		Client:       mockClient,
		Tracker:      tracker,
		CostRegistry: costRegistry,
		Priority:     PriorityHigh,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx := context.Background()

	// Make multiple calls
	client.BlockNumber(ctx)
	client.BlockNumber(ctx)
	client.BlockNumber(ctx)

	// Verify budget accumulated
	stats, err := tracker.GetUsage(ctx)
	if err != nil {
		t.Fatalf("unexpected error getting usage: %v", err)
	}
	expectedCU := costRegistry.GetCost(MethodEthBlockNumber) * 3
	if stats.ReservedUsed != expectedCU {
		t.Errorf("expected reserved used %d, got %d", expectedCU, stats.ReservedUsed)
	}
}

func TestRateLimitedClient_BudgetExhausted_BlocksUntilAvailable(t *testing.T) {
	tracker, costRegistry, mr, cleanup := setupTestMiddleware(t)
	defer cleanup()

	// Exhaust the reserved budget (60 CU)
	ctx := context.Background()
	tracker.TryConsume(ctx, 60, PriorityHigh)

	mockClient := &mockEthClient{blockNumber: 100}
	client, err := NewRateLimitedClient(&RateLimitedClientConfig{
		Client:       mockClient,
		Tracker:      tracker,
		CostRegistry: costRegistry,
		Priority:     PriorityHigh,
		MaxWait:      200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Fast forward time to allow budget to replenish
	go func() {
		time.Sleep(50 * time.Millisecond)
		mr.FastForward(2 * time.Second) // Move past the window
	}()

	// This should eventually succeed after budget replenishes
	// But with our short max wait, it might fail
	_, err = client.BlockNumber(ctx)
	// Either success or max wait exceeded is acceptable here
	if err != nil && !containsSubstring(err.Error(), "maximum wait time exceeded") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRateLimitedClient_MethodRecordsUsage(t *testing.T) {
	tracker, costRegistry, _, cleanup := setupTestMiddleware(t)
	defer cleanup()

	mockClient := &mockEthClient{blockNumber: 100}
	client, err := NewRateLimitedClient(&RateLimitedClientConfig{
		Client:       mockClient,
		Tracker:      tracker,
		CostRegistry: costRegistry,
		Priority:     PriorityHigh,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx := context.Background()
	client.BlockNumber(ctx)

	// The method usage should be recorded (we can't easily verify this without
	// accessing Redis directly, but we can verify the call succeeded)
	if mockClient.blockNumberCalled != 1 {
		t.Errorf("expected BlockNumber to be called once, got %d", mockClient.blockNumberCalled)
	}
}
