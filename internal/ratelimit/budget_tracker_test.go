package ratelimit

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// getTestRedisClient returns a Redis client for testing.
// It uses the default Redis address (localhost:6379).
func getTestRedisClient(t *testing.T) *redis.Client {
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   15, // Use a separate DB for testing
	})

	// Verify connection
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis not available: %v", err)
	}

	// Clean up test keys before each test
	t.Cleanup(func() {
		client.FlushDB(context.Background())
		client.Close()
	})

	return client
}

func TestNewCUBudgetTracker(t *testing.T) {
	client := getTestRedisClient(t)

	tests := []struct {
		name    string
		cfg     *CUBudgetTrackerConfig
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
			name: "nil redis client",
			cfg: &CUBudgetTrackerConfig{
				Redis: nil,
			},
			wantErr: true,
			errMsg:  "redis client is required",
		},
		{
			name: "valid config with defaults",
			cfg: &CUBudgetTrackerConfig{
				Redis: client,
			},
			wantErr: false,
		},
		{
			name: "valid config with custom values",
			cfg: &CUBudgetTrackerConfig{
				Redis:          client,
				TotalBudget:    1000,
				ReservedBudget: 600,
				WindowSize:     2 * time.Second,
			},
			wantErr: false,
		},
		{
			name: "reserved exceeds total",
			cfg: &CUBudgetTrackerConfig{
				Redis:          client,
				TotalBudget:    500,
				ReservedBudget: 600,
			},
			wantErr: true,
			errMsg:  "reserved budget (600) cannot exceed total budget (500)",
		},
		{
			name: "negative total budget",
			cfg: &CUBudgetTrackerConfig{
				Redis:       client,
				TotalBudget: -100,
			},
			wantErr: true,
			errMsg:  "total budget cannot be negative",
		},
		{
			name: "negative reserved budget",
			cfg: &CUBudgetTrackerConfig{
				Redis:          client,
				ReservedBudget: -100,
			},
			wantErr: true,
			errMsg:  "reserved budget cannot be negative",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tracker, err := NewCUBudgetTracker(tt.cfg)

			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error containing %q, got nil", tt.errMsg)
					return
				}
				if tt.errMsg != "" && err.Error() != tt.errMsg && !contains(err.Error(), tt.errMsg) {
					t.Errorf("expected error containing %q, got %q", tt.errMsg, err.Error())
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if tracker == nil {
				t.Error("expected non-nil tracker")
			}
		})
	}
}

func TestCUBudgetTracker_DefaultValues(t *testing.T) {
	client := getTestRedisClient(t)

	tracker, err := NewCUBudgetTracker(&CUBudgetTrackerConfig{
		Redis: client,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if tracker.GetTotalBudget() != DefaultTotalBudget {
		t.Errorf("expected total budget %d, got %d", DefaultTotalBudget, tracker.GetTotalBudget())
	}

	if tracker.GetReservedBudget() != DefaultReservedBudget {
		t.Errorf("expected reserved budget %d, got %d", DefaultReservedBudget, tracker.GetReservedBudget())
	}

	if tracker.GetSharedBudget() != DefaultSharedBudget {
		t.Errorf("expected shared budget %d, got %d", DefaultSharedBudget, tracker.GetSharedBudget())
	}

	if tracker.GetWindowSize() != DefaultWindowSize {
		t.Errorf("expected window size %v, got %v", DefaultWindowSize, tracker.GetWindowSize())
	}
}

func TestCUBudgetTracker_TryConsume_Basic(t *testing.T) {
	client := getTestRedisClient(t)

	tracker, err := NewCUBudgetTracker(&CUBudgetTrackerConfig{
		Redis:          client,
		TotalBudget:    100,
		ReservedBudget: 60,
		WindowSize:     time.Second,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx := context.Background()

	// Test consuming from reserved pool (high priority)
	allowed, waitTime := tracker.TryConsume(ctx, 30, PriorityHigh)
	if !allowed {
		t.Error("expected consumption to be allowed")
	}
	if waitTime != 0 {
		t.Errorf("expected zero wait time, got %v", waitTime)
	}

	// Verify usage
	stats, err := tracker.GetUsage(ctx)
	if err != nil {
		t.Fatalf("unexpected error getting usage: %v", err)
	}
	if stats.TotalUsed != 30 {
		t.Errorf("expected total used 30, got %d", stats.TotalUsed)
	}
	if stats.ReservedUsed != 30 {
		t.Errorf("expected reserved used 30, got %d", stats.ReservedUsed)
	}
	if stats.SharedUsed != 0 {
		t.Errorf("expected shared used 0, got %d", stats.SharedUsed)
	}
}

func TestCUBudgetTracker_TryConsume_SharedPool(t *testing.T) {
	client := getTestRedisClient(t)

	tracker, err := NewCUBudgetTracker(&CUBudgetTrackerConfig{
		Redis:          client,
		TotalBudget:    100,
		ReservedBudget: 60,
		WindowSize:     time.Second,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx := context.Background()

	// Test consuming from shared pool (low priority)
	allowed, waitTime := tracker.TryConsume(ctx, 20, PriorityLow)
	if !allowed {
		t.Error("expected consumption to be allowed")
	}
	if waitTime != 0 {
		t.Errorf("expected zero wait time, got %v", waitTime)
	}

	// Verify usage
	stats, err := tracker.GetUsage(ctx)
	if err != nil {
		t.Fatalf("unexpected error getting usage: %v", err)
	}
	if stats.TotalUsed != 20 {
		t.Errorf("expected total used 20, got %d", stats.TotalUsed)
	}
	if stats.ReservedUsed != 0 {
		t.Errorf("expected reserved used 0, got %d", stats.ReservedUsed)
	}
	if stats.SharedUsed != 20 {
		t.Errorf("expected shared used 20, got %d", stats.SharedUsed)
	}
}

func TestCUBudgetTracker_TryConsume_ExceedsBudget(t *testing.T) {
	client := getTestRedisClient(t)

	tracker, err := NewCUBudgetTracker(&CUBudgetTrackerConfig{
		Redis:          client,
		TotalBudget:    100,
		ReservedBudget: 60,
		WindowSize:     time.Second,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx := context.Background()

	// Consume all reserved budget
	allowed, _ := tracker.TryConsume(ctx, 60, PriorityHigh)
	if !allowed {
		t.Error("expected first consumption to be allowed")
	}

	// Try to consume more from reserved - should fail
	allowed, waitTime := tracker.TryConsume(ctx, 10, PriorityHigh)
	if allowed {
		t.Error("expected consumption to be denied when budget exhausted")
	}
	if waitTime <= 0 {
		t.Error("expected positive wait time when denied")
	}
}

func TestCUBudgetTracker_TryConsume_SeparatePools(t *testing.T) {
	client := getTestRedisClient(t)

	tracker, err := NewCUBudgetTracker(&CUBudgetTrackerConfig{
		Redis:          client,
		TotalBudget:    100,
		ReservedBudget: 60,
		WindowSize:     time.Second,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx := context.Background()

	// Exhaust shared budget (40 CU)
	allowed, _ := tracker.TryConsume(ctx, 40, PriorityLow)
	if !allowed {
		t.Error("expected shared consumption to be allowed")
	}

	// Try to consume more from shared - should fail
	allowed, _ = tracker.TryConsume(ctx, 10, PriorityLow)
	if allowed {
		t.Error("expected shared consumption to be denied when exhausted")
	}

	// But reserved should still work
	allowed, _ = tracker.TryConsume(ctx, 30, PriorityHigh)
	if !allowed {
		t.Error("expected reserved consumption to be allowed even when shared is exhausted")
	}
}

func TestCUBudgetTracker_TryConsume_ZeroOrNegative(t *testing.T) {
	client := getTestRedisClient(t)

	tracker, err := NewCUBudgetTracker(&CUBudgetTrackerConfig{
		Redis: client,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx := context.Background()

	// Zero CU should always be allowed
	allowed, waitTime := tracker.TryConsume(ctx, 0, PriorityHigh)
	if !allowed {
		t.Error("expected zero CU consumption to be allowed")
	}
	if waitTime != 0 {
		t.Errorf("expected zero wait time for zero CU, got %v", waitTime)
	}

	// Negative CU should always be allowed (no-op)
	allowed, waitTime = tracker.TryConsume(ctx, -10, PriorityLow)
	if !allowed {
		t.Error("expected negative CU consumption to be allowed")
	}
	if waitTime != 0 {
		t.Errorf("expected zero wait time for negative CU, got %v", waitTime)
	}
}

func TestCUBudgetTracker_GetUsage_Empty(t *testing.T) {
	client := getTestRedisClient(t)

	tracker, err := NewCUBudgetTracker(&CUBudgetTrackerConfig{
		Redis: client,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx := context.Background()

	stats, err := tracker.GetUsage(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if stats.TotalUsed != 0 {
		t.Errorf("expected total used 0, got %d", stats.TotalUsed)
	}
	if stats.ReservedUsed != 0 {
		t.Errorf("expected reserved used 0, got %d", stats.ReservedUsed)
	}
	if stats.SharedUsed != 0 {
		t.Errorf("expected shared used 0, got %d", stats.SharedUsed)
	}
	if stats.TotalBudget != DefaultTotalBudget {
		t.Errorf("expected total budget %d, got %d", DefaultTotalBudget, stats.TotalBudget)
	}
	if stats.ReservedBudget != DefaultReservedBudget {
		t.Errorf("expected reserved budget %d, got %d", DefaultReservedBudget, stats.ReservedBudget)
	}
	if stats.SharedBudget != DefaultSharedBudget {
		t.Errorf("expected shared budget %d, got %d", DefaultSharedBudget, stats.SharedBudget)
	}
}

func TestCUBudgetTracker_AvailableBudget(t *testing.T) {
	client := getTestRedisClient(t)

	tracker, err := NewCUBudgetTracker(&CUBudgetTrackerConfig{
		Redis:          client,
		TotalBudget:    100,
		ReservedBudget: 60,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx := context.Background()

	// Initially all budget should be available
	available, err := tracker.AvailableBudget(ctx, PriorityHigh)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if available != 60 {
		t.Errorf("expected available reserved budget 60, got %d", available)
	}

	available, err = tracker.AvailableBudget(ctx, PriorityLow)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if available != 40 {
		t.Errorf("expected available shared budget 40, got %d", available)
	}

	// Consume some budget
	tracker.TryConsume(ctx, 30, PriorityHigh)
	tracker.TryConsume(ctx, 20, PriorityLow)

	// Check remaining
	available, err = tracker.AvailableBudget(ctx, PriorityHigh)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if available != 30 {
		t.Errorf("expected available reserved budget 30, got %d", available)
	}

	available, err = tracker.AvailableBudget(ctx, PriorityLow)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if available != 20 {
		t.Errorf("expected available shared budget 20, got %d", available)
	}
}

func TestCUBudgetTracker_TotalUtilization(t *testing.T) {
	client := getTestRedisClient(t)

	tracker, err := NewCUBudgetTracker(&CUBudgetTrackerConfig{
		Redis:          client,
		TotalBudget:    100,
		ReservedBudget: 60,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx := context.Background()

	// Initially 0%
	util, err := tracker.TotalUtilization(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if util != 0 {
		t.Errorf("expected 0%% utilization, got %.2f%%", util)
	}

	// Consume 50 CU (50%)
	tracker.TryConsume(ctx, 30, PriorityHigh)
	tracker.TryConsume(ctx, 20, PriorityLow)

	util, err = tracker.TotalUtilization(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if util != 50 {
		t.Errorf("expected 50%% utilization, got %.2f%%", util)
	}
}

func TestCUBudgetTracker_ThresholdDetection(t *testing.T) {
	client := getTestRedisClient(t)

	tracker, err := NewCUBudgetTracker(&CUBudgetTrackerConfig{
		Redis:          client,
		TotalBudget:    100,
		ReservedBudget: 60,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx := context.Background()

	// Initially below thresholds
	warning, err := tracker.IsWarningThreshold(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if warning {
		t.Error("expected warning threshold to be false initially")
	}

	pause, err := tracker.IsPauseThreshold(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pause {
		t.Error("expected pause threshold to be false initially")
	}

	// Consume 80 CU (80%) - should trigger warning
	tracker.TryConsume(ctx, 60, PriorityHigh)
	tracker.TryConsume(ctx, 20, PriorityLow)

	warning, err = tracker.IsWarningThreshold(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !warning {
		t.Error("expected warning threshold to be true at 80%")
	}

	pause, err = tracker.IsPauseThreshold(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pause {
		t.Error("expected pause threshold to be false at 80%")
	}

	// Consume 10 more CU (90%) - should trigger pause
	tracker.TryConsume(ctx, 10, PriorityLow)

	pause, err = tracker.IsPauseThreshold(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !pause {
		t.Error("expected pause threshold to be true at 90%")
	}
}

func TestCUBudgetTracker_RecordMethodUsage(t *testing.T) {
	client := getTestRedisClient(t)

	tracker, err := NewCUBudgetTracker(&CUBudgetTrackerConfig{
		Redis: client,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx := context.Background()

	// Record method usage
	err = tracker.RecordMethodUsage(ctx, "eth_blockNumber", 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Empty method should be no-op
	err = tracker.RecordMethodUsage(ctx, "", 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Zero CU should be no-op
	err = tracker.RecordMethodUsage(ctx, "eth_blockNumber", 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Negative CU should be no-op
	err = tracker.RecordMethodUsage(ctx, "eth_blockNumber", -10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCUBudgetTracker_TotalBudgetExceeded(t *testing.T) {
	client := getTestRedisClient(t)

	tracker, err := NewCUBudgetTracker(&CUBudgetTrackerConfig{
		Redis:          client,
		TotalBudget:    100,
		ReservedBudget: 60,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx := context.Background()

	// Consume all reserved budget (60 CU)
	allowed, _ := tracker.TryConsume(ctx, 60, PriorityHigh)
	if !allowed {
		t.Error("expected first consumption to be allowed")
	}

	// Consume all shared budget (40 CU)
	allowed, _ = tracker.TryConsume(ctx, 40, PriorityLow)
	if !allowed {
		t.Error("expected second consumption to be allowed")
	}

	// Now total budget is exhausted - both priorities should fail
	allowed, _ = tracker.TryConsume(ctx, 1, PriorityHigh)
	if allowed {
		t.Error("expected high priority to be denied when total budget exhausted")
	}

	allowed, _ = tracker.TryConsume(ctx, 1, PriorityLow)
	if allowed {
		t.Error("expected low priority to be denied when total budget exhausted")
	}
}

func TestPriority_String(t *testing.T) {
	tests := []struct {
		priority Priority
		expected string
	}{
		{PriorityHigh, "high"},
		{PriorityLow, "low"},
		{Priority(99), "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			if got := tt.priority.String(); got != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, got)
			}
		})
	}
}

func TestCUBudgetTrackerConfig_Validate(t *testing.T) {
	client := getTestRedisClient(t)

	tests := []struct {
		name    string
		cfg     *CUBudgetTrackerConfig
		wantErr bool
	}{
		{
			name: "valid config",
			cfg: &CUBudgetTrackerConfig{
				Redis:          client,
				TotalBudget:    500,
				ReservedBudget: 300,
			},
			wantErr: false,
		},
		{
			name: "nil redis",
			cfg: &CUBudgetTrackerConfig{
				Redis: nil,
			},
			wantErr: true,
		},
		{
			name: "reserved equals total",
			cfg: &CUBudgetTrackerConfig{
				Redis:          client,
				TotalBudget:    500,
				ReservedBudget: 500,
			},
			wantErr: false,
		},
		{
			name: "reserved exceeds total",
			cfg: &CUBudgetTrackerConfig{
				Redis:          client,
				TotalBudget:    500,
				ReservedBudget: 600,
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

// Helper function to check if a string contains a substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
