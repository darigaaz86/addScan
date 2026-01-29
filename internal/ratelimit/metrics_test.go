package ratelimit

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// getTestRedisClientForMetrics returns a Redis client for testing.
// It uses the default Redis address (localhost:6379).
func getTestRedisClientForMetrics(t *testing.T) *redis.Client {
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

func TestNewMetricsCollector(t *testing.T) {
	client := getTestRedisClientForMetrics(t)
	tracker, _ := NewCUBudgetTracker(&CUBudgetTrackerConfig{
		Redis: client,
	})
	costRegistry := NewCUCostRegistry(nil)

	tests := []struct {
		name    string
		cfg     *MetricsCollectorConfig
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
			name: "nil tracker",
			cfg: &MetricsCollectorConfig{
				Tracker:      nil,
				CostRegistry: costRegistry,
				Redis:        client,
			},
			wantErr: true,
			errMsg:  "budget tracker is required",
		},
		{
			name: "nil cost registry",
			cfg: &MetricsCollectorConfig{
				Tracker:      tracker,
				CostRegistry: nil,
				Redis:        client,
			},
			wantErr: true,
			errMsg:  "cost registry is required",
		},
		{
			name: "nil redis",
			cfg: &MetricsCollectorConfig{
				Tracker:      tracker,
				CostRegistry: costRegistry,
				Redis:        nil,
			},
			wantErr: true,
			errMsg:  "redis client is required",
		},
		{
			name: "valid config",
			cfg: &MetricsCollectorConfig{
				Tracker:      tracker,
				CostRegistry: costRegistry,
				Redis:        client,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			collector, err := NewMetricsCollector(tt.cfg)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
				assert.Nil(t, collector)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, collector)
			}
		})
	}
}

func TestMetricsCollector_RecordThrottle(t *testing.T) {
	client := getTestRedisClientForMetrics(t)
	tracker, _ := NewCUBudgetTracker(&CUBudgetTrackerConfig{
		Redis: client,
	})
	costRegistry := NewCUCostRegistry(nil)

	collector, err := NewMetricsCollector(&MetricsCollectorConfig{
		Tracker:      tracker,
		CostRegistry: costRegistry,
		Redis:        client,
	})
	require.NoError(t, err)

	ctx := context.Background()
	waitTime := 100 * time.Millisecond

	// Record throttle event
	collector.RecordThrottle(ctx, waitTime)

	// Verify local counters were updated
	assert.Equal(t, int64(1), collector.GetLocalThrottleCount())
	assert.Equal(t, waitTime, collector.GetLocalWaitTime())

	// Record another throttle event
	collector.RecordThrottle(ctx, waitTime)

	assert.Equal(t, int64(2), collector.GetLocalThrottleCount())
	assert.Equal(t, 2*waitTime, collector.GetLocalWaitTime())
}

func TestMetricsCollector_ResetLocalCounters(t *testing.T) {
	client := getTestRedisClientForMetrics(t)
	tracker, _ := NewCUBudgetTracker(&CUBudgetTrackerConfig{
		Redis: client,
	})
	costRegistry := NewCUCostRegistry(nil)

	collector, err := NewMetricsCollector(&MetricsCollectorConfig{
		Tracker:      tracker,
		CostRegistry: costRegistry,
		Redis:        client,
	})
	require.NoError(t, err)

	ctx := context.Background()
	waitTime := 100 * time.Millisecond

	// Record some throttle events
	collector.RecordThrottle(ctx, waitTime)

	assert.Equal(t, int64(1), collector.GetLocalThrottleCount())
	assert.Equal(t, waitTime, collector.GetLocalWaitTime())

	// Reset counters
	collector.ResetLocalCounters()

	assert.Equal(t, int64(0), collector.GetLocalThrottleCount())
	assert.Equal(t, time.Duration(0), collector.GetLocalWaitTime())
}

func TestMetricsCollector_GetMetrics(t *testing.T) {
	client := getTestRedisClientForMetrics(t)
	tracker, _ := NewCUBudgetTracker(&CUBudgetTrackerConfig{
		Redis:          client,
		TotalBudget:    500,
		ReservedBudget: 300,
	})
	costRegistry := NewCUCostRegistry(nil)

	collector, err := NewMetricsCollector(&MetricsCollectorConfig{
		Tracker:      tracker,
		CostRegistry: costRegistry,
		Redis:        client,
	})
	require.NoError(t, err)

	ctx := context.Background()

	// Get initial metrics
	metrics, err := collector.GetMetrics(ctx)
	require.NoError(t, err)

	assert.Equal(t, 0, metrics.CurrentUsage)
	assert.Equal(t, 0, metrics.CurrentReserved)
	assert.Equal(t, 0, metrics.CurrentShared)
	assert.Equal(t, 500, metrics.TotalBudget)
	assert.Equal(t, 300, metrics.ReservedBudget)
	assert.Equal(t, 200, metrics.SharedBudget)
	assert.Equal(t, 0.0, metrics.TotalUtilization)
	assert.Equal(t, 0.0, metrics.ReservedUtilization)
	assert.Equal(t, 0.0, metrics.SharedUtilization)
	assert.NotNil(t, metrics.MethodUsage)
	assert.Equal(t, int64(0), metrics.ThrottleCount)
	assert.Equal(t, time.Duration(0), metrics.WaitTimeTotal)
}

func TestMetricsCollector_GetMetrics_WithUsage(t *testing.T) {
	client := getTestRedisClientForMetrics(t)
	tracker, _ := NewCUBudgetTracker(&CUBudgetTrackerConfig{
		Redis:          client,
		TotalBudget:    500,
		ReservedBudget: 300,
	})
	costRegistry := NewCUCostRegistry(nil)

	collector, err := NewMetricsCollector(&MetricsCollectorConfig{
		Tracker:      tracker,
		CostRegistry: costRegistry,
		Redis:        client,
	})
	require.NoError(t, err)

	ctx := context.Background()

	// Consume some budget
	tracker.TryConsume(ctx, 150, PriorityHigh)
	tracker.TryConsume(ctx, 100, PriorityLow)

	// Record method usage
	tracker.RecordMethodUsage(ctx, MethodEthBlockNumber, 10)
	tracker.RecordMethodUsage(ctx, MethodEthGetBlockByNumber, 32)

	// Record throttle events
	collector.RecordThrottle(ctx, 50*time.Millisecond)
	collector.RecordThrottle(ctx, 100*time.Millisecond)

	// Get metrics
	metrics, err := collector.GetMetrics(ctx)
	require.NoError(t, err)

	assert.Equal(t, 250, metrics.CurrentUsage)
	assert.Equal(t, 150, metrics.CurrentReserved)
	assert.Equal(t, 100, metrics.CurrentShared)
	assert.Equal(t, 500, metrics.TotalBudget)
	assert.Equal(t, 300, metrics.ReservedBudget)
	assert.Equal(t, 200, metrics.SharedBudget)
	assert.Equal(t, 50.0, metrics.TotalUtilization)
	assert.Equal(t, 50.0, metrics.ReservedUtilization)
	assert.Equal(t, 50.0, metrics.SharedUtilization)

	// Check method usage
	assert.Equal(t, 10, metrics.MethodUsage[MethodEthBlockNumber])
	assert.Equal(t, 32, metrics.MethodUsage[MethodEthGetBlockByNumber])

	// Check throttle metrics
	assert.Equal(t, int64(2), metrics.ThrottleCount)
	assert.Equal(t, 150*time.Millisecond, metrics.WaitTimeTotal)
}

func TestMetricsCollector_GetMethodUsageForWindow(t *testing.T) {
	client := getTestRedisClientForMetrics(t)
	tracker, _ := NewCUBudgetTracker(&CUBudgetTrackerConfig{
		Redis: client,
	})
	costRegistry := NewCUCostRegistry(nil)

	collector, err := NewMetricsCollector(&MetricsCollectorConfig{
		Tracker:      tracker,
		CostRegistry: costRegistry,
		Redis:        client,
	})
	require.NoError(t, err)

	ctx := context.Background()

	// Record method usage
	tracker.RecordMethodUsage(ctx, MethodEthBlockNumber, 10)
	tracker.RecordMethodUsage(ctx, MethodEthGetLogs, 75)

	// Get method usage for current window
	windowTS := tracker.getWindowTimestamp()
	methodUsage, err := collector.GetMethodUsageForWindow(ctx, windowTS)
	require.NoError(t, err)

	assert.Equal(t, 10, methodUsage[MethodEthBlockNumber])
	assert.Equal(t, 75, methodUsage[MethodEthGetLogs])
}

func TestCUMetrics_String(t *testing.T) {
	metrics := &CUMetrics{
		CurrentUsage:        250,
		CurrentReserved:     150,
		CurrentShared:       100,
		TotalBudget:         500,
		ReservedBudget:      300,
		SharedBudget:        200,
		TotalUtilization:    50.0,
		ReservedUtilization: 50.0,
		SharedUtilization:   50.0,
		MethodUsage: map[string]int{
			"eth_blockNumber":      10,
			"eth_getBlockByNumber": 32,
		},
		ThrottleCount: 5,
		WaitTimeTotal: 500 * time.Millisecond,
		CollectedAt:   time.Now(),
		WindowStart:   time.Now().Truncate(time.Second),
	}

	str := metrics.String()

	// Verify key information is present
	assert.Contains(t, str, "CU Metrics")
	assert.Contains(t, str, "Usage: 250/500 CU (50.0%)")
	assert.Contains(t, str, "Reserved: 150/300 CU (50.0%)")
	assert.Contains(t, str, "Shared: 100/200 CU (50.0%)")
	assert.Contains(t, str, "Throttle Count: 5")
	assert.Contains(t, str, "Method Usage:")
	assert.Contains(t, str, "eth_blockNumber: 10 CU")
	assert.Contains(t, str, "eth_getBlockByNumber: 32 CU")
}

func TestCUMetrics_String_NoMethodUsage(t *testing.T) {
	metrics := &CUMetrics{
		CurrentUsage:     0,
		TotalBudget:      500,
		TotalUtilization: 0.0,
		MethodUsage:      map[string]int{},
		ThrottleCount:    0,
		WaitTimeTotal:    0,
		CollectedAt:      time.Now(),
		WindowStart:      time.Now().Truncate(time.Second),
	}

	str := metrics.String()

	// Verify method usage section is not present when empty
	assert.Contains(t, str, "CU Metrics")
	assert.Contains(t, str, "Usage: 0/500 CU (0.0%)")
	assert.NotContains(t, str, "Method Usage:")
}

func TestCUMetrics_Utilization_EdgeCases(t *testing.T) {
	client := getTestRedisClientForMetrics(t)

	// Test with zero budgets
	tracker, _ := NewCUBudgetTracker(&CUBudgetTrackerConfig{
		Redis:          client,
		TotalBudget:    100,
		ReservedBudget: 100, // All reserved, no shared
	})
	costRegistry := NewCUCostRegistry(nil)

	collector, err := NewMetricsCollector(&MetricsCollectorConfig{
		Tracker:      tracker,
		CostRegistry: costRegistry,
		Redis:        client,
	})
	require.NoError(t, err)

	ctx := context.Background()

	metrics, err := collector.GetMetrics(ctx)
	require.NoError(t, err)

	// Shared budget is 0, so shared utilization should be 0 (not NaN or Inf)
	assert.Equal(t, 0.0, metrics.SharedUtilization)
}
