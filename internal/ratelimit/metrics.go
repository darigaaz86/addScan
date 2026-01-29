// Package ratelimit provides CU (Compute Unit) rate limiting for Alchemy RPC calls.
package ratelimit

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// Redis key prefixes for metrics tracking.
const (
	KeyPrefixThrottle = "cu:throttle:count:"
	KeyPrefixWaitTime = "cu:throttle:waittime:"
)

// CUMetrics contains rate limiter metrics for monitoring.
// This struct provides a snapshot of the current rate limiter state
// for observability and debugging purposes.
type CUMetrics struct {
	// Current window stats
	CurrentUsage    int `json:"current_usage"`
	CurrentReserved int `json:"current_reserved"`
	CurrentShared   int `json:"current_shared"`

	// Budget info
	TotalBudget    int `json:"total_budget"`
	ReservedBudget int `json:"reserved_budget"`
	SharedBudget   int `json:"shared_budget"`

	// Utilization percentages (0-100)
	TotalUtilization    float64 `json:"total_utilization"`
	ReservedUtilization float64 `json:"reserved_utilization"`
	SharedUtilization   float64 `json:"shared_utilization"`

	// Per-method breakdown
	MethodUsage map[string]int `json:"method_usage"`

	// Rate limit events
	ThrottleCount int64         `json:"throttle_count"`
	WaitTimeTotal time.Duration `json:"wait_time_total"`

	// Timestamp of metrics collection
	CollectedAt time.Time `json:"collected_at"`
	WindowStart time.Time `json:"window_start"`
}

// MetricsCollector collects and aggregates rate limiter metrics.
// It tracks throttle events and wait times in addition to the
// budget tracker's consumption data.
type MetricsCollector struct {
	tracker      *CUBudgetTracker
	costRegistry *CUCostRegistry
	redis        redis.Cmdable

	// In-memory counters for current process
	// These are aggregated with Redis for cross-process metrics
	localThrottleCount int64
	localWaitTimeNs    int64

	mu sync.RWMutex
}

// MetricsCollectorConfig holds configuration for the metrics collector.
type MetricsCollectorConfig struct {
	// Tracker is the CU budget tracker to collect metrics from.
	// Required.
	Tracker *CUBudgetTracker

	// CostRegistry is used to get the list of known methods.
	// Required.
	CostRegistry *CUCostRegistry

	// Redis is the Redis client for storing cross-process metrics.
	// Required.
	Redis redis.Cmdable
}

// NewMetricsCollector creates a new metrics collector.
func NewMetricsCollector(cfg *MetricsCollectorConfig) (*MetricsCollector, error) {
	if cfg == nil {
		return nil, fmt.Errorf("configuration is required")
	}
	if cfg.Tracker == nil {
		return nil, fmt.Errorf("budget tracker is required")
	}
	if cfg.CostRegistry == nil {
		return nil, fmt.Errorf("cost registry is required")
	}
	if cfg.Redis == nil {
		return nil, fmt.Errorf("redis client is required")
	}

	return &MetricsCollector{
		tracker:      cfg.Tracker,
		costRegistry: cfg.CostRegistry,
		redis:        cfg.Redis,
	}, nil
}

// RecordThrottle records a throttle event with the associated wait time.
// This should be called whenever a request is throttled (rate limited).
func (m *MetricsCollector) RecordThrottle(ctx context.Context, waitTime time.Duration) {
	// Update local counters
	atomic.AddInt64(&m.localThrottleCount, 1)
	atomic.AddInt64(&m.localWaitTimeNs, int64(waitTime))

	// Also record in Redis for cross-process aggregation
	// Use minute-based keys for throttle counts
	minuteTS := time.Now().Truncate(time.Minute).Unix()
	throttleKey := fmt.Sprintf("%s%d", KeyPrefixThrottle, minuteTS)
	waitTimeKey := fmt.Sprintf("%s%d", KeyPrefixWaitTime, minuteTS)

	pipe := m.redis.Pipeline()
	pipe.Incr(ctx, throttleKey)
	pipe.Expire(ctx, throttleKey, 5*time.Minute) // Keep for 5 minutes
	pipe.IncrBy(ctx, waitTimeKey, int64(waitTime))
	pipe.Expire(ctx, waitTimeKey, 5*time.Minute)
	pipe.Exec(ctx) // Ignore errors - metrics are best-effort
}

// GetMetrics collects and returns current rate limiter metrics.
// This provides a comprehensive snapshot of the rate limiter state.
func (m *MetricsCollector) GetMetrics(ctx context.Context) (*CUMetrics, error) {
	// Get usage stats from budget tracker
	usage, err := m.tracker.GetUsage(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get usage stats: %w", err)
	}

	// Get per-method breakdown
	methodUsage, err := m.getMethodUsage(ctx)
	if err != nil {
		// Log but don't fail - method usage is supplementary
		methodUsage = make(map[string]int)
	}

	// Get throttle metrics
	throttleCount, waitTimeTotal := m.getThrottleMetrics(ctx)

	// Calculate utilization percentages
	var totalUtil, reservedUtil, sharedUtil float64
	if usage.TotalBudget > 0 {
		totalUtil = float64(usage.TotalUsed) * 100 / float64(usage.TotalBudget)
	}
	if usage.ReservedBudget > 0 {
		reservedUtil = float64(usage.ReservedUsed) * 100 / float64(usage.ReservedBudget)
	}
	if usage.SharedBudget > 0 {
		sharedUtil = float64(usage.SharedUsed) * 100 / float64(usage.SharedBudget)
	}

	return &CUMetrics{
		CurrentUsage:        usage.TotalUsed,
		CurrentReserved:     usage.ReservedUsed,
		CurrentShared:       usage.SharedUsed,
		TotalBudget:         usage.TotalBudget,
		ReservedBudget:      usage.ReservedBudget,
		SharedBudget:        usage.SharedBudget,
		TotalUtilization:    totalUtil,
		ReservedUtilization: reservedUtil,
		SharedUtilization:   sharedUtil,
		MethodUsage:         methodUsage,
		ThrottleCount:       throttleCount,
		WaitTimeTotal:       waitTimeTotal,
		CollectedAt:         time.Now(),
		WindowStart:         usage.WindowStart,
	}, nil
}

// getMethodUsage retrieves per-method CU consumption from Redis.
func (m *MetricsCollector) getMethodUsage(ctx context.Context) (map[string]int, error) {
	methodUsage := make(map[string]int)

	// Get the current window timestamp
	windowTS := m.tracker.getWindowTimestamp()

	// Get all known methods from the cost registry
	knownMethods := m.costRegistry.KnownMethods()

	if len(knownMethods) == 0 {
		return methodUsage, nil
	}

	// Build keys for all known methods
	keys := make([]string, len(knownMethods))
	for i, method := range knownMethods {
		keys[i] = fmt.Sprintf("%s%s:%d", KeyPrefixMethod, method, windowTS)
	}

	// Use MGET to fetch all values in one round trip
	results, err := m.redis.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get method usage: %w", err)
	}

	// Parse results
	for i, result := range results {
		if result == nil {
			continue
		}
		if strVal, ok := result.(string); ok {
			if val, err := strconv.Atoi(strVal); err == nil && val > 0 {
				methodUsage[knownMethods[i]] = val
			}
		}
	}

	return methodUsage, nil
}

// getThrottleMetrics retrieves throttle count and wait time metrics.
// It combines local in-memory counters with Redis-stored cross-process metrics.
func (m *MetricsCollector) getThrottleMetrics(ctx context.Context) (int64, time.Duration) {
	// Get local counters
	localCount := atomic.LoadInt64(&m.localThrottleCount)
	localWaitNs := atomic.LoadInt64(&m.localWaitTimeNs)

	// Get Redis counters for the current minute
	minuteTS := time.Now().Truncate(time.Minute).Unix()
	throttleKey := fmt.Sprintf("%s%d", KeyPrefixThrottle, minuteTS)
	waitTimeKey := fmt.Sprintf("%s%d", KeyPrefixWaitTime, minuteTS)

	pipe := m.redis.Pipeline()
	throttleCmd := pipe.Get(ctx, throttleKey)
	waitTimeCmd := pipe.Get(ctx, waitTimeKey)
	pipe.Exec(ctx) // Ignore errors

	redisCount, _ := throttleCmd.Int64()
	redisWaitNs, _ := waitTimeCmd.Int64()

	// Return the maximum of local and Redis values
	// (they should be similar, but local might be more up-to-date)
	totalCount := localCount
	if redisCount > totalCount {
		totalCount = redisCount
	}

	totalWaitNs := localWaitNs
	if redisWaitNs > totalWaitNs {
		totalWaitNs = redisWaitNs
	}

	return totalCount, time.Duration(totalWaitNs)
}

// ResetLocalCounters resets the local in-memory counters.
// This is useful for testing or when starting a new metrics window.
func (m *MetricsCollector) ResetLocalCounters() {
	atomic.StoreInt64(&m.localThrottleCount, 0)
	atomic.StoreInt64(&m.localWaitTimeNs, 0)
}

// GetLocalThrottleCount returns the local throttle count for this process.
func (m *MetricsCollector) GetLocalThrottleCount() int64 {
	return atomic.LoadInt64(&m.localThrottleCount)
}

// GetLocalWaitTime returns the local total wait time for this process.
func (m *MetricsCollector) GetLocalWaitTime() time.Duration {
	return time.Duration(atomic.LoadInt64(&m.localWaitTimeNs))
}

// GetMethodUsageForWindow retrieves per-method CU consumption for a specific window.
// This is useful for historical analysis.
func (m *MetricsCollector) GetMethodUsageForWindow(ctx context.Context, windowTS int64) (map[string]int, error) {
	methodUsage := make(map[string]int)

	// Get all known methods from the cost registry
	knownMethods := m.costRegistry.KnownMethods()

	if len(knownMethods) == 0 {
		return methodUsage, nil
	}

	// Build keys for all known methods
	keys := make([]string, len(knownMethods))
	for i, method := range knownMethods {
		keys[i] = fmt.Sprintf("%s%s:%d", KeyPrefixMethod, method, windowTS)
	}

	// Use MGET to fetch all values in one round trip
	results, err := m.redis.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get method usage: %w", err)
	}

	// Parse results
	for i, result := range results {
		if result == nil {
			continue
		}
		if strVal, ok := result.(string); ok {
			if val, err := strconv.Atoi(strVal); err == nil && val > 0 {
				methodUsage[knownMethods[i]] = val
			}
		}
	}

	return methodUsage, nil
}

// String returns a human-readable string representation of the metrics.
func (m *CUMetrics) String() string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("CU Metrics (collected at %s):\n", m.CollectedAt.Format(time.RFC3339)))
	sb.WriteString(fmt.Sprintf("  Window Start: %s\n", m.WindowStart.Format(time.RFC3339)))
	sb.WriteString(fmt.Sprintf("  Usage: %d/%d CU (%.1f%%)\n", m.CurrentUsage, m.TotalBudget, m.TotalUtilization))
	sb.WriteString(fmt.Sprintf("  Reserved: %d/%d CU (%.1f%%)\n", m.CurrentReserved, m.ReservedBudget, m.ReservedUtilization))
	sb.WriteString(fmt.Sprintf("  Shared: %d/%d CU (%.1f%%)\n", m.CurrentShared, m.SharedBudget, m.SharedUtilization))
	sb.WriteString(fmt.Sprintf("  Throttle Count: %d\n", m.ThrottleCount))
	sb.WriteString(fmt.Sprintf("  Total Wait Time: %v\n", m.WaitTimeTotal))

	if len(m.MethodUsage) > 0 {
		sb.WriteString("  Method Usage:\n")
		for method, usage := range m.MethodUsage {
			sb.WriteString(fmt.Sprintf("    %s: %d CU\n", method, usage))
		}
	}

	return sb.String()
}

// MetricsLogger provides periodic logging of CU consumption summaries.
// It runs in the background and logs metrics at configurable intervals.
type MetricsLogger struct {
	collector *MetricsCollector
	interval  time.Duration
	logger    Logger

	stopCh chan struct{}
	doneCh chan struct{}
}

// Logger interface for metrics logging.
// This allows integration with any logging framework.
type Logger interface {
	Info(msg string, keysAndValues ...interface{})
	Warn(msg string, keysAndValues ...interface{})
}

// MetricsLoggerConfig holds configuration for the metrics logger.
type MetricsLoggerConfig struct {
	// Collector is the metrics collector to get metrics from.
	// Required.
	Collector *MetricsCollector

	// Interval is how often to log metrics. Default: 30s.
	Interval time.Duration

	// Logger is the logger to use. Required.
	Logger Logger
}

// DefaultMetricsLogInterval is the default interval for logging metrics.
const DefaultMetricsLogInterval = 30 * time.Second

// NewMetricsLogger creates a new metrics logger.
func NewMetricsLogger(cfg *MetricsLoggerConfig) (*MetricsLogger, error) {
	if cfg == nil {
		return nil, fmt.Errorf("configuration is required")
	}
	if cfg.Collector == nil {
		return nil, fmt.Errorf("metrics collector is required")
	}
	if cfg.Logger == nil {
		return nil, fmt.Errorf("logger is required")
	}

	interval := cfg.Interval
	if interval == 0 {
		interval = DefaultMetricsLogInterval
	}

	return &MetricsLogger{
		collector: cfg.Collector,
		interval:  interval,
		logger:    cfg.Logger,
		stopCh:    make(chan struct{}),
		doneCh:    make(chan struct{}),
	}, nil
}

// Start begins periodic logging of CU consumption summaries.
// This runs in a background goroutine until Stop is called.
func (l *MetricsLogger) Start(ctx context.Context) {
	go l.run(ctx)
}

// Stop stops the periodic logging and waits for cleanup.
func (l *MetricsLogger) Stop() {
	close(l.stopCh)
	<-l.doneCh
}

// run is the main loop that logs metrics at the configured interval.
func (l *MetricsLogger) run(ctx context.Context) {
	defer close(l.doneCh)

	ticker := time.NewTicker(l.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-l.stopCh:
			return
		case <-ticker.C:
			l.logMetrics(ctx)
		}
	}
}

// logMetrics collects and logs the current CU consumption summary.
func (l *MetricsLogger) logMetrics(ctx context.Context) {
	metrics, err := l.collector.GetMetrics(ctx)
	if err != nil {
		l.logger.Warn("failed to collect CU metrics",
			"error", err.Error(),
		)
		return
	}

	// Log summary with utilization percentages
	l.logger.Info("CU consumption summary",
		"total_usage", metrics.CurrentUsage,
		"total_budget", metrics.TotalBudget,
		"total_utilization_pct", fmt.Sprintf("%.1f", metrics.TotalUtilization),
		"reserved_usage", metrics.CurrentReserved,
		"reserved_budget", metrics.ReservedBudget,
		"reserved_utilization_pct", fmt.Sprintf("%.1f", metrics.ReservedUtilization),
		"shared_usage", metrics.CurrentShared,
		"shared_budget", metrics.SharedBudget,
		"shared_utilization_pct", fmt.Sprintf("%.1f", metrics.SharedUtilization),
		"throttle_count", metrics.ThrottleCount,
		"total_wait_time", metrics.WaitTimeTotal.String(),
	)

	// Log warning if utilization is high
	if metrics.TotalUtilization >= 80 {
		l.logger.Warn("CU utilization above warning threshold",
			"utilization_pct", fmt.Sprintf("%.1f", metrics.TotalUtilization),
			"threshold_pct", 80,
		)
	}

	// Log per-method breakdown if there's activity
	if len(metrics.MethodUsage) > 0 {
		for method, usage := range metrics.MethodUsage {
			l.logger.Info("CU method usage",
				"method", method,
				"usage", usage,
			)
		}
	}
}

// LogNow immediately logs the current metrics (useful for on-demand logging).
func (l *MetricsLogger) LogNow(ctx context.Context) {
	l.logMetrics(ctx)
}
