// Package ratelimit provides CU (Compute Unit) rate limiting for Alchemy RPC calls.
package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// Default budget configuration values.
const (
	DefaultTotalBudget    = 500             // Total CU/s
	DefaultReservedBudget = 300             // Reserved for priority (Real-Time Sync)
	DefaultSharedBudget   = 200             // Available for best-effort (Backfill)
	DefaultWindowSize     = time.Second     // 1 second sliding window
	DefaultKeyTTL         = 2 * time.Second // TTL for Redis keys (window + buffer)
)

// Redis key prefixes for CU tracking.
const (
	KeyPrefixTotal    = "cu:total:"
	KeyPrefixReserved = "cu:reserved:"
	KeyPrefixShared   = "cu:shared:"
	KeyPrefixMethod   = "cu:method:"
)

// Priority levels for budget allocation.
type Priority int

const (
	// PriorityHigh is for Real-time sync operations (uses reserved budget).
	PriorityHigh Priority = iota
	// PriorityLow is for Backfill operations (uses shared budget).
	PriorityLow
)

// String returns a string representation of the priority level.
func (p Priority) String() string {
	switch p {
	case PriorityHigh:
		return "high"
	case PriorityLow:
		return "low"
	default:
		return "unknown"
	}
}

// CUBudgetTracker coordinates CU consumption across services using Redis.
// It implements a sliding window rate limiter with separate pools for
// priority (reserved) and best-effort (shared) operations.
type CUBudgetTracker struct {
	redis          redis.Cmdable
	totalBudget    int
	reservedBudget int
	sharedBudget   int
	windowSize     time.Duration
	keyTTL         time.Duration
}

// CUBudgetTrackerConfig holds configuration for the budget tracker.
type CUBudgetTrackerConfig struct {
	// Redis is the Redis client for cross-service coordination.
	// Required - tracker cannot function without Redis.
	Redis redis.Cmdable

	// TotalBudget is the total CU/s budget. Default: 500.
	TotalBudget int

	// ReservedBudget is the CU/s reserved for priority operations. Default: 300.
	ReservedBudget int

	// WindowSize is the sliding window duration. Default: 1s.
	WindowSize time.Duration

	// KeyTTL is the TTL for Redis keys. Default: 2s (window + buffer).
	// Should be at least WindowSize to ensure proper expiration.
	KeyTTL time.Duration
}

// CUUsageStats contains current consumption metrics.
type CUUsageStats struct {
	// TotalUsed is the total CU consumed in the current window.
	TotalUsed int

	// ReservedUsed is the CU consumed from the reserved pool.
	ReservedUsed int

	// SharedUsed is the CU consumed from the shared pool.
	SharedUsed int

	// TotalBudget is the configured total CU/s budget.
	TotalBudget int

	// ReservedBudget is the configured reserved CU/s budget.
	ReservedBudget int

	// SharedBudget is the configured shared CU/s budget.
	SharedBudget int

	// WindowStart is the start time of the current window.
	WindowStart time.Time
}

// Validate checks if the configuration is valid.
func (c *CUBudgetTrackerConfig) Validate() error {
	if c.Redis == nil {
		return errors.New("redis client is required")
	}

	// Check for negative values
	if c.TotalBudget < 0 {
		return errors.New("total budget cannot be negative")
	}
	if c.ReservedBudget < 0 {
		return errors.New("reserved budget cannot be negative")
	}

	// Calculate shared budget
	totalBudget := c.TotalBudget
	if totalBudget == 0 {
		totalBudget = DefaultTotalBudget
	}
	reservedBudget := c.ReservedBudget
	if reservedBudget == 0 {
		reservedBudget = DefaultReservedBudget
	}

	// Validate that reserved doesn't exceed total
	if reservedBudget > totalBudget {
		return fmt.Errorf("reserved budget (%d) cannot exceed total budget (%d)", reservedBudget, totalBudget)
	}

	return nil
}

// NewCUBudgetTracker creates a new tracker with the given configuration.
// Returns an error if the configuration is invalid.
func NewCUBudgetTracker(cfg *CUBudgetTrackerConfig) (*CUBudgetTracker, error) {
	if cfg == nil {
		return nil, errors.New("configuration is required")
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	// Apply defaults
	totalBudget := cfg.TotalBudget
	if totalBudget == 0 {
		totalBudget = DefaultTotalBudget
	}

	reservedBudget := cfg.ReservedBudget
	if reservedBudget == 0 {
		reservedBudget = DefaultReservedBudget
	}

	sharedBudget := totalBudget - reservedBudget

	windowSize := cfg.WindowSize
	if windowSize == 0 {
		windowSize = DefaultWindowSize
	}

	keyTTL := cfg.KeyTTL
	if keyTTL == 0 {
		keyTTL = DefaultKeyTTL
	}

	return &CUBudgetTracker{
		redis:          cfg.Redis,
		totalBudget:    totalBudget,
		reservedBudget: reservedBudget,
		sharedBudget:   sharedBudget,
		windowSize:     windowSize,
		keyTTL:         keyTTL,
	}, nil
}

// getWindowTimestamp returns the timestamp for the current sliding window.
// The window is aligned to the window size boundary.
func (t *CUBudgetTracker) getWindowTimestamp() int64 {
	now := time.Now()
	// Align to window boundary
	windowStart := now.Truncate(t.windowSize)
	return windowStart.UnixMilli()
}

// getKeys returns the Redis keys for the current window.
func (t *CUBudgetTracker) getKeys(windowTS int64) (totalKey, reservedKey, sharedKey string) {
	tsStr := strconv.FormatInt(windowTS, 10)
	totalKey = KeyPrefixTotal + tsStr
	reservedKey = KeyPrefixReserved + tsStr
	sharedKey = KeyPrefixShared + tsStr
	return
}

// TryConsume attempts to consume CU from the appropriate budget pool.
// For PriorityHigh, it uses the reserved budget pool.
// For PriorityLow, it uses the shared budget pool.
//
// Returns:
//   - allowed: true if the consumption was allowed
//   - waitTime: suggested wait time before retrying if not allowed
func (t *CUBudgetTracker) TryConsume(ctx context.Context, cu int, priority Priority) (bool, time.Duration) {
	if cu <= 0 {
		return true, 0
	}

	windowTS := t.getWindowTimestamp()
	totalKey, reservedKey, sharedKey := t.getKeys(windowTS)

	// Determine which pool to use based on priority
	var poolKey string
	var poolBudget int
	if priority == PriorityHigh {
		poolKey = reservedKey
		poolBudget = t.reservedBudget
	} else {
		poolKey = sharedKey
		poolBudget = t.sharedBudget
	}

	// Use a Lua script for atomic check-and-increment
	// This ensures we don't exceed the budget even under concurrent access
	script := redis.NewScript(`
		local totalKey = KEYS[1]
		local poolKey = KEYS[2]
		local cu = tonumber(ARGV[1])
		local totalBudget = tonumber(ARGV[2])
		local poolBudget = tonumber(ARGV[3])
		local ttl = tonumber(ARGV[4])

		-- Get current values
		local totalUsed = tonumber(redis.call('GET', totalKey) or '0')
		local poolUsed = tonumber(redis.call('GET', poolKey) or '0')

		-- Check if we have budget in both total and pool
		if totalUsed + cu > totalBudget then
			return {0, totalUsed, poolUsed}
		end
		if poolUsed + cu > poolBudget then
			return {0, totalUsed, poolUsed}
		end

		-- Atomically increment both counters
		redis.call('INCRBY', totalKey, cu)
		redis.call('EXPIRE', totalKey, ttl)
		redis.call('INCRBY', poolKey, cu)
		redis.call('EXPIRE', poolKey, ttl)

		return {1, totalUsed + cu, poolUsed + cu}
	`)

	ttlSeconds := int(t.keyTTL.Seconds())
	if ttlSeconds < 1 {
		ttlSeconds = 1
	}

	result, err := script.Run(ctx, t.redis, []string{totalKey, poolKey},
		cu, t.totalBudget, poolBudget, ttlSeconds).Int64Slice()

	if err != nil {
		// On Redis error, deny the request to be safe
		// Calculate wait time as remaining time in current window
		return false, t.calculateWaitTime(windowTS)
	}

	allowed := result[0] == 1
	if !allowed {
		return false, t.calculateWaitTime(windowTS)
	}

	return true, 0
}

// calculateWaitTime returns the time until the next window starts.
func (t *CUBudgetTracker) calculateWaitTime(windowTS int64) time.Duration {
	windowStart := time.UnixMilli(windowTS)
	windowEnd := windowStart.Add(t.windowSize)
	waitTime := time.Until(windowEnd)
	if waitTime < 0 {
		waitTime = 0
	}
	// Add a small buffer to ensure we're in the new window
	return waitTime + time.Millisecond
}

// GetUsage returns current CU usage statistics.
func (t *CUBudgetTracker) GetUsage(ctx context.Context) (*CUUsageStats, error) {
	windowTS := t.getWindowTimestamp()
	totalKey, reservedKey, sharedKey := t.getKeys(windowTS)

	// Use pipeline to get all values in one round trip
	pipe := t.redis.Pipeline()
	totalCmd := pipe.Get(ctx, totalKey)
	reservedCmd := pipe.Get(ctx, reservedKey)
	sharedCmd := pipe.Get(ctx, sharedKey)

	_, err := pipe.Exec(ctx)
	// Ignore redis.Nil errors - they just mean the key doesn't exist yet
	if err != nil && !errors.Is(err, redis.Nil) {
		// Check if all commands failed or just some returned nil
		// We need to handle the case where some keys exist and some don't
	}

	// Parse results, treating missing keys as 0
	totalUsed := parseIntOrZero(totalCmd)
	reservedUsed := parseIntOrZero(reservedCmd)
	sharedUsed := parseIntOrZero(sharedCmd)

	return &CUUsageStats{
		TotalUsed:      totalUsed,
		ReservedUsed:   reservedUsed,
		SharedUsed:     sharedUsed,
		TotalBudget:    t.totalBudget,
		ReservedBudget: t.reservedBudget,
		SharedBudget:   t.sharedBudget,
		WindowStart:    time.UnixMilli(windowTS),
	}, nil
}

// parseIntOrZero parses a Redis string command result as int, returning 0 on error.
func parseIntOrZero(cmd *redis.StringCmd) int {
	val, err := cmd.Int()
	if err != nil {
		return 0
	}
	return val
}

// RecordMethodUsage records CU consumption for a specific RPC method.
// This is used for monitoring and does not affect budget allocation.
func (t *CUBudgetTracker) RecordMethodUsage(ctx context.Context, method string, cu int) error {
	if cu <= 0 || method == "" {
		return nil
	}

	windowTS := t.getWindowTimestamp()
	key := fmt.Sprintf("%s%s:%d", KeyPrefixMethod, method, windowTS)

	pipe := t.redis.Pipeline()
	pipe.IncrBy(ctx, key, int64(cu))
	pipe.Expire(ctx, key, t.keyTTL)
	_, err := pipe.Exec(ctx)

	return err
}

// GetTotalBudget returns the configured total CU/s budget.
func (t *CUBudgetTracker) GetTotalBudget() int {
	return t.totalBudget
}

// GetReservedBudget returns the configured reserved CU/s budget.
func (t *CUBudgetTracker) GetReservedBudget() int {
	return t.reservedBudget
}

// GetSharedBudget returns the configured shared CU/s budget.
func (t *CUBudgetTracker) GetSharedBudget() int {
	return t.sharedBudget
}

// GetWindowSize returns the configured window size.
func (t *CUBudgetTracker) GetWindowSize() time.Duration {
	return t.windowSize
}

// AvailableBudget returns the available budget for a given priority level.
func (t *CUBudgetTracker) AvailableBudget(ctx context.Context, priority Priority) (int, error) {
	stats, err := t.GetUsage(ctx)
	if err != nil {
		return 0, err
	}

	if priority == PriorityHigh {
		available := t.reservedBudget - stats.ReservedUsed
		if available < 0 {
			available = 0
		}
		return available, nil
	}

	// For low priority, return available shared budget
	available := t.sharedBudget - stats.SharedUsed
	if available < 0 {
		available = 0
	}
	return available, nil
}

// TotalUtilization returns the current total budget utilization as a percentage (0-100).
func (t *CUBudgetTracker) TotalUtilization(ctx context.Context) (float64, error) {
	stats, err := t.GetUsage(ctx)
	if err != nil {
		return 0, err
	}

	if t.totalBudget == 0 {
		return 100, nil
	}

	return float64(stats.TotalUsed) * 100 / float64(t.totalBudget), nil
}

// IsWarningThreshold returns true if total utilization is at or above 80%.
func (t *CUBudgetTracker) IsWarningThreshold(ctx context.Context) (bool, error) {
	utilization, err := t.TotalUtilization(ctx)
	if err != nil {
		return false, err
	}
	return utilization >= 80, nil
}

// IsPauseThreshold returns true if total utilization is at or above 90%.
func (t *CUBudgetTracker) IsPauseThreshold(ctx context.Context) (bool, error) {
	utilization, err := t.TotalUtilization(ctx)
	if err != nil {
		return false, err
	}
	return utilization >= 90, nil
}
