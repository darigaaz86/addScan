// Package ratelimit provides CU (Compute Unit) rate limiting for Alchemy RPC calls.
package ratelimit

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Default rate controller configuration values.
const (
	DefaultBaseDelay = 100 * time.Millisecond
	DefaultMaxDelay  = 10 * time.Second
)

// ErrContextCancelled is returned when the context is cancelled while waiting for budget.
var ErrContextCancelled = errors.New("context cancelled while waiting for budget")

// BackfillRateController manages backfill request pacing with exponential backoff.
// It coordinates with the CUBudgetTracker to ensure backfill operations
// respect the shared budget pool and implement graceful degradation.
type BackfillRateController struct {
	tracker          *CUBudgetTracker
	baseDelay        time.Duration
	maxDelay         time.Duration
	currentDelay     time.Duration
	consecutiveFails int
	mu               sync.Mutex
}

// BackfillRateControllerConfig holds configuration for the rate controller.
type BackfillRateControllerConfig struct {
	// Tracker is the CU budget tracker for coordination.
	// Required - controller cannot function without a tracker.
	Tracker *CUBudgetTracker

	// BaseDelay is the initial delay between retries. Default: 100ms.
	BaseDelay time.Duration

	// MaxDelay is the maximum delay between retries. Default: 10s.
	MaxDelay time.Duration
}

// Validate checks if the configuration is valid.
func (c *BackfillRateControllerConfig) Validate() error {
	if c.Tracker == nil {
		return errors.New("tracker is required")
	}
	if c.BaseDelay < 0 {
		return errors.New("base delay cannot be negative")
	}
	if c.MaxDelay < 0 {
		return errors.New("max delay cannot be negative")
	}
	if c.MaxDelay > 0 && c.BaseDelay > 0 && c.BaseDelay > c.MaxDelay {
		return errors.New("base delay cannot exceed max delay")
	}
	return nil
}

// NewBackfillRateController creates a new controller with the given configuration.
// Returns an error if the configuration is invalid.
func NewBackfillRateController(cfg *BackfillRateControllerConfig) (*BackfillRateController, error) {
	if cfg == nil {
		return nil, errors.New("configuration is required")
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	// Apply defaults
	baseDelay := cfg.BaseDelay
	if baseDelay == 0 {
		baseDelay = DefaultBaseDelay
	}

	maxDelay := cfg.MaxDelay
	if maxDelay == 0 {
		maxDelay = DefaultMaxDelay
	}

	return &BackfillRateController{
		tracker:          cfg.Tracker,
		baseDelay:        baseDelay,
		maxDelay:         maxDelay,
		currentDelay:     baseDelay,
		consecutiveFails: 0,
	}, nil
}

// WaitForBudget blocks until budget is available or context is cancelled.
// It uses the shared budget pool (PriorityLow) for backfill operations.
// Returns ErrContextCancelled if the context is cancelled while waiting.
func (c *BackfillRateController) WaitForBudget(ctx context.Context, requiredCU int) error {
	if requiredCU <= 0 {
		return nil
	}

	for {
		// Check if context is already cancelled
		select {
		case <-ctx.Done():
			return ErrContextCancelled
		default:
		}

		// Try to consume budget from the shared pool (low priority)
		allowed, waitTime := c.tracker.TryConsume(ctx, requiredCU, PriorityLow)
		if allowed {
			c.RecordSuccess()
			return nil
		}

		// Budget not available, record failure and wait
		c.RecordFailure()

		// Use the longer of the suggested wait time or our backoff delay
		c.mu.Lock()
		delay := c.currentDelay
		c.mu.Unlock()

		if waitTime > delay {
			delay = waitTime
		}

		// Wait for the delay or context cancellation
		select {
		case <-ctx.Done():
			return ErrContextCancelled
		case <-time.After(delay):
			// Continue to retry
		}
	}
}

// RecordSuccess resets backoff on successful request.
// This should be called after a successful RPC operation.
func (c *BackfillRateController) RecordSuccess() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.consecutiveFails = 0
	c.currentDelay = c.baseDelay
}

// RecordFailure increases backoff on budget exhaustion.
// This should be called when a budget request is denied.
func (c *BackfillRateController) RecordFailure() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.consecutiveFails++

	// Calculate exponential backoff: baseDelay * 2^failures
	// Cap at maxDelay to prevent excessive waiting
	newDelay := c.baseDelay
	for i := 0; i < c.consecutiveFails; i++ {
		newDelay *= 2
		if newDelay > c.maxDelay {
			newDelay = c.maxDelay
			break
		}
	}
	c.currentDelay = newDelay
}

// ShouldPause returns true if backfill should pause (>90% usage).
// This implements graceful degradation per requirement 6.2.
func (c *BackfillRateController) ShouldPause(ctx context.Context) bool {
	isPause, err := c.tracker.IsPauseThreshold(ctx)
	if err != nil {
		// On error, be conservative and suggest pausing
		return true
	}
	return isPause
}

// GetCurrentDelay returns the current backoff delay.
// This is useful for monitoring and testing.
func (c *BackfillRateController) GetCurrentDelay() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.currentDelay
}

// GetConsecutiveFailures returns the number of consecutive failures.
// This is useful for monitoring and testing.
func (c *BackfillRateController) GetConsecutiveFailures() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.consecutiveFails
}

// GetBaseDelay returns the configured base delay.
func (c *BackfillRateController) GetBaseDelay() time.Duration {
	return c.baseDelay
}

// GetMaxDelay returns the configured max delay.
func (c *BackfillRateController) GetMaxDelay() time.Duration {
	return c.maxDelay
}
