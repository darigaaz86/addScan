package retry

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/address-scanner/internal/logging"
)

// RetryConfig configures retry behavior
type RetryConfig struct {
	MaxAttempts     int           // Maximum number of retry attempts
	InitialDelay    time.Duration // Initial delay before first retry
	MaxDelay        time.Duration // Maximum delay between retries
	Multiplier      float64       // Multiplier for exponential backoff
	RetryableErrors []string      // List of error codes that should trigger retry
}

// DefaultRetryConfig returns a default retry configuration
// Pattern: 1s, 2s, 4s, 8s, 16s, max 60s
func DefaultRetryConfig() *RetryConfig {
	return &RetryConfig{
		MaxAttempts:  5,
		InitialDelay: 1 * time.Second,
		MaxDelay:     60 * time.Second,
		Multiplier:   2.0,
	}
}

// RetryResult contains information about the retry operation
type RetryResult struct {
	Attempts      int           `json:"attempts"`
	Success       bool          `json:"success"`
	TotalDuration time.Duration `json:"totalDuration"`
	LastError     error         `json:"lastError,omitempty"`
}

// RetryFunc is a function that can be retried
type RetryFunc func(ctx context.Context, attempt int) error

// WithExponentialBackoff executes a function with exponential backoff retry logic
func WithExponentialBackoff(ctx context.Context, config *RetryConfig, fn RetryFunc) *RetryResult {
	logger := logging.FromContext(ctx)
	startTime := time.Now()

	result := &RetryResult{
		Attempts: 0,
		Success:  false,
	}

	var lastErr error

	for attempt := 1; attempt <= config.MaxAttempts; attempt++ {
		result.Attempts = attempt

		// Execute the function
		err := fn(ctx, attempt)
		if err == nil {
			// Success!
			result.Success = true
			result.TotalDuration = time.Since(startTime)
			
			if attempt > 1 {
				logger.WithFields(map[string]interface{}{
					"attempts":      attempt,
					"totalDuration": result.TotalDuration,
				}).Info("Operation succeeded after retry")
			}
			
			return result
		}

		lastErr = err
		result.LastError = err

		// Check if we should retry
		if attempt >= config.MaxAttempts {
			logger.WithFields(map[string]interface{}{
				"attempts":      attempt,
				"totalDuration": time.Since(startTime),
				"error":         err.Error(),
			}).Error("Operation failed after max retry attempts")
			break
		}

		// Check context cancellation
		if ctx.Err() != nil {
			logger.WithError(ctx.Err()).Warn("Retry cancelled due to context cancellation")
			result.LastError = ctx.Err()
			break
		}

		// Calculate delay with exponential backoff
		delay := calculateDelay(config, attempt)

		logger.WithFields(map[string]interface{}{
			"attempt":    attempt,
			"maxAttempts": config.MaxAttempts,
			"delay":      delay,
			"error":      err.Error(),
		}).Warn("Operation failed, retrying with exponential backoff")

		// Wait before retry
		select {
		case <-time.After(delay):
			// Continue to next attempt
		case <-ctx.Done():
			logger.WithError(ctx.Err()).Warn("Retry cancelled during backoff")
			result.LastError = ctx.Err()
			result.TotalDuration = time.Since(startTime)
			return result
		}
	}

	result.TotalDuration = time.Since(startTime)
	result.LastError = lastErr
	return result
}

// calculateDelay calculates the delay for the next retry attempt
func calculateDelay(config *RetryConfig, attempt int) time.Duration {
	// Calculate exponential delay: initialDelay * multiplier^(attempt-1)
	delay := float64(config.InitialDelay) * math.Pow(config.Multiplier, float64(attempt-1))

	// Cap at max delay
	if delay > float64(config.MaxDelay) {
		delay = float64(config.MaxDelay)
	}

	return time.Duration(delay)
}

// WithRetry is a simpler retry function that uses default configuration
func WithRetry(ctx context.Context, fn RetryFunc) error {
	config := DefaultRetryConfig()
	result := WithExponentialBackoff(ctx, config, fn)
	
	if !result.Success {
		return fmt.Errorf("operation failed after %d attempts: %w", result.Attempts, result.LastError)
	}
	
	return nil
}

// IsRetryable determines if an error should trigger a retry
func IsRetryable(err error, retryableErrors []string) bool {
	if err == nil {
		return false
	}

	errStr := err.Error()

	// If no specific retryable errors are configured, retry on all errors
	if len(retryableErrors) == 0 {
		return true
	}

	// Check if error matches any retryable error pattern
	for _, retryableErr := range retryableErrors {
		if contains(errStr, retryableErr) {
			return true
		}
	}

	return false
}

// contains checks if a string contains a substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && containsSubstring(s, substr))
}

func containsSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// RetryableFunc wraps a function to make it retryable with custom logic
type RetryableFunc struct {
	config *RetryConfig
	fn     RetryFunc
}

// NewRetryableFunc creates a new retryable function wrapper
func NewRetryableFunc(config *RetryConfig, fn RetryFunc) *RetryableFunc {
	return &RetryableFunc{
		config: config,
		fn:     fn,
	}
}

// Execute executes the retryable function
func (rf *RetryableFunc) Execute(ctx context.Context) *RetryResult {
	return WithExponentialBackoff(ctx, rf.config, rf.fn)
}

// ExecuteWithResult executes the retryable function and returns the result or error
func (rf *RetryableFunc) ExecuteWithResult(ctx context.Context) error {
	result := rf.Execute(ctx)
	if !result.Success {
		return fmt.Errorf("operation failed after %d attempts: %w", result.Attempts, result.LastError)
	}
	return nil
}

// RetryStats tracks statistics about retry operations
type RetryStats struct {
	TotalOperations   int           `json:"totalOperations"`
	SuccessfulOps     int           `json:"successfulOps"`
	FailedOps         int           `json:"failedOps"`
	TotalRetries      int           `json:"totalRetries"`
	AverageAttempts   float64       `json:"averageAttempts"`
	AverageDuration   time.Duration `json:"averageDuration"`
}

// RetryStatsTracker tracks retry statistics
type RetryStatsTracker struct {
	stats RetryStats
}

// NewRetryStatsTracker creates a new retry stats tracker
func NewRetryStatsTracker() *RetryStatsTracker {
	return &RetryStatsTracker{}
}

// RecordResult records the result of a retry operation
func (rst *RetryStatsTracker) RecordResult(result *RetryResult) {
	rst.stats.TotalOperations++
	
	if result.Success {
		rst.stats.SuccessfulOps++
	} else {
		rst.stats.FailedOps++
	}
	
	if result.Attempts > 1 {
		rst.stats.TotalRetries += (result.Attempts - 1)
	}
	
	// Update averages
	rst.stats.AverageAttempts = float64(rst.stats.TotalRetries+rst.stats.TotalOperations) / float64(rst.stats.TotalOperations)
}

// GetStats returns the current retry statistics
func (rst *RetryStatsTracker) GetStats() RetryStats {
	return rst.stats
}

// Reset resets the retry statistics
func (rst *RetryStatsTracker) Reset() {
	rst.stats = RetryStats{}
}
