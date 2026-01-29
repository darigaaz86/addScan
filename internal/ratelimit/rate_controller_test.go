package ratelimit

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupTestRateController creates a BackfillRateController with a test Redis instance.
func setupTestRateController(t *testing.T, baseDelay, maxDelay time.Duration) (*BackfillRateController, *CUBudgetTracker, *miniredis.Miniredis) {
	t.Helper()

	mr, err := miniredis.Run()
	require.NoError(t, err)

	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})

	tracker, err := NewCUBudgetTracker(&CUBudgetTrackerConfig{
		Redis:          client,
		TotalBudget:    500,
		ReservedBudget: 300,
		WindowSize:     time.Second,
	})
	require.NoError(t, err)

	cfg := &BackfillRateControllerConfig{
		Tracker:   tracker,
		BaseDelay: baseDelay,
		MaxDelay:  maxDelay,
	}

	controller, err := NewBackfillRateController(cfg)
	require.NoError(t, err)

	return controller, tracker, mr
}

func TestNewBackfillRateController(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})

	tracker, err := NewCUBudgetTracker(&CUBudgetTrackerConfig{
		Redis: client,
	})
	require.NoError(t, err)

	t.Run("creates controller with valid config", func(t *testing.T) {
		cfg := &BackfillRateControllerConfig{
			Tracker:   tracker,
			BaseDelay: 50 * time.Millisecond,
			MaxDelay:  5 * time.Second,
		}

		controller, err := NewBackfillRateController(cfg)
		require.NoError(t, err)
		assert.NotNil(t, controller)
		assert.Equal(t, 50*time.Millisecond, controller.GetBaseDelay())
		assert.Equal(t, 5*time.Second, controller.GetMaxDelay())
	})

	t.Run("applies defaults when not specified", func(t *testing.T) {
		cfg := &BackfillRateControllerConfig{
			Tracker: tracker,
		}

		controller, err := NewBackfillRateController(cfg)
		require.NoError(t, err)
		assert.Equal(t, DefaultBaseDelay, controller.GetBaseDelay())
		assert.Equal(t, DefaultMaxDelay, controller.GetMaxDelay())
	})

	t.Run("returns error for nil config", func(t *testing.T) {
		controller, err := NewBackfillRateController(nil)
		assert.Error(t, err)
		assert.Nil(t, controller)
		assert.Contains(t, err.Error(), "configuration is required")
	})

	t.Run("returns error for nil tracker", func(t *testing.T) {
		cfg := &BackfillRateControllerConfig{
			Tracker: nil,
		}

		controller, err := NewBackfillRateController(cfg)
		assert.Error(t, err)
		assert.Nil(t, controller)
		assert.Contains(t, err.Error(), "tracker is required")
	})

	t.Run("returns error for negative base delay", func(t *testing.T) {
		cfg := &BackfillRateControllerConfig{
			Tracker:   tracker,
			BaseDelay: -1 * time.Millisecond,
		}

		controller, err := NewBackfillRateController(cfg)
		assert.Error(t, err)
		assert.Nil(t, controller)
		assert.Contains(t, err.Error(), "base delay cannot be negative")
	})

	t.Run("returns error for negative max delay", func(t *testing.T) {
		cfg := &BackfillRateControllerConfig{
			Tracker:  tracker,
			MaxDelay: -1 * time.Millisecond,
		}

		controller, err := NewBackfillRateController(cfg)
		assert.Error(t, err)
		assert.Nil(t, controller)
		assert.Contains(t, err.Error(), "max delay cannot be negative")
	})

	t.Run("returns error when base delay exceeds max delay", func(t *testing.T) {
		cfg := &BackfillRateControllerConfig{
			Tracker:   tracker,
			BaseDelay: 10 * time.Second,
			MaxDelay:  1 * time.Second,
		}

		controller, err := NewBackfillRateController(cfg)
		assert.Error(t, err)
		assert.Nil(t, controller)
		assert.Contains(t, err.Error(), "base delay cannot exceed max delay")
	})
}

func TestBackfillRateController_RecordSuccessAndFailure(t *testing.T) {
	controller, _, mr := setupTestRateController(t, 100*time.Millisecond, 10*time.Second)
	defer mr.Close()

	t.Run("initial state has zero failures and base delay", func(t *testing.T) {
		assert.Equal(t, 0, controller.GetConsecutiveFailures())
		assert.Equal(t, 100*time.Millisecond, controller.GetCurrentDelay())
	})

	t.Run("RecordFailure increases consecutive failures and delay", func(t *testing.T) {
		controller.RecordFailure()
		assert.Equal(t, 1, controller.GetConsecutiveFailures())
		assert.Equal(t, 200*time.Millisecond, controller.GetCurrentDelay())

		controller.RecordFailure()
		assert.Equal(t, 2, controller.GetConsecutiveFailures())
		assert.Equal(t, 400*time.Millisecond, controller.GetCurrentDelay())

		controller.RecordFailure()
		assert.Equal(t, 3, controller.GetConsecutiveFailures())
		assert.Equal(t, 800*time.Millisecond, controller.GetCurrentDelay())
	})

	t.Run("RecordSuccess resets failures and delay", func(t *testing.T) {
		controller.RecordSuccess()
		assert.Equal(t, 0, controller.GetConsecutiveFailures())
		assert.Equal(t, 100*time.Millisecond, controller.GetCurrentDelay())
	})
}

func TestBackfillRateController_ExponentialBackoff(t *testing.T) {
	t.Run("delay doubles with each failure", func(t *testing.T) {
		controller, _, mr := setupTestRateController(t, 100*time.Millisecond, 10*time.Second)
		defer mr.Close()

		expectedDelays := []time.Duration{
			200 * time.Millisecond,  // 100 * 2^1
			400 * time.Millisecond,  // 100 * 2^2
			800 * time.Millisecond,  // 100 * 2^3
			1600 * time.Millisecond, // 100 * 2^4
			3200 * time.Millisecond, // 100 * 2^5
			6400 * time.Millisecond, // 100 * 2^6
			10 * time.Second,        // capped at max
			10 * time.Second,        // stays at max
		}

		for i, expected := range expectedDelays {
			controller.RecordFailure()
			actual := controller.GetCurrentDelay()
			assert.Equal(t, expected, actual, "failure %d: expected %v, got %v", i+1, expected, actual)
		}
	})

	t.Run("delay is capped at max delay", func(t *testing.T) {
		controller, _, mr := setupTestRateController(t, 100*time.Millisecond, 500*time.Millisecond)
		defer mr.Close()

		// Record many failures
		for i := 0; i < 10; i++ {
			controller.RecordFailure()
		}

		// Delay should be capped at max
		assert.Equal(t, 500*time.Millisecond, controller.GetCurrentDelay())
	})
}

func TestBackfillRateController_WaitForBudget(t *testing.T) {
	t.Run("returns immediately when budget is available", func(t *testing.T) {
		controller, _, mr := setupTestRateController(t, 10*time.Millisecond, 100*time.Millisecond)
		defer mr.Close()

		ctx := context.Background()
		start := time.Now()

		err := controller.WaitForBudget(ctx, 50) // Request 50 CU from shared pool (200 available)
		elapsed := time.Since(start)

		assert.NoError(t, err)
		assert.Less(t, elapsed, 50*time.Millisecond, "should return quickly when budget available")
		assert.Equal(t, 0, controller.GetConsecutiveFailures())
	})

	t.Run("returns nil for zero or negative CU", func(t *testing.T) {
		controller, _, mr := setupTestRateController(t, 10*time.Millisecond, 100*time.Millisecond)
		defer mr.Close()

		ctx := context.Background()

		err := controller.WaitForBudget(ctx, 0)
		assert.NoError(t, err)

		err = controller.WaitForBudget(ctx, -10)
		assert.NoError(t, err)
	})

	t.Run("returns error when context is cancelled", func(t *testing.T) {
		controller, tracker, mr := setupTestRateController(t, 10*time.Millisecond, 100*time.Millisecond)
		defer mr.Close()

		// Exhaust the shared budget
		ctx := context.Background()
		for i := 0; i < 10; i++ {
			tracker.TryConsume(ctx, 20, PriorityLow) // Consume all 200 CU shared budget
		}

		// Create a context that will be cancelled
		cancelCtx, cancel := context.WithCancel(context.Background())

		// Cancel after a short delay
		go func() {
			time.Sleep(50 * time.Millisecond)
			cancel()
		}()

		err := controller.WaitForBudget(cancelCtx, 50)
		assert.Error(t, err)
		assert.Equal(t, ErrContextCancelled, err)
	})

	t.Run("returns error when context is already cancelled", func(t *testing.T) {
		controller, _, mr := setupTestRateController(t, 10*time.Millisecond, 100*time.Millisecond)
		defer mr.Close()

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		err := controller.WaitForBudget(ctx, 50)
		assert.Error(t, err)
		assert.Equal(t, ErrContextCancelled, err)
	})

	t.Run("resets backoff on successful budget acquisition", func(t *testing.T) {
		controller, _, mr := setupTestRateController(t, 10*time.Millisecond, 100*time.Millisecond)
		defer mr.Close()

		// Simulate some failures first
		controller.RecordFailure()
		controller.RecordFailure()
		assert.Equal(t, 2, controller.GetConsecutiveFailures())

		// Now get budget successfully
		ctx := context.Background()
		err := controller.WaitForBudget(ctx, 10)
		assert.NoError(t, err)

		// Failures should be reset
		assert.Equal(t, 0, controller.GetConsecutiveFailures())
		assert.Equal(t, 10*time.Millisecond, controller.GetCurrentDelay())
	})
}

func TestBackfillRateController_ShouldPause(t *testing.T) {
	t.Run("returns false when utilization is below 90%", func(t *testing.T) {
		controller, tracker, mr := setupTestRateController(t, 10*time.Millisecond, 100*time.Millisecond)
		defer mr.Close()

		ctx := context.Background()

		// Consume 400 CU (80% of 500 total)
		tracker.TryConsume(ctx, 200, PriorityHigh)
		tracker.TryConsume(ctx, 100, PriorityHigh)
		tracker.TryConsume(ctx, 100, PriorityLow)

		shouldPause := controller.ShouldPause(ctx)
		assert.False(t, shouldPause)
	})

	t.Run("returns true when utilization is at or above 90%", func(t *testing.T) {
		controller, tracker, mr := setupTestRateController(t, 10*time.Millisecond, 100*time.Millisecond)
		defer mr.Close()

		ctx := context.Background()

		// Consume 450 CU (90% of 500 total)
		tracker.TryConsume(ctx, 250, PriorityHigh)
		tracker.TryConsume(ctx, 100, PriorityLow)
		tracker.TryConsume(ctx, 100, PriorityLow)

		shouldPause := controller.ShouldPause(ctx)
		assert.True(t, shouldPause)
	})

	t.Run("returns true when utilization exceeds 90%", func(t *testing.T) {
		controller, tracker, mr := setupTestRateController(t, 10*time.Millisecond, 100*time.Millisecond)
		defer mr.Close()

		ctx := context.Background()

		// Consume 480 CU (96% of 500 total)
		tracker.TryConsume(ctx, 280, PriorityHigh)
		tracker.TryConsume(ctx, 200, PriorityLow)

		shouldPause := controller.ShouldPause(ctx)
		assert.True(t, shouldPause)
	})
}

func TestBackfillRateController_ConcurrentAccess(t *testing.T) {
	controller, _, mr := setupTestRateController(t, 10*time.Millisecond, 100*time.Millisecond)
	defer mr.Close()

	// Test concurrent RecordFailure and RecordSuccess calls
	done := make(chan bool)

	// Goroutine 1: Record failures
	go func() {
		for i := 0; i < 100; i++ {
			controller.RecordFailure()
		}
		done <- true
	}()

	// Goroutine 2: Record successes
	go func() {
		for i := 0; i < 100; i++ {
			controller.RecordSuccess()
		}
		done <- true
	}()

	// Goroutine 3: Read current delay
	go func() {
		for i := 0; i < 100; i++ {
			_ = controller.GetCurrentDelay()
			_ = controller.GetConsecutiveFailures()
		}
		done <- true
	}()

	// Wait for all goroutines to complete
	for i := 0; i < 3; i++ {
		<-done
	}

	// No panic means the test passed - concurrent access is safe
}

func TestBackfillRateControllerConfig_Validate(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})

	tracker, err := NewCUBudgetTracker(&CUBudgetTrackerConfig{
		Redis: client,
	})
	require.NoError(t, err)

	t.Run("valid config passes validation", func(t *testing.T) {
		cfg := &BackfillRateControllerConfig{
			Tracker:   tracker,
			BaseDelay: 100 * time.Millisecond,
			MaxDelay:  10 * time.Second,
		}
		err := cfg.Validate()
		assert.NoError(t, err)
	})

	t.Run("nil tracker fails validation", func(t *testing.T) {
		cfg := &BackfillRateControllerConfig{
			Tracker: nil,
		}
		err := cfg.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "tracker is required")
	})

	t.Run("negative base delay fails validation", func(t *testing.T) {
		cfg := &BackfillRateControllerConfig{
			Tracker:   tracker,
			BaseDelay: -1 * time.Millisecond,
		}
		err := cfg.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "base delay cannot be negative")
	})

	t.Run("negative max delay fails validation", func(t *testing.T) {
		cfg := &BackfillRateControllerConfig{
			Tracker:  tracker,
			MaxDelay: -1 * time.Millisecond,
		}
		err := cfg.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "max delay cannot be negative")
	})

	t.Run("base delay exceeding max delay fails validation", func(t *testing.T) {
		cfg := &BackfillRateControllerConfig{
			Tracker:   tracker,
			BaseDelay: 10 * time.Second,
			MaxDelay:  1 * time.Second,
		}
		err := cfg.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "base delay cannot exceed max delay")
	})

	t.Run("zero delays are allowed (will use defaults)", func(t *testing.T) {
		cfg := &BackfillRateControllerConfig{
			Tracker:   tracker,
			BaseDelay: 0,
			MaxDelay:  0,
		}
		err := cfg.Validate()
		assert.NoError(t, err)
	})
}
