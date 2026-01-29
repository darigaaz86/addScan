// Package ratelimit provides CU (Compute Unit) rate limiting for Alchemy RPC calls.
package ratelimit

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
)

// Default configuration values for rate limiting.
const (
	DefaultTotalCUPerSecond  = 500  // Total CU/s budget
	DefaultReservedCU        = 300  // Reserved for priority (Real-Time Sync)
	DefaultSharedCU          = 200  // Available for best-effort (Backfill)
	DefaultWindowSizeMs      = 1000 // 1 second sliding window
	DefaultWarningThreshold  = 80   // Percentage at which to emit warning
	DefaultPauseThreshold    = 90   // Percentage at which backfill should pause
	DefaultDefaultMethodCost = 20   // Default CU cost for unknown methods
)

// Environment variable names for rate limit configuration.
const (
	EnvTotalCUPerSecond  = "ALCHEMY_CU_PER_SECOND"
	EnvReservedCU        = "ALCHEMY_RESERVED_CU"
	EnvSharedCU          = "ALCHEMY_SHARED_CU"
	EnvWindowSizeMs      = "ALCHEMY_WINDOW_SIZE_MS"
	EnvWarningThreshold  = "ALCHEMY_WARNING_THRESHOLD"
	EnvPauseThreshold    = "ALCHEMY_PAUSE_THRESHOLD"
	EnvDefaultMethodCost = "ALCHEMY_DEFAULT_METHOD_COST"
)

// RateLimitConfig holds all rate limiting configuration.
// Configuration is loaded from environment variables with fallback to defaults.
type RateLimitConfig struct {
	// TotalCUPerSecond is the total CU budget per second.
	// Environment: ALCHEMY_CU_PER_SECOND, Default: 500
	TotalCUPerSecond int

	// ReservedCU is the CU budget reserved for priority operations (Real-Time Sync).
	// Environment: ALCHEMY_RESERVED_CU, Default: 300
	ReservedCU int

	// SharedCU is the CU budget available for best-effort operations (Backfill).
	// Environment: ALCHEMY_SHARED_CU, Default: 200
	SharedCU int

	// WindowSizeMs is the sliding window size in milliseconds.
	// Environment: ALCHEMY_WINDOW_SIZE_MS, Default: 1000
	WindowSizeMs int

	// WarningThreshold is the percentage of budget usage at which to emit warnings.
	// Environment: ALCHEMY_WARNING_THRESHOLD, Default: 80
	WarningThreshold int

	// PauseThreshold is the percentage of budget usage at which backfill should pause.
	// Environment: ALCHEMY_PAUSE_THRESHOLD, Default: 90
	PauseThreshold int

	// DefaultMethodCost is the default CU cost for unknown RPC methods.
	// Environment: ALCHEMY_DEFAULT_METHOD_COST, Default: 20
	DefaultMethodCost int
}

// NewRateLimitConfig creates a new RateLimitConfig with default values.
func NewRateLimitConfig() *RateLimitConfig {
	return &RateLimitConfig{
		TotalCUPerSecond:  DefaultTotalCUPerSecond,
		ReservedCU:        DefaultReservedCU,
		SharedCU:          DefaultSharedCU,
		WindowSizeMs:      DefaultWindowSizeMs,
		WarningThreshold:  DefaultWarningThreshold,
		PauseThreshold:    DefaultPauseThreshold,
		DefaultMethodCost: DefaultDefaultMethodCost,
	}
}

// LoadFromEnv loads configuration from environment variables.
// Invalid values are logged as warnings and defaults are used instead.
// Returns a new RateLimitConfig with values from environment or defaults.
func LoadFromEnv() *RateLimitConfig {
	cfg := NewRateLimitConfig()

	// Load each configuration value from environment
	if val := getEnvInt(EnvTotalCUPerSecond, DefaultTotalCUPerSecond); val > 0 {
		cfg.TotalCUPerSecond = val
	} else if os.Getenv(EnvTotalCUPerSecond) != "" {
		log.Printf("WARNING: Invalid %s value, using default %d", EnvTotalCUPerSecond, DefaultTotalCUPerSecond)
	}

	if val := getEnvInt(EnvReservedCU, DefaultReservedCU); val >= 0 {
		cfg.ReservedCU = val
	} else if os.Getenv(EnvReservedCU) != "" {
		log.Printf("WARNING: Invalid %s value, using default %d", EnvReservedCU, DefaultReservedCU)
	}

	if val := getEnvInt(EnvSharedCU, DefaultSharedCU); val >= 0 {
		cfg.SharedCU = val
	} else if os.Getenv(EnvSharedCU) != "" {
		log.Printf("WARNING: Invalid %s value, using default %d", EnvSharedCU, DefaultSharedCU)
	}

	if val := getEnvInt(EnvWindowSizeMs, DefaultWindowSizeMs); val > 0 {
		cfg.WindowSizeMs = val
	} else if os.Getenv(EnvWindowSizeMs) != "" {
		log.Printf("WARNING: Invalid %s value, using default %d", EnvWindowSizeMs, DefaultWindowSizeMs)
	}

	if val := getEnvInt(EnvWarningThreshold, DefaultWarningThreshold); val >= 0 && val <= 100 {
		cfg.WarningThreshold = val
	} else if os.Getenv(EnvWarningThreshold) != "" {
		log.Printf("WARNING: Invalid %s value, using default %d", EnvWarningThreshold, DefaultWarningThreshold)
	}

	if val := getEnvInt(EnvPauseThreshold, DefaultPauseThreshold); val >= 0 && val <= 100 {
		cfg.PauseThreshold = val
	} else if os.Getenv(EnvPauseThreshold) != "" {
		log.Printf("WARNING: Invalid %s value, using default %d", EnvPauseThreshold, DefaultPauseThreshold)
	}

	if val := getEnvInt(EnvDefaultMethodCost, DefaultDefaultMethodCost); val > 0 {
		cfg.DefaultMethodCost = val
	} else if os.Getenv(EnvDefaultMethodCost) != "" {
		log.Printf("WARNING: Invalid %s value, using default %d", EnvDefaultMethodCost, DefaultDefaultMethodCost)
	}

	// Validate the loaded configuration
	if err := cfg.Validate(); err != nil {
		log.Printf("WARNING: Configuration validation failed: %v. Using defaults.", err)
		return NewRateLimitConfig()
	}

	return cfg
}

// Validate ensures configuration is valid.
// Returns an error if:
// - TotalCUPerSecond is not positive
// - ReservedCU or SharedCU is negative
// - ReservedCU + SharedCU exceeds TotalCUPerSecond
// - WindowSizeMs is not positive
// - WarningThreshold or PauseThreshold is not in range [0, 100]
// - WarningThreshold is greater than PauseThreshold
// - DefaultMethodCost is not positive
func (c *RateLimitConfig) Validate() error {
	// Validate TotalCUPerSecond
	if c.TotalCUPerSecond <= 0 {
		return errors.New("TotalCUPerSecond must be positive")
	}

	// Validate ReservedCU
	if c.ReservedCU < 0 {
		return errors.New("ReservedCU cannot be negative")
	}

	// Validate SharedCU
	if c.SharedCU < 0 {
		return errors.New("SharedCU cannot be negative")
	}

	// Validate that reserved + shared does not exceed total budget (Requirement 7.4)
	if c.ReservedCU+c.SharedCU > c.TotalCUPerSecond {
		return fmt.Errorf("ReservedCU (%d) + SharedCU (%d) = %d exceeds TotalCUPerSecond (%d)",
			c.ReservedCU, c.SharedCU, c.ReservedCU+c.SharedCU, c.TotalCUPerSecond)
	}

	// Validate WindowSizeMs
	if c.WindowSizeMs <= 0 {
		return errors.New("WindowSizeMs must be positive")
	}

	// Validate WarningThreshold
	if c.WarningThreshold < 0 || c.WarningThreshold > 100 {
		return fmt.Errorf("WarningThreshold must be between 0 and 100, got %d", c.WarningThreshold)
	}

	// Validate PauseThreshold
	if c.PauseThreshold < 0 || c.PauseThreshold > 100 {
		return fmt.Errorf("PauseThreshold must be between 0 and 100, got %d", c.PauseThreshold)
	}

	// Validate that warning threshold is less than or equal to pause threshold
	if c.WarningThreshold > c.PauseThreshold {
		return fmt.Errorf("WarningThreshold (%d) cannot be greater than PauseThreshold (%d)",
			c.WarningThreshold, c.PauseThreshold)
	}

	// Validate DefaultMethodCost
	if c.DefaultMethodCost <= 0 {
		return errors.New("DefaultMethodCost must be positive")
	}

	return nil
}

// getEnvInt reads an environment variable and parses it as an integer.
// Returns the default value if the environment variable is not set or cannot be parsed.
func getEnvInt(key string, defaultVal int) int {
	val := os.Getenv(key)
	if val == "" {
		return defaultVal
	}

	intVal, err := strconv.Atoi(val)
	if err != nil {
		return -1 // Signal invalid value
	}

	return intVal
}

// String returns a string representation of the configuration for logging.
func (c *RateLimitConfig) String() string {
	return fmt.Sprintf(
		"RateLimitConfig{TotalCUPerSecond: %d, ReservedCU: %d, SharedCU: %d, WindowSizeMs: %d, WarningThreshold: %d%%, PauseThreshold: %d%%, DefaultMethodCost: %d}",
		c.TotalCUPerSecond, c.ReservedCU, c.SharedCU, c.WindowSizeMs,
		c.WarningThreshold, c.PauseThreshold, c.DefaultMethodCost,
	)
}
