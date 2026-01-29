package ratelimit

import (
	"os"
	"testing"
)

func TestNewRateLimitConfig_Defaults(t *testing.T) {
	cfg := NewRateLimitConfig()

	if cfg.TotalCUPerSecond != DefaultTotalCUPerSecond {
		t.Errorf("TotalCUPerSecond = %d, want %d", cfg.TotalCUPerSecond, DefaultTotalCUPerSecond)
	}
	if cfg.ReservedCU != DefaultReservedCU {
		t.Errorf("ReservedCU = %d, want %d", cfg.ReservedCU, DefaultReservedCU)
	}
	if cfg.SharedCU != DefaultSharedCU {
		t.Errorf("SharedCU = %d, want %d", cfg.SharedCU, DefaultSharedCU)
	}
	if cfg.WindowSizeMs != DefaultWindowSizeMs {
		t.Errorf("WindowSizeMs = %d, want %d", cfg.WindowSizeMs, DefaultWindowSizeMs)
	}
	if cfg.WarningThreshold != DefaultWarningThreshold {
		t.Errorf("WarningThreshold = %d, want %d", cfg.WarningThreshold, DefaultWarningThreshold)
	}
	if cfg.PauseThreshold != DefaultPauseThreshold {
		t.Errorf("PauseThreshold = %d, want %d", cfg.PauseThreshold, DefaultPauseThreshold)
	}
	if cfg.DefaultMethodCost != DefaultDefaultMethodCost {
		t.Errorf("DefaultMethodCost = %d, want %d", cfg.DefaultMethodCost, DefaultDefaultMethodCost)
	}
}

func TestRateLimitConfig_Validate_ValidConfig(t *testing.T) {
	tests := []struct {
		name string
		cfg  *RateLimitConfig
	}{
		{
			name: "default config",
			cfg:  NewRateLimitConfig(),
		},
		{
			name: "custom valid config",
			cfg: &RateLimitConfig{
				TotalCUPerSecond:  1000,
				ReservedCU:        600,
				SharedCU:          400,
				WindowSizeMs:      2000,
				WarningThreshold:  70,
				PauseThreshold:    85,
				DefaultMethodCost: 25,
			},
		},
		{
			name: "reserved + shared equals total",
			cfg: &RateLimitConfig{
				TotalCUPerSecond:  500,
				ReservedCU:        300,
				SharedCU:          200,
				WindowSizeMs:      1000,
				WarningThreshold:  80,
				PauseThreshold:    90,
				DefaultMethodCost: 20,
			},
		},
		{
			name: "zero reserved CU",
			cfg: &RateLimitConfig{
				TotalCUPerSecond:  500,
				ReservedCU:        0,
				SharedCU:          500,
				WindowSizeMs:      1000,
				WarningThreshold:  80,
				PauseThreshold:    90,
				DefaultMethodCost: 20,
			},
		},
		{
			name: "zero shared CU",
			cfg: &RateLimitConfig{
				TotalCUPerSecond:  500,
				ReservedCU:        500,
				SharedCU:          0,
				WindowSizeMs:      1000,
				WarningThreshold:  80,
				PauseThreshold:    90,
				DefaultMethodCost: 20,
			},
		},
		{
			name: "warning equals pause threshold",
			cfg: &RateLimitConfig{
				TotalCUPerSecond:  500,
				ReservedCU:        300,
				SharedCU:          200,
				WindowSizeMs:      1000,
				WarningThreshold:  90,
				PauseThreshold:    90,
				DefaultMethodCost: 20,
			},
		},
		{
			name: "zero thresholds",
			cfg: &RateLimitConfig{
				TotalCUPerSecond:  500,
				ReservedCU:        300,
				SharedCU:          200,
				WindowSizeMs:      1000,
				WarningThreshold:  0,
				PauseThreshold:    0,
				DefaultMethodCost: 20,
			},
		},
		{
			name: "100% thresholds",
			cfg: &RateLimitConfig{
				TotalCUPerSecond:  500,
				ReservedCU:        300,
				SharedCU:          200,
				WindowSizeMs:      1000,
				WarningThreshold:  100,
				PauseThreshold:    100,
				DefaultMethodCost: 20,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.cfg.Validate(); err != nil {
				t.Errorf("Validate() returned error for valid config: %v", err)
			}
		})
	}
}

func TestRateLimitConfig_Validate_InvalidConfig(t *testing.T) {
	tests := []struct {
		name        string
		cfg         *RateLimitConfig
		errContains string
	}{
		{
			name: "zero total CU",
			cfg: &RateLimitConfig{
				TotalCUPerSecond:  0,
				ReservedCU:        300,
				SharedCU:          200,
				WindowSizeMs:      1000,
				WarningThreshold:  80,
				PauseThreshold:    90,
				DefaultMethodCost: 20,
			},
			errContains: "TotalCUPerSecond must be positive",
		},
		{
			name: "negative total CU",
			cfg: &RateLimitConfig{
				TotalCUPerSecond:  -100,
				ReservedCU:        300,
				SharedCU:          200,
				WindowSizeMs:      1000,
				WarningThreshold:  80,
				PauseThreshold:    90,
				DefaultMethodCost: 20,
			},
			errContains: "TotalCUPerSecond must be positive",
		},
		{
			name: "negative reserved CU",
			cfg: &RateLimitConfig{
				TotalCUPerSecond:  500,
				ReservedCU:        -100,
				SharedCU:          200,
				WindowSizeMs:      1000,
				WarningThreshold:  80,
				PauseThreshold:    90,
				DefaultMethodCost: 20,
			},
			errContains: "ReservedCU cannot be negative",
		},
		{
			name: "negative shared CU",
			cfg: &RateLimitConfig{
				TotalCUPerSecond:  500,
				ReservedCU:        300,
				SharedCU:          -100,
				WindowSizeMs:      1000,
				WarningThreshold:  80,
				PauseThreshold:    90,
				DefaultMethodCost: 20,
			},
			errContains: "SharedCU cannot be negative",
		},
		{
			name: "reserved + shared exceeds total",
			cfg: &RateLimitConfig{
				TotalCUPerSecond:  500,
				ReservedCU:        400,
				SharedCU:          200,
				WindowSizeMs:      1000,
				WarningThreshold:  80,
				PauseThreshold:    90,
				DefaultMethodCost: 20,
			},
			errContains: "exceeds TotalCUPerSecond",
		},
		{
			name: "zero window size",
			cfg: &RateLimitConfig{
				TotalCUPerSecond:  500,
				ReservedCU:        300,
				SharedCU:          200,
				WindowSizeMs:      0,
				WarningThreshold:  80,
				PauseThreshold:    90,
				DefaultMethodCost: 20,
			},
			errContains: "WindowSizeMs must be positive",
		},
		{
			name: "negative window size",
			cfg: &RateLimitConfig{
				TotalCUPerSecond:  500,
				ReservedCU:        300,
				SharedCU:          200,
				WindowSizeMs:      -1000,
				WarningThreshold:  80,
				PauseThreshold:    90,
				DefaultMethodCost: 20,
			},
			errContains: "WindowSizeMs must be positive",
		},
		{
			name: "negative warning threshold",
			cfg: &RateLimitConfig{
				TotalCUPerSecond:  500,
				ReservedCU:        300,
				SharedCU:          200,
				WindowSizeMs:      1000,
				WarningThreshold:  -10,
				PauseThreshold:    90,
				DefaultMethodCost: 20,
			},
			errContains: "WarningThreshold must be between 0 and 100",
		},
		{
			name: "warning threshold over 100",
			cfg: &RateLimitConfig{
				TotalCUPerSecond:  500,
				ReservedCU:        300,
				SharedCU:          200,
				WindowSizeMs:      1000,
				WarningThreshold:  110,
				PauseThreshold:    90,
				DefaultMethodCost: 20,
			},
			errContains: "WarningThreshold must be between 0 and 100",
		},
		{
			name: "negative pause threshold",
			cfg: &RateLimitConfig{
				TotalCUPerSecond:  500,
				ReservedCU:        300,
				SharedCU:          200,
				WindowSizeMs:      1000,
				WarningThreshold:  80,
				PauseThreshold:    -10,
				DefaultMethodCost: 20,
			},
			errContains: "PauseThreshold must be between 0 and 100",
		},
		{
			name: "pause threshold over 100",
			cfg: &RateLimitConfig{
				TotalCUPerSecond:  500,
				ReservedCU:        300,
				SharedCU:          200,
				WindowSizeMs:      1000,
				WarningThreshold:  80,
				PauseThreshold:    150,
				DefaultMethodCost: 20,
			},
			errContains: "PauseThreshold must be between 0 and 100",
		},
		{
			name: "warning threshold greater than pause threshold",
			cfg: &RateLimitConfig{
				TotalCUPerSecond:  500,
				ReservedCU:        300,
				SharedCU:          200,
				WindowSizeMs:      1000,
				WarningThreshold:  95,
				PauseThreshold:    90,
				DefaultMethodCost: 20,
			},
			errContains: "WarningThreshold (95) cannot be greater than PauseThreshold (90)",
		},
		{
			name: "zero default method cost",
			cfg: &RateLimitConfig{
				TotalCUPerSecond:  500,
				ReservedCU:        300,
				SharedCU:          200,
				WindowSizeMs:      1000,
				WarningThreshold:  80,
				PauseThreshold:    90,
				DefaultMethodCost: 0,
			},
			errContains: "DefaultMethodCost must be positive",
		},
		{
			name: "negative default method cost",
			cfg: &RateLimitConfig{
				TotalCUPerSecond:  500,
				ReservedCU:        300,
				SharedCU:          200,
				WindowSizeMs:      1000,
				WarningThreshold:  80,
				PauseThreshold:    90,
				DefaultMethodCost: -10,
			},
			errContains: "DefaultMethodCost must be positive",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if err == nil {
				t.Error("Validate() should return error for invalid config")
				return
			}
			if tt.errContains != "" && !configContainsString(err.Error(), tt.errContains) {
				t.Errorf("Validate() error = %q, want error containing %q", err.Error(), tt.errContains)
			}
		})
	}
}

func TestLoadFromEnv_NoEnvVars(t *testing.T) {
	// Clear all relevant environment variables
	clearEnvVars(t)

	cfg := LoadFromEnv()

	// Should return defaults
	if cfg.TotalCUPerSecond != DefaultTotalCUPerSecond {
		t.Errorf("TotalCUPerSecond = %d, want %d", cfg.TotalCUPerSecond, DefaultTotalCUPerSecond)
	}
	if cfg.ReservedCU != DefaultReservedCU {
		t.Errorf("ReservedCU = %d, want %d", cfg.ReservedCU, DefaultReservedCU)
	}
	if cfg.SharedCU != DefaultSharedCU {
		t.Errorf("SharedCU = %d, want %d", cfg.SharedCU, DefaultSharedCU)
	}
}

func TestLoadFromEnv_ValidEnvVars(t *testing.T) {
	clearEnvVars(t)

	// Set valid environment variables
	os.Setenv(EnvTotalCUPerSecond, "1000")
	os.Setenv(EnvReservedCU, "600")
	os.Setenv(EnvSharedCU, "400")
	os.Setenv(EnvWindowSizeMs, "2000")
	os.Setenv(EnvWarningThreshold, "75")
	os.Setenv(EnvPauseThreshold, "85")
	os.Setenv(EnvDefaultMethodCost, "30")

	cfg := LoadFromEnv()

	if cfg.TotalCUPerSecond != 1000 {
		t.Errorf("TotalCUPerSecond = %d, want 1000", cfg.TotalCUPerSecond)
	}
	if cfg.ReservedCU != 600 {
		t.Errorf("ReservedCU = %d, want 600", cfg.ReservedCU)
	}
	if cfg.SharedCU != 400 {
		t.Errorf("SharedCU = %d, want 400", cfg.SharedCU)
	}
	if cfg.WindowSizeMs != 2000 {
		t.Errorf("WindowSizeMs = %d, want 2000", cfg.WindowSizeMs)
	}
	if cfg.WarningThreshold != 75 {
		t.Errorf("WarningThreshold = %d, want 75", cfg.WarningThreshold)
	}
	if cfg.PauseThreshold != 85 {
		t.Errorf("PauseThreshold = %d, want 85", cfg.PauseThreshold)
	}
	if cfg.DefaultMethodCost != 30 {
		t.Errorf("DefaultMethodCost = %d, want 30", cfg.DefaultMethodCost)
	}
}

func TestLoadFromEnv_InvalidEnvVars_FallbackToDefaults(t *testing.T) {
	clearEnvVars(t)

	// Set invalid environment variables (non-numeric)
	os.Setenv(EnvTotalCUPerSecond, "invalid")
	os.Setenv(EnvReservedCU, "not_a_number")

	cfg := LoadFromEnv()

	// Should fall back to defaults
	if cfg.TotalCUPerSecond != DefaultTotalCUPerSecond {
		t.Errorf("TotalCUPerSecond = %d, want %d (default)", cfg.TotalCUPerSecond, DefaultTotalCUPerSecond)
	}
	if cfg.ReservedCU != DefaultReservedCU {
		t.Errorf("ReservedCU = %d, want %d (default)", cfg.ReservedCU, DefaultReservedCU)
	}
}

func TestLoadFromEnv_InvalidConfig_FallbackToDefaults(t *testing.T) {
	clearEnvVars(t)

	// Set values that would fail validation (reserved + shared > total)
	os.Setenv(EnvTotalCUPerSecond, "500")
	os.Setenv(EnvReservedCU, "400")
	os.Setenv(EnvSharedCU, "300") // 400 + 300 = 700 > 500

	cfg := LoadFromEnv()

	// Should fall back to all defaults due to validation failure
	if cfg.TotalCUPerSecond != DefaultTotalCUPerSecond {
		t.Errorf("TotalCUPerSecond = %d, want %d (default)", cfg.TotalCUPerSecond, DefaultTotalCUPerSecond)
	}
	if cfg.ReservedCU != DefaultReservedCU {
		t.Errorf("ReservedCU = %d, want %d (default)", cfg.ReservedCU, DefaultReservedCU)
	}
	if cfg.SharedCU != DefaultSharedCU {
		t.Errorf("SharedCU = %d, want %d (default)", cfg.SharedCU, DefaultSharedCU)
	}
}

func TestLoadFromEnv_NegativeValues_FallbackToDefaults(t *testing.T) {
	clearEnvVars(t)

	// Set negative values
	os.Setenv(EnvTotalCUPerSecond, "-100")

	cfg := LoadFromEnv()

	// Should fall back to defaults
	if cfg.TotalCUPerSecond != DefaultTotalCUPerSecond {
		t.Errorf("TotalCUPerSecond = %d, want %d (default)", cfg.TotalCUPerSecond, DefaultTotalCUPerSecond)
	}
}

func TestLoadFromEnv_ZeroTotalCU_FallbackToDefaults(t *testing.T) {
	clearEnvVars(t)

	// Set zero total CU (invalid)
	os.Setenv(EnvTotalCUPerSecond, "0")

	cfg := LoadFromEnv()

	// Should fall back to defaults
	if cfg.TotalCUPerSecond != DefaultTotalCUPerSecond {
		t.Errorf("TotalCUPerSecond = %d, want %d (default)", cfg.TotalCUPerSecond, DefaultTotalCUPerSecond)
	}
}

func TestLoadFromEnv_ThresholdOutOfRange_FallbackToDefaults(t *testing.T) {
	clearEnvVars(t)

	// Set threshold over 100
	os.Setenv(EnvWarningThreshold, "150")

	cfg := LoadFromEnv()

	// Should fall back to default for warning threshold
	if cfg.WarningThreshold != DefaultWarningThreshold {
		t.Errorf("WarningThreshold = %d, want %d (default)", cfg.WarningThreshold, DefaultWarningThreshold)
	}
}

func TestLoadFromEnv_PartialEnvVars(t *testing.T) {
	clearEnvVars(t)

	// Set only some environment variables
	os.Setenv(EnvTotalCUPerSecond, "1000")
	os.Setenv(EnvReservedCU, "500")
	os.Setenv(EnvSharedCU, "500")
	// Leave others unset

	cfg := LoadFromEnv()

	// Set values should be used
	if cfg.TotalCUPerSecond != 1000 {
		t.Errorf("TotalCUPerSecond = %d, want 1000", cfg.TotalCUPerSecond)
	}
	if cfg.ReservedCU != 500 {
		t.Errorf("ReservedCU = %d, want 500", cfg.ReservedCU)
	}
	if cfg.SharedCU != 500 {
		t.Errorf("SharedCU = %d, want 500", cfg.SharedCU)
	}

	// Unset values should use defaults
	if cfg.WindowSizeMs != DefaultWindowSizeMs {
		t.Errorf("WindowSizeMs = %d, want %d (default)", cfg.WindowSizeMs, DefaultWindowSizeMs)
	}
	if cfg.WarningThreshold != DefaultWarningThreshold {
		t.Errorf("WarningThreshold = %d, want %d (default)", cfg.WarningThreshold, DefaultWarningThreshold)
	}
}

func TestRateLimitConfig_String(t *testing.T) {
	cfg := NewRateLimitConfig()
	str := cfg.String()

	// Verify the string contains key information
	if !configContainsString(str, "TotalCUPerSecond: 500") {
		t.Errorf("String() should contain TotalCUPerSecond, got: %s", str)
	}
	if !configContainsString(str, "ReservedCU: 300") {
		t.Errorf("String() should contain ReservedCU, got: %s", str)
	}
	if !configContainsString(str, "SharedCU: 200") {
		t.Errorf("String() should contain SharedCU, got: %s", str)
	}
}

func TestLoadFromEnv_ZeroReservedCU_Valid(t *testing.T) {
	clearEnvVars(t)

	// Zero reserved CU is valid
	os.Setenv(EnvTotalCUPerSecond, "500")
	os.Setenv(EnvReservedCU, "0")
	os.Setenv(EnvSharedCU, "500")

	cfg := LoadFromEnv()

	if cfg.ReservedCU != 0 {
		t.Errorf("ReservedCU = %d, want 0", cfg.ReservedCU)
	}
	if cfg.SharedCU != 500 {
		t.Errorf("SharedCU = %d, want 500", cfg.SharedCU)
	}
}

func TestLoadFromEnv_ZeroSharedCU_Valid(t *testing.T) {
	clearEnvVars(t)

	// Zero shared CU is valid
	os.Setenv(EnvTotalCUPerSecond, "500")
	os.Setenv(EnvReservedCU, "500")
	os.Setenv(EnvSharedCU, "0")

	cfg := LoadFromEnv()

	if cfg.ReservedCU != 500 {
		t.Errorf("ReservedCU = %d, want 500", cfg.ReservedCU)
	}
	if cfg.SharedCU != 0 {
		t.Errorf("SharedCU = %d, want 0", cfg.SharedCU)
	}
}

// Helper functions

func clearEnvVars(t *testing.T) {
	t.Helper()
	envVars := []string{
		EnvTotalCUPerSecond,
		EnvReservedCU,
		EnvSharedCU,
		EnvWindowSizeMs,
		EnvWarningThreshold,
		EnvPauseThreshold,
		EnvDefaultMethodCost,
	}
	for _, env := range envVars {
		os.Unsetenv(env)
	}
	// Cleanup after test
	t.Cleanup(func() {
		for _, env := range envVars {
			os.Unsetenv(env)
		}
	})
}

func configContainsString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
