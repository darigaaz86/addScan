package config

import (
	"os"
	"testing"
	"time"
)

func TestLoadConfig(t *testing.T) {
	// Set some test environment variables
	if err := os.Setenv("SERVER_PORT", "9090"); err != nil {
		t.Fatalf("Failed to set SERVER_PORT: %v", err)
	}
	if err := os.Setenv("POSTGRES_HOST", "testhost"); err != nil {
		t.Fatalf("Failed to set POSTGRES_HOST: %v", err)
	}
	if err := os.Setenv("CACHE_TTL", "30s"); err != nil {
		t.Fatalf("Failed to set CACHE_TTL: %v", err)
	}
	defer func() {
		_ = os.Unsetenv("SERVER_PORT")
		_ = os.Unsetenv("POSTGRES_HOST")
		_ = os.Unsetenv("CACHE_TTL")
	}()

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}

	if cfg.Server.Port != "9090" {
		t.Errorf("Server.Port = %v, want %v", cfg.Server.Port, "9090")
	}

	if cfg.Database.Postgres.Host != "testhost" {
		t.Errorf("Database.Postgres.Host = %v, want %v", cfg.Database.Postgres.Host, "testhost")
	}

	if cfg.Cache.TTL != 30*time.Second {
		t.Errorf("Cache.TTL = %v, want %v", cfg.Cache.TTL, 30*time.Second)
	}
}

func TestGetEnv(t *testing.T) {
	tests := []struct {
		name         string
		key          string
		defaultValue string
		envValue     string
		want         string
	}{
		{
			name:         "returns environment variable when set",
			key:          "TEST_KEY",
			defaultValue: "default",
			envValue:     "custom",
			want:         "custom",
		},
		{
			name:         "returns default when environment variable not set",
			key:          "NONEXISTENT_KEY",
			defaultValue: "default",
			envValue:     "",
			want:         "default",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.envValue != "" {
				if err := os.Setenv(tt.key, tt.envValue); err != nil {
					t.Fatalf("Failed to set env var: %v", err)
				}
				defer func() {
					_ = os.Unsetenv(tt.key)
				}()
			}

			got := getEnv(tt.key, tt.defaultValue)
			if got != tt.want {
				t.Errorf("getEnv() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetEnvAsInt(t *testing.T) {
	tests := []struct {
		name         string
		key          string
		defaultValue int
		envValue     string
		want         int
	}{
		{
			name:         "returns integer when valid",
			key:          "TEST_INT",
			defaultValue: 100,
			envValue:     "200",
			want:         200,
		},
		{
			name:         "returns default when invalid",
			key:          "TEST_INT_INVALID",
			defaultValue: 100,
			envValue:     "invalid",
			want:         100,
		},
		{
			name:         "returns default when not set",
			key:          "TEST_INT_NOTSET",
			defaultValue: 100,
			envValue:     "",
			want:         100,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.envValue != "" {
				if err := os.Setenv(tt.key, tt.envValue); err != nil {
					t.Fatalf("Failed to set env var: %v", err)
				}
				defer func() {
					_ = os.Unsetenv(tt.key)
				}()
			}

			got := getEnvAsInt(tt.key, tt.defaultValue)
			if got != tt.want {
				t.Errorf("getEnvAsInt() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetEnvAsDuration(t *testing.T) {
	tests := []struct {
		name         string
		key          string
		defaultValue time.Duration
		envValue     string
		want         time.Duration
	}{
		{
			name:         "returns duration when valid",
			key:          "TEST_DURATION",
			defaultValue: 10 * time.Second,
			envValue:     "30s",
			want:         30 * time.Second,
		},
		{
			name:         "returns default when invalid",
			key:          "TEST_DURATION_INVALID",
			defaultValue: 10 * time.Second,
			envValue:     "invalid",
			want:         10 * time.Second,
		},
		{
			name:         "returns default when not set",
			key:          "TEST_DURATION_NOTSET",
			defaultValue: 10 * time.Second,
			envValue:     "",
			want:         10 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.envValue != "" {
				if err := os.Setenv(tt.key, tt.envValue); err != nil {
					t.Fatalf("Failed to set env var: %v", err)
				}
				defer func() {
					_ = os.Unsetenv(tt.key)
				}()
			}

			got := getEnvAsDuration(tt.key, tt.defaultValue)
			if got != tt.want {
				t.Errorf("getEnvAsDuration() = %v, want %v", got, tt.want)
			}
		})
	}
}
