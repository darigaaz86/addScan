// Package config provides configuration management for the address scanner application.
// It loads configuration from environment variables and .env files.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

// Config holds all application configuration
type Config struct {
	Server    ServerConfig
	Database  DatabaseConfig
	Chains    ChainsConfig
	Cache     CacheConfig
	Backfill  BackfillConfig
	Sync      SyncConfig
	RateLimit RateLimitConfig
	Logging   LoggingConfig
	Etherscan EtherscanConfig
}

// EtherscanConfig holds Etherscan API configuration
type EtherscanConfig struct {
	APIKey string
}

// ServerConfig holds server configuration
type ServerConfig struct {
	Port string
	Host string
}

// DatabaseConfig holds database configuration
type DatabaseConfig struct {
	Postgres   PostgresConfig
	ClickHouse ClickHouseConfig
	Redis      RedisConfig
}

// PostgresConfig holds Postgres configuration
type PostgresConfig struct {
	Host           string
	Port           string
	Database       string
	User           string
	Password       string
	MaxConnections int
}

// ClickHouseConfig holds ClickHouse configuration
type ClickHouseConfig struct {
	Host     string
	Port     string
	Database string
	User     string
	Password string
}

// RedisConfig holds Redis configuration
type RedisConfig struct {
	Host           string
	Port           string
	Password       string
	DB             int
	MaxConnections int
}

// ChainsConfig holds chain configuration
type ChainsConfig struct {
	Enabled []string
	Chains  map[string]ChainConfig
}

// ChainConfig holds configuration for a specific chain
type ChainConfig struct {
	RPCPrimary   string
	RPCSecondary string
	PollInterval time.Duration
}

// CacheConfig holds cache configuration
type CacheConfig struct {
	TTL               time.Duration
	TransactionWindow int
}

// BackfillConfig holds backfill configuration
// Note: Limits are hardcoded (free: 1000, paid: unlimited)
type BackfillConfig struct {
	// Reserved for future use
}

// SyncConfig holds sync worker configuration
type SyncConfig struct {
	MaxBlocksPerPoll int // Maximum blocks to process per poll cycle (default: 30)
}

// RateLimitConfig holds rate limiting configuration
type RateLimitConfig struct {
	FreeTier    int
	BasicTier   int
	PremiumTier int
}

// LoggingConfig holds logging configuration
type LoggingConfig struct {
	Level  string
	Format string
}

// LoadConfig loads configuration from .env file and environment variables
func LoadConfig() (*Config, error) {
	// Load .env file (optional in production)
	if err := godotenv.Load(); err != nil {
		// .env file is optional - environment variables can be set directly
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("error loading .env file: %w", err)
		}
	}

	config := &Config{
		Server: ServerConfig{
			Port: getEnv("SERVER_PORT", "8080"),
			Host: getEnv("SERVER_HOST", "0.0.0.0"),
		},
		Database: DatabaseConfig{
			Postgres: PostgresConfig{
				Host:           getEnv("POSTGRES_HOST", "localhost"),
				Port:           getEnv("POSTGRES_PORT", "5432"),
				Database:       getEnv("POSTGRES_DB", "address_scanner"),
				User:           getEnv("POSTGRES_USER", "scanner"),
				Password:       getEnv("POSTGRES_PASSWORD", ""),
				MaxConnections: getEnvAsInt("POSTGRES_MAX_CONNECTIONS", 100),
			},
			ClickHouse: ClickHouseConfig{
				Host:     getEnv("CLICKHOUSE_HOST", "localhost"),
				Port:     getEnv("CLICKHOUSE_PORT", "9000"),
				Database: getEnv("CLICKHOUSE_DB", "address_scanner"),
				User:     getEnv("CLICKHOUSE_USER", "default"),
				Password: getEnv("CLICKHOUSE_PASSWORD", ""),
			},
			Redis: RedisConfig{
				Host:           getEnv("REDIS_HOST", "localhost"),
				Port:           getEnv("REDIS_PORT", "6379"),
				Password:       getEnv("REDIS_PASSWORD", ""),
				DB:             getEnvAsInt("REDIS_DB", 0),
				MaxConnections: getEnvAsInt("REDIS_MAX_CONNECTIONS", 50),
			},
		},
		Cache: CacheConfig{
			TTL:               getEnvAsDuration("CACHE_TTL", 20*time.Second),
			TransactionWindow: getEnvAsInt("CACHE_TRANSACTION_WINDOW", 1000),
		},
		Backfill: BackfillConfig{},
		Sync: SyncConfig{
			MaxBlocksPerPoll: getEnvAsInt("SYNC_MAX_BLOCKS_PER_POLL", 30),
		},
		RateLimit: RateLimitConfig{
			FreeTier:    getEnvAsInt("RATE_LIMIT_FREE_TIER", 1000),
			BasicTier:   getEnvAsInt("RATE_LIMIT_BASIC_TIER", 10000),
			PremiumTier: getEnvAsInt("RATE_LIMIT_PREMIUM_TIER", 100000),
		},
		Logging: LoggingConfig{
			Level:  getEnv("LOG_LEVEL", "info"),
			Format: getEnv("LOG_FORMAT", "json"),
		},
		Etherscan: EtherscanConfig{
			APIKey: getEnv("ETHERSCAN_API_KEY", ""),
		},
	}

	// Load chain configurations
	config.Chains = loadChainConfigs()

	return config, nil
}

// loadChainConfigs loads chain-specific configurations
func loadChainConfigs() ChainsConfig {
	enabledChains := strings.Split(getEnv("ENABLED_CHAINS", "ethereum,polygon,arbitrum,optimism,base"), ",")

	chains := make(map[string]ChainConfig)
	for _, chain := range enabledChains {
		chain = strings.TrimSpace(chain)
		if chain == "" {
			continue
		}

		prefix := strings.ToUpper(chain)
		chains[chain] = ChainConfig{
			RPCPrimary:   getEnv(prefix+"_RPC_PRIMARY", ""),
			RPCSecondary: getEnv(prefix+"_RPC_SECONDARY", ""),
			PollInterval: getEnvAsDuration(prefix+"_POLL_INTERVAL", 15*time.Second),
		}
	}

	return ChainsConfig{
		Enabled: enabledChains,
		Chains:  chains,
	}
}

// getEnv gets an environment variable with a default value
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// getEnvAsInt gets an environment variable as an integer with a default value
func getEnvAsInt(key string, defaultValue int) int {
	valueStr := getEnv(key, "")
	if valueStr == "" {
		return defaultValue
	}

	value, err := strconv.Atoi(valueStr)
	if err != nil {
		return defaultValue
	}
	return value
}

// getEnvAsDuration gets an environment variable as a duration with a default value
func getEnvAsDuration(key string, defaultValue time.Duration) time.Duration {
	valueStr := getEnv(key, "")
	if valueStr == "" {
		return defaultValue
	}

	value, err := time.ParseDuration(valueStr)
	if err != nil {
		return defaultValue
	}
	return value
}
