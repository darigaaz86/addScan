package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/address-scanner/internal/config"
	"github.com/address-scanner/internal/types"
	"github.com/redis/go-redis/v9"
)

// RedisCache wraps the Redis client
type RedisCache struct {
	client *redis.Client
}

// NewRedisCache creates a new Redis cache connection
func NewRedisCache(cfg *config.RedisConfig) (*RedisCache, error) {
	client := redis.NewClient(&redis.Options{
		Addr:         fmt.Sprintf("%s:%s", cfg.Host, cfg.Port),
		Password:     cfg.Password,
		DB:           cfg.DB,
		PoolSize:     cfg.MaxConnections,
		MinIdleConns: 5,
		MaxRetries:   3,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		PoolTimeout:  4 * time.Second,
	})

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	return &RedisCache{client: client}, nil
}

// Close closes the Redis connection
func (r *RedisCache) Close() error {
	if r.client != nil {
		return r.client.Close()
	}
	return nil
}

// Client returns the underlying Redis client
func (r *RedisCache) Client() *redis.Client {
	return r.client
}

// Ping checks if Redis is reachable
func (r *RedisCache) Ping(ctx context.Context) error {
	return r.client.Ping(ctx).Err()
}

// Set sets a key-value pair with TTL
func (r *RedisCache) Set(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	return r.client.Set(ctx, key, value, ttl).Err()
}

// Get retrieves a value by key
func (r *RedisCache) Get(ctx context.Context, key string) (string, error) {
	return r.client.Get(ctx, key).Result()
}

// Del deletes one or more keys
func (r *RedisCache) Del(ctx context.Context, keys ...string) error {
	return r.client.Del(ctx, keys...).Err()
}

// Exists checks if a key exists
func (r *RedisCache) Exists(ctx context.Context, key string) (bool, error) {
	count, err := r.client.Exists(ctx, key).Result()
	return count > 0, err
}

// Expire sets a TTL on an existing key
func (r *RedisCache) Expire(ctx context.Context, key string, ttl time.Duration) error {
	return r.client.Expire(ctx, key, ttl).Err()
}

// Keys returns all keys matching a pattern
func (r *RedisCache) Keys(ctx context.Context, pattern string) ([]string, error) {
	return r.client.Keys(ctx, pattern).Result()
}

// UpdateTransactions updates the cached transaction window for an address
// This is called by sync workers when new transactions are detected
func (r *RedisCache) UpdateTransactions(ctx context.Context, address string, chain types.ChainID, newTransactions []*types.NormalizedTransaction) error {
	if len(newTransactions) == 0 {
		return nil
	}

	// Generate cache key for this address and chain
	cacheKey := fmt.Sprintf("txs:%s:%s", address, chain)

	// For now, we'll invalidate the cache and let it be repopulated on next query
	// This is simpler than trying to merge transactions while maintaining the 1000-tx window
	// In a production system, we could implement a more sophisticated merge strategy
	return r.Del(ctx, cacheKey)
}
