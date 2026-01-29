package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/address-scanner/internal/models"
	"github.com/address-scanner/internal/types"
)

// CacheService provides high-level caching operations for the address scanner
type CacheService struct {
	redis *RedisCache
	ttl   time.Duration
}

// NewCacheService creates a new cache service
func NewCacheService(redis *RedisCache, ttl time.Duration) *CacheService {
	return &CacheService{
		redis: redis,
		ttl:   ttl,
	}
}

// CacheKeyType represents different types of cache keys
type CacheKeyType string

const (
	// CacheKeyTransactions is for transaction windows
	CacheKeyTransactions CacheKeyType = "txs"
	// CacheKeyBalance is for address balances
	CacheKeyBalance CacheKeyType = "balance"
	// CacheKeyQuery is for query results
	CacheKeyQuery CacheKeyType = "query"
	// CacheKeyPortfolio is for portfolio data
	CacheKeyPortfolio CacheKeyType = "portfolio"
)

// GenerateCacheKey generates a cache key for a given type and parameters
// Format: <type>:<param1>:<param2>:...
func (c *CacheService) GenerateCacheKey(keyType CacheKeyType, params ...string) string {
	// Normalize all parameters to lowercase for consistency
	normalizedParams := make([]string, len(params))
	for i, param := range params {
		normalizedParams[i] = strings.ToLower(param)
	}
	
	parts := append([]string{string(keyType)}, normalizedParams...)
	return strings.Join(parts, ":")
}

// GenerateTransactionKey generates a cache key for transaction windows
// Format: txs:<address>:<chain>
func (c *CacheService) GenerateTransactionKey(address string, chain types.ChainID) string {
	return c.GenerateCacheKey(CacheKeyTransactions, address, string(chain))
}

// GenerateBalanceKey generates a cache key for address balances
// Format: balance:<address>
func (c *CacheService) GenerateBalanceKey(address string) string {
	return c.GenerateCacheKey(CacheKeyBalance, address)
}

// GenerateQueryKey generates a cache key for query results
// Format: query:<hash-of-query-params>
func (c *CacheService) GenerateQueryKey(queryHash string) string {
	return c.GenerateCacheKey(CacheKeyQuery, queryHash)
}

// GeneratePortfolioKey generates a cache key for portfolio data
// Format: portfolio:<portfolio-id>
func (c *CacheService) GeneratePortfolioKey(portfolioID string) string {
	return c.GenerateCacheKey(CacheKeyPortfolio, portfolioID)
}

// Set stores a value in cache with the configured TTL
func (c *CacheService) Set(ctx context.Context, key string, value interface{}) error {
	return c.SetWithTTL(ctx, key, value, c.ttl)
}

// SetWithTTL stores a value in cache with a custom TTL
func (c *CacheService) SetWithTTL(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	// Serialize value to JSON
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("failed to marshal value: %w", err)
	}
	
	return c.redis.Set(ctx, key, data, ttl)
}

// Get retrieves a value from cache and deserializes it
func (c *CacheService) Get(ctx context.Context, key string, dest interface{}) (bool, error) {
	data, err := c.redis.Get(ctx, key)
	if err != nil {
		// Key not found is not an error, just a cache miss
		if err.Error() == "redis: nil" {
			return false, nil
		}
		return false, fmt.Errorf("failed to get from cache: %w", err)
	}
	
	// Deserialize JSON into destination
	if err := json.Unmarshal([]byte(data), dest); err != nil {
		return false, fmt.Errorf("failed to unmarshal cached value: %w", err)
	}
	
	return true, nil
}

// Invalidate removes one or more keys from cache
func (c *CacheService) Invalidate(ctx context.Context, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	return c.redis.Del(ctx, keys...)
}

// InvalidatePattern removes all keys matching a pattern
// Pattern examples: "txs:0x123*", "portfolio:*"
func (c *CacheService) InvalidatePattern(ctx context.Context, pattern string) error {
	keys, err := c.redis.Keys(ctx, pattern)
	if err != nil {
		return fmt.Errorf("failed to find keys matching pattern: %w", err)
	}
	
	if len(keys) == 0 {
		return nil
	}
	
	return c.redis.Del(ctx, keys...)
}

// InvalidateAddress invalidates all cache entries for an address
func (c *CacheService) InvalidateAddress(ctx context.Context, address string) error {
	address = strings.ToLower(address)
	
	// Invalidate transaction windows for all chains
	pattern := fmt.Sprintf("txs:%s:*", address)
	if err := c.InvalidatePattern(ctx, pattern); err != nil {
		return fmt.Errorf("failed to invalidate transaction cache: %w", err)
	}
	
	// Invalidate balance cache
	balanceKey := c.GenerateBalanceKey(address)
	if err := c.Invalidate(ctx, balanceKey); err != nil {
		return fmt.Errorf("failed to invalidate balance cache: %w", err)
	}
	
	return nil
}

// InvalidatePortfolio invalidates all cache entries for a portfolio
func (c *CacheService) InvalidatePortfolio(ctx context.Context, portfolioID string) error {
	pattern := fmt.Sprintf("portfolio:%s*", strings.ToLower(portfolioID))
	return c.InvalidatePattern(ctx, pattern)
}

// Exists checks if a key exists in cache
func (c *CacheService) Exists(ctx context.Context, key string) (bool, error) {
	return c.redis.Exists(ctx, key)
}

// Refresh updates the TTL on an existing key
func (c *CacheService) Refresh(ctx context.Context, key string) error {
	return c.redis.Expire(ctx, key, c.ttl)
}

// GetTTL returns the configured TTL for this cache service
func (c *CacheService) GetTTL() time.Duration {
	return c.ttl
}

// SetTTL updates the default TTL for this cache service
func (c *CacheService) SetTTL(ttl time.Duration) {
	c.ttl = ttl
}

// CachedTransactionWindow represents a cached transaction window
type CachedTransactionWindow struct {
	Address      string                  `json:"address"`
	Chain        types.ChainID           `json:"chain"`
	Transactions []*models.Transaction   `json:"transactions"`
	CachedAt     time.Time               `json:"cachedAt"`
	TotalCount   int64                   `json:"totalCount"` // Total transactions in DB
}

// CachedBalance represents a cached balance
type CachedBalance struct {
	Address   string                `json:"address"`
	Balance   *types.MultiChainBalance `json:"balance"`
	CachedAt  time.Time             `json:"cachedAt"`
}

// CachedQuery represents a cached query result
type CachedQuery struct {
	QueryHash    string                `json:"queryHash"`
	Transactions []*models.Transaction `json:"transactions"`
	TotalCount   int64                 `json:"totalCount"`
	CachedAt     time.Time             `json:"cachedAt"`
}
