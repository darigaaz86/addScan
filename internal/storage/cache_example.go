package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/address-scanner/internal/config"
	"github.com/address-scanner/internal/models"
	"github.com/address-scanner/internal/types"
)

// CacheManager provides a unified interface to all caching components
// This demonstrates how to wire together all the caching layers
type CacheManager struct {
	// Core components
	redis              *RedisCache
	cacheService       *CacheService
	
	// Transaction caching
	transactionCache   *TransactionCache
	concurrentTxCache  *ConcurrentTransactionCache
	
	// Query caching
	queryCache         *QueryCache
	concurrentQryCache *ConcurrentQueryCache
	
	// Configuration
	ttl                time.Duration
	windowSize         int
}

// NewCacheManager creates a new cache manager with all components initialized
func NewCacheManager(
	redisConfig *config.RedisConfig,
	clickhouseDB *ClickHouseDB,
	ttl time.Duration,
	windowSize int,
) (*CacheManager, error) {
	// Initialize Redis connection
	redis, err := NewRedisCache(redisConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create Redis cache: %w", err)
	}
	
	// Create cache service
	cacheService := NewCacheService(redis, ttl)
	
	// Create transaction repository
	txRepo := NewTransactionRepository(clickhouseDB)
	
	// Create transaction cache
	txCache := NewTransactionCache(cacheService, txRepo, windowSize)
	concurrentTxCache := NewConcurrentTransactionCache(txCache)
	
	// Create query cache
	qryCache := NewQueryCache(cacheService, txRepo)
	concurrentQryCache := NewConcurrentQueryCache(qryCache)
	
	return &CacheManager{
		redis:              redis,
		cacheService:       cacheService,
		transactionCache:   txCache,
		concurrentTxCache:  concurrentTxCache,
		queryCache:         qryCache,
		concurrentQryCache: concurrentQryCache,
		ttl:                ttl,
		windowSize:         windowSize,
	}, nil
}

// GetTransactionWindow retrieves a transaction window with full concurrency support
func (cm *CacheManager) GetTransactionWindow(ctx context.Context, address string, chain types.ChainID) (*CachedTransactionWindow, error) {
	return cm.concurrentTxCache.GetTransactionWindow(ctx, address, chain)
}

// GetMergedWindow retrieves and merges windows from multiple chains
func (cm *CacheManager) GetMergedWindow(ctx context.Context, address string, chains []types.ChainID, limit int) ([]*models.Transaction, error) {
	return cm.concurrentTxCache.GetMergedWindow(ctx, address, chains, limit)
}

// ExecuteQuery executes a query with caching and concurrency support
func (cm *CacheManager) ExecuteQuery(ctx context.Context, params *QueryParams) (*QueryResult, error) {
	return cm.concurrentQryCache.ExecuteQuery(ctx, params)
}

// InvalidateAddress invalidates all cache entries for an address
func (cm *CacheManager) InvalidateAddress(ctx context.Context, address string) error {
	return cm.cacheService.InvalidateAddress(ctx, address)
}

// InvalidatePortfolio invalidates all cache entries for a portfolio
func (cm *CacheManager) InvalidatePortfolio(ctx context.Context, portfolioID string) error {
	return cm.cacheService.InvalidatePortfolio(ctx, portfolioID)
}

// GetStats returns comprehensive cache statistics
func (cm *CacheManager) GetStats() *CacheManagerStats {
	txStats := cm.concurrentTxCache.GetStats()
	qryStats := cm.concurrentQryCache.GetStats()
	
	return &CacheManagerStats{
		TransactionCache: txStats,
		QueryCache:       qryStats,
		TTL:              cm.ttl,
		WindowSize:       cm.windowSize,
	}
}

// Close closes all cache connections
func (cm *CacheManager) Close() error {
	return cm.redis.Close()
}

// CacheManagerStats represents comprehensive cache statistics
type CacheManagerStats struct {
	TransactionCache *ConcurrentCacheStats
	QueryCache       *ConcurrentCacheStats
	TTL              time.Duration
	WindowSize       int
}

// ExampleUsage demonstrates how to use the cache manager
func ExampleUsage() {
	// This is a demonstration of how to use the caching layer
	// In production, this would be called from your service layer
	
	ctx := context.Background()
	
	// Configuration
	redisConfig := &config.RedisConfig{
		Host:           "localhost",
		Port:           "6379",
		Password:       "",
		DB:             0,
		MaxConnections: 50,
	}
	
	clickhouseConfig := &config.ClickHouseConfig{
		Host:     "localhost",
		Port:     "9000",
		Database: "address_scanner",
		User:     "default",
		Password: "",
	}
	
	// Initialize ClickHouse
	clickhouseDB, err := NewClickHouseDB(clickhouseConfig)
	if err != nil {
		fmt.Printf("Failed to connect to ClickHouse: %v\n", err)
		return
	}
	defer clickhouseDB.Close()
	
	// Create cache manager
	cacheManager, err := NewCacheManager(
		redisConfig,
		clickhouseDB,
		20*time.Second, // TTL
		1000,           // Window size
	)
	if err != nil {
		fmt.Printf("Failed to create cache manager: %v\n", err)
		return
	}
	defer cacheManager.Close()
	
	// Example 1: Get transaction window for a single chain
	address := "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb"
	chain := types.ChainEthereum
	
	window, err := cacheManager.GetTransactionWindow(ctx, address, chain)
	if err != nil {
		fmt.Printf("Failed to get transaction window: %v\n", err)
		return
	}
	
	fmt.Printf("Retrieved %d transactions for %s on %s\n", 
		len(window.Transactions), address, chain)
	fmt.Printf("Total transactions in DB: %d\n", window.TotalCount)
	
	// Example 2: Get merged window across multiple chains
	chains := []types.ChainID{
		types.ChainEthereum,
		types.ChainPolygon,
		types.ChainArbitrum,
	}
	
	mergedTxs, err := cacheManager.GetMergedWindow(ctx, address, chains, 100)
	if err != nil {
		fmt.Printf("Failed to get merged window: %v\n", err)
		return
	}
	
	fmt.Printf("Retrieved %d merged transactions across %d chains\n", 
		len(mergedTxs), len(chains))
	
	// Example 3: Execute a filtered query
	dateFrom := time.Now().AddDate(0, -1, 0) // Last month
	minValue := 1000.0
	
	queryParams := &QueryParams{
		Address:   address,
		Chains:    chains,
		DateFrom:  &dateFrom,
		MinValue:  &minValue,
		SortBy:    "value",
		SortOrder: "desc",
		Limit:     50,
		Offset:    0,
	}
	
	result, err := cacheManager.ExecuteQuery(ctx, queryParams)
	if err != nil {
		fmt.Printf("Failed to execute query: %v\n", err)
		return
	}
	
	fmt.Printf("Query returned %d transactions (cached: %v, query time: %dms)\n",
		len(result.Transactions), result.Cached, result.QueryTimeMs)
	
	// Example 4: Get cache statistics
	stats := cacheManager.GetStats()
	fmt.Printf("\nCache Statistics:\n")
	fmt.Printf("Transaction Cache - Hits: %d, Misses: %d, Hit Rate: %.2f%%\n",
		stats.TransactionCache.CacheHits,
		stats.TransactionCache.CacheMisses,
		stats.TransactionCache.HitRate)
	fmt.Printf("Query Cache - Hits: %d, Misses: %d, Hit Rate: %.2f%%\n",
		stats.QueryCache.CacheHits,
		stats.QueryCache.CacheMisses,
		stats.QueryCache.HitRate)
	
	// Example 5: Invalidate cache when new transactions arrive
	// This would typically be called by sync workers
	if err := cacheManager.InvalidateAddress(ctx, address); err != nil {
		fmt.Printf("Failed to invalidate cache: %v\n", err)
		return
	}
	
	fmt.Println("\nCache invalidated successfully")
}
