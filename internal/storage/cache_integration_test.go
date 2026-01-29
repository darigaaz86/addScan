package storage

import (
	"context"
	"testing"
	"time"

	"github.com/address-scanner/internal/config"
	"github.com/address-scanner/internal/types"
)

// TestCacheServiceIntegration tests the cache service with Redis
func TestCacheServiceIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	// Setup Redis
	redisConfig := &config.RedisConfig{
		Host:           "localhost",
		Port:           "6379",
		Password:       "",
		DB:             1, // Use DB 1 for tests
		MaxConnections: 10,
	}

	redis, err := NewRedisCache(redisConfig)
	if err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	defer redis.Close()

	// Create cache service
	cacheService := NewCacheService(redis, 5*time.Second)

	t.Run("cache key generation", func(t *testing.T) {
		key1 := cacheService.GenerateTransactionKey("0xABC", types.ChainEthereum)
		key2 := cacheService.GenerateTransactionKey("0xabc", types.ChainEthereum)

		// Keys should be normalized to lowercase
		if key1 != key2 {
			t.Errorf("Expected normalized keys to match: %s != %s", key1, key2)
		}

		expected := "txs:0xabc:ethereum"
		if key1 != expected {
			t.Errorf("Expected key %s, got %s", expected, key1)
		}
	})

	t.Run("set and get", func(t *testing.T) {
		key := "test:key:1"
		value := map[string]interface{}{
			"address": "0x123",
			"count":   42,
		}

		// Set value
		err := cacheService.Set(ctx, key, value)
		if err != nil {
			t.Fatalf("Failed to set cache: %v", err)
		}

		// Get value
		var retrieved map[string]interface{}
		found, err := cacheService.Get(ctx, key, &retrieved)
		if err != nil {
			t.Fatalf("Failed to get from cache: %v", err)
		}

		if !found {
			t.Fatal("Expected cache hit, got miss")
		}

		if retrieved["address"] != "0x123" {
			t.Errorf("Expected address 0x123, got %v", retrieved["address"])
		}

		// Clean up
		cacheService.Invalidate(ctx, key)
	})

	t.Run("cache miss", func(t *testing.T) {
		key := "test:nonexistent:key"

		var value string
		found, err := cacheService.Get(ctx, key, &value)
		if err != nil {
			t.Fatalf("Unexpected error on cache miss: %v", err)
		}

		if found {
			t.Error("Expected cache miss, got hit")
		}
	})

	t.Run("invalidation", func(t *testing.T) {
		key := "test:invalidate:1"
		value := "test value"

		// Set value
		cacheService.Set(ctx, key, value)

		// Verify it exists
		exists, _ := cacheService.Exists(ctx, key)
		if !exists {
			t.Fatal("Expected key to exist")
		}

		// Invalidate
		err := cacheService.Invalidate(ctx, key)
		if err != nil {
			t.Fatalf("Failed to invalidate: %v", err)
		}

		// Verify it's gone
		exists, _ = cacheService.Exists(ctx, key)
		if exists {
			t.Error("Expected key to be invalidated")
		}
	})

	t.Run("pattern invalidation", func(t *testing.T) {
		// Set multiple keys with same pattern
		keys := []string{
			"test:pattern:1",
			"test:pattern:2",
			"test:pattern:3",
		}

		for _, key := range keys {
			cacheService.Set(ctx, key, "value")
		}

		// Invalidate by pattern
		err := cacheService.InvalidatePattern(ctx, "test:pattern:*")
		if err != nil {
			t.Fatalf("Failed to invalidate pattern: %v", err)
		}

		// Verify all are gone
		for _, key := range keys {
			exists, _ := cacheService.Exists(ctx, key)
			if exists {
				t.Errorf("Expected key %s to be invalidated", key)
			}
		}
	})

	t.Run("TTL expiration", func(t *testing.T) {
		key := "test:ttl:1"
		value := "expires soon"

		// Set with short TTL
		err := cacheService.SetWithTTL(ctx, key, value, 1*time.Second)
		if err != nil {
			t.Fatalf("Failed to set with TTL: %v", err)
		}

		// Should exist immediately
		var retrieved string
		found, _ := cacheService.Get(ctx, key, &retrieved)
		if !found {
			t.Fatal("Expected key to exist")
		}

		// Wait for expiration
		time.Sleep(2 * time.Second)

		// Should be gone
		found, _ = cacheService.Get(ctx, key, &retrieved)
		if found {
			t.Error("Expected key to be expired")
		}
	})
}

// TestTransactionCacheIntegration tests transaction window caching
func TestTransactionCacheIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	// Setup Redis
	redisConfig := &config.RedisConfig{
		Host:           "localhost",
		Port:           "6379",
		Password:       "",
		DB:             1,
		MaxConnections: 10,
	}

	redis, err := NewRedisCache(redisConfig)
	if err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	defer redis.Close()

	// Setup ClickHouse
	clickhouseConfig := &config.ClickHouseConfig{
		Host:     "localhost",
		Port:     "9000",
		Database: "address_scanner",
		User:     "default",
		Password: "",
	}

	clickhouse, err := NewClickHouseDB(clickhouseConfig)
	if err != nil {
		t.Skipf("ClickHouse not available: %v", err)
	}
	defer clickhouse.Close()

	// Create cache components
	cacheService := NewCacheService(redis, 5*time.Second)
	txRepo := NewTransactionRepository(clickhouse)
	txCache := NewTransactionCache(cacheService, txRepo, 1000)

	t.Run("cache window structure", func(t *testing.T) {
		// This tests the structure without requiring actual data
		address := "0x742d35cc6634c0532925a3b844bc9e7595f0beb"
		chain := types.ChainEthereum

		// First call will be a cache miss (no data in DB for test address)
		window, err := txCache.GetTransactionWindow(ctx, address, chain)
		if err != nil {
			t.Fatalf("Failed to get transaction window: %v", err)
		}

		// Verify structure
		if window.Address != address {
			t.Errorf("Expected address %s, got %s", address, window.Address)
		}

		if window.Chain != chain {
			t.Errorf("Expected chain %s, got %s", chain, window.Chain)
		}

		if window.Transactions == nil {
			t.Error("Expected transactions slice to be initialized")
		}

		// Second call should be cached
		window2, err := txCache.GetTransactionWindow(ctx, address, chain)
		if err != nil {
			t.Fatalf("Failed to get cached window: %v", err)
		}

		// Should have same cached timestamp
		if !window.CachedAt.Equal(window2.CachedAt) {
			t.Error("Expected second call to return cached data")
		}
	})

	t.Run("window invalidation", func(t *testing.T) {
		address := "0x123abc"
		chain := types.ChainEthereum

		// Get window (will cache it)
		txCache.GetTransactionWindow(ctx, address, chain)

		// Invalidate
		err := txCache.InvalidateWindow(ctx, address, chain)
		if err != nil {
			t.Fatalf("Failed to invalidate window: %v", err)
		}

		// Next call should fetch fresh data
		window, err := txCache.GetTransactionWindow(ctx, address, chain)
		if err != nil {
			t.Fatalf("Failed to get window after invalidation: %v", err)
		}

		// Should have new cached timestamp
		if time.Since(window.CachedAt) > 1*time.Second {
			t.Error("Expected fresh cache after invalidation")
		}
	})
}

// TestConcurrentCacheIntegration tests concurrent cache access
func TestConcurrentCacheIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	// Setup
	redisConfig := &config.RedisConfig{
		Host:           "localhost",
		Port:           "6379",
		Password:       "",
		DB:             1,
		MaxConnections: 10,
	}

	redis, err := NewRedisCache(redisConfig)
	if err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	defer redis.Close()

	clickhouseConfig := &config.ClickHouseConfig{
		Host:     "localhost",
		Port:     "9000",
		Database: "address_scanner",
		User:     "default",
		Password: "",
	}

	clickhouse, err := NewClickHouseDB(clickhouseConfig)
	if err != nil {
		t.Skipf("ClickHouse not available: %v", err)
	}
	defer clickhouse.Close()

	cacheService := NewCacheService(redis, 5*time.Second)
	txRepo := NewTransactionRepository(clickhouse)
	txCache := NewTransactionCache(cacheService, txRepo, 1000)
	concurrentCache := NewConcurrentTransactionCache(txCache)

	t.Run("concurrent access", func(t *testing.T) {
		address := "0xtest"
		chain := types.ChainEthereum

		// Clear any existing cache
		txCache.InvalidateWindow(ctx, address, chain)

		// Launch multiple concurrent requests
		done := make(chan bool, 10)
		for i := 0; i < 10; i++ {
			go func() {
				_, err := concurrentCache.GetTransactionWindow(ctx, address, chain)
				if err != nil {
					t.Errorf("Concurrent request failed: %v", err)
				}
				done <- true
			}()
		}

		// Wait for all to complete
		for i := 0; i < 10; i++ {
			<-done
		}

		// Check stats
		stats := concurrentCache.GetStats()
		
		// Should have 1 cache miss (first request) and 9 cache hits
		// Or all 10 might share the in-flight request
		if stats.CacheHits+stats.CacheMisses != 10 {
			t.Errorf("Expected 10 total requests, got %d", stats.CacheHits+stats.CacheMisses)
		}
	})

	t.Run("statistics tracking", func(t *testing.T) {
		// Reset stats
		concurrentCache.ResetStats()

		address := "0xstats"
		chain := types.ChainEthereum

		// Clear cache
		txCache.InvalidateWindow(ctx, address, chain)

		// First request - cache miss
		concurrentCache.GetTransactionWindow(ctx, address, chain)

		stats := concurrentCache.GetStats()
		if stats.CacheMisses != 1 {
			t.Errorf("Expected 1 cache miss, got %d", stats.CacheMisses)
		}

		// Second request - cache hit
		concurrentCache.GetTransactionWindow(ctx, address, chain)

		stats = concurrentCache.GetStats()
		if stats.CacheHits != 1 {
			t.Errorf("Expected 1 cache hit, got %d", stats.CacheHits)
		}

		// Verify hit rate calculation
		expectedHitRate := 50.0 // 1 hit out of 2 total
		if stats.HitRate != expectedHitRate {
			t.Errorf("Expected hit rate %.2f%%, got %.2f%%", expectedHitRate, stats.HitRate)
		}
	})
}
