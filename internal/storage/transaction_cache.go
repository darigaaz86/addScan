package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/address-scanner/internal/models"
	"github.com/address-scanner/internal/types"
)

// TransactionCache handles caching of transaction windows
type TransactionCache struct {
	cache      *CacheService
	repository *TransactionRepository
	windowSize int
}

// NewTransactionCache creates a new transaction cache
func NewTransactionCache(cache *CacheService, repository *TransactionRepository, windowSize int) *TransactionCache {
	return &TransactionCache{
		cache:      cache,
		repository: repository,
		windowSize: windowSize,
	}
}

// GetTransactionWindow retrieves the most recent N transactions for an address on a chain
// Returns cached data if available, otherwise fetches from ClickHouse and caches
func (tc *TransactionCache) GetTransactionWindow(ctx context.Context, address string, chain types.ChainID) (*CachedTransactionWindow, error) {
	// Generate cache key
	cacheKey := tc.cache.GenerateTransactionKey(address, chain)
	
	// Try to get from cache
	var window CachedTransactionWindow
	found, err := tc.cache.Get(ctx, cacheKey, &window)
	if err != nil {
		return nil, fmt.Errorf("cache get error: %w", err)
	}
	
	if found {
		// Cache hit - return cached data
		return &window, nil
	}
	
	// Cache miss - fetch from database
	return tc.populateCache(ctx, address, chain)
}

// populateCache fetches transactions from database and populates the cache
func (tc *TransactionCache) populateCache(ctx context.Context, address string, chain types.ChainID) (*CachedTransactionWindow, error) {
	// Fetch the most recent windowSize transactions for this address and chain
	filters := &TransactionFilters{
		Chains:    []types.ChainID{chain},
		Limit:     tc.windowSize,
		SortBy:    "timestamp",
		SortOrder: "desc",
	}
	
	transactions, err := tc.repository.GetByAddress(ctx, address, filters)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch transactions: %w", err)
	}
	
	// Get total count for this address and chain
	totalCount, err := tc.repository.CountByAddressAndChain(ctx, address, chain)
	if err != nil {
		return nil, fmt.Errorf("failed to count transactions: %w", err)
	}
	
	// Create window
	window := &CachedTransactionWindow{
		Address:      address,
		Chain:        chain,
		Transactions: transactions,
		CachedAt:     time.Now(),
		TotalCount:   totalCount,
	}
	
	// Store in cache
	cacheKey := tc.cache.GenerateTransactionKey(address, chain)
	if err := tc.cache.Set(ctx, cacheKey, window); err != nil {
		// Log error but don't fail the request
		// The data is still valid, just not cached
		fmt.Printf("Warning: failed to cache transaction window: %v\n", err)
	}
	
	return window, nil
}

// UpdateWindow updates the cached transaction window with new transactions
// This implements a sliding window update strategy
func (tc *TransactionCache) UpdateWindow(ctx context.Context, address string, chain types.ChainID, newTransactions []*models.Transaction) error {
	if len(newTransactions) == 0 {
		return nil
	}
	
	// For simplicity and correctness, we invalidate the cache
	// The next query will repopulate it with the latest data
	// This ensures the window always contains the most recent 1000 transactions
	cacheKey := tc.cache.GenerateTransactionKey(address, chain)
	return tc.cache.Invalidate(ctx, cacheKey)
}

// GetMultiChainWindow retrieves transaction windows for an address across multiple chains
func (tc *TransactionCache) GetMultiChainWindow(ctx context.Context, address string, chains []types.ChainID) (map[types.ChainID]*CachedTransactionWindow, error) {
	windows := make(map[types.ChainID]*CachedTransactionWindow)
	
	for _, chain := range chains {
		window, err := tc.GetTransactionWindow(ctx, address, chain)
		if err != nil {
			return nil, fmt.Errorf("failed to get window for chain %s: %w", chain, err)
		}
		windows[chain] = window
	}
	
	return windows, nil
}

// GetMergedWindow retrieves and merges transaction windows from multiple chains
// Returns transactions sorted by timestamp (most recent first)
func (tc *TransactionCache) GetMergedWindow(ctx context.Context, address string, chains []types.ChainID, limit int) ([]*models.Transaction, error) {
	// Get windows for all chains
	windows, err := tc.GetMultiChainWindow(ctx, address, chains)
	if err != nil {
		return nil, err
	}
	
	// Merge all transactions
	var allTransactions []*models.Transaction
	for _, window := range windows {
		allTransactions = append(allTransactions, window.Transactions...)
	}
	
	// Sort by timestamp (most recent first)
	// Using a simple bubble sort for clarity - can be optimized with sort.Slice
	for i := 0; i < len(allTransactions)-1; i++ {
		for j := i + 1; j < len(allTransactions); j++ {
			if allTransactions[i].Timestamp.Before(allTransactions[j].Timestamp) {
				allTransactions[i], allTransactions[j] = allTransactions[j], allTransactions[i]
			}
		}
	}
	
	// Apply limit
	if limit > 0 && limit < len(allTransactions) {
		allTransactions = allTransactions[:limit]
	}
	
	return allTransactions, nil
}

// InvalidateWindow invalidates the cached transaction window for an address and chain
func (tc *TransactionCache) InvalidateWindow(ctx context.Context, address string, chain types.ChainID) error {
	cacheKey := tc.cache.GenerateTransactionKey(address, chain)
	return tc.cache.Invalidate(ctx, cacheKey)
}

// InvalidateAllWindows invalidates all cached transaction windows for an address
func (tc *TransactionCache) InvalidateAllWindows(ctx context.Context, address string) error {
	return tc.cache.InvalidateAddress(ctx, address)
}

// GetWindowSize returns the configured window size
func (tc *TransactionCache) GetWindowSize() int {
	return tc.windowSize
}

// RefreshWindow forces a refresh of the cached window by fetching latest data
func (tc *TransactionCache) RefreshWindow(ctx context.Context, address string, chain types.ChainID) (*CachedTransactionWindow, error) {
	// Invalidate existing cache
	if err := tc.InvalidateWindow(ctx, address, chain); err != nil {
		return nil, fmt.Errorf("failed to invalidate cache: %w", err)
	}
	
	// Fetch and cache fresh data
	return tc.populateCache(ctx, address, chain)
}

// WarmCache pre-populates the cache for a list of addresses
// Useful for batch operations or after system startup
func (tc *TransactionCache) WarmCache(ctx context.Context, addresses []string, chains []types.ChainID) error {
	for _, address := range addresses {
		for _, chain := range chains {
			_, err := tc.GetTransactionWindow(ctx, address, chain)
			if err != nil {
				// Log error but continue with other addresses
				fmt.Printf("Warning: failed to warm cache for %s on %s: %v\n", address, chain, err)
			}
		}
	}
	return nil
}

// GetCacheStats returns statistics about cached transaction windows
type CacheStats struct {
	TotalWindows   int
	TotalCacheHits int64
	TotalCacheMiss int64
	AverageSize    int
}

// Note: Actual cache hit/miss tracking would require additional instrumentation
// This is a placeholder for future implementation
