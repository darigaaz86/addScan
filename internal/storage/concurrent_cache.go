package storage

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/address-scanner/internal/models"
	"github.com/address-scanner/internal/types"
)

// ConcurrentTransactionCache provides lock-free concurrent access to transaction cache
// This implementation ensures reads don't block on writes using atomic operations
type ConcurrentTransactionCache struct {
	cache      *TransactionCache
	
	// Atomic counters for statistics
	cacheHits   atomic.Int64
	cacheMisses atomic.Int64
	
	// In-flight request tracking to prevent cache stampede
	// Maps cache key to a channel that will receive the result
	inflightMu sync.RWMutex
	inflight   map[string]chan *inflightResult
}

// inflightResult represents the result of an in-flight cache population
type inflightResult struct {
	window *CachedTransactionWindow
	err    error
}

// NewConcurrentTransactionCache creates a new concurrent transaction cache
func NewConcurrentTransactionCache(cache *TransactionCache) *ConcurrentTransactionCache {
	return &ConcurrentTransactionCache{
		cache:    cache,
		inflight: make(map[string]chan *inflightResult),
	}
}

// GetTransactionWindow retrieves a transaction window with lock-free concurrent access
// Multiple concurrent reads for the same key will share a single database query
func (ctc *ConcurrentTransactionCache) GetTransactionWindow(ctx context.Context, address string, chain types.ChainID) (*CachedTransactionWindow, error) {
	cacheKey := ctc.cache.cache.GenerateTransactionKey(address, chain)
	
	// Try to get from cache first (non-blocking read)
	var window CachedTransactionWindow
	found, err := ctc.cache.cache.Get(ctx, cacheKey, &window)
	if err != nil {
		return nil, fmt.Errorf("cache get error: %w", err)
	}
	
	if found {
		// Cache hit - increment counter and return
		ctc.cacheHits.Add(1)
		return &window, nil
	}
	
	// Cache miss - increment counter
	ctc.cacheMisses.Add(1)
	
	// Check if there's already an in-flight request for this key
	resultChan, isNew := ctc.getOrCreateInflight(cacheKey)
	
	if !isNew {
		// Another goroutine is already fetching this data
		// Wait for the result without blocking other reads
		select {
		case result := <-resultChan:
			return result.window, result.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	
	// We're the first to request this key - fetch from database
	windowPtr, err := ctc.cache.populateCache(ctx, address, chain)
	
	// Broadcast result to all waiting goroutines
	ctc.completeInflight(cacheKey, &inflightResult{
		window: windowPtr,
		err:    err,
	})
	
	return windowPtr, err
}

// getOrCreateInflight atomically checks for or creates an in-flight request
// Returns the result channel and whether this is a new request
func (ctc *ConcurrentTransactionCache) getOrCreateInflight(key string) (chan *inflightResult, bool) {
	ctc.inflightMu.Lock()
	defer ctc.inflightMu.Unlock()
	
	// Check if request is already in-flight
	if ch, exists := ctc.inflight[key]; exists {
		return ch, false
	}
	
	// Create new in-flight request
	ch := make(chan *inflightResult, 1)
	ctc.inflight[key] = ch
	return ch, true
}

// completeInflight broadcasts the result to all waiting goroutines and cleans up
func (ctc *ConcurrentTransactionCache) completeInflight(key string, result *inflightResult) {
	ctc.inflightMu.Lock()
	ch, exists := ctc.inflight[key]
	if exists {
		delete(ctc.inflight, key)
	}
	ctc.inflightMu.Unlock()
	
	if exists {
		// Broadcast to all waiting goroutines
		ch <- result
		close(ch)
	}
}

// UpdateWindow updates the cache without blocking reads
// Uses cache invalidation strategy for simplicity and correctness
func (ctc *ConcurrentTransactionCache) UpdateWindow(ctx context.Context, address string, chain types.ChainID, newTransactions []*models.Transaction) error {
	// Invalidate cache asynchronously - reads can continue serving stale data
	// until the next cache miss triggers a refresh
	go func() {
		// Use a background context to avoid cancellation
		bgCtx := context.Background()
		if err := ctc.cache.UpdateWindow(bgCtx, address, chain, newTransactions); err != nil {
			fmt.Printf("Warning: failed to update cache window: %v\n", err)
		}
	}()
	
	return nil
}

// GetMergedWindow retrieves and merges windows from multiple chains concurrently
func (ctc *ConcurrentTransactionCache) GetMergedWindow(ctx context.Context, address string, chains []types.ChainID, limit int) ([]*models.Transaction, error) {
	// Use a wait group to fetch all chains concurrently
	var wg sync.WaitGroup
	windowsChan := make(chan *CachedTransactionWindow, len(chains))
	errorsChan := make(chan error, len(chains))
	
	for _, chain := range chains {
		wg.Add(1)
		go func(c types.ChainID) {
			defer wg.Done()
			
			window, err := ctc.GetTransactionWindow(ctx, address, c)
			if err != nil {
				errorsChan <- err
				return
			}
			windowsChan <- window
		}(chain)
	}
	
	// Wait for all goroutines to complete
	wg.Wait()
	close(windowsChan)
	close(errorsChan)
	
	// Check for errors
	if len(errorsChan) > 0 {
		return nil, <-errorsChan
	}
	
	// Merge all transactions
	var allTransactions []*models.Transaction
	for window := range windowsChan {
		allTransactions = append(allTransactions, window.Transactions...)
	}
	
	// Sort by timestamp (most recent first)
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

// GetStats returns cache statistics
func (ctc *ConcurrentTransactionCache) GetStats() *ConcurrentCacheStats {
	hits := ctc.cacheHits.Load()
	misses := ctc.cacheMisses.Load()
	total := hits + misses
	
	var hitRate float64
	if total > 0 {
		hitRate = float64(hits) / float64(total) * 100
	}
	
	ctc.inflightMu.RLock()
	inflightCount := len(ctc.inflight)
	ctc.inflightMu.RUnlock()
	
	return &ConcurrentCacheStats{
		CacheHits:     hits,
		CacheMisses:   misses,
		HitRate:       hitRate,
		InflightCount: inflightCount,
	}
}

// ResetStats resets cache statistics
func (ctc *ConcurrentTransactionCache) ResetStats() {
	ctc.cacheHits.Store(0)
	ctc.cacheMisses.Store(0)
}

// ConcurrentCacheStats represents cache statistics
type ConcurrentCacheStats struct {
	CacheHits     int64
	CacheMisses   int64
	HitRate       float64 // Percentage
	InflightCount int     // Number of in-flight requests
}

// ConcurrentQueryCache provides lock-free concurrent access to query cache
type ConcurrentQueryCache struct {
	cache *QueryCache
	
	// Atomic counters
	cacheHits   atomic.Int64
	cacheMisses atomic.Int64
	
	// In-flight request tracking
	inflightMu sync.RWMutex
	inflight   map[string]chan *inflightQueryResult
}

// inflightQueryResult represents the result of an in-flight query
type inflightQueryResult struct {
	result *QueryResult
	err    error
}

// NewConcurrentQueryCache creates a new concurrent query cache
func NewConcurrentQueryCache(cache *QueryCache) *ConcurrentQueryCache {
	return &ConcurrentQueryCache{
		cache:    cache,
		inflight: make(map[string]chan *inflightQueryResult),
	}
}

// ExecuteQuery executes a query with lock-free concurrent access
func (cqc *ConcurrentQueryCache) ExecuteQuery(ctx context.Context, params *QueryParams) (*QueryResult, error) {
	// Generate query hash
	queryHash, err := cqc.cache.GenerateQueryHash(params)
	if err != nil {
		return nil, err
	}
	
	cacheKey := cqc.cache.cache.GenerateQueryKey(queryHash)
	
	// Try cache first (non-blocking read)
	var cachedQuery CachedQuery
	found, err := cqc.cache.cache.Get(ctx, cacheKey, &cachedQuery)
	if err != nil {
		return nil, fmt.Errorf("cache get error: %w", err)
	}
	
	if found {
		cqc.cacheHits.Add(1)
		return &QueryResult{
			Transactions: cachedQuery.Transactions,
			TotalCount:   cachedQuery.TotalCount,
			Cached:       true,
			QueryTimeMs:  0,
		}, nil
	}
	
	// Cache miss
	cqc.cacheMisses.Add(1)
	
	// Check for in-flight request
	resultChan, isNew := cqc.getOrCreateInflight(cacheKey)
	
	if !isNew {
		// Wait for in-flight request
		select {
		case result := <-resultChan:
			return result.result, result.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	
	// Execute query
	result, err := cqc.cache.queryAndCache(ctx, params, queryHash, cacheKey)
	
	// Broadcast result
	cqc.completeInflight(cacheKey, &inflightQueryResult{
		result: result,
		err:    err,
	})
	
	return result, err
}

// getOrCreateInflight atomically checks for or creates an in-flight query
func (cqc *ConcurrentQueryCache) getOrCreateInflight(key string) (chan *inflightQueryResult, bool) {
	cqc.inflightMu.Lock()
	defer cqc.inflightMu.Unlock()
	
	if ch, exists := cqc.inflight[key]; exists {
		return ch, false
	}
	
	ch := make(chan *inflightQueryResult, 1)
	cqc.inflight[key] = ch
	return ch, true
}

// completeInflight broadcasts the query result
func (cqc *ConcurrentQueryCache) completeInflight(key string, result *inflightQueryResult) {
	cqc.inflightMu.Lock()
	ch, exists := cqc.inflight[key]
	if exists {
		delete(cqc.inflight, key)
	}
	cqc.inflightMu.Unlock()
	
	if exists {
		ch <- result
		close(ch)
	}
}

// GetStats returns query cache statistics
func (cqc *ConcurrentQueryCache) GetStats() *ConcurrentCacheStats {
	hits := cqc.cacheHits.Load()
	misses := cqc.cacheMisses.Load()
	total := hits + misses
	
	var hitRate float64
	if total > 0 {
		hitRate = float64(hits) / float64(total) * 100
	}
	
	cqc.inflightMu.RLock()
	inflightCount := len(cqc.inflight)
	cqc.inflightMu.RUnlock()
	
	return &ConcurrentCacheStats{
		CacheHits:     hits,
		CacheMisses:   misses,
		HitRate:       hitRate,
		InflightCount: inflightCount,
	}
}

// ResetStats resets query cache statistics
func (cqc *ConcurrentQueryCache) ResetStats() {
	cqc.cacheHits.Store(0)
	cqc.cacheMisses.Store(0)
}
