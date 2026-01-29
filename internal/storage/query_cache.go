package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/address-scanner/internal/models"
	"github.com/address-scanner/internal/types"
)

// QueryCache handles caching of complex query results
// This implements cache-miss handling by querying ClickHouse and populating cache
type QueryCache struct {
	cache      *CacheService
	repository *TransactionRepository
}

// NewQueryCache creates a new query cache
func NewQueryCache(cache *CacheService, repository *TransactionRepository) *QueryCache {
	return &QueryCache{
		cache:      cache,
		repository: repository,
	}
}

// QueryParams represents parameters for a transaction query
type QueryParams struct {
	Address   string
	Chains    []types.ChainID
	DateFrom  *time.Time
	DateTo    *time.Time
	MinValue  *float64
	MaxValue  *float64
	Status    *types.TransactionStatus
	SortBy    string
	SortOrder string
	Limit     int
	Offset    int
}

// GenerateQueryHash generates a unique hash for query parameters
// This is used as the cache key for query results
func (qc *QueryCache) GenerateQueryHash(params *QueryParams) (string, error) {
	// Serialize params to JSON for consistent hashing
	data, err := json.Marshal(params)
	if err != nil {
		return "", fmt.Errorf("failed to marshal query params: %w", err)
	}
	
	// Generate SHA256 hash
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:]), nil
}

// ExecuteQuery executes a query with cache-miss handling
// Returns cached results if available, otherwise queries ClickHouse and caches the result
func (qc *QueryCache) ExecuteQuery(ctx context.Context, params *QueryParams) (*QueryResult, error) {
	// Generate query hash for cache key
	queryHash, err := qc.GenerateQueryHash(params)
	if err != nil {
		return nil, fmt.Errorf("failed to generate query hash: %w", err)
	}
	
	cacheKey := qc.cache.GenerateQueryKey(queryHash)
	
	// Try to get from cache
	var cachedQuery CachedQuery
	found, err := qc.cache.Get(ctx, cacheKey, &cachedQuery)
	if err != nil {
		return nil, fmt.Errorf("cache get error: %w", err)
	}
	
	if found {
		// Cache hit - return cached data
		return &QueryResult{
			Transactions: cachedQuery.Transactions,
			TotalCount:   cachedQuery.TotalCount,
			Cached:       true,
			QueryTimeMs:  0, // Cached queries are instant
		}, nil
	}
	
	// Cache miss - query ClickHouse
	return qc.queryAndCache(ctx, params, queryHash, cacheKey)
}

// queryAndCache queries ClickHouse and caches the result
func (qc *QueryCache) queryAndCache(ctx context.Context, params *QueryParams, queryHash, cacheKey string) (*QueryResult, error) {
	startTime := time.Now()
	
	// Convert QueryParams to TransactionFilters
	filters := &TransactionFilters{
		Chains:    params.Chains,
		DateFrom:  params.DateFrom,
		DateTo:    params.DateTo,
		MinValue:  params.MinValue,
		MaxValue:  params.MaxValue,
		Status:    params.Status,
		SortBy:    params.SortBy,
		SortOrder: params.SortOrder,
		Limit:     params.Limit,
		Offset:    params.Offset,
	}
	
	// Query transactions from ClickHouse
	transactions, err := qc.repository.GetByAddress(ctx, params.Address, filters)
	if err != nil {
		return nil, fmt.Errorf("failed to query transactions: %w", err)
	}
	
	// Get total count (without limit/offset)
	countFilters := &TransactionFilters{
		Chains:   params.Chains,
		DateFrom: params.DateFrom,
		DateTo:   params.DateTo,
		MinValue: params.MinValue,
		MaxValue: params.MaxValue,
		Status:   params.Status,
	}
	totalCount, err := qc.repository.CountByAddress(ctx, params.Address, countFilters)
	if err != nil {
		return nil, fmt.Errorf("failed to count transactions: %w", err)
	}
	
	queryTimeMs := time.Since(startTime).Milliseconds()
	
	// Cache the result
	cachedQuery := CachedQuery{
		QueryHash:    queryHash,
		Transactions: transactions,
		TotalCount:   totalCount,
		CachedAt:     time.Now(),
	}
	
	if err := qc.cache.Set(ctx, cacheKey, cachedQuery); err != nil {
		// Log error but don't fail the request
		fmt.Printf("Warning: failed to cache query result: %v\n", err)
	}
	
	return &QueryResult{
		Transactions: transactions,
		TotalCount:   totalCount,
		Cached:       false,
		QueryTimeMs:  queryTimeMs,
	}, nil
}

// ExecuteMultiAddressQuery executes a query across multiple addresses (for portfolios)
// This also implements cache-miss handling
func (qc *QueryCache) ExecuteMultiAddressQuery(ctx context.Context, addresses []string, params *QueryParams) (*QueryResult, error) {
	// For multi-address queries, we generate a different hash
	multiParams := struct {
		Addresses []string
		Params    *QueryParams
	}{
		Addresses: addresses,
		Params:    params,
	}
	
	data, err := json.Marshal(multiParams)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal multi-address params: %w", err)
	}
	
	hash := sha256.Sum256(data)
	queryHash := hex.EncodeToString(hash[:])
	cacheKey := qc.cache.GenerateQueryKey(queryHash)
	
	// Try to get from cache
	var cachedQuery CachedQuery
	found, err := qc.cache.Get(ctx, cacheKey, &cachedQuery)
	if err != nil {
		return nil, fmt.Errorf("cache get error: %w", err)
	}
	
	if found {
		// Cache hit
		return &QueryResult{
			Transactions: cachedQuery.Transactions,
			TotalCount:   cachedQuery.TotalCount,
			Cached:       true,
			QueryTimeMs:  0,
		}, nil
	}
	
	// Cache miss - query ClickHouse
	startTime := time.Now()
	
	filters := &TransactionFilters{
		Chains:    params.Chains,
		DateFrom:  params.DateFrom,
		DateTo:    params.DateTo,
		MinValue:  params.MinValue,
		MaxValue:  params.MaxValue,
		Status:    params.Status,
		SortBy:    params.SortBy,
		SortOrder: params.SortOrder,
		Limit:     params.Limit,
		Offset:    params.Offset,
	}
	
	transactions, err := qc.repository.GetByAddresses(ctx, addresses, filters)
	if err != nil {
		return nil, fmt.Errorf("failed to query transactions: %w", err)
	}
	
	// For multi-address queries, counting is more complex
	// We'll use the length of results as an approximation for now
	totalCount := int64(len(transactions))
	
	queryTimeMs := time.Since(startTime).Milliseconds()
	
	// Cache the result
	cachedQuery = CachedQuery{
		QueryHash:    queryHash,
		Transactions: transactions,
		TotalCount:   totalCount,
		CachedAt:     time.Now(),
	}
	
	if err := qc.cache.Set(ctx, cacheKey, cachedQuery); err != nil {
		fmt.Printf("Warning: failed to cache multi-address query result: %v\n", err)
	}
	
	return &QueryResult{
		Transactions: transactions,
		TotalCount:   totalCount,
		Cached:       false,
		QueryTimeMs:  queryTimeMs,
	}, nil
}

// InvalidateQueryCache invalidates all cached queries for an address
func (qc *QueryCache) InvalidateQueryCache(ctx context.Context, address string) error {
	// Since query hashes include the address, we need to invalidate by pattern
	// This is a simplified approach - in production, you might maintain an index
	pattern := "query:*"
	return qc.cache.InvalidatePattern(ctx, pattern)
}

// InvalidateAllQueries invalidates all cached query results
func (qc *QueryCache) InvalidateAllQueries(ctx context.Context) error {
	pattern := "query:*"
	return qc.cache.InvalidatePattern(ctx, pattern)
}

// QueryResult represents the result of a query execution
type QueryResult struct {
	Transactions []*models.Transaction
	TotalCount   int64
	Cached       bool  // True if served from cache
	QueryTimeMs  int64 // Query execution time (0 for cached)
}

// GetTransactionByHash retrieves a transaction by hash with caching
// This is a special case of cache-miss handling for hash lookups
func (qc *QueryCache) GetTransactionByHash(ctx context.Context, hash string) (*models.Transaction, error) {
	// Generate cache key for this hash
	cacheKey := qc.cache.GenerateCacheKey(CacheKeyType("tx"), hash)
	
	// Try to get from cache
	var tx models.Transaction
	found, err := qc.cache.Get(ctx, cacheKey, &tx)
	if err != nil {
		return nil, fmt.Errorf("cache get error: %w", err)
	}
	
	if found {
		// Cache hit
		return &tx, nil
	}
	
	// Cache miss - query ClickHouse
	transaction, err := qc.repository.GetByHash(ctx, hash)
	if err != nil {
		return nil, fmt.Errorf("failed to get transaction by hash: %w", err)
	}
	
	// Cache the result with a longer TTL (transactions are immutable)
	if err := qc.cache.SetWithTTL(ctx, cacheKey, transaction, 24*time.Hour); err != nil {
		fmt.Printf("Warning: failed to cache transaction: %v\n", err)
	}
	
	return transaction, nil
}
