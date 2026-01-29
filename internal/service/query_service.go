package service

import (
	"context"
	"fmt"
	"time"

	"github.com/address-scanner/internal/models"
	"github.com/address-scanner/internal/storage"
	"github.com/address-scanner/internal/types"
)

// TransactionQuerier defines the interface for querying transactions
type TransactionQuerier interface {
	GetByAddress(ctx context.Context, address string, filters *storage.TransactionFilters) ([]*models.Transaction, error)
	CountByAddress(ctx context.Context, address string, filters *storage.TransactionFilters) (int64, error)
	GetByHash(ctx context.Context, hash string) (*models.Transaction, error)
}

// QueryService handles transaction queries with filtering, sorting, and pagination
// Requirements: 14.1, 14.2, 14.3, 14.4, 14.7, 9.1, 9.2, 9.3
type QueryService struct {
	transactionRepo     TransactionQuerier
	unifiedTimelineRepo *storage.UnifiedTimelineRepository
	cacheService        *storage.CacheService
	cacheWindowSize     int                 // Number of transactions to cache (default: 1000)
	perfMonitor         *PerformanceMonitor // Performance monitoring
}

// NewQueryService creates a new query service
func NewQueryService(
	transactionRepo TransactionQuerier,
	unifiedTimelineRepo *storage.UnifiedTimelineRepository,
	cacheService *storage.CacheService,
) *QueryService {
	return &QueryService{
		transactionRepo:     transactionRepo,
		unifiedTimelineRepo: unifiedTimelineRepo,
		cacheService:        cacheService,
		cacheWindowSize:     1000, // Default cache window size
		perfMonitor:         NewPerformanceMonitor(),
	}
}

// QueryInput defines input parameters for transaction queries
type QueryInput struct {
	Address   string                   `json:"address"`
	Chains    []types.ChainID          `json:"chains,omitempty"`
	DateFrom  *time.Time               `json:"dateFrom,omitempty"`
	DateTo    *time.Time               `json:"dateTo,omitempty"`
	MinValue  *float64                 `json:"minValue,omitempty"`
	MaxValue  *float64                 `json:"maxValue,omitempty"`
	Status    *types.TransactionStatus `json:"status,omitempty"`
	SortBy    string                   `json:"sortBy,omitempty"`    // timestamp, value, block_number
	SortOrder string                   `json:"sortOrder,omitempty"` // asc, desc
	Limit     int                      `json:"limit,omitempty"`     // Default: 50, Max: 1000
	Offset    int                      `json:"offset,omitempty"`    // Default: 0
}

// QueryResult represents the result of a transaction query
type QueryResult struct {
	Address      string                         `json:"address"`
	Transactions []*types.NormalizedTransaction `json:"transactions"`
	Pagination   PaginationInfo                 `json:"pagination"`
	Cached       bool                           `json:"cached"`
	QueryTimeMs  int64                          `json:"queryTimeMs"`
}

// PaginationInfo contains pagination metadata
type PaginationInfo struct {
	Total   int64 `json:"total"`
	Limit   int   `json:"limit"`
	Offset  int   `json:"offset"`
	HasMore bool  `json:"hasMore"`
}

// Query executes a transaction query with filtering, sorting, and pagination
// Requirement 14.1: Date range filtering
// Requirement 14.2: Value filtering
// Requirement 14.3: Value filtering with min/max
// Requirement 14.4: Sorting by timestamp/value
// Requirement 14.7: Route to cache for simple queries, ClickHouse for complex filters
// Requirement 5.1: Return first 50 transactions within 100ms (progressive loading)
// Requirement 5.2: Support lazy loading for additional data
func (s *QueryService) Query(ctx context.Context, input *QueryInput) (*QueryResult, error) {
	startTime := time.Now()

	// Validate input
	if err := s.validateQueryInput(input); err != nil {
		return nil, fmt.Errorf("invalid query input: %w", err)
	}

	// Apply defaults
	s.applyDefaults(input)

	// Implement progressive loading: return first 50 transactions immediately
	// Requirement 5.1: Return first 50 transactions within 100ms
	progressiveLimit := 50
	isProgressiveLoad := input.Limit > progressiveLimit && input.Offset == 0

	// For progressive loading, fetch only the first batch quickly
	originalLimit := input.Limit
	if isProgressiveLoad {
		input.Limit = progressiveLimit
	}

	// Determine query routing: cache vs ClickHouse
	// Requirement 14.7: Route to cache for simple queries, ClickHouse for complex filters
	useCache := s.shouldUseCache(input)

	var transactions []*models.Transaction
	var totalCount int64
	var cached bool
	var err error

	if useCache {
		// Try cache first for simple queries
		transactions, totalCount, cached, err = s.queryFromCache(ctx, input)
		if err != nil || !cached {
			// Cache miss or error - fall back to ClickHouse
			transactions, totalCount, err = s.queryFromClickHouse(ctx, input)
			if err != nil {
				return nil, fmt.Errorf("failed to query from ClickHouse: %w", err)
			}
			cached = false
		}
	} else {
		// Complex filters - query ClickHouse directly
		transactions, totalCount, err = s.queryFromClickHouse(ctx, input)
		if err != nil {
			return nil, fmt.Errorf("failed to query from ClickHouse: %w", err)
		}
		cached = false
	}

	// Convert to normalized transactions
	normalizedTxs := s.convertToNormalizedTransactions(transactions)

	// Build pagination info
	// For progressive loading, indicate there's more data available
	actualLimit := input.Limit
	if isProgressiveLoad {
		actualLimit = originalLimit // Restore original limit for pagination info
	}

	pagination := PaginationInfo{
		Total:   totalCount,
		Limit:   actualLimit,
		Offset:  input.Offset,
		HasMore: int64(input.Offset+len(normalizedTxs)) < totalCount,
	}

	result := &QueryResult{
		Address:      input.Address,
		Transactions: normalizedTxs,
		Pagination:   pagination,
		Cached:       cached,
		QueryTimeMs:  time.Since(startTime).Milliseconds(),
	}

	// Record performance metrics
	// Requirement 4.1: Monitor cached query performance
	s.perfMonitor.RecordQuery(ctx, time.Since(startTime), cached)

	return result, nil
}

// SearchByHash searches for a transaction by hash across all tracked addresses
// Requirement 9.1: Transaction hash search within 200ms
// Requirement 9.2: Cross-address hash search
// Requirement 9.3: Handle not-found cases
func (s *QueryService) SearchByHash(ctx context.Context, hash string) (*types.NormalizedTransaction, error) {
	startTime := time.Now()

	if hash == "" {
		return nil, fmt.Errorf("transaction hash is required")
	}

	// Query ClickHouse for transaction by hash
	// The hash index (bloom filter) makes this fast
	tx, err := s.transactionRepo.GetByHash(ctx, hash)
	if err != nil {
		// Check if it's already a ServiceError
		if _, ok := err.(*types.ServiceError); ok {
			return nil, err
		}
		// Check if it's a not-found error
		if err.Error() == "sql: no rows in result set" || err.Error() == "EOF" {
			return nil, &types.ServiceError{
				Code:    "TRANSACTION_NOT_FOUND",
				Message: fmt.Sprintf("Transaction with hash %s not found", hash),
				Details: map[string]interface{}{
					"hash": hash,
				},
			}
		}
		return nil, fmt.Errorf("failed to search transaction by hash: %w", err)
	}

	// Convert to normalized transaction
	normalized := &types.NormalizedTransaction{
		Hash:           tx.Hash,
		Chain:          tx.Chain,
		From:           tx.From,
		To:             tx.To,
		Value:          tx.Value,
		Timestamp:      tx.Timestamp.Unix(),
		BlockNumber:    tx.BlockNumber,
		Status:         types.TransactionStatus(tx.Status),
		GasUsed:        tx.GasUsed,
		GasPrice:       tx.GasPrice,
		TokenTransfers: tx.TokenTransfers,
		MethodID:       tx.MethodID,
		Input:          tx.Input,
	}

	queryTime := time.Since(startTime).Milliseconds()
	fmt.Printf("Transaction hash search completed in %dms\n", queryTime)

	return normalized, nil
}

// shouldUseCache determines if a query should use cache or go directly to ClickHouse
// Requirement 14.7: Route to cache for simple queries, ClickHouse for complex filters
func (s *QueryService) shouldUseCache(input *QueryInput) bool {
	// Use cache only for simple queries without complex filters
	hasComplexFilters := input.DateFrom != nil ||
		input.DateTo != nil ||
		input.MinValue != nil ||
		input.MaxValue != nil ||
		input.Status != nil ||
		len(input.Chains) > 0

	// If query has complex filters, go directly to ClickHouse
	if hasComplexFilters {
		return false
	}

	// If offset is beyond cache window, go to ClickHouse
	if input.Offset >= s.cacheWindowSize {
		return false
	}

	// Simple query within cache window - try cache
	return true
}

// queryFromCache attempts to query from Redis cache
// Requirement 4.1: Ensure sub-100ms response for cached queries
func (s *QueryService) queryFromCache(ctx context.Context, input *QueryInput) ([]*models.Transaction, int64, bool, error) {
	// If cache service is not configured, return cache miss
	if s.cacheService == nil {
		return nil, 0, false, fmt.Errorf("cache service not configured")
	}

	// Generate cache key for this address
	// For simple queries, we cache the most recent 1000 transactions
	cacheKey := s.cacheService.GenerateCacheKey(storage.CacheKeyTransactions, input.Address)

	// Try to get from cache
	var cachedWindow storage.CachedTransactionWindow
	found, err := s.cacheService.Get(ctx, cacheKey, &cachedWindow)
	if err != nil {
		return nil, 0, false, fmt.Errorf("cache error: %w", err)
	}

	if !found {
		return nil, 0, false, fmt.Errorf("cache miss")
	}

	// Apply pagination to cached data
	transactions := cachedWindow.Transactions
	totalCount := cachedWindow.TotalCount

	// Apply offset and limit
	start := input.Offset
	end := input.Offset + input.Limit

	if start >= len(transactions) {
		// Offset beyond cached window - need to query ClickHouse
		return nil, 0, false, fmt.Errorf("offset beyond cache window")
	}

	if end > len(transactions) {
		end = len(transactions)
	}

	// Return slice of cached transactions
	result := transactions[start:end]

	return result, totalCount, true, nil
}

// queryFromClickHouse queries transactions from ClickHouse
// Requirement 4.4: On cache miss, query ClickHouse and populate cache
func (s *QueryService) queryFromClickHouse(ctx context.Context, input *QueryInput) ([]*models.Transaction, int64, error) {
	// Convert to storage filters
	filters := &storage.TransactionFilters{
		Chains:    input.Chains,
		DateFrom:  input.DateFrom,
		DateTo:    input.DateTo,
		MinValue:  input.MinValue,
		MaxValue:  input.MaxValue,
		Status:    input.Status,
		SortBy:    input.SortBy,
		SortOrder: input.SortOrder,
		Limit:     input.Limit,
		Offset:    input.Offset,
	}

	// Query transactions
	transactions, err := s.transactionRepo.GetByAddress(ctx, input.Address, filters)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to query transactions: %w", err)
	}

	// Get total count
	totalCount, err := s.transactionRepo.CountByAddress(ctx, input.Address, filters)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to count transactions: %w", err)
	}

	// If this is a simple query (no filters) and we're querying the first page,
	// populate the cache for future requests
	// Requirement 4.4: Populate cache with results before returning
	if s.shouldPopulateCache(input) {
		go func() {
			// Populate cache asynchronously to not block the response
			cacheCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			if err := s.PopulateCache(cacheCtx, input.Address); err != nil {
				fmt.Printf("Warning: failed to populate cache for address %s: %v\n", input.Address, err)
			}
		}()
	}

	return transactions, totalCount, nil
}

// shouldPopulateCache determines if we should populate cache after a ClickHouse query
func (s *QueryService) shouldPopulateCache(input *QueryInput) bool {
	// Only populate cache for simple queries without filters
	hasFilters := input.DateFrom != nil ||
		input.DateTo != nil ||
		input.MinValue != nil ||
		input.MaxValue != nil ||
		input.Status != nil ||
		len(input.Chains) > 0

	// Only populate for first page
	return !hasFilters && input.Offset == 0
}

// validateQueryInput validates query input parameters
func (s *QueryService) validateQueryInput(input *QueryInput) error {
	if input.Address == "" {
		return fmt.Errorf("address is required")
	}

	// Validate address format
	if err := storage.ValidateAddress(input.Address); err != nil {
		return fmt.Errorf("invalid address format: %w", err)
	}

	// Validate limit
	if input.Limit < 0 {
		return fmt.Errorf("limit must be non-negative")
	}
	if input.Limit > s.cacheWindowSize {
		return fmt.Errorf("limit cannot exceed %d", s.cacheWindowSize)
	}

	// Validate offset
	if input.Offset < 0 {
		return fmt.Errorf("offset must be non-negative")
	}

	// Validate sort by
	if input.SortBy != "" {
		validSortFields := map[string]bool{
			"timestamp":    true,
			"value":        true,
			"block_number": true,
		}
		if !validSortFields[input.SortBy] {
			return fmt.Errorf("invalid sortBy field: %s (valid: timestamp, value, block_number)", input.SortBy)
		}
	}

	// Validate sort order
	if input.SortOrder != "" {
		if input.SortOrder != "asc" && input.SortOrder != "desc" {
			return fmt.Errorf("invalid sortOrder: %s (valid: asc, desc)", input.SortOrder)
		}
	}

	// Validate date range
	if input.DateFrom != nil && input.DateTo != nil {
		if input.DateFrom.After(*input.DateTo) {
			return fmt.Errorf("dateFrom must be before dateTo")
		}
	}

	// Validate value range
	if input.MinValue != nil && input.MaxValue != nil {
		if *input.MinValue > *input.MaxValue {
			return fmt.Errorf("minValue must be less than or equal to maxValue")
		}
	}

	return nil
}

// applyDefaults applies default values to query input
func (s *QueryService) applyDefaults(input *QueryInput) {
	// Default limit: 50
	if input.Limit == 0 {
		input.Limit = 50
	}

	// Default sort by: timestamp
	if input.SortBy == "" {
		input.SortBy = "timestamp"
	}

	// Default sort order: desc (most recent first)
	if input.SortOrder == "" {
		input.SortOrder = "desc"
	}
}

// convertToNormalizedTransactions converts storage transactions to normalized format
func (s *QueryService) convertToNormalizedTransactions(transactions []*models.Transaction) []*types.NormalizedTransaction {
	normalized := make([]*types.NormalizedTransaction, len(transactions))
	for i, tx := range transactions {
		normalized[i] = &types.NormalizedTransaction{
			Hash:           tx.Hash,
			Chain:          tx.Chain,
			From:           tx.From,
			To:             tx.To,
			Value:          tx.Value,
			Timestamp:      tx.Timestamp.Unix(),
			BlockNumber:    tx.BlockNumber,
			Status:         types.TransactionStatus(tx.Status),
			GasUsed:        tx.GasUsed,
			GasPrice:       tx.GasPrice,
			TokenTransfers: tx.TokenTransfers,
			MethodID:       tx.MethodID,
			Input:          tx.Input,
		}
	}
	return normalized
}

// PopulateCache populates the cache with recent transactions for an address
// Requirement 4.2: Cache most recent 1000 transactions per address
// Requirement 4.3: Set cache TTL to 15-20 seconds
func (s *QueryService) PopulateCache(ctx context.Context, address string) error {
	// If cache service is not configured, skip caching
	if s.cacheService == nil {
		return nil
	}

	// Query most recent 1000 transactions from ClickHouse
	filters := &storage.TransactionFilters{
		SortBy:    "timestamp",
		SortOrder: "desc",
		Limit:     s.cacheWindowSize,
		Offset:    0,
	}

	transactions, err := s.transactionRepo.GetByAddress(ctx, address, filters)
	if err != nil {
		return fmt.Errorf("failed to query transactions for cache: %w", err)
	}

	// Get total count
	totalCount, err := s.transactionRepo.CountByAddress(ctx, address, &storage.TransactionFilters{})
	if err != nil {
		return fmt.Errorf("failed to count transactions: %w", err)
	}

	// Create cached window
	cachedWindow := storage.CachedTransactionWindow{
		Address:      address,
		Transactions: transactions,
		CachedAt:     time.Now(),
		TotalCount:   totalCount,
	}

	// Store in cache with configured TTL
	cacheKey := s.cacheService.GenerateCacheKey(storage.CacheKeyTransactions, address)

	if err := s.cacheService.Set(ctx, cacheKey, cachedWindow); err != nil {
		return fmt.Errorf("failed to populate cache: %w", err)
	}

	return nil
}

// SetCacheWindowSize sets the cache window size (for testing)
func (s *QueryService) SetCacheWindowSize(size int) {
	s.cacheWindowSize = size
}

// GetPerformanceStats returns current performance statistics
// Requirement 4.1: Add performance monitoring
func (s *QueryService) GetPerformanceStats() *PerformanceStats {
	return s.perfMonitor.GetStats()
}

// CheckPerformance checks if performance meets requirements
// Requirement 4.1: Ensure sub-100ms response for cached queries
func (s *QueryService) CheckPerformance() *PerformanceCheck {
	return s.perfMonitor.CheckPerformance()
}

// ResetPerformanceStats resets performance statistics
func (s *QueryService) ResetPerformanceStats() {
	s.perfMonitor.Reset()
}

// QueryNextPage queries the next page of transactions for lazy loading
// Requirement 5.2: Support lazy loading for additional data
// This is a convenience method that automatically calculates the next offset
func (s *QueryService) QueryNextPage(ctx context.Context, previousResult *QueryResult, pageSize int) (*QueryResult, error) {
	if !previousResult.Pagination.HasMore {
		return nil, fmt.Errorf("no more data available")
	}

	// Calculate next offset
	nextOffset := previousResult.Pagination.Offset + len(previousResult.Transactions)

	// Create input for next page
	input := &QueryInput{
		Address:   previousResult.Address,
		Limit:     pageSize,
		Offset:    nextOffset,
		SortBy:    "timestamp",
		SortOrder: "desc",
	}

	return s.Query(ctx, input)
}
