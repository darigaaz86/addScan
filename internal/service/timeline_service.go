package service

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/address-scanner/internal/models"
	"github.com/address-scanner/internal/storage"
	"github.com/address-scanner/internal/types"
)

// TimelineService handles multi-chain timeline aggregation
type TimelineService struct {
	transactionRepo       *storage.TransactionRepository
	unifiedTimelineRepo   *storage.UnifiedTimelineRepository
	cacheService          *storage.CacheService
	usePrecomputedTimeline bool // Feature flag to enable/disable pre-computed timeline
}

// NewTimelineService creates a new timeline service
func NewTimelineService(
	transactionRepo *storage.TransactionRepository,
	unifiedTimelineRepo *storage.UnifiedTimelineRepository,
	cacheService *storage.CacheService,
) *TimelineService {
	return &TimelineService{
		transactionRepo:       transactionRepo,
		unifiedTimelineRepo:   unifiedTimelineRepo,
		cacheService:          cacheService,
		usePrecomputedTimeline: true, // Enable by default
	}
}

// TimelineFilters defines filters for timeline queries
type TimelineFilters struct {
	Chains    []types.ChainID
	DateFrom  *time.Time
	DateTo    *time.Time
	MinValue  *float64
	MaxValue  *float64
	Status    *types.TransactionStatus
	Limit     int
	Offset    int
}

// TimelineResult represents the result of a timeline query
type TimelineResult struct {
	Transactions []*types.NormalizedTransaction `json:"transactions"`
	TotalCount   int64                          `json:"totalCount"`
	Cached       bool                           `json:"cached"`
	QueryTimeMs  int64                          `json:"queryTimeMs"`
}

// GetUnifiedTimeline retrieves and merges transactions from all chains for an address
// Requirement 3.2: Present unified timeline merging transactions from all chains
// Requirement 3.4: Sort unified timeline by transaction timestamp across all chains
func (s *TimelineService) GetUnifiedTimeline(ctx context.Context, address string, filters *TimelineFilters) (*TimelineResult, error) {
	startTime := time.Now()

	// Try to get from cache first
	cacheKey := s.buildCacheKey(address, filters)
	if cachedResult, err := s.getCachedTimeline(ctx, cacheKey); err == nil && cachedResult != nil {
		cachedResult.Cached = true
		cachedResult.QueryTimeMs = time.Since(startTime).Milliseconds()
		return cachedResult, nil
	}

	var transactions []*models.Transaction
	var err error

	// Use pre-computed timeline if enabled (faster)
	// Requirement 3.2: Store unified timeline in ClickHouse for faster retrieval
	if s.usePrecomputedTimeline && s.unifiedTimelineRepo != nil {
		storageFilters := s.convertToStorageFilters(filters)
		transactions, err = s.unifiedTimelineRepo.GetByAddress(ctx, address, storageFilters)
		if err != nil {
			// Fall back to regular transaction table if pre-computed timeline fails
			fmt.Printf("Warning: pre-computed timeline query failed, falling back to transactions table: %v\n", err)
			transactions, err = s.transactionRepo.GetByAddress(ctx, address, storageFilters)
			if err != nil {
				return nil, fmt.Errorf("failed to fetch transactions: %w", err)
			}
		}
	} else {
		// Fetch from regular transactions table
		storageFilters := s.convertToStorageFilters(filters)
		transactions, err = s.transactionRepo.GetByAddress(ctx, address, storageFilters)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch transactions: %w", err)
		}
	}

	// Convert to normalized transactions
	normalizedTxs := s.convertToNormalizedTransactions(transactions)

	// Merge and sort by timestamp (descending - most recent first)
	// This implements the unified timeline requirement
	mergedTimeline := s.mergeAndSortTransactions(normalizedTxs)

	// Get total count
	var totalCount int64
	if s.usePrecomputedTimeline && s.unifiedTimelineRepo != nil {
		storageFilters := s.convertToStorageFilters(filters)
		totalCount, err = s.unifiedTimelineRepo.CountByAddress(ctx, address, storageFilters)
		if err != nil {
			// Fall back to regular count
			totalCount, err = s.transactionRepo.CountByAddress(ctx, address, storageFilters)
			if err != nil {
				return nil, fmt.Errorf("failed to count transactions: %w", err)
			}
		}
	} else {
		storageFilters := s.convertToStorageFilters(filters)
		totalCount, err = s.transactionRepo.CountByAddress(ctx, address, storageFilters)
		if err != nil {
			return nil, fmt.Errorf("failed to count transactions: %w", err)
		}
	}

	result := &TimelineResult{
		Transactions: mergedTimeline,
		TotalCount:   totalCount,
		Cached:       false,
		QueryTimeMs:  time.Since(startTime).Milliseconds(),
	}

	// Cache the result
	if err := s.cacheTimeline(ctx, cacheKey, result); err != nil {
		// Log error but don't fail the request
		fmt.Printf("Warning: failed to cache timeline for address %s: %v\n", address, err)
	}

	return result, nil
}

// GetUnifiedTimelineForAddresses retrieves and merges transactions from multiple addresses
// Used for portfolio timeline aggregation
// Requirement 3.2: Present unified timeline merging transactions from all chains
// Requirement 3.4: Sort unified timeline by transaction timestamp across all chains
func (s *TimelineService) GetUnifiedTimelineForAddresses(ctx context.Context, addresses []string, filters *TimelineFilters) (*TimelineResult, error) {
	startTime := time.Now()

	if len(addresses) == 0 {
		return &TimelineResult{
			Transactions: []*types.NormalizedTransaction{},
			TotalCount:   0,
			Cached:       false,
			QueryTimeMs:  time.Since(startTime).Milliseconds(),
		}, nil
	}

	var transactions []*models.Transaction
	var err error

	// Use pre-computed timeline if enabled (faster)
	// Requirement 3.2: Store unified timeline in ClickHouse for faster retrieval
	if s.usePrecomputedTimeline && s.unifiedTimelineRepo != nil {
		storageFilters := s.convertToStorageFilters(filters)
		transactions, err = s.unifiedTimelineRepo.GetByAddresses(ctx, addresses, storageFilters)
		if err != nil {
			// Fall back to regular transaction table if pre-computed timeline fails
			fmt.Printf("Warning: pre-computed timeline query failed, falling back to transactions table: %v\n", err)
			transactions, err = s.transactionRepo.GetByAddresses(ctx, addresses, storageFilters)
			if err != nil {
				return nil, fmt.Errorf("failed to fetch transactions for addresses: %w", err)
			}
		}
	} else {
		// Fetch from regular transactions table
		storageFilters := s.convertToStorageFilters(filters)
		transactions, err = s.transactionRepo.GetByAddresses(ctx, addresses, storageFilters)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch transactions for addresses: %w", err)
		}
	}

	// Convert to normalized transactions
	normalizedTxs := s.convertToNormalizedTransactions(transactions)

	// Merge and sort by timestamp (descending - most recent first)
	mergedTimeline := s.mergeAndSortTransactions(normalizedTxs)

	result := &TimelineResult{
		Transactions: mergedTimeline,
		TotalCount:   int64(len(mergedTimeline)),
		Cached:       false,
		QueryTimeMs:  time.Since(startTime).Milliseconds(),
	}

	return result, nil
}

// mergeAndSortTransactions merges transactions from multiple chains and sorts by timestamp
// This is the core timeline merging logic
// Requirement 3.2: Merge transactions from all chains
// Requirement 3.4: Sort by transaction timestamp across all chains
func (s *TimelineService) mergeAndSortTransactions(transactions []*types.NormalizedTransaction) []*types.NormalizedTransaction {
	if len(transactions) == 0 {
		return transactions
	}

	// Normalize all timestamps to UTC to handle timezone differences
	// Timestamps are already stored as Unix timestamps (int64)
	// No timezone normalization needed as Unix timestamps are timezone-agnostic

	// Sort by timestamp descending (most recent first)
	// If timestamps are equal, sort by hash for deterministic ordering
	sort.Slice(transactions, func(i, j int) bool {
		if transactions[i].Timestamp == transactions[j].Timestamp {
			// Secondary sort by hash for deterministic ordering
			return transactions[i].Hash > transactions[j].Hash
		}
		return transactions[i].Timestamp > transactions[j].Timestamp
	})

	return transactions
}

// convertToNormalizedTransactions converts storage transactions to normalized format
func (s *TimelineService) convertToNormalizedTransactions(transactions []*models.Transaction) []*types.NormalizedTransaction {
	normalized := make([]*types.NormalizedTransaction, len(transactions))
	for i, tx := range transactions {
		normalized[i] = &types.NormalizedTransaction{
			Hash:           tx.Hash,
			Chain:          tx.Chain,
			From:           tx.From,
			To:             tx.To,
			Value:          tx.Value,
			Timestamp:      tx.Timestamp.Unix(), // Convert to Unix timestamp
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

// convertToStorageFilters converts timeline filters to storage filters
func (s *TimelineService) convertToStorageFilters(filters *TimelineFilters) *storage.TransactionFilters {
	if filters == nil {
		return &storage.TransactionFilters{
			SortBy:    "timestamp",
			SortOrder: "desc",
		}
	}

	return &storage.TransactionFilters{
		Chains:    filters.Chains,
		DateFrom:  filters.DateFrom,
		DateTo:    filters.DateTo,
		MinValue:  filters.MinValue,
		MaxValue:  filters.MaxValue,
		Status:    filters.Status,
		SortBy:    "timestamp",
		SortOrder: "desc",
		Limit:     filters.Limit,
		Offset:    filters.Offset,
	}
}

// buildCacheKey builds a cache key for timeline queries
func (s *TimelineService) buildCacheKey(address string, filters *TimelineFilters) string {
	// Simple cache key for MVP - can be enhanced with filter parameters
	if filters == nil || (len(filters.Chains) == 0 && filters.DateFrom == nil && filters.DateTo == nil) {
		return fmt.Sprintf("timeline:%s", address)
	}

	// Include filter parameters in cache key
	key := fmt.Sprintf("timeline:%s", address)
	if len(filters.Chains) > 0 {
		key += fmt.Sprintf(":chains:%v", filters.Chains)
	}
	if filters.DateFrom != nil {
		key += fmt.Sprintf(":from:%d", filters.DateFrom.Unix())
	}
	if filters.DateTo != nil {
		key += fmt.Sprintf(":to:%d", filters.DateTo.Unix())
	}
	return key
}

// getCachedTimeline retrieves timeline from cache
func (s *TimelineService) getCachedTimeline(ctx context.Context, cacheKey string) (*TimelineResult, error) {
	// This is a placeholder - actual implementation depends on cache service structure
	// For now, return nil to indicate cache miss
	return nil, fmt.Errorf("cache miss")
}

// cacheTimeline stores timeline in cache
func (s *TimelineService) cacheTimeline(ctx context.Context, cacheKey string, result *TimelineResult) error {
	// This is a placeholder - actual implementation depends on cache service structure
	// For now, just log that we would cache
	fmt.Printf("Would cache timeline with key: %s (count: %d)\n", cacheKey, len(result.Transactions))
	return nil
}

// RefreshPrecomputedTimeline manually refreshes the pre-computed timeline for an address
// This is typically not needed as the materialized view handles updates automatically
// But can be useful for maintenance or recovery scenarios
func (s *TimelineService) RefreshPrecomputedTimeline(ctx context.Context, address string) error {
	if s.unifiedTimelineRepo == nil {
		return fmt.Errorf("unified timeline repository not available")
	}

	return s.unifiedTimelineRepo.RefreshForAddress(ctx, address)
}

// SetUsePrecomputedTimeline enables or disables the pre-computed timeline feature
// Useful for testing or gradual rollout
func (s *TimelineService) SetUsePrecomputedTimeline(enabled bool) {
	s.usePrecomputedTimeline = enabled
}
