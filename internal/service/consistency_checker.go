package service

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/address-scanner/internal/models"
	"github.com/address-scanner/internal/storage"
	"github.com/address-scanner/internal/types"
)

// ConsistencyChecker verifies cache consistency with ClickHouse
// Requirements: 12.1, 12.2, 12.5
type ConsistencyChecker struct {
	transactionCache *storage.TransactionCache
	transactionRepo  *storage.TransactionRepository
	cacheService     *storage.CacheService
	cacheTTL         time.Duration
}

// NewConsistencyChecker creates a new consistency checker
func NewConsistencyChecker(
	transactionCache *storage.TransactionCache,
	transactionRepo *storage.TransactionRepository,
	cacheService *storage.CacheService,
	cacheTTL time.Duration,
) *ConsistencyChecker {
	return &ConsistencyChecker{
		transactionCache: transactionCache,
		transactionRepo:  transactionRepo,
		cacheService:     cacheService,
		cacheTTL:         cacheTTL,
	}
}

// ConsistencyCheckResult represents the result of a consistency check
type ConsistencyCheckResult struct {
	Address          string        `json:"address"`
	Chain            types.ChainID `json:"chain"`
	Consistent       bool          `json:"consistent"`
	CacheCount       int           `json:"cacheCount"`
	DatabaseCount    int64         `json:"databaseCount"`
	Inconsistencies  []string      `json:"inconsistencies,omitempty"`
	CheckedAt        time.Time     `json:"checkedAt"`
	CacheInvalidated bool          `json:"cacheInvalidated"`
}

// CheckConsistency verifies that cached data matches ClickHouse after TTL
// Requirement 12.1: Maintain eventual consistency between cache and persistent storage
// Requirement 12.2: Refresh from ClickHouse when cache expires
func (cc *ConsistencyChecker) CheckConsistency(ctx context.Context, address string, chain types.ChainID) (*ConsistencyCheckResult, error) {
	result := &ConsistencyCheckResult{
		Address:   address,
		Chain:     chain,
		CheckedAt: time.Now(),
	}

	// Get cached window
	cachedWindow, err := cc.transactionCache.GetTransactionWindow(ctx, address, chain)
	if err != nil {
		return nil, fmt.Errorf("failed to get cached window: %w", err)
	}

	// Get fresh data from database
	filters := &storage.TransactionFilters{
		Chains:    []types.ChainID{chain},
		Limit:     cc.transactionCache.GetWindowSize(),
		SortBy:    "timestamp",
		SortOrder: "desc",
	}
	dbTransactions, err := cc.transactionRepo.GetByAddress(ctx, address, filters)
	if err != nil {
		return nil, fmt.Errorf("failed to get database transactions: %w", err)
	}

	// Compare counts
	result.CacheCount = len(cachedWindow.Transactions)
	result.DatabaseCount = int64(len(dbTransactions))

	// Check if counts match
	if result.CacheCount != int(result.DatabaseCount) {
		result.Consistent = false
		result.Inconsistencies = append(result.Inconsistencies,
			fmt.Sprintf("count mismatch: cache=%d, db=%d", result.CacheCount, result.DatabaseCount))
	} else {
		// Counts match, now verify transaction hashes
		cacheHashes := make(map[string]bool)
		for _, tx := range cachedWindow.Transactions {
			cacheHashes[tx.TxHash] = true
		}

		dbHashes := make(map[string]bool)
		for _, tx := range dbTransactions {
			dbHashes[tx.TxHash] = true
		}

		// Check for missing transactions in cache
		for hash := range dbHashes {
			if !cacheHashes[hash] {
				result.Consistent = false
				result.Inconsistencies = append(result.Inconsistencies,
					fmt.Sprintf("transaction %s missing from cache", hash))
			}
		}

		// Check for extra transactions in cache
		for hash := range cacheHashes {
			if !dbHashes[hash] {
				result.Consistent = false
				result.Inconsistencies = append(result.Inconsistencies,
					fmt.Sprintf("transaction %s in cache but not in database", hash))
			}
		}

		// If no inconsistencies found, mark as consistent
		if len(result.Inconsistencies) == 0 {
			result.Consistent = true
		}
	}

	// If inconsistent, invalidate cache
	// Requirement 12.5: Trigger cache invalidation on inconsistency
	if !result.Consistent {
		log.Printf("Consistency check failed for %s on %s: %v", address, chain, result.Inconsistencies)
		if err := cc.transactionCache.InvalidateWindow(ctx, address, chain); err != nil {
			log.Printf("Failed to invalidate cache for %s on %s: %v", address, chain, err)
		} else {
			result.CacheInvalidated = true
			log.Printf("Cache invalidated for %s on %s", address, chain)
		}
	}

	return result, nil
}

// CheckMultiChainConsistency checks consistency across multiple chains for an address
func (cc *ConsistencyChecker) CheckMultiChainConsistency(ctx context.Context, address string, chains []types.ChainID) ([]*ConsistencyCheckResult, error) {
	results := make([]*ConsistencyCheckResult, 0, len(chains))

	for _, chain := range chains {
		result, err := cc.CheckConsistency(ctx, address, chain)
		if err != nil {
			log.Printf("Failed to check consistency for %s on %s: %v", address, chain, err)
			continue
		}
		results = append(results, result)
	}

	return results, nil
}

// VerifyAfterTTL waits for cache TTL to expire, then verifies consistency
// This is useful for testing eventual consistency guarantees
func (cc *ConsistencyChecker) VerifyAfterTTL(ctx context.Context, address string, chain types.ChainID) (*ConsistencyCheckResult, error) {
	// Wait for TTL + small buffer
	waitDuration := cc.cacheTTL + (2 * time.Second)
	log.Printf("Waiting %v for cache TTL to expire before consistency check", waitDuration)

	select {
	case <-time.After(waitDuration):
		return cc.CheckConsistency(ctx, address, chain)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// DetectInconsistencies scans all cached addresses and detects inconsistencies
// This can be run periodically as a background job
func (cc *ConsistencyChecker) DetectInconsistencies(ctx context.Context, addresses []string, chains []types.ChainID) ([]ConsistencyCheckResult, error) {
	var inconsistencies []ConsistencyCheckResult

	for _, address := range addresses {
		for _, chain := range chains {
			result, err := cc.CheckConsistency(ctx, address, chain)
			if err != nil {
				log.Printf("Error checking consistency for %s on %s: %v", address, chain, err)
				continue
			}

			if !result.Consistent {
				inconsistencies = append(inconsistencies, *result)
			}
		}
	}

	return inconsistencies, nil
}

// StartPeriodicConsistencyCheck starts a background goroutine that periodically checks consistency
// This ensures eventual consistency is maintained over time
func (cc *ConsistencyChecker) StartPeriodicConsistencyCheck(ctx context.Context, addresses []string, chains []types.ChainID, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	log.Printf("Starting periodic consistency checks every %v", interval)

	for {
		select {
		case <-ctx.Done():
			log.Println("Stopping periodic consistency checks")
			return
		case <-ticker.C:
			log.Println("Running periodic consistency check")
			inconsistencies, err := cc.DetectInconsistencies(ctx, addresses, chains)
			if err != nil {
				log.Printf("Error during periodic consistency check: %v", err)
				continue
			}

			if len(inconsistencies) > 0 {
				log.Printf("Found %d inconsistencies during periodic check", len(inconsistencies))
				for _, inc := range inconsistencies {
					log.Printf("Inconsistency: %s on %s - %v", inc.Address, inc.Chain, inc.Inconsistencies)
				}
			} else {
				log.Println("No inconsistencies found during periodic check")
			}
		}
	}
}

// CompareTransactionData performs deep comparison of transaction data
// This verifies not just hashes but also transaction details
func (cc *ConsistencyChecker) CompareTransactionData(cached *models.Transaction, db *models.Transaction) []string {
	var differences []string

	if cached.TxHash != db.TxHash {
		differences = append(differences, fmt.Sprintf("hash mismatch: cache=%s, db=%s", cached.TxHash, db.TxHash))
	}

	if cached.Chain != db.Chain {
		differences = append(differences, fmt.Sprintf("chain mismatch: cache=%s, db=%s", cached.Chain, db.Chain))
	}

	if cached.TxFrom != db.TxFrom {
		differences = append(differences, fmt.Sprintf("from mismatch: cache=%s, db=%s", cached.TxFrom, db.TxFrom))
	}

	if cached.TxTo != db.TxTo {
		differences = append(differences, fmt.Sprintf("to mismatch: cache=%s, db=%s", cached.TxTo, db.TxTo))
	}

	if cached.Value != db.Value {
		differences = append(differences, fmt.Sprintf("value mismatch: cache=%s, db=%s", cached.Value, db.Value))
	}

	if !cached.Timestamp.Equal(db.Timestamp) {
		differences = append(differences, fmt.Sprintf("timestamp mismatch: cache=%v, db=%v", cached.Timestamp, db.Timestamp))
	}

	if cached.BlockNumber != db.BlockNumber {
		differences = append(differences, fmt.Sprintf("block number mismatch: cache=%d, db=%d", cached.BlockNumber, db.BlockNumber))
	}

	if cached.Status != db.Status {
		differences = append(differences, fmt.Sprintf("status mismatch: cache=%s, db=%s", cached.Status, db.Status))
	}

	return differences
}

// GetConsistencyStats returns statistics about cache consistency
type ConsistencyStats struct {
	TotalChecks        int       `json:"totalChecks"`
	ConsistentChecks   int       `json:"consistentChecks"`
	InconsistentChecks int       `json:"inconsistentChecks"`
	LastCheckTime      time.Time `json:"lastCheckTime"`
	ConsistencyRate    float64   `json:"consistencyRate"`
}

// This would require additional state tracking in a production implementation
// For now, this is a placeholder for the interface
func (cc *ConsistencyChecker) GetConsistencyStats() *ConsistencyStats {
	// In a real implementation, this would track statistics over time
	return &ConsistencyStats{
		TotalChecks:        0,
		ConsistentChecks:   0,
		InconsistentChecks: 0,
		LastCheckTime:      time.Now(),
		ConsistencyRate:    0.0,
	}
}
