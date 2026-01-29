package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/address-scanner/internal/types"
)

// QueryOptimizer optimizes ClickHouse queries using caching and materialized views
// Requirement 7.4: Use materialized views for aggregations, optimize indexes, add query result caching
type QueryOptimizer struct {
	clickhouse *ClickHouseDB
	cacheTTL   time.Duration
}

// NewQueryOptimizer creates a new query optimizer
func NewQueryOptimizer(clickhouse *ClickHouseDB) *QueryOptimizer {
	return &QueryOptimizer{
		clickhouse: clickhouse,
		cacheTTL:   5 * time.Minute, // Cache query results for 5 minutes
	}
}

// QueryWithCache executes a query with result caching
// If the query result is cached and not expired, returns cached result
// Otherwise executes the query and caches the result
func (qo *QueryOptimizer) QueryWithCache(ctx context.Context, query string, args []interface{}, dest interface{}) (bool, error) {
	// Generate query hash for cache key
	queryHash := qo.generateQueryHash(query, args)
	
	// Try to get cached result
	cached, err := qo.getCachedResult(ctx, queryHash, dest)
	if err == nil && cached {
		return true, nil // Cache hit
	}
	
	// Cache miss - execute query
	rows, err := qo.clickhouse.Conn().Query(ctx, query, args...)
	if err != nil {
		return false, fmt.Errorf("query execution failed: %w", err)
	}
	defer rows.Close()
	
	// Scan results into destination
	if err := rows.Scan(dest); err != nil {
		return false, fmt.Errorf("failed to scan results: %w", err)
	}
	
	// Cache the result asynchronously
	go func() {
		cacheCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		
		if err := qo.cacheResult(cacheCtx, queryHash, dest); err != nil {
			fmt.Printf("Warning: failed to cache query result: %v\n", err)
		}
	}()
	
	return false, nil // Cache miss
}

// generateQueryHash generates a hash for a query and its arguments
func (qo *QueryOptimizer) generateQueryHash(query string, args []interface{}) string {
	// Combine query and args into a single string
	data := query
	for _, arg := range args {
		data += fmt.Sprintf(":%v", arg)
	}
	
	// Generate SHA256 hash
	hash := sha256.Sum256([]byte(data))
	return hex.EncodeToString(hash[:])
}

// getCachedResult retrieves a cached query result
func (qo *QueryOptimizer) getCachedResult(ctx context.Context, queryHash string, dest interface{}) (bool, error) {
	query := `
		SELECT result_data
		FROM query_result_cache
		WHERE query_hash = ?
		  AND expires_at > now()
		ORDER BY created_at DESC
		LIMIT 1
	`
	
	var resultData string
	err := qo.clickhouse.Conn().QueryRow(ctx, query, queryHash).Scan(&resultData)
	if err != nil {
		return false, err
	}
	
	// Deserialize cached result
	if err := json.Unmarshal([]byte(resultData), dest); err != nil {
		return false, fmt.Errorf("failed to unmarshal cached result: %w", err)
	}
	
	return true, nil
}

// cacheResult stores a query result in the cache
func (qo *QueryOptimizer) cacheResult(ctx context.Context, queryHash string, result interface{}) error {
	// Serialize result to JSON
	resultData, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("failed to marshal result: %w", err)
	}
	
	// Calculate expiration time
	expiresAt := time.Now().Add(qo.cacheTTL)
	
	// Insert into cache table
	query := `
		INSERT INTO query_result_cache (query_hash, result_data, created_at, expires_at)
		VALUES (?, ?, now(), ?)
	`
	
	return qo.clickhouse.Exec(ctx, query, queryHash, string(resultData), expiresAt)
}

// InvalidateQueryCache invalidates cached query results for an address
func (qo *QueryOptimizer) InvalidateQueryCache(ctx context.Context, address string) error {
	query := `
		ALTER TABLE query_result_cache
		DELETE WHERE address = ?
	`
	
	return qo.clickhouse.Exec(ctx, query, address)
}

// OptimizeQuery analyzes a query and suggests optimizations
func (qo *QueryOptimizer) OptimizeQuery(query string, filters *TransactionFilters) *QueryOptimization {
	optimization := &QueryOptimization{
		OriginalQuery: query,
		Suggestions:   make([]string, 0),
	}
	
	// Check if query can use materialized views
	if filters != nil {
		// If querying for statistics, suggest using materialized views
		if filters.DateFrom != nil || filters.DateTo != nil {
			optimization.Suggestions = append(optimization.Suggestions,
				"Consider using daily_activity_mv for date range queries")
		}
		
		// If querying for counterparties, suggest using materialized view
		optimization.Suggestions = append(optimization.Suggestions,
			"Consider using counterparty_stats_mv for counterparty analysis")
		
		// If querying recent data, suggest using recent_transactions_mv
		if filters.Limit <= 1000 && filters.Offset == 0 {
			optimization.Suggestions = append(optimization.Suggestions,
				"Consider using recent_transactions_mv for recent data queries")
		}
	}
	
	// Check if query can benefit from indexes
	optimization.Suggestions = append(optimization.Suggestions,
		"Ensure query uses indexed columns: address, timestamp, hash, chain")
	
	// Check if query can use query result cache
	optimization.Suggestions = append(optimization.Suggestions,
		"Query results will be cached for 5 minutes")
	
	return optimization
}

// GetQueryStats returns statistics about query performance
func (qo *QueryOptimizer) GetQueryStats(ctx context.Context) (*QueryStats, error) {
	stats := &QueryStats{}
	
	// Get cache hit rate
	query := `
		SELECT
			count() as total_queries,
			countIf(expires_at > now()) as cached_queries
		FROM query_result_cache
		WHERE created_at > now() - INTERVAL 1 HOUR
	`
	
	err := qo.clickhouse.Conn().QueryRow(ctx, query).Scan(&stats.TotalQueries, &stats.CachedQueries)
	if err != nil {
		return nil, fmt.Errorf("failed to get query stats: %w", err)
	}
	
	if stats.TotalQueries > 0 {
		stats.CacheHitRate = float64(stats.CachedQueries) / float64(stats.TotalQueries) * 100
	}
	
	return stats, nil
}

// QueryOptimization contains query optimization suggestions
type QueryOptimization struct {
	OriginalQuery string   `json:"originalQuery"`
	Suggestions   []string `json:"suggestions"`
}

// QueryStats contains query performance statistics
type QueryStats struct {
	TotalQueries   int64   `json:"totalQueries"`
	CachedQueries  int64   `json:"cachedQueries"`
	CacheHitRate   float64 `json:"cacheHitRate"` // Percentage
}

// UseRecentTransactionsView returns a query that uses the recent_transactions_mv
// This is optimized for queries requesting the most recent transactions
func (qo *QueryOptimizer) UseRecentTransactionsView(address string, limit int) string {
	return fmt.Sprintf(`
		SELECT
			hash,
			chain,
			from,
			to,
			value,
			timestamp,
			block_number,
			status,
			gas_used,
			gas_price,
			token_transfers,
			method_id,
			input
		FROM recent_transactions_mv
		WHERE address = '%s'
		  AND row_num <= %d
		ORDER BY timestamp DESC
		LIMIT %d
	`, address, limit, limit)
}

// UseDailyActivityView returns a query that uses the daily_activity_mv
// This is optimized for date range queries and activity analysis
func (qo *QueryOptimizer) UseDailyActivityView(address string, dateFrom, dateTo time.Time) string {
	return fmt.Sprintf(`
		SELECT
			date,
			sum(transaction_count) as transaction_count,
			sum(total_volume) as total_volume
		FROM daily_activity_mv
		WHERE address = '%s'
		  AND date >= '%s'
		  AND date <= '%s'
		GROUP BY date
		ORDER BY date DESC
	`, address, dateFrom.Format("2006-01-02"), dateTo.Format("2006-01-02"))
}

// UseCounterpartyStatsView returns a query that uses the counterparty_stats_mv
// This is optimized for counterparty analysis
func (qo *QueryOptimizer) UseCounterpartyStatsView(address string, limit int) string {
	return fmt.Sprintf(`
		SELECT
			counterparty,
			sum(transaction_count) as transaction_count,
			sum(total_volume) as total_volume
		FROM counterparty_stats_mv
		WHERE address = '%s'
		GROUP BY counterparty
		ORDER BY transaction_count DESC
		LIMIT %d
	`, address, limit)
}

// UseUnifiedTimelineView returns a query that uses the unified_timeline table
// This is optimized for multi-chain timeline queries
func (qo *QueryOptimizer) UseUnifiedTimelineView(address string, chains []types.ChainID, limit int, offset int) string {
	query := fmt.Sprintf(`
		SELECT
			hash,
			chain,
			from,
			to,
			value,
			timestamp,
			block_number,
			status,
			gas_used,
			gas_price,
			token_transfers,
			method_id,
			input
		FROM unified_timeline
		WHERE address = '%s'
	`, address)
	
	// Add chain filter if specified
	if len(chains) > 0 {
		chainList := ""
		for i, chain := range chains {
			if i > 0 {
				chainList += ", "
			}
			chainList += fmt.Sprintf("'%s'", chain)
		}
		query += fmt.Sprintf(" AND chain IN (%s)", chainList)
	}
	
	query += fmt.Sprintf(`
		ORDER BY timestamp DESC
		LIMIT %d OFFSET %d
	`, limit, offset)
	
	return query
}
