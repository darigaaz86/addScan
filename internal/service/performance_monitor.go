package service

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// PerformanceMonitor tracks query performance metrics
// Requirement 4.1: Ensure sub-100ms response for cached queries
type PerformanceMonitor struct {
	mu                sync.RWMutex
	cachedQueryTimes  []time.Duration
	dbQueryTimes      []time.Duration
	cacheHits         int64
	cacheMisses       int64
	slowQueries       int64 // Queries > 100ms
	totalQueries      int64
	maxSamples        int
}

// NewPerformanceMonitor creates a new performance monitor
func NewPerformanceMonitor() *PerformanceMonitor {
	return &PerformanceMonitor{
		cachedQueryTimes: make([]time.Duration, 0, 1000),
		dbQueryTimes:     make([]time.Duration, 0, 1000),
		maxSamples:       1000, // Keep last 1000 samples
	}
}

// RecordQuery records a query execution time
func (pm *PerformanceMonitor) RecordQuery(ctx context.Context, duration time.Duration, cached bool) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	pm.totalQueries++

	if cached {
		pm.cacheHits++
		pm.cachedQueryTimes = append(pm.cachedQueryTimes, duration)
		
		// Keep only last maxSamples
		if len(pm.cachedQueryTimes) > pm.maxSamples {
			pm.cachedQueryTimes = pm.cachedQueryTimes[len(pm.cachedQueryTimes)-pm.maxSamples:]
		}
	} else {
		pm.cacheMisses++
		pm.dbQueryTimes = append(pm.dbQueryTimes, duration)
		
		// Keep only last maxSamples
		if len(pm.dbQueryTimes) > pm.maxSamples {
			pm.dbQueryTimes = pm.dbQueryTimes[len(pm.dbQueryTimes)-pm.maxSamples:]
		}
	}

	// Track slow queries (> 100ms)
	if duration > 100*time.Millisecond {
		pm.slowQueries++
	}
}

// GetStats returns current performance statistics
func (pm *PerformanceMonitor) GetStats() *PerformanceStats {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	stats := &PerformanceStats{
		TotalQueries:  pm.totalQueries,
		CacheHits:     pm.cacheHits,
		CacheMisses:   pm.cacheMisses,
		SlowQueries:   pm.slowQueries,
	}

	// Calculate cache hit rate
	if pm.totalQueries > 0 {
		stats.CacheHitRate = float64(pm.cacheHits) / float64(pm.totalQueries) * 100
	}

	// Calculate average cached query time
	if len(pm.cachedQueryTimes) > 0 {
		var total time.Duration
		for _, d := range pm.cachedQueryTimes {
			total += d
		}
		stats.AvgCachedQueryMs = float64(total.Milliseconds()) / float64(len(pm.cachedQueryTimes))
	}

	// Calculate average DB query time
	if len(pm.dbQueryTimes) > 0 {
		var total time.Duration
		for _, d := range pm.dbQueryTimes {
			total += d
		}
		stats.AvgDBQueryMs = float64(total.Milliseconds()) / float64(len(pm.dbQueryTimes))
	}

	// Calculate p95 and p99 for cached queries
	if len(pm.cachedQueryTimes) > 0 {
		sorted := make([]time.Duration, len(pm.cachedQueryTimes))
		copy(sorted, pm.cachedQueryTimes)
		sortDurations(sorted)
		
		p95Index := int(float64(len(sorted)) * 0.95)
		p99Index := int(float64(len(sorted)) * 0.99)
		
		if p95Index < len(sorted) {
			stats.P95CachedQueryMs = float64(sorted[p95Index].Milliseconds())
		}
		if p99Index < len(sorted) {
			stats.P99CachedQueryMs = float64(sorted[p99Index].Milliseconds())
		}
	}

	return stats
}

// Reset resets all performance metrics
func (pm *PerformanceMonitor) Reset() {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	pm.cachedQueryTimes = make([]time.Duration, 0, 1000)
	pm.dbQueryTimes = make([]time.Duration, 0, 1000)
	pm.cacheHits = 0
	pm.cacheMisses = 0
	pm.slowQueries = 0
	pm.totalQueries = 0
}

// CheckPerformance checks if performance meets requirements
// Requirement 4.1: Cached queries should be < 100ms
func (pm *PerformanceMonitor) CheckPerformance() *PerformanceCheck {
	stats := pm.GetStats()
	
	check := &PerformanceCheck{
		Passed: true,
		Issues: make([]string, 0),
	}

	// Check average cached query time
	if stats.AvgCachedQueryMs > 100 {
		check.Passed = false
		check.Issues = append(check.Issues, 
			fmt.Sprintf("Average cached query time (%.2fms) exceeds 100ms threshold", stats.AvgCachedQueryMs))
	}

	// Check p95 cached query time
	if stats.P95CachedQueryMs > 100 {
		check.Passed = false
		check.Issues = append(check.Issues, 
			fmt.Sprintf("P95 cached query time (%.2fms) exceeds 100ms threshold", stats.P95CachedQueryMs))
	}

	// Check cache hit rate (should be > 70% for good performance)
	if stats.CacheHitRate < 70 && pm.totalQueries > 100 {
		check.Issues = append(check.Issues, 
			fmt.Sprintf("Cache hit rate (%.2f%%) is below 70%% - consider increasing cache size or TTL", stats.CacheHitRate))
	}

	return check
}

// PerformanceStats contains performance statistics
type PerformanceStats struct {
	TotalQueries      int64   `json:"totalQueries"`
	CacheHits         int64   `json:"cacheHits"`
	CacheMisses       int64   `json:"cacheMisses"`
	SlowQueries       int64   `json:"slowQueries"`
	CacheHitRate      float64 `json:"cacheHitRate"`      // Percentage
	AvgCachedQueryMs  float64 `json:"avgCachedQueryMs"`
	AvgDBQueryMs      float64 `json:"avgDBQueryMs"`
	P95CachedQueryMs  float64 `json:"p95CachedQueryMs"`
	P99CachedQueryMs  float64 `json:"p99CachedQueryMs"`
}

// PerformanceCheck contains performance check results
type PerformanceCheck struct {
	Passed bool     `json:"passed"`
	Issues []string `json:"issues"`
}

// sortDurations sorts a slice of durations in ascending order
func sortDurations(durations []time.Duration) {
	// Simple bubble sort for small slices
	n := len(durations)
	for i := 0; i < n-1; i++ {
		for j := 0; j < n-i-1; j++ {
			if durations[j] > durations[j+1] {
				durations[j], durations[j+1] = durations[j+1], durations[j]
			}
		}
	}
}
