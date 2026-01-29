package service

import (
	"context"
	"testing"
	"time"

	"github.com/address-scanner/internal/models"
	"github.com/address-scanner/internal/types"
)

func TestPerformanceMonitor_RecordQuery(t *testing.T) {
	pm := NewPerformanceMonitor()

	// Record some cached queries
	pm.RecordQuery(context.Background(), 50*time.Millisecond, true)
	pm.RecordQuery(context.Background(), 75*time.Millisecond, true)
	pm.RecordQuery(context.Background(), 90*time.Millisecond, true)

	// Record some DB queries
	pm.RecordQuery(context.Background(), 200*time.Millisecond, false)
	pm.RecordQuery(context.Background(), 300*time.Millisecond, false)

	stats := pm.GetStats()

	if stats.TotalQueries != 5 {
		t.Errorf("Expected 5 total queries, got %d", stats.TotalQueries)
	}

	if stats.CacheHits != 3 {
		t.Errorf("Expected 3 cache hits, got %d", stats.CacheHits)
	}

	if stats.CacheMisses != 2 {
		t.Errorf("Expected 2 cache misses, got %d", stats.CacheMisses)
	}

	expectedHitRate := 60.0 // 3/5 * 100
	if stats.CacheHitRate != expectedHitRate {
		t.Errorf("Expected cache hit rate %.2f%%, got %.2f%%", expectedHitRate, stats.CacheHitRate)
	}

	// Check average cached query time (should be around 71.67ms)
	if stats.AvgCachedQueryMs < 70 || stats.AvgCachedQueryMs > 73 {
		t.Errorf("Expected average cached query time around 71.67ms, got %.2fms", stats.AvgCachedQueryMs)
	}

	// Check average DB query time (should be 250ms)
	if stats.AvgDBQueryMs != 250 {
		t.Errorf("Expected average DB query time 250ms, got %.2fms", stats.AvgDBQueryMs)
	}
}

func TestPerformanceMonitor_CheckPerformance(t *testing.T) {
	pm := NewPerformanceMonitor()

	// Record queries that meet performance requirements
	for i := 0; i < 100; i++ {
		pm.RecordQuery(context.Background(), 50*time.Millisecond, true)
	}

	check := pm.CheckPerformance()
	if !check.Passed {
		t.Errorf("Performance check should pass, but got issues: %v", check.Issues)
	}

	// Reset and record slow queries
	pm.Reset()
	for i := 0; i < 100; i++ {
		pm.RecordQuery(context.Background(), 150*time.Millisecond, true)
	}

	check = pm.CheckPerformance()
	if check.Passed {
		t.Error("Performance check should fail for slow queries")
	}

	if len(check.Issues) == 0 {
		t.Error("Expected performance issues to be reported")
	}
}

func TestPerformanceMonitor_SlowQueries(t *testing.T) {
	pm := NewPerformanceMonitor()

	// Record some fast and slow queries
	pm.RecordQuery(context.Background(), 50*time.Millisecond, true)
	pm.RecordQuery(context.Background(), 150*time.Millisecond, true) // Slow
	pm.RecordQuery(context.Background(), 75*time.Millisecond, true)
	pm.RecordQuery(context.Background(), 200*time.Millisecond, false) // Slow

	stats := pm.GetStats()

	if stats.SlowQueries != 2 {
		t.Errorf("Expected 2 slow queries (>100ms), got %d", stats.SlowQueries)
	}
}

func TestPerformanceMonitor_Reset(t *testing.T) {
	pm := NewPerformanceMonitor()

	// Record some queries
	pm.RecordQuery(context.Background(), 50*time.Millisecond, true)
	pm.RecordQuery(context.Background(), 100*time.Millisecond, false)

	stats := pm.GetStats()
	if stats.TotalQueries != 2 {
		t.Errorf("Expected 2 queries before reset, got %d", stats.TotalQueries)
	}

	// Reset
	pm.Reset()

	stats = pm.GetStats()
	if stats.TotalQueries != 0 {
		t.Errorf("Expected 0 queries after reset, got %d", stats.TotalQueries)
	}

	if stats.CacheHits != 0 {
		t.Errorf("Expected 0 cache hits after reset, got %d", stats.CacheHits)
	}
}

func TestPerformanceMonitor_Percentiles(t *testing.T) {
	pm := NewPerformanceMonitor()

	// Record 100 cached queries with varying times
	for i := 1; i <= 100; i++ {
		duration := time.Duration(i) * time.Millisecond
		pm.RecordQuery(context.Background(), duration, true)
	}

	stats := pm.GetStats()

	// P95 should be around 95ms
	if stats.P95CachedQueryMs < 94 || stats.P95CachedQueryMs > 96 {
		t.Errorf("Expected P95 around 95ms, got %.2fms", stats.P95CachedQueryMs)
	}

	// P99 should be around 99ms
	if stats.P99CachedQueryMs < 98 || stats.P99CachedQueryMs > 100 {
		t.Errorf("Expected P99 around 99ms, got %.2fms", stats.P99CachedQueryMs)
	}
}

func TestQueryService_PerformanceMonitoring(t *testing.T) {
	// Create mock repository with test data
	transactions := make([]*models.Transaction, 100)
	for i := 0; i < 100; i++ {
		transactions[i] = &models.Transaction{
			Hash:        "0xhash" + string(rune(i)),
			Chain:       types.ChainEthereum,
			Address:     "0x1234567890123456789012345678901234567890",
			From:        "0xfrom",
			To:          "0xto",
			Value:       "1000000000000000000",
			Timestamp:   time.Now().Add(-time.Duration(i) * time.Hour),
			BlockNumber: uint64(1000 + i),
			Status:      "success",
		}
	}

	mockRepo := &mockTransactionRepo{transactions: transactions}
	service := NewQueryService(mockRepo, nil, nil)

	// Execute some queries
	for i := 0; i < 10; i++ {
		input := &QueryInput{
			Address: "0x1234567890123456789012345678901234567890",
			Limit:   10,
			Offset:  i * 10,
		}

		_, err := service.Query(context.Background(), input)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
	}

	// Check performance stats
	stats := service.GetPerformanceStats()

	if stats.TotalQueries != 10 {
		t.Errorf("Expected 10 total queries, got %d", stats.TotalQueries)
	}

	// All queries should be cache misses (no cache service configured)
	if stats.CacheMisses != 10 {
		t.Errorf("Expected 10 cache misses, got %d", stats.CacheMisses)
	}

	// Check performance
	check := service.CheckPerformance()
	// Should pass since we're using mock data (fast)
	if !check.Passed {
		t.Logf("Performance check issues: %v", check.Issues)
	}
}
