package service

import (
	"testing"
	"time"

	"github.com/address-scanner/internal/types"
)

// TestMergeAndSortTransactions tests the core timeline merging logic
func TestMergeAndSortTransactions(t *testing.T) {
	service := &TimelineService{}

	tests := []struct {
		name     string
		input    []*types.NormalizedTransaction
		expected []string // Expected order of transaction hashes
	}{
		{
			name:     "empty transactions",
			input:    []*types.NormalizedTransaction{},
			expected: []string{},
		},
		{
			name: "single transaction",
			input: []*types.NormalizedTransaction{
				{Hash: "0x1", Chain: types.ChainEthereum, Timestamp: 1000},
			},
			expected: []string{"0x1"},
		},
		{
			name: "multiple chains sorted by timestamp descending",
			input: []*types.NormalizedTransaction{
				{Hash: "0x1", Chain: types.ChainEthereum, Timestamp: 1000},
				{Hash: "0x2", Chain: types.ChainPolygon, Timestamp: 2000},
				{Hash: "0x3", Chain: types.ChainArbitrum, Timestamp: 1500},
			},
			expected: []string{"0x2", "0x3", "0x1"}, // Most recent first
		},
		{
			name: "same timestamp different chains",
			input: []*types.NormalizedTransaction{
				{Hash: "0xa", Chain: types.ChainEthereum, Timestamp: 1000},
				{Hash: "0xb", Chain: types.ChainPolygon, Timestamp: 1000},
				{Hash: "0xc", Chain: types.ChainArbitrum, Timestamp: 1000},
			},
			expected: []string{"0xc", "0xb", "0xa"}, // Sorted by hash when timestamps equal
		},
		{
			name: "mixed timestamps and chains",
			input: []*types.NormalizedTransaction{
				{Hash: "0x1", Chain: types.ChainEthereum, Timestamp: 3000},
				{Hash: "0x2", Chain: types.ChainPolygon, Timestamp: 1000},
				{Hash: "0x3", Chain: types.ChainArbitrum, Timestamp: 2000},
				{Hash: "0x4", Chain: types.ChainOptimism, Timestamp: 3000},
				{Hash: "0x5", Chain: types.ChainBase, Timestamp: 1500},
			},
			expected: []string{"0x4", "0x1", "0x3", "0x5", "0x2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := service.mergeAndSortTransactions(tt.input)

			if len(result) != len(tt.expected) {
				t.Errorf("expected %d transactions, got %d", len(tt.expected), len(result))
				return
			}

			for i, expectedHash := range tt.expected {
				if result[i].Hash != expectedHash {
					t.Errorf("at index %d: expected hash %s, got %s", i, expectedHash, result[i].Hash)
				}
			}
		})
	}
}

// TestMergeAndSortTransactions_TimestampOrdering tests that transactions are properly ordered by timestamp
func TestMergeAndSortTransactions_TimestampOrdering(t *testing.T) {
	service := &TimelineService{}

	// Create transactions with various timestamps
	now := time.Now().Unix()
	transactions := []*types.NormalizedTransaction{
		{Hash: "0x1", Chain: types.ChainEthereum, Timestamp: now - 3600},     // 1 hour ago
		{Hash: "0x2", Chain: types.ChainPolygon, Timestamp: now},             // now
		{Hash: "0x3", Chain: types.ChainArbitrum, Timestamp: now - 7200},     // 2 hours ago
		{Hash: "0x4", Chain: types.ChainOptimism, Timestamp: now - 1800},     // 30 minutes ago
		{Hash: "0x5", Chain: types.ChainBase, Timestamp: now - 86400},        // 1 day ago
	}

	result := service.mergeAndSortTransactions(transactions)

	// Verify descending order (most recent first)
	for i := 0; i < len(result)-1; i++ {
		if result[i].Timestamp < result[i+1].Timestamp {
			t.Errorf("transactions not in descending order: tx[%d].Timestamp=%d < tx[%d].Timestamp=%d",
				i, result[i].Timestamp, i+1, result[i+1].Timestamp)
		}
	}

	// Verify the expected order
	expectedOrder := []string{"0x2", "0x4", "0x1", "0x3", "0x5"}
	for i, expectedHash := range expectedOrder {
		if result[i].Hash != expectedHash {
			t.Errorf("at index %d: expected hash %s, got %s", i, expectedHash, result[i].Hash)
		}
	}
}

// TestMergeAndSortTransactions_DeterministicOrdering tests that equal timestamps produce deterministic ordering
func TestMergeAndSortTransactions_DeterministicOrdering(t *testing.T) {
	service := &TimelineService{}

	// Create transactions with same timestamp
	sameTimestamp := time.Now().Unix()
	transactions := []*types.NormalizedTransaction{
		{Hash: "0xaaa", Chain: types.ChainEthereum, Timestamp: sameTimestamp},
		{Hash: "0xbbb", Chain: types.ChainPolygon, Timestamp: sameTimestamp},
		{Hash: "0xccc", Chain: types.ChainArbitrum, Timestamp: sameTimestamp},
	}

	// Run multiple times to ensure deterministic ordering
	for i := 0; i < 10; i++ {
		result := service.mergeAndSortTransactions(transactions)

		// Verify all have same timestamp
		for _, tx := range result {
			if tx.Timestamp != sameTimestamp {
				t.Errorf("unexpected timestamp: got %d, want %d", tx.Timestamp, sameTimestamp)
			}
		}

		// Verify deterministic ordering by hash (descending)
		expectedOrder := []string{"0xccc", "0xbbb", "0xaaa"}
		for j, expectedHash := range expectedOrder {
			if result[j].Hash != expectedHash {
				t.Errorf("run %d, index %d: expected hash %s, got %s", i, j, expectedHash, result[j].Hash)
			}
		}
	}
}

// TestConvertToNormalizedTransactions tests conversion from storage to normalized format
func TestConvertToNormalizedTransactions(t *testing.T) {
	// This test would require models.Transaction which we'll skip for now
	// The conversion is straightforward field mapping
	t.Skip("Requires models.Transaction - tested via integration tests")
}

// TestConvertToStorageFilters tests filter conversion
func TestConvertToStorageFilters(t *testing.T) {
	service := &TimelineService{}

	t.Run("nil filters", func(t *testing.T) {
		result := service.convertToStorageFilters(nil)
		if result.SortBy != "timestamp" {
			t.Errorf("expected SortBy=timestamp, got %s", result.SortBy)
		}
		if result.SortOrder != "desc" {
			t.Errorf("expected SortOrder=desc, got %s", result.SortOrder)
		}
	})

	t.Run("with filters", func(t *testing.T) {
		dateFrom := time.Now().Add(-24 * time.Hour)
		dateTo := time.Now()
		minValue := 1.0
		maxValue := 100.0
		status := types.StatusSuccess

		filters := &TimelineFilters{
			Chains:   []types.ChainID{types.ChainEthereum, types.ChainPolygon},
			DateFrom: &dateFrom,
			DateTo:   &dateTo,
			MinValue: &minValue,
			MaxValue: &maxValue,
			Status:   &status,
			Limit:    50,
			Offset:   10,
		}

		result := service.convertToStorageFilters(filters)

		if len(result.Chains) != 2 {
			t.Errorf("expected 2 chains, got %d", len(result.Chains))
		}
		if result.DateFrom == nil || !result.DateFrom.Equal(dateFrom) {
			t.Errorf("DateFrom not set correctly")
		}
		if result.DateTo == nil || !result.DateTo.Equal(dateTo) {
			t.Errorf("DateTo not set correctly")
		}
		if result.MinValue == nil || *result.MinValue != minValue {
			t.Errorf("MinValue not set correctly")
		}
		if result.MaxValue == nil || *result.MaxValue != maxValue {
			t.Errorf("MaxValue not set correctly")
		}
		if result.Status == nil || *result.Status != status {
			t.Errorf("Status not set correctly")
		}
		if result.Limit != 50 {
			t.Errorf("expected Limit=50, got %d", result.Limit)
		}
		if result.Offset != 10 {
			t.Errorf("expected Offset=10, got %d", result.Offset)
		}
	})
}

// TestBuildCacheKey tests cache key generation
func TestBuildCacheKey(t *testing.T) {
	service := &TimelineService{}

	tests := []struct {
		name     string
		address  string
		filters  *TimelineFilters
		expected string
	}{
		{
			name:     "no filters",
			address:  "0x1234",
			filters:  nil,
			expected: "timeline:0x1234",
		},
		{
			name:    "empty filters",
			address: "0x1234",
			filters: &TimelineFilters{},
			expected: "timeline:0x1234",
		},
		{
			name:    "with chains",
			address: "0x1234",
			filters: &TimelineFilters{
				Chains: []types.ChainID{types.ChainEthereum, types.ChainPolygon},
			},
			expected: "timeline:0x1234:chains:[ethereum polygon]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := service.buildCacheKey(tt.address, tt.filters)
			if result != tt.expected {
				t.Errorf("expected cache key %s, got %s", tt.expected, result)
			}
		})
	}
}
