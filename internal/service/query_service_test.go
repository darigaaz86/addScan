package service

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/address-scanner/internal/models"
	"github.com/address-scanner/internal/storage"
	"github.com/address-scanner/internal/types"
)

// Mock transaction repository for testing
type mockTransactionRepo struct {
	transactions []*models.Transaction
	getByHashFn  func(ctx context.Context, hash string) (*models.Transaction, error)
}

func (m *mockTransactionRepo) GetByAddress(ctx context.Context, address string, filters *storage.TransactionFilters) ([]*models.Transaction, error) {
	// Apply filters and return matching transactions
	result := make([]*models.Transaction, 0)
	for _, tx := range m.transactions {
		if tx.Address != address {
			continue
		}

		// Apply filters
		if filters != nil {
			// Chain filter
			if len(filters.Chains) > 0 {
				found := false
				for _, chain := range filters.Chains {
					if tx.Chain == chain {
						found = true
						break
					}
				}
				if !found {
					continue
				}
			}

			// Date range filter
			if filters.DateFrom != nil && tx.Timestamp.Before(*filters.DateFrom) {
				continue
			}
			if filters.DateTo != nil && tx.Timestamp.After(*filters.DateTo) {
				continue
			}

			// Status filter
			if filters.Status != nil && types.TransactionStatus(tx.Status) != *filters.Status {
				continue
			}
		}

		result = append(result, tx)
	}

	// Apply pagination
	if filters != nil {
		if filters.Offset > 0 && filters.Offset < len(result) {
			result = result[filters.Offset:]
		} else if filters.Offset >= len(result) {
			result = []*models.Transaction{}
		}

		if filters.Limit > 0 && filters.Limit < len(result) {
			result = result[:filters.Limit]
		}
	}

	return result, nil
}

func (m *mockTransactionRepo) CountByAddress(ctx context.Context, address string, filters *storage.TransactionFilters) (int64, error) {
	count := int64(0)
	for _, tx := range m.transactions {
		if tx.Address != address {
			continue
		}

		// Apply filters
		if filters != nil {
			// Chain filter
			if len(filters.Chains) > 0 {
				found := false
				for _, chain := range filters.Chains {
					if tx.Chain == chain {
						found = true
						break
					}
				}
				if !found {
					continue
				}
			}

			// Date range filter
			if filters.DateFrom != nil && tx.Timestamp.Before(*filters.DateFrom) {
				continue
			}
			if filters.DateTo != nil && tx.Timestamp.After(*filters.DateTo) {
				continue
			}

			// Status filter
			if filters.Status != nil && types.TransactionStatus(tx.Status) != *filters.Status {
				continue
			}
		}

		count++
	}
	return count, nil
}

func (m *mockTransactionRepo) GetByHash(ctx context.Context, hash string) (*models.Transaction, error) {
	if m.getByHashFn != nil {
		return m.getByHashFn(ctx, hash)
	}

	for _, tx := range m.transactions {
		if tx.Hash == hash {
			return tx, nil
		}
	}
	
	// Return a proper ServiceError for not found
	return nil, &types.ServiceError{
		Code:    "TRANSACTION_NOT_FOUND",
		Message: "Transaction not found",
	}
}

func TestQueryService_Query_Pagination(t *testing.T) {
	// Create test transactions
	address := "0x1234567890123456789012345678901234567890"
	transactions := make([]*models.Transaction, 100)
	for i := 0; i < 100; i++ {
		transactions[i] = &models.Transaction{
			Hash:        string(rune('a' + i)),
			Chain:       types.ChainEthereum,
			Address:     address,
			From:        "0xfrom",
			To:          "0xto",
			Value:       "1000000000000000000",
			Timestamp:   time.Now().Add(-time.Duration(i) * time.Hour),
			BlockNumber: uint64(1000 + i),
			Status:      string(types.StatusSuccess),
		}
	}

	mockRepo := &mockTransactionRepo{transactions: transactions}
	service := NewQueryService(mockRepo, nil, nil)

	tests := []struct {
		name           string
		limit          int
		offset         int
		expectedCount  int
		expectedHasMore bool
	}{
		{
			name:           "First page with default limit",
			limit:          50,
			offset:         0,
			expectedCount:  50,
			expectedHasMore: true,
		},
		{
			name:           "Second page",
			limit:          50,
			offset:         50,
			expectedCount:  50,
			expectedHasMore: false,
		},
		{
			name:           "Small page",
			limit:          10,
			offset:         0,
			expectedCount:  10,
			expectedHasMore: true,
		},
		{
			name:           "Beyond available data",
			limit:          50,
			offset:         100,
			expectedCount:  0,
			expectedHasMore: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := &QueryInput{
				Address: address,
				Limit:   tt.limit,
				Offset:  tt.offset,
			}

			result, err := service.Query(context.Background(), input)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}

			if len(result.Transactions) != tt.expectedCount {
				t.Errorf("Expected %d transactions, got %d", tt.expectedCount, len(result.Transactions))
			}

			if result.Pagination.HasMore != tt.expectedHasMore {
				t.Errorf("Expected HasMore=%v, got %v", tt.expectedHasMore, result.Pagination.HasMore)
			}

			if result.Pagination.Total != 100 {
				t.Errorf("Expected total=100, got %d", result.Pagination.Total)
			}
		})
	}
}

func TestQueryService_Query_DateRangeFilter(t *testing.T) {
	address := "0x1234567890123456789012345678901234567890"
	now := time.Now()

	transactions := []*models.Transaction{
		{
			Hash:        "tx1",
			Chain:       types.ChainEthereum,
			Address:     address,
			From:        "0xfrom",
			To:          "0xto",
			Value:       "1000000000000000000",
			Timestamp:   now.Add(-10 * time.Hour),
			BlockNumber: 1000,
			Status:      string(types.StatusSuccess),
		},
		{
			Hash:        "tx2",
			Chain:       types.ChainEthereum,
			Address:     address,
			From:        "0xfrom",
			To:          "0xto",
			Value:       "2000000000000000000",
			Timestamp:   now.Add(-5 * time.Hour),
			BlockNumber: 1001,
			Status:      string(types.StatusSuccess),
		},
		{
			Hash:        "tx3",
			Chain:       types.ChainEthereum,
			Address:     address,
			From:        "0xfrom",
			To:          "0xto",
			Value:       "3000000000000000000",
			Timestamp:   now.Add(-1 * time.Hour),
			BlockNumber: 1002,
			Status:      string(types.StatusSuccess),
		},
	}

	mockRepo := &mockTransactionRepo{transactions: transactions}
	service := NewQueryService(mockRepo, nil, nil)

	// Test date range filter
	dateFrom := now.Add(-6 * time.Hour)
	dateTo := now.Add(-2 * time.Hour)

	input := &QueryInput{
		Address:  address,
		DateFrom: &dateFrom,
		DateTo:   &dateTo,
		Limit:    100,
	}

	result, err := service.Query(context.Background(), input)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// Should only return tx2 (within range)
	if len(result.Transactions) != 1 {
		t.Errorf("Expected 1 transaction, got %d", len(result.Transactions))
	}

	if len(result.Transactions) > 0 && result.Transactions[0].Hash != "tx2" {
		t.Errorf("Expected tx2, got %s", result.Transactions[0].Hash)
	}
}

func TestQueryService_Query_ChainFilter(t *testing.T) {
	address := "0x1234567890123456789012345678901234567890"

	transactions := []*models.Transaction{
		{
			Hash:        "tx1",
			Chain:       types.ChainEthereum,
			Address:     address,
			From:        "0xfrom",
			To:          "0xto",
			Value:       "1000000000000000000",
			Timestamp:   time.Now(),
			BlockNumber: 1000,
			Status:      string(types.StatusSuccess),
		},
		{
			Hash:        "tx2",
			Chain:       types.ChainPolygon,
			Address:     address,
			From:        "0xfrom",
			To:          "0xto",
			Value:       "2000000000000000000",
			Timestamp:   time.Now(),
			BlockNumber: 1001,
			Status:      string(types.StatusSuccess),
		},
		{
			Hash:        "tx3",
			Chain:       types.ChainArbitrum,
			Address:     address,
			From:        "0xfrom",
			To:          "0xto",
			Value:       "3000000000000000000",
			Timestamp:   time.Now(),
			BlockNumber: 1002,
			Status:      string(types.StatusSuccess),
		},
	}

	mockRepo := &mockTransactionRepo{transactions: transactions}
	service := NewQueryService(mockRepo, nil, nil)

	// Test chain filter
	input := &QueryInput{
		Address: address,
		Chains:  []types.ChainID{types.ChainEthereum, types.ChainPolygon},
		Limit:   100,
	}

	result, err := service.Query(context.Background(), input)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// Should return tx1 and tx2 (Ethereum and Polygon)
	if len(result.Transactions) != 2 {
		t.Errorf("Expected 2 transactions, got %d", len(result.Transactions))
	}
}

func TestQueryService_SearchByHash(t *testing.T) {
	transactions := []*models.Transaction{
		{
			Hash:        "0xabc123",
			Chain:       types.ChainEthereum,
			Address:     "0x1234567890123456789012345678901234567890",
			From:        "0xfrom",
			To:          "0xto",
			Value:       "1000000000000000000",
			Timestamp:   time.Now(),
			BlockNumber: 1000,
			Status:      string(types.StatusSuccess),
		},
	}

	mockRepo := &mockTransactionRepo{transactions: transactions}
	service := NewQueryService(mockRepo, nil, nil)

	// Test successful search
	tx, err := service.SearchByHash(context.Background(), "0xabc123")
	if err != nil {
		t.Fatalf("SearchByHash failed: %v", err)
	}

	if tx.Hash != "0xabc123" {
		t.Errorf("Expected hash 0xabc123, got %s", tx.Hash)
	}

	// Test not found
	_, err = service.SearchByHash(context.Background(), "0xnotfound")
	if err == nil {
		t.Error("Expected error for not found transaction")
	}

	// Check error type
	if serviceErr, ok := err.(*types.ServiceError); ok {
		if serviceErr.Code != "TRANSACTION_NOT_FOUND" {
			t.Errorf("Expected TRANSACTION_NOT_FOUND error code, got %s", serviceErr.Code)
		}
	}
}

func TestQueryService_ValidateQueryInput(t *testing.T) {
	service := NewQueryService(nil, nil, nil)

	tests := []struct {
		name        string
		input       *QueryInput
		expectError bool
	}{
		{
			name: "Valid input",
			input: &QueryInput{
				Address: "0x1234567890123456789012345678901234567890",
				Limit:   50,
				Offset:  0,
			},
			expectError: false,
		},
		{
			name: "Missing address",
			input: &QueryInput{
				Limit:  50,
				Offset: 0,
			},
			expectError: true,
		},
		{
			name: "Invalid limit",
			input: &QueryInput{
				Address: "0x1234567890123456789012345678901234567890",
				Limit:   2000, // Exceeds max
			},
			expectError: true,
		},
		{
			name: "Negative offset",
			input: &QueryInput{
				Address: "0x1234567890123456789012345678901234567890",
				Offset:  -1,
			},
			expectError: true,
		},
		{
			name: "Invalid sort field",
			input: &QueryInput{
				Address: "0x1234567890123456789012345678901234567890",
				SortBy:  "invalid_field",
			},
			expectError: true,
		},
		{
			name: "Invalid sort order",
			input: &QueryInput{
				Address:   "0x1234567890123456789012345678901234567890",
				SortOrder: "invalid",
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := service.validateQueryInput(tt.input)
			if tt.expectError && err == nil {
				t.Error("Expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("Expected no error but got: %v", err)
			}
		})
	}
}

func TestQueryService_ShouldUseCache(t *testing.T) {
	service := NewQueryService(nil, nil, nil)
	service.SetCacheWindowSize(1000)

	tests := []struct {
		name     string
		input    *QueryInput
		expected bool
	}{
		{
			name: "Simple query within cache window",
			input: &QueryInput{
				Address: "0x1234567890123456789012345678901234567890",
				Limit:   50,
				Offset:  0,
			},
			expected: true,
		},
		{
			name: "Query with date filter",
			input: &QueryInput{
				Address:  "0x1234567890123456789012345678901234567890",
				DateFrom: &time.Time{},
			},
			expected: false,
		},
		{
			name: "Query with value filter",
			input: &QueryInput{
				Address:  "0x1234567890123456789012345678901234567890",
				MinValue: func() *float64 { v := 1000.0; return &v }(),
			},
			expected: false,
		},
		{
			name: "Query beyond cache window",
			input: &QueryInput{
				Address: "0x1234567890123456789012345678901234567890",
				Offset:  1000,
			},
			expected: false,
		},
		{
			name: "Query with chain filter",
			input: &QueryInput{
				Address: "0x1234567890123456789012345678901234567890",
				Chains:  []types.ChainID{types.ChainEthereum},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := service.shouldUseCache(tt.input)
			if result != tt.expected {
				t.Errorf("Expected shouldUseCache=%v, got %v", tt.expected, result)
			}
		})
	}
}


func TestQueryService_QueryRouting_SimpleToCache(t *testing.T) {
	// Test that simple queries attempt to use cache
	address := "0x1234567890123456789012345678901234567890"
	transactions := []*models.Transaction{
		{
			Hash:        "tx1",
			Chain:       types.ChainEthereum,
			Address:     address,
			From:        "0xfrom",
			To:          "0xto",
			Value:       "1000000000000000000",
			Timestamp:   time.Now(),
			BlockNumber: 1000,
			Status:      string(types.StatusSuccess),
		},
	}

	mockRepo := &mockTransactionRepo{transactions: transactions}
	service := NewQueryService(mockRepo, nil, nil)

	// Simple query - should try cache first (but will fall back to ClickHouse in our mock)
	input := &QueryInput{
		Address: address,
		Limit:   50,
		Offset:  0,
	}

	result, err := service.Query(context.Background(), input)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// Verify query completed successfully
	if len(result.Transactions) != 1 {
		t.Errorf("Expected 1 transaction, got %d", len(result.Transactions))
	}

	// In our implementation, cache miss falls back to ClickHouse
	// So cached should be false
	if result.Cached {
		t.Error("Expected cached=false for cache miss")
	}
}

func TestQueryService_QueryRouting_ComplexToClickHouse(t *testing.T) {
	// Test that complex queries go directly to ClickHouse
	address := "0x1234567890123456789012345678901234567890"
	now := time.Now()
	dateFrom := now.Add(-24 * time.Hour)

	transactions := []*models.Transaction{
		{
			Hash:        "tx1",
			Chain:       types.ChainEthereum,
			Address:     address,
			From:        "0xfrom",
			To:          "0xto",
			Value:       "1000000000000000000",
			Timestamp:   now.Add(-12 * time.Hour),
			BlockNumber: 1000,
			Status:      string(types.StatusSuccess),
		},
	}

	mockRepo := &mockTransactionRepo{transactions: transactions}
	service := NewQueryService(mockRepo, nil, nil)

	// Complex query with date filter - should go directly to ClickHouse
	input := &QueryInput{
		Address:  address,
		DateFrom: &dateFrom,
		Limit:    50,
		Offset:   0,
	}

	result, err := service.Query(context.Background(), input)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// Verify query completed successfully
	if len(result.Transactions) != 1 {
		t.Errorf("Expected 1 transaction, got %d", len(result.Transactions))
	}

	// Should not be cached since it's a complex query
	if result.Cached {
		t.Error("Expected cached=false for complex query")
	}
}

func TestQueryService_QueryRouting_BeyondCacheWindow(t *testing.T) {
	// Test that queries beyond cache window go to ClickHouse
	address := "0x1234567890123456789012345678901234567890"
	transactions := make([]*models.Transaction, 2000)
	for i := 0; i < 2000; i++ {
		transactions[i] = &models.Transaction{
			Hash:        fmt.Sprintf("tx%d", i),
			Chain:       types.ChainEthereum,
			Address:     address,
			From:        "0xfrom",
			To:          "0xto",
			Value:       "1000000000000000000",
			Timestamp:   time.Now().Add(-time.Duration(i) * time.Hour),
			BlockNumber: uint64(1000 + i),
			Status:      string(types.StatusSuccess),
		}
	}

	mockRepo := &mockTransactionRepo{transactions: transactions}
	service := NewQueryService(mockRepo, nil, nil)
	service.SetCacheWindowSize(1000)

	// Query beyond cache window - should go directly to ClickHouse
	input := &QueryInput{
		Address: address,
		Limit:   50,
		Offset:  1500, // Beyond cache window of 1000
	}

	result, err := service.Query(context.Background(), input)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// Verify query completed successfully
	if len(result.Transactions) != 50 {
		t.Errorf("Expected 50 transactions, got %d", len(result.Transactions))
	}

	// Should not be cached since offset is beyond cache window
	if result.Cached {
		t.Error("Expected cached=false for query beyond cache window")
	}
}

func TestQueryService_QueryRouting_MultipleFilters(t *testing.T) {
	// Test that queries with multiple filters go to ClickHouse
	address := "0x1234567890123456789012345678901234567890"
	now := time.Now()
	dateFrom := now.Add(-24 * time.Hour)
	minValue := 1000.0

	transactions := []*models.Transaction{
		{
			Hash:        "tx1",
			Chain:       types.ChainEthereum,
			Address:     address,
			From:        "0xfrom",
			To:          "0xto",
			Value:       "2000000000000000000",
			Timestamp:   now.Add(-12 * time.Hour),
			BlockNumber: 1000,
			Status:      string(types.StatusSuccess),
		},
	}

	mockRepo := &mockTransactionRepo{transactions: transactions}
	service := NewQueryService(mockRepo, nil, nil)

	// Query with multiple filters - should go directly to ClickHouse
	input := &QueryInput{
		Address:  address,
		DateFrom: &dateFrom,
		MinValue: &minValue,
		Chains:   []types.ChainID{types.ChainEthereum},
		Limit:    50,
		Offset:   0,
	}

	result, err := service.Query(context.Background(), input)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// Verify query completed successfully
	if len(result.Transactions) != 1 {
		t.Errorf("Expected 1 transaction, got %d", len(result.Transactions))
	}

	// Should not be cached since it has complex filters
	if result.Cached {
		t.Error("Expected cached=false for query with multiple filters")
	}
}


func TestQueryService_SearchByHash_Performance(t *testing.T) {
	// Test that hash search completes quickly
	// Requirement 9.1: Transaction hash search within 200ms
	transactions := []*models.Transaction{
		{
			Hash:        "0xabc123def456",
			Chain:       types.ChainEthereum,
			Address:     "0x1234567890123456789012345678901234567890",
			From:        "0xfrom",
			To:          "0xto",
			Value:       "1000000000000000000",
			Timestamp:   time.Now(),
			BlockNumber: 1000,
			Status:      string(types.StatusSuccess),
		},
	}

	mockRepo := &mockTransactionRepo{transactions: transactions}
	service := NewQueryService(mockRepo, nil, nil)

	start := time.Now()
	tx, err := service.SearchByHash(context.Background(), "0xabc123def456")
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("SearchByHash failed: %v", err)
	}

	if tx.Hash != "0xabc123def456" {
		t.Errorf("Expected hash 0xabc123def456, got %s", tx.Hash)
	}

	// In a real implementation with ClickHouse bloom filter index,
	// this should be under 200ms. For our mock, it should be instant.
	if elapsed > 200*time.Millisecond {
		t.Errorf("Hash search took %v, expected < 200ms", elapsed)
	}
}

func TestQueryService_SearchByHash_NotFound(t *testing.T) {
	// Test not-found case handling
	// Requirement 9.3: Handle not-found cases
	mockRepo := &mockTransactionRepo{transactions: []*models.Transaction{}}
	service := NewQueryService(mockRepo, nil, nil)

	_, err := service.SearchByHash(context.Background(), "0xnonexistent")
	if err == nil {
		t.Fatal("Expected error for non-existent transaction")
	}

	// Check that it's a ServiceError with correct code
	serviceErr, ok := err.(*types.ServiceError)
	if !ok {
		t.Fatalf("Expected ServiceError, got %T", err)
	}

	if serviceErr.Code != "TRANSACTION_NOT_FOUND" {
		t.Errorf("Expected error code TRANSACTION_NOT_FOUND, got %s", serviceErr.Code)
	}

	if serviceErr.Message == "" {
		t.Error("Expected non-empty error message")
	}
}

func TestQueryService_SearchByHash_CrossAddress(t *testing.T) {
	// Test that hash search works across multiple addresses
	// Requirement 9.2: Cross-address hash search
	transactions := []*models.Transaction{
		{
			Hash:        "0xhash1",
			Chain:       types.ChainEthereum,
			Address:     "0xaddress1",
			From:        "0xfrom",
			To:          "0xto",
			Value:       "1000000000000000000",
			Timestamp:   time.Now(),
			BlockNumber: 1000,
			Status:      string(types.StatusSuccess),
		},
		{
			Hash:        "0xhash2",
			Chain:       types.ChainPolygon,
			Address:     "0xaddress2",
			From:        "0xfrom",
			To:          "0xto",
			Value:       "2000000000000000000",
			Timestamp:   time.Now(),
			BlockNumber: 1001,
			Status:      string(types.StatusSuccess),
		},
		{
			Hash:        "0xhash3",
			Chain:       types.ChainArbitrum,
			Address:     "0xaddress3",
			From:        "0xfrom",
			To:          "0xto",
			Value:       "3000000000000000000",
			Timestamp:   time.Now(),
			BlockNumber: 1002,
			Status:      string(types.StatusSuccess),
		},
	}

	mockRepo := &mockTransactionRepo{transactions: transactions}
	service := NewQueryService(mockRepo, nil, nil)

	// Search for transaction from address1
	tx1, err := service.SearchByHash(context.Background(), "0xhash1")
	if err != nil {
		t.Fatalf("SearchByHash failed for hash1: %v", err)
	}
	if tx1.Hash != "0xhash1" {
		t.Errorf("Expected hash 0xhash1, got %s", tx1.Hash)
	}
	if tx1.Chain != types.ChainEthereum {
		t.Errorf("Expected chain ethereum, got %s", tx1.Chain)
	}

	// Search for transaction from address2
	tx2, err := service.SearchByHash(context.Background(), "0xhash2")
	if err != nil {
		t.Fatalf("SearchByHash failed for hash2: %v", err)
	}
	if tx2.Hash != "0xhash2" {
		t.Errorf("Expected hash 0xhash2, got %s", tx2.Hash)
	}
	if tx2.Chain != types.ChainPolygon {
		t.Errorf("Expected chain polygon, got %s", tx2.Chain)
	}

	// Search for transaction from address3
	tx3, err := service.SearchByHash(context.Background(), "0xhash3")
	if err != nil {
		t.Fatalf("SearchByHash failed for hash3: %v", err)
	}
	if tx3.Hash != "0xhash3" {
		t.Errorf("Expected hash 0xhash3, got %s", tx3.Hash)
	}
	if tx3.Chain != types.ChainArbitrum {
		t.Errorf("Expected chain arbitrum, got %s", tx3.Chain)
	}
}

func TestQueryService_SearchByHash_EmptyHash(t *testing.T) {
	// Test that empty hash is rejected
	service := NewQueryService(&mockTransactionRepo{}, nil, nil)

	_, err := service.SearchByHash(context.Background(), "")
	if err == nil {
		t.Fatal("Expected error for empty hash")
	}

	if err.Error() != "transaction hash is required" {
		t.Errorf("Expected 'transaction hash is required' error, got: %v", err)
	}
}
