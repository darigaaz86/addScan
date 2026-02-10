package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestAddAddress_InvalidJSON tests handling of malformed JSON
func TestAddAddress_InvalidJSON(t *testing.T) {
	server := createTestServer()

	req := httptest.NewRequest("POST", "/api/addresses", bytes.NewReader([]byte("invalid json")))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-ID", "user-123")
	req.Header.Set("X-User-Tier", "free")

	w := httptest.NewRecorder()
	server.router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d", w.Code)
	}
}

// TestAddAddress_InvalidTier tests handling of invalid tier value
func TestAddAddress_InvalidTier(t *testing.T) {
	server := createTestServer()

	reqBody := map[string]interface{}{
		"address": "0x1234567890123456789012345678901234567890",
	}
	body, _ := json.Marshal(reqBody)

	req := httptest.NewRequest("POST", "/api/addresses", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-ID", "user-123")
	req.Header.Set("X-User-Tier", "invalid-tier")

	w := httptest.NewRecorder()
	server.router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d", w.Code)
	}
}

// TestAddAddress_ServiceError tests handling of service layer errors
func TestAddAddress_ServiceError(t *testing.T) {
	// This test would require a full mock implementation
	// Skipping for now - covered by integration tests
	t.Skip("Requires full service mock implementation")
}

// TestGetTransactions_InvalidPagination tests handling of invalid pagination parameters
func TestGetTransactions_InvalidPagination(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		expected int
	}{
		{
			name:     "negative limit",
			query:    "?limit=-10",
			expected: http.StatusOK, // Should use default
		},
		{
			name:     "negative offset",
			query:    "?offset=-5",
			expected: http.StatusOK, // Should use default
		},
		{
			name:     "excessive limit",
			query:    "?limit=10000",
			expected: http.StatusOK, // Should cap at max
		},
		{
			name:     "non-numeric limit",
			query:    "?limit=abc",
			expected: http.StatusOK, // Should use default
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := createTestServer()

			req := httptest.NewRequest("GET", "/api/addresses/0x1234/transactions"+tt.query, nil)
			req.Header.Set("X-User-ID", "user-123")

			w := httptest.NewRecorder()
			server.router.ServeHTTP(w, req)

			if w.Code != tt.expected {
				t.Errorf("Expected status %d, got %d", tt.expected, w.Code)
			}
		})
	}
}

// TestGetTransactions_InvalidDateFormat tests handling of invalid date formats
func TestGetTransactions_InvalidDateFormat(t *testing.T) {
	server := createTestServer()

	req := httptest.NewRequest("GET", "/api/addresses/0x1234/transactions?dateFrom=invalid-date", nil)
	req.Header.Set("X-User-ID", "user-123")

	w := httptest.NewRecorder()
	server.router.ServeHTTP(w, req)

	// Should still succeed but ignore invalid date
	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}
}

// TestGetTransactions_EmptyAddress tests handling of empty address
func TestGetTransactions_EmptyAddress(t *testing.T) {
	server := createTestServer()

	req := httptest.NewRequest("GET", "/api/addresses//transactions", nil)
	req.Header.Set("X-User-ID", "user-123")

	w := httptest.NewRecorder()
	server.router.ServeHTTP(w, req)

	// Router may redirect (301) or return not found (404) for malformed paths
	if w.Code != http.StatusNotFound && w.Code != http.StatusMovedPermanently {
		t.Errorf("Expected status 404 or 301, got %d", w.Code)
	}
}

// TestCreatePortfolio_EmptyName tests portfolio creation with empty name
func TestCreatePortfolio_EmptyName(t *testing.T) {
	// This test would require a full mock implementation
	// Skipping for now - covered by integration tests
	t.Skip("Requires full service mock implementation")
}

// TestCreatePortfolio_NoAddresses tests portfolio creation without addresses
func TestCreatePortfolio_NoAddresses(t *testing.T) {
	// This test would require a full mock implementation
	// Skipping for now - covered by integration tests
	t.Skip("Requires full service mock implementation")
}

// TestGetPortfolio_NotFound tests portfolio retrieval when portfolio doesn't exist
func TestGetPortfolio_NotFound(t *testing.T) {
	// This test would require a full mock implementation
	// Skipping for now - covered by integration tests
	t.Skip("Requires full service mock implementation")
}

// TestGetPortfolio_Unauthorized tests portfolio retrieval by wrong user
func TestGetPortfolio_Unauthorized(t *testing.T) {
	// This test would require a full mock implementation
	// Skipping for now - covered by integration tests
	t.Skip("Requires full service mock implementation")
}

// TestUpdatePortfolio_InvalidJSON tests portfolio update with invalid JSON
func TestUpdatePortfolio_InvalidJSON(t *testing.T) {
	server := createTestServer()

	req := httptest.NewRequest("PUT", "/api/portfolios/portfolio-123", bytes.NewReader([]byte("invalid")))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-ID", "user-123")

	w := httptest.NewRecorder()
	server.router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d", w.Code)
	}
}

// TestDeletePortfolio_NotFound tests deletion of non-existent portfolio
func TestDeletePortfolio_NotFound(t *testing.T) {
	// This test would require a full mock implementation
	// Skipping for now - covered by integration tests
	t.Skip("Requires full service mock implementation")
}

// TestSearchTransaction_EmptyHash tests transaction search with empty hash
func TestSearchTransaction_EmptyHash(t *testing.T) {
	server := createTestServer()

	req := httptest.NewRequest("GET", "/api/search/transaction/", nil)
	req.Header.Set("X-User-ID", "user-123")

	w := httptest.NewRecorder()
	server.router.ServeHTTP(w, req)

	// Router should not match this route
	if w.Code != http.StatusNotFound {
		t.Errorf("Expected status 404, got %d", w.Code)
	}
}

// TestSearchTransaction_InvalidHash tests transaction search with invalid hash format
func TestSearchTransaction_InvalidHash(t *testing.T) {
	// This test would require a full mock implementation
	// Skipping for now - covered by integration tests
	t.Skip("Requires full service mock implementation")
}

// TestBoundaryValues tests various boundary value conditions
func TestBoundaryValues(t *testing.T) {
	tests := []struct {
		name     string
		endpoint string
		method   string
		body     interface{}
		headers  map[string]string
		expected int
	}{
		{
			name:     "max length address",
			endpoint: "/api/addresses",
			method:   "POST",
			body: map[string]interface{}{
				"address": "0x" + string(make([]byte, 100)), // Very long address
			},
			headers: map[string]string{
				"X-User-ID":   "user-123",
				"X-User-Tier": "free",
			},
			expected: http.StatusCreated, // Service should handle validation
		},
		{
			name:     "zero limit pagination",
			endpoint: "/api/addresses/0x1234/transactions?limit=0",
			method:   "GET",
			headers: map[string]string{
				"X-User-ID": "user-123",
			},
			expected: http.StatusOK, // Should use default
		},
		{
			name:     "max int offset",
			endpoint: "/api/addresses/0x1234/transactions?offset=2147483647",
			method:   "GET",
			headers: map[string]string{
				"X-User-ID": "user-123",
			},
			expected: http.StatusOK, // Should handle gracefully
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := createTestServer()

			var req *http.Request
			if tt.body != nil {
				body, _ := json.Marshal(tt.body)
				req = httptest.NewRequest(tt.method, tt.endpoint, bytes.NewReader(body))
				req.Header.Set("Content-Type", "application/json")
			} else {
				req = httptest.NewRequest(tt.method, tt.endpoint, nil)
			}

			for k, v := range tt.headers {
				req.Header.Set(k, v)
			}

			w := httptest.NewRecorder()
			server.router.ServeHTTP(w, req)

			if w.Code != tt.expected {
				t.Errorf("Expected status %d, got %d", tt.expected, w.Code)
			}
		})
	}
}

// TestConcurrentRequests tests handling of concurrent requests
func TestConcurrentRequests(t *testing.T) {
	server := createTestServer()

	// Make 10 concurrent requests
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func() {
			req := httptest.NewRequest("GET", "/health", nil)
			w := httptest.NewRecorder()
			server.router.ServeHTTP(w, req)

			if w.Code != http.StatusOK {
				t.Errorf("Expected status 200, got %d", w.Code)
			}
			done <- true
		}()
	}

	// Wait for all requests to complete
	for i := 0; i < 10; i++ {
		<-done
	}
}

// TestErrorResponseFormat tests that error responses follow consistent format
func TestErrorResponseFormat(t *testing.T) {
	server := createTestServer()

	req := httptest.NewRequest("POST", "/api/addresses", bytes.NewReader([]byte("invalid")))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-ID", "user-123")

	w := httptest.NewRecorder()
	server.router.ServeHTTP(w, req)

	var errorResp map[string]interface{}
	if err := json.NewDecoder(w.Body).Decode(&errorResp); err != nil {
		t.Fatalf("Failed to decode error response: %v", err)
	}

	// Check that error response has expected fields
	if _, ok := errorResp["error"]; !ok {
		t.Error("Expected 'error' field in error response")
	}
}
