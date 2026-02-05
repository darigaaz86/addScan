package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/address-scanner/internal/models"
	"github.com/address-scanner/internal/service"
	"github.com/address-scanner/internal/types"
	"github.com/gorilla/mux"
)

// Mock services for testing
type mockAddressService struct {
	addAddressFunc func(ctx context.Context, input *service.AddAddressInput) (*service.AddressTrackingResult, error)
}

func (m *mockAddressService) AddAddress(ctx context.Context, input *service.AddAddressInput) (*service.AddressTrackingResult, error) {
	if m.addAddressFunc != nil {
		return m.addAddressFunc(ctx, input)
	}
	return &service.AddressTrackingResult{
		Success:         true,
		Address:         input.Address,
		Chains:          input.Chains,
		TrackingCreated: true,
		BackfillJobID:   "backfill-123",
		BackfillStatus:  "pending",
	}, nil
}

func (m *mockAddressService) GetBalance(ctx context.Context, input *service.GetBalanceInput) (*service.GetBalanceResult, error) {
	return &service.GetBalanceResult{
		Address: input.Address,
		Balances: []types.ChainBalance{
			{
				Chain:         types.ChainEthereum,
				NativeBalance: "1000000000000000000",
			},
		},
		UpdatedAt: time.Now(),
	}, nil
}

type mockPortfolioService struct {
	createFunc   func(ctx context.Context, input *service.CreatePortfolioInput) (*models.Portfolio, error)
	getFunc      func(ctx context.Context, portfolioID, userID string) (*service.PortfolioView, error)
	updateFunc   func(ctx context.Context, input *service.UpdatePortfolioInput) (*models.Portfolio, error)
	deleteFunc   func(ctx context.Context, portfolioID, userID string) (*service.DeletePortfolioResult, error)
	getStatsFunc func(ctx context.Context, portfolioID, userID string) (*service.PortfolioStatistics, error)
}

func (m *mockPortfolioService) CreatePortfolio(ctx context.Context, input *service.CreatePortfolioInput) (*models.Portfolio, error) {
	if m.createFunc != nil {
		return m.createFunc(ctx, input)
	}
	return &models.Portfolio{
		ID:        "portfolio-123",
		UserID:    input.UserID,
		Name:      input.Name,
		Addresses: input.Addresses,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}, nil
}

func (m *mockPortfolioService) GetPortfolio(ctx context.Context, portfolioID, userID string) (*service.PortfolioView, error) {
	if m.getFunc != nil {
		return m.getFunc(ctx, portfolioID, userID)
	}
	return &service.PortfolioView{
		ID:        portfolioID,
		Name:      "Test Portfolio",
		Addresses: []string{"0x1234"},
		TotalBalance: types.MultiChainBalance{
			Chains: []types.ChainBalance{
				{
					Chain:         types.ChainEthereum,
					NativeBalance: "1000000000000000000",
				},
			},
		},
	}, nil
}

func (m *mockPortfolioService) UpdatePortfolio(ctx context.Context, input *service.UpdatePortfolioInput) (*models.Portfolio, error) {
	if m.updateFunc != nil {
		return m.updateFunc(ctx, input)
	}
	return &models.Portfolio{
		ID:        input.PortfolioID,
		UserID:    input.UserID,
		Name:      *input.Name,
		Addresses: input.Addresses,
		UpdatedAt: time.Now(),
	}, nil
}

func (m *mockPortfolioService) DeletePortfolio(ctx context.Context, portfolioID, userID string) (*service.DeletePortfolioResult, error) {
	if m.deleteFunc != nil {
		return m.deleteFunc(ctx, portfolioID, userID)
	}
	message := "Portfolio deleted successfully"
	return &service.DeletePortfolioResult{
		Success:     true,
		PortfolioID: portfolioID,
		Message:     &message,
	}, nil
}

func (m *mockPortfolioService) GetStatistics(ctx context.Context, portfolioID, userID string) (*service.PortfolioStatistics, error) {
	if m.getStatsFunc != nil {
		return m.getStatsFunc(ctx, portfolioID, userID)
	}
	return &service.PortfolioStatistics{
		PortfolioID: portfolioID,
		TotalBalance: types.MultiChainBalance{
			Chains: []types.ChainBalance{
				{
					Chain:         types.ChainEthereum,
					NativeBalance: "1000000000000000000",
				},
			},
		},
		TransactionCount: 100,
		TotalVolume:      "50000000000000000000000",
		TopCounterparties: []types.Counterparty{
			{Address: "0xabc", TransactionCount: 10, TotalVolume: "1000"},
			{Address: "0xdef", TransactionCount: 5, TotalVolume: "500"},
		},
		TokenHoldings: []types.TokenHolding{
			{Token: "0xtoken", Symbol: "ETH", Balance: "10000000000000000000", Decimals: 18},
		},
	}, nil
}

type mockQueryService struct {
	queryFunc        func(ctx context.Context, input *service.QueryInput) (*service.QueryResult, error)
	searchByHashFunc func(ctx context.Context, hash string) ([]*types.NormalizedTransaction, error)
}

func (m *mockQueryService) Query(ctx context.Context, input *service.QueryInput) (*service.QueryResult, error) {
	if m.queryFunc != nil {
		return m.queryFunc(ctx, input)
	}
	return &service.QueryResult{
		Transactions: []*types.NormalizedTransaction{
			{
				Hash:      "0xabc123",
				Chain:     types.ChainEthereum,
				From:      "0x1234",
				To:        "0x5678",
				Value:     "1000000000000000000",
				Timestamp: time.Now().Unix(),
			},
		},
	}, nil
}

func (m *mockQueryService) SearchByHash(ctx context.Context, hash string) ([]*types.NormalizedTransaction, error) {
	if m.searchByHashFunc != nil {
		return m.searchByHashFunc(ctx, hash)
	}
	return []*types.NormalizedTransaction{
		{
			Hash:      hash,
			Chain:     types.ChainEthereum,
			From:      "0x1234",
			To:        "0x5678",
			Value:     "1000000000000000000",
			Timestamp: time.Now().Unix(),
		},
	}, nil
}

type mockSnapshotService struct {
	getSnapshotsFunc func(ctx context.Context, portfolioID, userID string, from, to time.Time) ([]*models.PortfolioSnapshot, error)
}

func (m *mockSnapshotService) GetSnapshots(ctx context.Context, portfolioID, userID string, from, to time.Time) ([]*models.PortfolioSnapshot, error) {
	if m.getSnapshotsFunc != nil {
		return m.getSnapshotsFunc(ctx, portfolioID, userID, from, to)
	}
	return []*models.PortfolioSnapshot{
		{
			PortfolioID:      portfolioID,
			SnapshotDate:     time.Now(),
			TotalBalance:     types.MultiChainBalance{},
			TransactionCount: 100,
			TotalVolume:      "50000000000000000000000",
		},
	}, nil
}

// Helper function to create test server
// Note: This creates a server with mock-backed services for testing
// For full integration tests, use real service implementations
func createTestServer() *Server {
	config := &ServerConfig{
		Host:           "localhost",
		Port:           "8080",
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		IdleTimeout:    60 * time.Second,
		FreeTierRPS:    10,
		BasicTierRPS:   100,
		PremiumTierRPS: 1000,
	}

	// Create mock services for testing
	addressService := &mockAddressService{}
	portfolioService := &mockPortfolioService{}
	queryService := &mockQueryService{}
	snapshotService := &mockSnapshotService{}

	server := &Server{
		router:           mux.NewRouter(),
		addressService:   addressService,
		portfolioService: portfolioService,
		queryService:     queryService,
		snapshotService:  snapshotService,
		config:           config,
	}
	server.setupRouter()
	return server
}

// TestHealthEndpoint tests the health check endpoint
func TestHealthEndpoint(t *testing.T) {
	server := createTestServer()

	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()

	server.router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	var response map[string]string
	if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if response["status"] != "healthy" {
		t.Errorf("Expected status 'healthy', got '%s'", response["status"])
	}
}

// TestAddAddress_Success tests successful address addition
func TestAddAddress_Success(t *testing.T) {
	server := createTestServer()

	reqBody := map[string]interface{}{
		"address": "0x1234567890123456789012345678901234567890",
		"chains":  []string{"ethereum", "polygon"},
	}
	body, _ := json.Marshal(reqBody)

	req := httptest.NewRequest("POST", "/api/addresses", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-ID", "user-123")
	req.Header.Set("X-User-Tier", "free")

	w := httptest.NewRecorder()
	server.router.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("Expected status 201, got %d", w.Code)
	}

	var response service.AddressTrackingResult
	if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if response.Address != "0x1234567890123456789012345678901234567890" {
		t.Errorf("Expected address to match, got %s", response.Address)
	}
}

// TestAddAddress_MissingUserID tests address addition without user ID
func TestAddAddress_MissingUserID(t *testing.T) {
	server := createTestServer()

	reqBody := map[string]interface{}{
		"address": "0x1234567890123456789012345678901234567890",
	}
	body, _ := json.Marshal(reqBody)

	req := httptest.NewRequest("POST", "/api/addresses", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	w := httptest.NewRecorder()
	server.router.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("Expected status 401, got %d", w.Code)
	}
}

// TestGetTransactions_Success tests successful transaction retrieval
func TestGetTransactions_Success(t *testing.T) {
	server := createTestServer()

	req := httptest.NewRequest("GET", "/api/addresses/0x1234/transactions?limit=10&offset=0", nil)
	req.Header.Set("X-User-ID", "user-123")

	w := httptest.NewRecorder()
	server.router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	var response service.QueryResult
	if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if len(response.Transactions) == 0 {
		t.Error("Expected at least one transaction")
	}
}

// TestGetTransactions_WithFilters tests transaction retrieval with filters
func TestGetTransactions_WithFilters(t *testing.T) {
	server := createTestServer()

	req := httptest.NewRequest("GET", "/api/addresses/0x1234/transactions?dateFrom=2024-01-01T00:00:00Z&dateTo=2024-12-31T23:59:59Z&minValue=1.0&sortBy=value&sortOrder=desc", nil)
	req.Header.Set("X-User-ID", "user-123")

	w := httptest.NewRecorder()
	server.router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}
}

// TestCreatePortfolio_Success tests successful portfolio creation
func TestCreatePortfolio_Success(t *testing.T) {
	server := createTestServer()

	reqBody := map[string]interface{}{
		"name":      "My Portfolio",
		"addresses": []string{"0x1234", "0x5678"},
	}
	body, _ := json.Marshal(reqBody)

	req := httptest.NewRequest("POST", "/api/portfolios", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-ID", "user-123")

	w := httptest.NewRecorder()
	server.router.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("Expected status 201, got %d", w.Code)
	}

	var response models.Portfolio
	if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if response.Name != "My Portfolio" {
		t.Errorf("Expected name 'My Portfolio', got '%s'", response.Name)
	}
}

// TestGetPortfolio_Success tests successful portfolio retrieval
func TestGetPortfolio_Success(t *testing.T) {
	server := createTestServer()

	req := httptest.NewRequest("GET", "/api/portfolios/portfolio-123", nil)
	req.Header.Set("X-User-ID", "user-123")

	w := httptest.NewRecorder()
	server.router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	var response service.PortfolioView
	if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if response.ID != "portfolio-123" {
		t.Errorf("Expected portfolio ID 'portfolio-123', got '%s'", response.ID)
	}
}

// TestUpdatePortfolio_Success tests successful portfolio update
func TestUpdatePortfolio_Success(t *testing.T) {
	server := createTestServer()

	reqBody := map[string]interface{}{
		"name":      "Updated Portfolio",
		"addresses": []string{"0x1234", "0x5678", "0x9abc"},
	}
	body, _ := json.Marshal(reqBody)

	req := httptest.NewRequest("PUT", "/api/portfolios/portfolio-123", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-User-ID", "user-123")

	w := httptest.NewRecorder()
	server.router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}
}

// TestDeletePortfolio_Success tests successful portfolio deletion
func TestDeletePortfolio_Success(t *testing.T) {
	server := createTestServer()

	req := httptest.NewRequest("DELETE", "/api/portfolios/portfolio-123", nil)
	req.Header.Set("X-User-ID", "user-123")

	w := httptest.NewRecorder()
	server.router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	var response service.DeletePortfolioResult
	if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if !response.Success {
		t.Error("Expected success to be true")
	}
}

// TestGetStatistics_Success tests successful statistics retrieval
func TestGetStatistics_Success(t *testing.T) {
	server := createTestServer()

	req := httptest.NewRequest("GET", "/api/portfolios/portfolio-123/statistics", nil)
	req.Header.Set("X-User-ID", "user-123")

	w := httptest.NewRecorder()
	server.router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	var response service.PortfolioStatistics
	if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if response.TransactionCount <= 0 {
		t.Error("Expected positive transaction count")
	}
}

// TestGetSnapshots_Success tests successful snapshot retrieval
func TestGetSnapshots_Success(t *testing.T) {
	server := createTestServer()

	req := httptest.NewRequest("GET", "/api/portfolios/portfolio-123/snapshots?dateFrom=2024-01-01T00:00:00Z&dateTo=2024-12-31T23:59:59Z", nil)
	req.Header.Set("X-User-ID", "user-123")

	w := httptest.NewRecorder()
	server.router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	var response []*models.PortfolioSnapshot
	if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if len(response) == 0 {
		t.Error("Expected at least one snapshot")
	}
}

// TestGetSnapshots_MissingDateParams tests snapshot retrieval without required date parameters
func TestGetSnapshots_MissingDateParams(t *testing.T) {
	server := createTestServer()

	req := httptest.NewRequest("GET", "/api/portfolios/portfolio-123/snapshots", nil)
	req.Header.Set("X-User-ID", "user-123")

	w := httptest.NewRecorder()
	server.router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d", w.Code)
	}
}

// TestSearchTransaction_Success tests successful transaction search
func TestSearchTransaction_Success(t *testing.T) {
	server := createTestServer()

	req := httptest.NewRequest("GET", "/api/search/transaction/0xabc123", nil)
	req.Header.Set("X-User-ID", "user-123")

	w := httptest.NewRecorder()
	server.router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	var response []*types.NormalizedTransaction
	if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if len(response) == 0 {
		t.Error("Expected at least one transaction")
	}

	if response[0].Hash == "" {
		t.Error("Expected transaction hash to be present")
	}
}

// TestSearchTransaction_NotFound tests transaction search with not found result
func TestSearchTransaction_NotFound(t *testing.T) {
	// This test requires full service implementation
	// Skipping for now - covered by integration tests
	t.Skip("Requires full service implementation")
}

// TestCORSHeaders tests that CORS headers are properly set
func TestCORSHeaders(t *testing.T) {
	server := createTestServer()

	// Test CORS headers on a regular GET request (not OPTIONS)
	// The middleware should add CORS headers to all responses
	req := httptest.NewRequest("GET", "/health", nil)
	req.Header.Set("Origin", "http://localhost:3000")

	w := httptest.NewRecorder()
	server.router.ServeHTTP(w, req)

	if w.Header().Get("Access-Control-Allow-Origin") == "" {
		t.Error("Expected CORS headers to be set")
	}
}
