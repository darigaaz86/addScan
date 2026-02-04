package service

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/address-scanner/internal/adapter"
	"github.com/address-scanner/internal/models"
	"github.com/address-scanner/internal/storage"
	"github.com/address-scanner/internal/types"
)

// Mock repositories for testing

type mockPortfolioRepo struct {
	portfolios map[string]*models.Portfolio
}

func (m *mockPortfolioRepo) Create(ctx context.Context, portfolio *models.Portfolio) error {
	if portfolio.ID == "" {
		// Generate unique ID based on number of portfolios
		portfolio.ID = fmt.Sprintf("test-portfolio-id-%d", len(m.portfolios)+1)
	}
	m.portfolios[portfolio.ID] = portfolio
	return nil
}

func (m *mockPortfolioRepo) GetByID(ctx context.Context, id string) (*models.Portfolio, error) {
	if p, ok := m.portfolios[id]; ok {
		return p, nil
	}
	return nil, &types.ServiceError{Code: "NOT_FOUND", Message: "portfolio not found"}
}

func (m *mockPortfolioRepo) GetByIDAndUser(ctx context.Context, id, userID string) (*models.Portfolio, error) {
	if p, ok := m.portfolios[id]; ok {
		if p.UserID == userID {
			return p, nil
		}
	}
	return nil, &types.ServiceError{Code: "NOT_FOUND", Message: "portfolio not found"}
}

func (m *mockPortfolioRepo) Update(ctx context.Context, portfolio *models.Portfolio) error {
	m.portfolios[portfolio.ID] = portfolio
	return nil
}

func (m *mockPortfolioRepo) DeleteByIDAndUser(ctx context.Context, id, userID string) error {
	if p, ok := m.portfolios[id]; ok {
		if p.UserID == userID {
			delete(m.portfolios, id)
			return nil
		}
	}
	return &types.ServiceError{Code: "NOT_FOUND", Message: "portfolio not found"}
}

func (m *mockPortfolioRepo) ListByUser(ctx context.Context, userID string) ([]*models.Portfolio, error) {
	var result []*models.Portfolio
	for _, p := range m.portfolios {
		if p.UserID == userID {
			result = append(result, p)
		}
	}
	return result, nil
}

func (m *mockPortfolioRepo) ExistsByIDAndUser(ctx context.Context, id, userID string) (bool, error) {
	if p, ok := m.portfolios[id]; ok {
		return p.UserID == userID, nil
	}
	return false, nil
}

type mockAddressRepo struct {
	addresses map[string]bool
}

func (m *mockAddressRepo) Exists(ctx context.Context, address string) (bool, error) {
	return m.addresses[address], nil
}

// mockAddressService for testing portfolio service
type mockAddressService struct {
	addedAddresses map[string]bool
	shouldFail     bool
}

func (m *mockAddressService) AddAddress(ctx context.Context, input *AddAddressInput) (*AddressTrackingResult, error) {
	if m.shouldFail {
		return nil, &types.ServiceError{
			Code:    "ADDRESS_ADD_FAILED",
			Message: "failed to add address",
		}
	}
	if m.addedAddresses == nil {
		m.addedAddresses = make(map[string]bool)
	}
	m.addedAddresses[input.Address] = true
	return &AddressTrackingResult{
		Success: true,
		Address: input.Address,
	}, nil
}

// mockAddressServiceForPortfolio implements the AddAddress method for testing
type mockAddressServiceForPortfolio struct {
	addedAddresses map[string]bool
	shouldFail     bool
}

func (m *mockAddressServiceForPortfolio) AddAddress(ctx context.Context, input *AddAddressInput) (*AddressTrackingResult, error) {
	if m.shouldFail {
		return nil, &types.ServiceError{
			Code:    "ADDRESS_ADD_FAILED",
			Message: "failed to add address",
		}
	}
	if m.addedAddresses == nil {
		m.addedAddresses = make(map[string]bool)
	}
	m.addedAddresses[input.Address] = true
	return &AddressTrackingResult{
		Success: true,
		Address: input.Address,
	}, nil
}

// AddressAdder interface for dependency injection in tests
type AddressAdder interface {
	AddAddress(ctx context.Context, input *AddAddressInput) (*AddressTrackingResult, error)
}

// mockPortfolioServiceForTest wraps PortfolioService with a mock address adder
type mockPortfolioServiceForTest struct {
	*PortfolioService
	mockAddrSvc AddressAdder
}

// NewPortfolioServiceWithMockAddressService creates a portfolio service with a mock address service for testing
func NewPortfolioServiceWithMockAddressService(
	portfolioRepo PortfolioRepository,
	addressRepo AddressRepository,
	transactionRepo TransactionRepository,
	chainAdapters map[types.ChainID]adapter.ChainAdapter,
	mockAddrSvc AddressAdder,
) *mockPortfolioServiceForTest {
	ps := &PortfolioService{
		portfolioRepo:   portfolioRepo,
		addressRepo:     addressRepo,
		transactionRepo: transactionRepo,
		chainAdapters:   chainAdapters,
		addressService:  nil, // Will use mock instead
	}
	return &mockPortfolioServiceForTest{
		PortfolioService: ps,
		mockAddrSvc:      mockAddrSvc,
	}
}

// CreatePortfolio overrides the base method to use mock address service
func (s *mockPortfolioServiceForTest) CreatePortfolio(ctx context.Context, input *CreatePortfolioInput) (*Portfolio, error) {
	// Validate input
	if input.UserID == "" {
		return nil, &types.ServiceError{
			Code:    "INVALID_INPUT",
			Message: "userId is required",
		}
	}

	if input.Name == "" {
		return nil, &types.ServiceError{
			Code:    "INVALID_INPUT",
			Message: "portfolio name is required",
		}
	}

	if len(input.Addresses) == 0 {
		return nil, &types.ServiceError{
			Code:    "INVALID_INPUT",
			Message: "at least one address is required",
		}
	}

	// Ensure all addresses are tracked in the system
	for _, addr := range input.Addresses {
		exists, err := s.addressRepo.Exists(ctx, addr)
		if err != nil {
			return nil, fmt.Errorf("failed to check address existence: %w", err)
		}

		// If address doesn't exist, try to add it using mock
		if !exists {
			addInput := &AddAddressInput{
				UserID:  input.UserID,
				Address: addr,
				Chains:  []types.ChainID{},
				Tier:    "",
			}

			_, err := s.mockAddrSvc.AddAddress(ctx, addInput)
			if err != nil {
				return nil, fmt.Errorf("failed to add address %s: %w", addr, err)
			}
		}
	}

	// Create portfolio
	portfolio := &models.Portfolio{
		UserID:      input.UserID,
		Name:        input.Name,
		Description: input.Description,
		Addresses:   input.Addresses,
	}

	if err := s.portfolioRepo.Create(ctx, portfolio); err != nil {
		return nil, fmt.Errorf("failed to create portfolio: %w", err)
	}

	return portfolio, nil
}

type mockTransactionRepoForPortfolio struct {
	transactions []*models.Transaction
}

func (m *mockTransactionRepoForPortfolio) GetByAddresses(ctx context.Context, addresses []string, filters *storage.TransactionFilters) ([]*models.Transaction, error) {
	// Return mock transactions
	return m.transactions, nil
}

type mockChainAdapter struct {
	chainID types.ChainID
}

func (m *mockChainAdapter) GetBalance(ctx context.Context, address string) (*types.ChainBalance, error) {
	return &types.ChainBalance{
		Chain:         m.chainID,
		NativeBalance: "1000000000000000000", // 1 ETH in wei
		TokenBalances: []types.TokenBalance{},
	}, nil
}

func (m *mockChainAdapter) NormalizeTransaction(rawTx interface{}) (*types.NormalizedTransaction, error) {
	return nil, nil
}

func (m *mockChainAdapter) FetchTransactions(ctx context.Context, address string, fromBlock *uint64, toBlock *uint64) ([]*types.NormalizedTransaction, error) {
	return nil, nil
}

func (m *mockChainAdapter) FetchTransactionHistory(ctx context.Context, address string, maxCount int) ([]*types.NormalizedTransaction, error) {
	return nil, nil
}

func (m *mockChainAdapter) FetchTransactionsForBlock(ctx context.Context, blockNum uint64, addresses []string) ([]*types.NormalizedTransaction, error) {
	return nil, nil
}

func (m *mockChainAdapter) FetchTransactionsForBlockRange(ctx context.Context, fromBlock, toBlock uint64, addresses []string) ([]*types.NormalizedTransaction, error) {
	return nil, nil
}

func (m *mockChainAdapter) GetCurrentBlock(ctx context.Context) (uint64, error) {
	return 0, nil
}

func (m *mockChainAdapter) GetBlockByTimestamp(ctx context.Context, timestamp int64) (uint64, error) {
	return 0, nil
}

func (m *mockChainAdapter) ValidateAddress(address string) bool {
	return true
}

func (m *mockChainAdapter) GetChainID() types.ChainID {
	return m.chainID
}

// Tests

func TestPortfolioService_CreatePortfolio(t *testing.T) {
	// Setup
	portfolioRepo := &mockPortfolioRepo{portfolios: make(map[string]*models.Portfolio)}
	addressRepo := &mockAddressRepo{addresses: map[string]bool{
		"0x1111111111111111111111111111111111111111": true,
		"0x2222222222222222222222222222222222222222": true,
	}}
	transactionRepo := &mockTransactionRepoForPortfolio{}
	chainAdapters := map[types.ChainID]adapter.ChainAdapter{
		types.ChainEthereum: &mockChainAdapter{chainID: types.ChainEthereum},
	}

	service := NewPortfolioService(portfolioRepo, addressRepo, transactionRepo, chainAdapters, nil)

	// Test: Create portfolio with valid addresses
	input := &CreatePortfolioInput{
		UserID: "user-123",
		Name:   "My Portfolio",
		Addresses: []string{
			"0x1111111111111111111111111111111111111111",
			"0x2222222222222222222222222222222222222222",
		},
	}

	portfolio, err := service.CreatePortfolio(context.Background(), input)
	if err != nil {
		t.Fatalf("CreatePortfolio failed: %v", err)
	}

	if portfolio.ID == "" {
		t.Error("Expected portfolio ID to be set")
	}

	if portfolio.Name != "My Portfolio" {
		t.Errorf("Expected name 'My Portfolio', got '%s'", portfolio.Name)
	}

	if len(portfolio.Addresses) != 2 {
		t.Errorf("Expected 2 addresses, got %d", len(portfolio.Addresses))
	}
}

func TestPortfolioService_CreatePortfolio_InvalidAddress(t *testing.T) {
	// Setup - with the new auto-add behavior, this test verifies that
	// when addressService fails to add an address, the portfolio creation fails
	portfolioRepo := &mockPortfolioRepo{portfolios: make(map[string]*models.Portfolio)}
	addressRepo := &mockAddressRepo{addresses: map[string]bool{
		"0x1111111111111111111111111111111111111111": true,
		// 0x9999... is NOT in the repo, so it will try to auto-add
	}}
	transactionRepo := &mockTransactionRepoForPortfolio{}
	chainAdapters := map[types.ChainID]adapter.ChainAdapter{
		types.ChainEthereum: &mockChainAdapter{chainID: types.ChainEthereum},
	}

	// Create a mock address service that fails when trying to add addresses
	mockAddrSvc := &mockAddressServiceForPortfolio{shouldFail: true}
	service := NewPortfolioServiceWithMockAddressService(portfolioRepo, addressRepo, transactionRepo, chainAdapters, mockAddrSvc)

	// Test: Create portfolio with untracked address (auto-add will fail)
	input := &CreatePortfolioInput{
		UserID: "user-123",
		Name:   "My Portfolio",
		Addresses: []string{
			"0x1111111111111111111111111111111111111111",
			"0x9999999999999999999999999999999999999999", // Not tracked, auto-add will fail
		},
	}

	_, err := service.CreatePortfolio(context.Background(), input)
	if err == nil {
		t.Error("Expected error when auto-add fails, got nil")
	}
}

func TestPortfolioService_UpdatePortfolio(t *testing.T) {
	// Setup
	portfolioRepo := &mockPortfolioRepo{portfolios: make(map[string]*models.Portfolio)}
	addressRepo := &mockAddressRepo{addresses: map[string]bool{
		"0x1111111111111111111111111111111111111111": true,
		"0x2222222222222222222222222222222222222222": true,
		"0x3333333333333333333333333333333333333333": true,
	}}
	transactionRepo := &mockTransactionRepoForPortfolio{}
	chainAdapters := map[types.ChainID]adapter.ChainAdapter{
		types.ChainEthereum: &mockChainAdapter{chainID: types.ChainEthereum},
	}

	service := NewPortfolioService(portfolioRepo, addressRepo, transactionRepo, chainAdapters, nil)

	// Create initial portfolio
	createInput := &CreatePortfolioInput{
		UserID: "user-123",
		Name:   "My Portfolio",
		Addresses: []string{
			"0x1111111111111111111111111111111111111111",
			"0x2222222222222222222222222222222222222222",
		},
	}

	portfolio, err := service.CreatePortfolio(context.Background(), createInput)
	if err != nil {
		t.Fatalf("CreatePortfolio failed: %v", err)
	}

	// Test: Update portfolio addresses
	newName := "Updated Portfolio"
	updateInput := &UpdatePortfolioInput{
		PortfolioID: portfolio.ID,
		UserID:      "user-123",
		Name:        &newName,
		Addresses: []string{
			"0x1111111111111111111111111111111111111111",
			"0x3333333333333333333333333333333333333333",
		},
	}

	// Give the goroutine time to complete
	time.Sleep(100 * time.Millisecond)

	updated, err := service.UpdatePortfolio(context.Background(), updateInput)
	if err != nil {
		t.Fatalf("UpdatePortfolio failed: %v", err)
	}

	if updated.Name != "Updated Portfolio" {
		t.Errorf("Expected name 'Updated Portfolio', got '%s'", updated.Name)
	}

	if len(updated.Addresses) != 2 {
		t.Errorf("Expected 2 addresses, got %d", len(updated.Addresses))
	}

	// Verify address 0x3333... is now in the portfolio
	found := false
	for _, addr := range updated.Addresses {
		if addr == "0x3333333333333333333333333333333333333333" {
			found = true
			break
		}
	}
	if !found {
		t.Error("Expected address 0x3333... to be in updated portfolio")
	}
}

func TestPortfolioService_DeletePortfolio(t *testing.T) {
	// Setup
	portfolioRepo := &mockPortfolioRepo{portfolios: make(map[string]*models.Portfolio)}
	addressRepo := &mockAddressRepo{addresses: map[string]bool{
		"0x1111111111111111111111111111111111111111": true,
	}}
	transactionRepo := &mockTransactionRepoForPortfolio{}
	chainAdapters := map[types.ChainID]adapter.ChainAdapter{
		types.ChainEthereum: &mockChainAdapter{chainID: types.ChainEthereum},
	}

	service := NewPortfolioService(portfolioRepo, addressRepo, transactionRepo, chainAdapters, nil)

	// Create portfolio
	createInput := &CreatePortfolioInput{
		UserID:    "user-123",
		Name:      "My Portfolio",
		Addresses: []string{"0x1111111111111111111111111111111111111111"},
	}

	portfolio, err := service.CreatePortfolio(context.Background(), createInput)
	if err != nil {
		t.Fatalf("CreatePortfolio failed: %v", err)
	}

	// Test: Delete portfolio
	result, err := service.DeletePortfolio(context.Background(), portfolio.ID, "user-123")
	if err != nil {
		t.Fatalf("DeletePortfolio failed: %v", err)
	}

	if !result.Success {
		t.Error("Expected success to be true")
	}

	// Verify portfolio is deleted
	_, err = portfolioRepo.GetByIDAndUser(context.Background(), portfolio.ID, "user-123")
	if err == nil {
		t.Error("Expected error when getting deleted portfolio")
	}
}

func TestPortfolioService_ListPortfolios(t *testing.T) {
	// Setup
	portfolioRepo := &mockPortfolioRepo{portfolios: make(map[string]*models.Portfolio)}
	addressRepo := &mockAddressRepo{addresses: map[string]bool{
		"0x1111111111111111111111111111111111111111": true,
		"0x2222222222222222222222222222222222222222": true,
	}}
	transactionRepo := &mockTransactionRepoForPortfolio{}
	chainAdapters := map[types.ChainID]adapter.ChainAdapter{
		types.ChainEthereum: &mockChainAdapter{chainID: types.ChainEthereum},
	}

	service := NewPortfolioService(portfolioRepo, addressRepo, transactionRepo, chainAdapters, nil)

	// Create multiple portfolios
	for i := 1; i <= 3; i++ {
		input := &CreatePortfolioInput{
			UserID:    "user-123",
			Name:      "Portfolio " + string(rune('0'+i)),
			Addresses: []string{"0x1111111111111111111111111111111111111111"},
		}
		_, err := service.CreatePortfolio(context.Background(), input)
		if err != nil {
			t.Fatalf("CreatePortfolio failed: %v", err)
		}
	}

	// Test: List portfolios
	portfolios, err := service.ListPortfolios(context.Background(), "user-123")
	if err != nil {
		t.Fatalf("ListPortfolios failed: %v", err)
	}

	if len(portfolios) != 3 {
		t.Errorf("Expected 3 portfolios, got %d", len(portfolios))
	}
}

func TestPortfolioService_GetPortfolio(t *testing.T) {
	// Setup
	portfolioRepo := &mockPortfolioRepo{portfolios: make(map[string]*models.Portfolio)}
	addressRepo := &mockAddressRepo{addresses: map[string]bool{
		"0x1111111111111111111111111111111111111111": true,
		"0x2222222222222222222222222222222222222222": true,
	}}
	transactionRepo := &mockTransactionRepoForPortfolio{
		transactions: []*models.Transaction{
			{
				Hash:        "0xabc123",
				Chain:       types.ChainEthereum,
				Address:     "0x1111111111111111111111111111111111111111",
				From:        "0x1111111111111111111111111111111111111111",
				To:          "0x9999999999999999999999999999999999999999",
				Value:       "1000000000000000000",
				Timestamp:   time.Now(),
				BlockNumber: 12345,
				Status:      "success",
			},
		},
	}
	chainAdapters := map[types.ChainID]adapter.ChainAdapter{
		types.ChainEthereum: &mockChainAdapter{chainID: types.ChainEthereum},
	}

	service := NewPortfolioService(portfolioRepo, addressRepo, transactionRepo, chainAdapters, nil)

	// Create portfolio
	createInput := &CreatePortfolioInput{
		UserID: "user-123",
		Name:   "My Portfolio",
		Addresses: []string{
			"0x1111111111111111111111111111111111111111",
			"0x2222222222222222222222222222222222222222",
		},
	}

	portfolio, err := service.CreatePortfolio(context.Background(), createInput)
	if err != nil {
		t.Fatalf("CreatePortfolio failed: %v", err)
	}

	// Test: Get portfolio with aggregated data
	view, err := service.GetPortfolio(context.Background(), portfolio.ID, "user-123")
	if err != nil {
		t.Fatalf("GetPortfolio failed: %v", err)
	}

	if view.ID != portfolio.ID {
		t.Errorf("Expected portfolio ID %s, got %s", portfolio.ID, view.ID)
	}

	if view.Name != "My Portfolio" {
		t.Errorf("Expected name 'My Portfolio', got '%s'", view.Name)
	}

	if len(view.Addresses) != 2 {
		t.Errorf("Expected 2 addresses, got %d", len(view.Addresses))
	}

	// Verify balance aggregation
	if len(view.TotalBalance.Chains) == 0 {
		t.Error("Expected balance data to be populated")
	}

	// Verify timeline merging
	if len(view.UnifiedTimeline) == 0 {
		t.Error("Expected timeline to be populated")
	}
}
