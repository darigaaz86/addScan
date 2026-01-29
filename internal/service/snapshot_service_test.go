package service

import (
	"context"
	"testing"
	"time"

	"github.com/address-scanner/internal/models"
	"github.com/address-scanner/internal/storage"
	"github.com/address-scanner/internal/types"
)

// Mock repositories for testing

type mockSnapshotRepository struct {
	snapshots        []*models.PortfolioSnapshot
	activePortfolios []string
}

func (m *mockSnapshotRepository) Create(ctx context.Context, snapshot *models.PortfolioSnapshot) error {
	m.snapshots = append(m.snapshots, snapshot)
	return nil
}

func (m *mockSnapshotRepository) GetByPortfolioAndDateRange(ctx context.Context, portfolioID string, from, to time.Time) ([]*models.PortfolioSnapshot, error) {
	var result []*models.PortfolioSnapshot
	for _, s := range m.snapshots {
		if s.PortfolioID == portfolioID && !s.SnapshotDate.Before(from) && !s.SnapshotDate.After(to) {
			result = append(result, s)
		}
	}
	return result, nil
}

func (m *mockSnapshotRepository) DeleteOldSnapshots(ctx context.Context, portfolioID string, retentionDays int) error {
	return nil
}

func (m *mockSnapshotRepository) GetAllActivePortfolios(ctx context.Context) ([]string, error) {
	return m.activePortfolios, nil
}

type mockUserRepository struct {
	users map[string]*models.User
}

func (m *mockUserRepository) GetByID(ctx context.Context, userID string) (*models.User, error) {
	if user, ok := m.users[userID]; ok {
		return user, nil
	}
	return nil, &types.ServiceError{Code: "USER_NOT_FOUND", Message: "user not found"}
}

type mockPortfolioRepository struct {
	portfolios map[string]*models.Portfolio
}

func (m *mockPortfolioRepository) Create(ctx context.Context, portfolio *models.Portfolio) error {
	m.portfolios[portfolio.ID] = portfolio
	return nil
}

func (m *mockPortfolioRepository) GetByID(ctx context.Context, id string) (*models.Portfolio, error) {
	if p, ok := m.portfolios[id]; ok {
		return p, nil
	}
	return nil, &types.ServiceError{Code: "PORTFOLIO_NOT_FOUND", Message: "portfolio not found"}
}

func (m *mockPortfolioRepository) GetByIDAndUser(ctx context.Context, id, userID string) (*models.Portfolio, error) {
	if p, ok := m.portfolios[id]; ok && p.UserID == userID {
		return p, nil
	}
	return nil, &types.ServiceError{Code: "PORTFOLIO_NOT_FOUND", Message: "portfolio not found"}
}

func (m *mockPortfolioRepository) Update(ctx context.Context, portfolio *models.Portfolio) error {
	m.portfolios[portfolio.ID] = portfolio
	return nil
}

func (m *mockPortfolioRepository) DeleteByIDAndUser(ctx context.Context, id, userID string) error {
	if p, ok := m.portfolios[id]; ok && p.UserID == userID {
		delete(m.portfolios, id)
		return nil
	}
	return &types.ServiceError{Code: "PORTFOLIO_NOT_FOUND", Message: "portfolio not found"}
}

func (m *mockPortfolioRepository) ListByUser(ctx context.Context, userID string) ([]*models.Portfolio, error) {
	var result []*models.Portfolio
	for _, p := range m.portfolios {
		if p.UserID == userID {
			result = append(result, p)
		}
	}
	return result, nil
}

func (m *mockPortfolioRepository) ExistsByIDAndUser(ctx context.Context, id, userID string) (bool, error) {
	if p, ok := m.portfolios[id]; ok && p.UserID == userID {
		return true, nil
	}
	return false, nil
}

type mockTransactionRepository struct {
	transactions []*models.Transaction
}

func (m *mockTransactionRepository) GetByAddresses(ctx context.Context, addresses []string, filters *storage.TransactionFilters) ([]*models.Transaction, error) {
	return m.transactions, nil
}

type mockAddressRepository struct {
	addresses map[string]bool
}

func (m *mockAddressRepository) Exists(ctx context.Context, address string) (bool, error) {
	return m.addresses[address], nil
}

// Test snapshot capture
func TestCaptureSnapshot(t *testing.T) {
	ctx := context.Background()

	// Setup mocks
	snapshotRepo := &mockSnapshotRepository{
		snapshots:        []*models.PortfolioSnapshot{},
		activePortfolios: []string{"portfolio-1"},
	}

	portfolioRepo := &mockPortfolioRepository{
		portfolios: map[string]*models.Portfolio{
			"portfolio-1": {
				ID:        "portfolio-1",
				UserID:    "user-1",
				Name:      "Test Portfolio",
				Addresses: []string{"0x1234567890abcdef"},
				CreatedAt: time.Now(),
				UpdatedAt: time.Now(),
			},
		},
	}

	userRepo := &mockUserRepository{
		users: map[string]*models.User{
			"user-1": {
				ID:    "user-1",
				Email: "test@example.com",
				Tier:  types.TierFree,
			},
		},
	}

	// Create mock portfolio service (simplified for testing)
	transactionRepo := &mockTransactionRepository{
		transactions: []*models.Transaction{},
	}

	addressRepo := &mockAddressRepository{
		addresses: map[string]bool{
			"0x1234567890abcdef": true,
		},
	}

	portfolioSvc := &PortfolioService{
		portfolioRepo:   portfolioRepo,
		transactionRepo: transactionRepo,
		addressRepo:     addressRepo,
	}

	// Create snapshot service
	snapshotSvc := NewSnapshotService(snapshotRepo, portfolioRepo, userRepo, portfolioSvc)

	// Capture snapshot
	err := snapshotSvc.CaptureSnapshot(ctx, "portfolio-1")
	if err != nil {
		t.Fatalf("Failed to capture snapshot: %v", err)
	}

	// Verify snapshot was created
	if len(snapshotRepo.snapshots) != 1 {
		t.Errorf("Expected 1 snapshot, got %d", len(snapshotRepo.snapshots))
	}

	snapshot := snapshotRepo.snapshots[0]
	if snapshot.PortfolioID != "portfolio-1" {
		t.Errorf("Expected portfolio ID 'portfolio-1', got '%s'", snapshot.PortfolioID)
	}

	// Verify snapshot date is today at midnight UTC
	expectedDate := time.Now().UTC().Truncate(24 * time.Hour)
	if !snapshot.SnapshotDate.Equal(expectedDate) {
		t.Errorf("Expected snapshot date %v, got %v", expectedDate, snapshot.SnapshotDate)
	}
}

// Test snapshot retrieval
func TestGetSnapshots(t *testing.T) {
	ctx := context.Background()

	// Setup mocks with existing snapshots
	today := time.Now().UTC().Truncate(24 * time.Hour)
	yesterday := today.AddDate(0, 0, -1)
	twoDaysAgo := today.AddDate(0, 0, -2)

	snapshotRepo := &mockSnapshotRepository{
		snapshots: []*models.PortfolioSnapshot{
			{
				PortfolioID:      "portfolio-1",
				SnapshotDate:     twoDaysAgo,
				TransactionCount: 100,
				TotalVolume:      "1000",
				CreatedAt:        twoDaysAgo,
			},
			{
				PortfolioID:      "portfolio-1",
				SnapshotDate:     yesterday,
				TransactionCount: 150,
				TotalVolume:      "1500",
				CreatedAt:        yesterday,
			},
			{
				PortfolioID:      "portfolio-1",
				SnapshotDate:     today,
				TransactionCount: 200,
				TotalVolume:      "2000",
				CreatedAt:        today,
			},
		},
		activePortfolios: []string{"portfolio-1"},
	}

	portfolioRepo := &mockPortfolioRepository{
		portfolios: map[string]*models.Portfolio{
			"portfolio-1": {
				ID:        "portfolio-1",
				UserID:    "user-1",
				Name:      "Test Portfolio",
				Addresses: []string{"0x1234567890abcdef"},
			},
		},
	}

	userRepo := &mockUserRepository{
		users: map[string]*models.User{
			"user-1": {
				ID:   "user-1",
				Tier: types.TierFree,
			},
		},
	}

	portfolioSvc := &PortfolioService{
		portfolioRepo: portfolioRepo,
		transactionRepo: &mockTransactionRepository{
			transactions: []*models.Transaction{},
		},
		addressRepo: &mockAddressRepository{
			addresses: map[string]bool{},
		},
	}

	snapshotSvc := NewSnapshotService(snapshotRepo, portfolioRepo, userRepo, portfolioSvc)

	// Get snapshots for date range
	snapshots, err := snapshotSvc.GetSnapshots(ctx, "portfolio-1", "user-1", yesterday, today)
	if err != nil {
		t.Fatalf("Failed to get snapshots: %v", err)
	}

	// Verify we got 2 snapshots (yesterday and today)
	if len(snapshots) != 2 {
		t.Errorf("Expected 2 snapshots, got %d", len(snapshots))
	}

	// Verify snapshots are in chronological order
	if len(snapshots) >= 2 {
		if snapshots[0].SnapshotDate.After(snapshots[1].SnapshotDate) {
			t.Error("Snapshots are not in chronological order")
		}
	}
}

// Test retention policy
func TestApplyRetentionPolicy(t *testing.T) {
	ctx := context.Background()

	snapshotRepo := &mockSnapshotRepository{
		snapshots:        []*models.PortfolioSnapshot{},
		activePortfolios: []string{"portfolio-1"},
	}

	portfolioRepo := &mockPortfolioRepository{
		portfolios: map[string]*models.Portfolio{
			"portfolio-1": {
				ID:     "portfolio-1",
				UserID: "user-1",
			},
		},
	}

	userRepo := &mockUserRepository{
		users: map[string]*models.User{
			"user-1": {
				ID:   "user-1",
				Tier: types.TierFree,
			},
			"user-2": {
				ID:   "user-2",
				Tier: types.TierPaid,
			},
		},
	}

	portfolioSvc := &PortfolioService{
		portfolioRepo: portfolioRepo,
		transactionRepo: &mockTransactionRepository{
			transactions: []*models.Transaction{},
		},
		addressRepo: &mockAddressRepository{
			addresses: map[string]bool{},
		},
	}

	snapshotSvc := NewSnapshotService(snapshotRepo, portfolioRepo, userRepo, portfolioSvc)

	// Test free tier retention (should apply 90-day policy)
	err := snapshotSvc.ApplyRetentionPolicy(ctx, "portfolio-1")
	if err != nil {
		t.Errorf("Failed to apply retention policy for free tier: %v", err)
	}

	// Test paid tier retention (should skip deletion)
	portfolioRepo.portfolios["portfolio-2"] = &models.Portfolio{
		ID:     "portfolio-2",
		UserID: "user-2",
	}

	err = snapshotSvc.ApplyRetentionPolicy(ctx, "portfolio-2")
	if err != nil {
		t.Errorf("Failed to apply retention policy for paid tier: %v", err)
	}
}

// Test scheduler start/stop
func TestSchedulerStartStop(t *testing.T) {
	snapshotRepo := &mockSnapshotRepository{
		activePortfolios: []string{},
	}

	portfolioRepo := &mockPortfolioRepository{
		portfolios: map[string]*models.Portfolio{},
	}

	userRepo := &mockUserRepository{
		users: map[string]*models.User{},
	}

	portfolioSvc := &PortfolioService{
		portfolioRepo: portfolioRepo,
		transactionRepo: &mockTransactionRepository{
			transactions: []*models.Transaction{},
		},
		addressRepo: &mockAddressRepository{
			addresses: map[string]bool{},
		},
	}

	snapshotSvc := NewSnapshotService(snapshotRepo, portfolioRepo, userRepo, portfolioSvc)

	// Test that scheduler is not running initially
	if snapshotSvc.IsRunning() {
		t.Error("Scheduler should not be running initially")
	}

	// Start scheduler
	ctx := context.Background()
	err := snapshotSvc.Start(ctx)
	if err != nil {
		t.Fatalf("Failed to start scheduler: %v", err)
	}

	// Verify scheduler is running
	if !snapshotSvc.IsRunning() {
		t.Error("Scheduler should be running after Start()")
	}

	// Stop scheduler
	err = snapshotSvc.Stop()
	if err != nil {
		t.Fatalf("Failed to stop scheduler: %v", err)
	}

	// Give it a moment to stop
	time.Sleep(100 * time.Millisecond)

	// Verify scheduler is stopped
	if snapshotSvc.IsRunning() {
		t.Error("Scheduler should not be running after Stop()")
	}
}
