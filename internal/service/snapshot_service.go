package service

import (
	"context"
	"fmt"
	"time"

	"github.com/address-scanner/internal/models"
	"github.com/address-scanner/internal/types"
)

// SnapshotRepository interface for snapshot data operations
type SnapshotRepository interface {
	Create(ctx context.Context, snapshot *models.PortfolioSnapshot) error
	GetByPortfolioAndDateRange(ctx context.Context, portfolioID string, from, to time.Time) ([]*models.PortfolioSnapshot, error)
	DeleteOldSnapshots(ctx context.Context, portfolioID string, retentionDays int) error
	GetAllActivePortfolios(ctx context.Context) ([]string, error)
}

// UserRepository interface for user data operations
type UserRepository interface {
	GetByID(ctx context.Context, userID string) (*models.User, error)
}

// SnapshotService handles daily portfolio snapshots
type SnapshotService struct {
	snapshotRepo  SnapshotRepository
	portfolioRepo PortfolioRepository
	userRepo      UserRepository
	portfolioSvc  *PortfolioService
	ticker        *time.Ticker
	stopChan      chan struct{}
	running       bool
}

// NewSnapshotService creates a new snapshot service
func NewSnapshotService(
	snapshotRepo SnapshotRepository,
	portfolioRepo PortfolioRepository,
	userRepo UserRepository,
	portfolioSvc *PortfolioService,
) *SnapshotService {
	return &SnapshotService{
		snapshotRepo:  snapshotRepo,
		portfolioRepo: portfolioRepo,
		userRepo:      userRepo,
		portfolioSvc:  portfolioSvc,
		stopChan:      make(chan struct{}),
		running:       false,
	}
}

// Start begins the snapshot scheduler
// Requirements: 16.1, 16.5
// Schedules daily snapshots at midnight UTC
func (s *SnapshotService) Start(ctx context.Context) error {
	if s.running {
		return fmt.Errorf("snapshot scheduler is already running")
	}

	s.running = true

	// Calculate time until next midnight UTC
	now := time.Now().UTC()
	nextMidnight := time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, time.UTC)
	durationUntilMidnight := nextMidnight.Sub(now)

	fmt.Printf("Snapshot scheduler starting. Next snapshot at %s (in %v)\n", nextMidnight, durationUntilMidnight)

	// Wait until midnight, then start daily ticker
	go func() {
		// Wait for first midnight
		select {
		case <-time.After(durationUntilMidnight):
			// Capture snapshots at midnight
			if err := s.CaptureAllSnapshots(ctx); err != nil {
				fmt.Printf("Error capturing snapshots at midnight: %v\n", err)
			}
		case <-s.stopChan:
			return
		}

		// Start daily ticker (24 hours)
		s.ticker = time.NewTicker(24 * time.Hour)
		defer s.ticker.Stop()

		for {
			select {
			case <-s.ticker.C:
				// Capture snapshots every 24 hours
				if err := s.CaptureAllSnapshots(ctx); err != nil {
					fmt.Printf("Error capturing daily snapshots: %v\n", err)
				}
			case <-s.stopChan:
				fmt.Println("Snapshot scheduler stopped")
				return
			}
		}
	}()

	return nil
}

// Stop gracefully stops the snapshot scheduler
func (s *SnapshotService) Stop() error {
	if !s.running {
		return fmt.Errorf("snapshot scheduler is not running")
	}

	close(s.stopChan)
	s.running = false

	if s.ticker != nil {
		s.ticker.Stop()
	}

	fmt.Println("Snapshot scheduler stopping...")
	return nil
}

// CaptureAllSnapshots captures snapshots for all active portfolios
// Requirements: 16.1, 16.5
func (s *SnapshotService) CaptureAllSnapshots(ctx context.Context) error {
	fmt.Printf("Starting daily snapshot capture at %s\n", time.Now().UTC())

	// Get all active portfolio IDs
	portfolioIDs, err := s.snapshotRepo.GetAllActivePortfolios(ctx)
	if err != nil {
		return fmt.Errorf("failed to get active portfolios: %w", err)
	}

	fmt.Printf("Found %d active portfolios to snapshot\n", len(portfolioIDs))

	// Capture snapshot for each portfolio
	successCount := 0
	errorCount := 0

	for _, portfolioID := range portfolioIDs {
		if err := s.CaptureSnapshot(ctx, portfolioID); err != nil {
			fmt.Printf("Error capturing snapshot for portfolio %s: %v\n", portfolioID, err)
			errorCount++
		} else {
			successCount++
		}
	}

	fmt.Printf("Snapshot capture complete: %d successful, %d errors\n", successCount, errorCount)
	return nil
}

// CaptureSnapshot captures a snapshot for a specific portfolio
// Requirements: 16.2, 16.3
func (s *SnapshotService) CaptureSnapshot(ctx context.Context, portfolioID string) error {
	// Get portfolio
	portfolio, err := s.portfolioRepo.GetByID(ctx, portfolioID)
	if err != nil {
		return fmt.Errorf("failed to get portfolio: %w", err)
	}

	// Calculate statistics for the portfolio
	stats, err := s.portfolioSvc.calculateStatistics(ctx, portfolio.Addresses)
	if err != nil {
		return fmt.Errorf("failed to calculate statistics: %w", err)
	}

	// Get balance
	balance, err := s.portfolioSvc.aggregateBalance(ctx, portfolio.Addresses)
	if err != nil {
		return fmt.Errorf("failed to aggregate balance: %w", err)
	}

	// Create snapshot
	snapshot := &models.PortfolioSnapshot{
		PortfolioID:       portfolioID,
		SnapshotDate:      time.Now().UTC().Truncate(24 * time.Hour), // Midnight UTC
		TotalBalance:      balance,
		TransactionCount:  stats.transactionCount,
		TotalVolume:       stats.totalVolume,
		TopCounterparties: stats.topCounterparties,
		TokenHoldings:     stats.tokenHoldings,
		CreatedAt:         time.Now().UTC(),
	}

	// Store snapshot
	if err := s.snapshotRepo.Create(ctx, snapshot); err != nil {
		return fmt.Errorf("failed to store snapshot: %w", err)
	}

	fmt.Printf("Snapshot captured for portfolio %s on %s\n", portfolioID, snapshot.SnapshotDate.Format("2006-01-02"))
	return nil
}

// GetSnapshots retrieves snapshots for a portfolio within a date range
// Requirements: 16.4
func (s *SnapshotService) GetSnapshots(ctx context.Context, portfolioID, userID string, from, to time.Time) ([]*models.PortfolioSnapshot, error) {
	// Verify portfolio exists and user owns it
	exists, err := s.portfolioRepo.ExistsByIDAndUser(ctx, portfolioID, userID)
	if err != nil {
		return nil, fmt.Errorf("failed to check portfolio existence: %w", err)
	}

	if !exists {
		return nil, &types.ServiceError{
			Code:    "PORTFOLIO_NOT_FOUND",
			Message: fmt.Sprintf("portfolio not found or access denied: %s", portfolioID),
			Details: map[string]interface{}{
				"portfolioId": portfolioID,
			},
		}
	}

	// Retrieve snapshots from repository
	// Snapshots are returned in chronological order (Requirement 16.4)
	snapshots, err := s.snapshotRepo.GetByPortfolioAndDateRange(ctx, portfolioID, from, to)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve snapshots: %w", err)
	}

	return snapshots, nil
}

// ApplyRetentionPolicy applies tier-based retention policy to snapshots
// Requirements: 16.6, 16.7
func (s *SnapshotService) ApplyRetentionPolicy(ctx context.Context, portfolioID string) error {
	// Get portfolio to determine user tier
	portfolio, err := s.portfolioRepo.GetByID(ctx, portfolioID)
	if err != nil {
		return fmt.Errorf("failed to get portfolio: %w", err)
	}

	// Get user to check tier
	user, err := s.userRepo.GetByID(ctx, portfolio.UserID)
	if err != nil {
		return fmt.Errorf("failed to get user: %w", err)
	}

	// Determine retention period based on tier
	var retentionDays int
	switch user.Tier {
	case types.TierFree:
		retentionDays = 90 // Free tier: 90 days retention
	case types.TierPaid:
		retentionDays = -1 // Paid tier: unlimited retention (no deletion)
	default:
		retentionDays = 90 // Default to free tier
	}

	// Skip deletion for paid tier (unlimited retention)
	if retentionDays < 0 {
		return nil
	}

	// Delete old snapshots beyond retention period
	if err := s.snapshotRepo.DeleteOldSnapshots(ctx, portfolioID, retentionDays); err != nil {
		return fmt.Errorf("failed to delete old snapshots: %w", err)
	}

	fmt.Printf("Applied retention policy for portfolio %s: %d days\n", portfolioID, retentionDays)
	return nil
}

// IsRunning returns whether the scheduler is currently running
func (s *SnapshotService) IsRunning() bool {
	return s.running
}
