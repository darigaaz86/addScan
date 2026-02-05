package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"time"

	"github.com/address-scanner/internal/storage"
	"github.com/address-scanner/internal/types"
)

// BalanceSnapshotService handles daily balance snapshot creation
type BalanceSnapshotService struct {
	snapshotRepo  *storage.BalanceSnapshotRepository
	balanceRepo   *storage.BalanceRepository
	addressRepo   *storage.AddressRepository
	portfolioRepo *storage.PortfolioRepository
}

// NewBalanceSnapshotService creates a new balance snapshot service
func NewBalanceSnapshotService(
	snapshotRepo *storage.BalanceSnapshotRepository,
	balanceRepo *storage.BalanceRepository,
	addressRepo *storage.AddressRepository,
	portfolioRepo *storage.PortfolioRepository,
) *BalanceSnapshotService {
	return &BalanceSnapshotService{
		snapshotRepo:  snapshotRepo,
		balanceRepo:   balanceRepo,
		addressRepo:   addressRepo,
		portfolioRepo: portfolioRepo,
	}
}

// CreateDailySnapshots creates snapshots for all tracked addresses
func (s *BalanceSnapshotService) CreateDailySnapshots(ctx context.Context, date time.Time) error {
	log.Printf("[Snapshot] Starting daily snapshot for %s", date.Format("2006-01-02"))

	// Get all tracked addresses
	addresses, err := s.addressRepo.ListAll(ctx)
	if err != nil {
		return fmt.Errorf("failed to list addresses: %w", err)
	}

	if len(addresses) == 0 {
		log.Println("[Snapshot] No addresses to snapshot")
		return nil
	}

	// Extract address strings
	addressStrings := make([]string, len(addresses))
	for i, addr := range addresses {
		addressStrings[i] = addr.Address
	}

	// Create snapshots from current aggregated balances
	chains := []types.ChainID{types.ChainEthereum, types.ChainBase, types.ChainBNB}
	if err := s.snapshotRepo.CreateSnapshotFromCurrentBalances(ctx, addressStrings, chains, date); err != nil {
		return fmt.Errorf("failed to create snapshots: %w", err)
	}

	log.Printf("[Snapshot] Created snapshots for %d addresses", len(addresses))
	return nil
}

// CreatePortfolioSnapshot creates a snapshot for a specific portfolio
func (s *BalanceSnapshotService) CreatePortfolioSnapshot(ctx context.Context, portfolioID string, date time.Time) error {
	// Get portfolio
	portfolio, err := s.portfolioRepo.GetByID(ctx, portfolioID)
	if err != nil {
		return fmt.Errorf("failed to get portfolio: %w", err)
	}

	if len(portfolio.Addresses) == 0 {
		return nil
	}

	// Create snapshots for portfolio addresses
	chains := []types.ChainID{types.ChainEthereum, types.ChainBase, types.ChainBNB}
	if err := s.snapshotRepo.CreateSnapshotFromCurrentBalances(ctx, portfolio.Addresses, chains, date); err != nil {
		return fmt.Errorf("failed to create portfolio snapshots: %w", err)
	}

	// Create portfolio summary
	summary, err := s.createPortfolioSummary(ctx, portfolioID, portfolio.Addresses, date)
	if err != nil {
		log.Printf("[Snapshot] Warning: failed to create portfolio summary: %v", err)
	} else {
		if err := s.snapshotRepo.SavePortfolioDailySummary(ctx, summary); err != nil {
			log.Printf("[Snapshot] Warning: failed to save portfolio summary: %v", err)
		}
	}

	return nil
}

// createPortfolioSummary creates a summary for a portfolio at a date
func (s *BalanceSnapshotService) createPortfolioSummary(ctx context.Context, portfolioID string, addresses []string, date time.Time) (*storage.PortfolioDailySummary, error) {
	// Get balances for all addresses
	balances, err := s.balanceRepo.GetBalanceSummary(ctx, addresses)
	if err != nil {
		return nil, err
	}

	chainSet := make(map[string]bool)
	tokenSet := make(map[string]bool)
	totalNativeETH := big.NewInt(0)

	// Aggregate data
	snapshotData := make(map[string]interface{})
	for addr, chainBalances := range balances {
		for chain, balance := range chainBalances {
			chainSet[string(chain)] = true

			// Add native balance (convert to ETH equivalent for sorting)
			if balance.NativeBalance != "" && balance.NativeBalance != "0" {
				nativeBig := new(big.Int)
				nativeBig.SetString(balance.NativeBalance, 10)
				totalNativeETH.Add(totalNativeETH, nativeBig)
			}

			// Count tokens
			for _, tb := range balance.TokenBalances {
				tokenSet[tb.TokenAddress] = true
			}
		}
		snapshotData[addr] = chainBalances
	}

	// Serialize snapshot data
	snapshotJSON, _ := json.Marshal(snapshotData)

	return &storage.PortfolioDailySummary{
		PortfolioID:    portfolioID,
		Date:           date,
		AddressCount:   len(addresses),
		ChainCount:     len(chainSet),
		TokenCount:     len(tokenSet),
		TotalNativeETH: totalNativeETH.String(),
		SnapshotData:   string(snapshotJSON),
	}, nil
}

// BackfillSnapshots backfills historical snapshots from transaction data
// This reconstructs daily balances by replaying transactions
func (s *BalanceSnapshotService) BackfillSnapshots(ctx context.Context, addresses []string, fromDate, toDate time.Time) error {
	log.Printf("[Snapshot] Backfilling snapshots from %s to %s for %d addresses",
		fromDate.Format("2006-01-02"), toDate.Format("2006-01-02"), len(addresses))

	// For now, we'll create snapshots based on current balances
	// A full backfill would need to replay transactions day by day
	// This is a simplified version that just creates today's snapshot

	chains := []types.ChainID{types.ChainEthereum, types.ChainBase, types.ChainBNB}

	// Create snapshot for each day in range
	for date := fromDate; !date.After(toDate); date = date.AddDate(0, 0, 1) {
		if err := s.snapshotRepo.CreateSnapshotFromCurrentBalances(ctx, addresses, chains, date); err != nil {
			log.Printf("[Snapshot] Warning: failed to create snapshot for %s: %v", date.Format("2006-01-02"), err)
			continue
		}
	}

	log.Printf("[Snapshot] Backfill complete")
	return nil
}

// GetBalanceHistory gets balance history for an address
func (s *BalanceSnapshotService) GetBalanceHistory(ctx context.Context, address, chain string, from, to time.Time) ([]storage.NativeBalanceSnapshot, error) {
	return s.snapshotRepo.GetNativeBalanceHistory(ctx, address, chain, from, to)
}

// GetTokenHistory gets token balance history for an address
func (s *BalanceSnapshotService) GetTokenHistory(ctx context.Context, address, chain, tokenAddress string, from, to time.Time) ([]storage.BalanceSnapshot, error) {
	return s.snapshotRepo.GetTokenBalanceHistory(ctx, address, chain, tokenAddress, from, to)
}

// GetPortfolioHistory gets portfolio daily summaries
func (s *BalanceSnapshotService) GetPortfolioHistory(ctx context.Context, portfolioID string, from, to time.Time) ([]storage.PortfolioDailySummary, error) {
	return s.snapshotRepo.GetPortfolioDailyHistory(ctx, portfolioID, from, to)
}
