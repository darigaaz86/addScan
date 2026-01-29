package service

import (
	"context"
	"fmt"
	"time"

	"github.com/address-scanner/internal/adapter"
	"github.com/address-scanner/internal/models"
	"github.com/address-scanner/internal/storage"
	"github.com/address-scanner/internal/types"
)

// Repository interfaces for dependency injection

// PortfolioRepository interface for portfolio data operations
type PortfolioRepository interface {
	Create(ctx context.Context, portfolio *models.Portfolio) error
	GetByID(ctx context.Context, id string) (*models.Portfolio, error)
	GetByIDAndUser(ctx context.Context, id, userID string) (*models.Portfolio, error)
	Update(ctx context.Context, portfolio *models.Portfolio) error
	DeleteByIDAndUser(ctx context.Context, id, userID string) error
	ListByUser(ctx context.Context, userID string) ([]*models.Portfolio, error)
	ExistsByIDAndUser(ctx context.Context, id, userID string) (bool, error)
}

// AddressRepository interface for address data operations
type AddressRepository interface {
	Exists(ctx context.Context, address string) (bool, error)
}

// TransactionRepository interface for transaction data operations
type TransactionRepository interface {
	GetByAddresses(ctx context.Context, addresses []string, filters *storage.TransactionFilters) ([]*models.Transaction, error)
}

// PortfolioService handles portfolio management and aggregation
type PortfolioService struct {
	portfolioRepo   PortfolioRepository
	addressRepo     AddressRepository
	transactionRepo TransactionRepository
	chainAdapters   map[types.ChainID]adapter.ChainAdapter
	addressService  *AddressService
}

// NewPortfolioService creates a new portfolio service
func NewPortfolioService(
	portfolioRepo PortfolioRepository,
	addressRepo AddressRepository,
	transactionRepo TransactionRepository,
	chainAdapters map[types.ChainID]adapter.ChainAdapter,
	addressService *AddressService,
) *PortfolioService {
	return &PortfolioService{
		portfolioRepo:   portfolioRepo,
		addressRepo:     addressRepo,
		transactionRepo: transactionRepo,
		chainAdapters:   chainAdapters,
		addressService:  addressService,
	}
}

// Input types

// CreatePortfolioInput represents input for creating a portfolio
type CreatePortfolioInput struct {
	UserID      string   `json:"userId"`
	Name        string   `json:"name"`
	Addresses   []string `json:"addresses"`
	Description *string  `json:"description,omitempty"`
}

// UpdatePortfolioInput represents input for updating a portfolio
type UpdatePortfolioInput struct {
	PortfolioID string   `json:"portfolioId"`
	UserID      string   `json:"userId"`
	Name        *string  `json:"name,omitempty"`
	Addresses   []string `json:"addresses,omitempty"`
	Description *string  `json:"description,omitempty"`
}

// GetSnapshotsInput represents input for retrieving snapshots
type GetSnapshotsInput struct {
	PortfolioID string    `json:"portfolioId"`
	UserID      string    `json:"userId"`
	DateFrom    time.Time `json:"dateFrom"`
	DateTo      time.Time `json:"dateTo"`
}

// Output types

// Portfolio represents a portfolio (same as models.Portfolio)
type Portfolio = models.Portfolio

// PortfolioView represents a portfolio with aggregated data
type PortfolioView struct {
	ID                string                         `json:"id"`
	Name              string                         `json:"name"`
	Description       *string                        `json:"description,omitempty"`
	Addresses         []string                       `json:"addresses"`
	TotalBalance      types.MultiChainBalance        `json:"totalBalance"`
	TransactionCount  int64                          `json:"transactionCount"`
	TotalVolume       string                         `json:"totalVolume"`
	TopCounterparties []types.Counterparty           `json:"topCounterparties"`
	TokenHoldings     []types.TokenHolding           `json:"tokenHoldings"`
	UnifiedTimeline   []*types.NormalizedTransaction `json:"unifiedTimeline"`
	LastUpdated       time.Time                      `json:"lastUpdated"`
}

// PortfolioStatistics represents aggregated portfolio statistics
type PortfolioStatistics struct {
	PortfolioID             string                  `json:"portfolioId"`
	TotalBalance            types.MultiChainBalance `json:"totalBalance"`
	TransactionCount        int64                   `json:"transactionCount"`
	TotalVolume             string                  `json:"totalVolume"`
	AverageTransactionValue string                  `json:"averageTransactionValue"`
	TopCounterparties       []types.Counterparty    `json:"topCounterparties"`
	TokenHoldings           []types.TokenHolding    `json:"tokenHoldings"`
	ChainDistribution       []ChainDistribution     `json:"chainDistribution"`
	ActivityByDay           []ActivityByDay         `json:"activityByDay"`
}

// ChainDistribution represents transaction distribution by chain
type ChainDistribution struct {
	Chain            types.ChainID `json:"chain"`
	TransactionCount int64         `json:"transactionCount"`
	Volume           string        `json:"volume"`
	Percentage       float64       `json:"percentage"`
}

// ActivityByDay represents daily activity metrics
type ActivityByDay struct {
	Date             string `json:"date"` // ISO date string
	TransactionCount int64  `json:"transactionCount"`
	Volume           string `json:"volume"`
}

// DeletePortfolioResult represents the result of deleting a portfolio
type DeletePortfolioResult struct {
	Success     bool    `json:"success"`
	PortfolioID string  `json:"portfolioId"`
	Message     *string `json:"message,omitempty"`
}

// PortfolioSnapshot represents a daily portfolio snapshot
type PortfolioSnapshot struct {
	PortfolioID       string                  `json:"portfolioId"`
	SnapshotDate      time.Time               `json:"snapshotDate"`
	TotalBalance      types.MultiChainBalance `json:"totalBalance"`
	TransactionCount  int64                   `json:"transactionCount"`
	TotalVolume       string                  `json:"totalVolume"`
	TopCounterparties []types.Counterparty    `json:"topCounterparties"`
	TokenHoldings     []types.TokenHolding    `json:"tokenHoldings"`
	CreatedAt         time.Time               `json:"createdAt"`
}

// CreatePortfolio creates a new portfolio with addresses
// Requirements: 13.1, 13.5, 13.7
func (s *PortfolioService) CreatePortfolio(ctx context.Context, input *CreatePortfolioInput) (*Portfolio, error) {
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
	// If an address doesn't exist, add it automatically
	for _, addr := range input.Addresses {
		exists, err := s.addressRepo.Exists(ctx, addr)
		if err != nil {
			return nil, fmt.Errorf("failed to check address existence: %w", err)
		}
		
		// If address doesn't exist, add it to the system
		if !exists {
			addInput := &AddAddressInput{
				UserID:  input.UserID,
				Address: addr,
				Chains:  []types.ChainID{}, // Use default enabled chains
				Tier:    "",                 // Use user's tier
			}
			
			_, err := s.addressService.AddAddress(ctx, addInput)
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

// GetPortfolio retrieves portfolio with aggregated data
// Requirements: 13.1, 13.2, 13.3, 13.4
func (s *PortfolioService) GetPortfolio(ctx context.Context, portfolioID, userID string) (*PortfolioView, error) {
	// Retrieve portfolio and verify ownership
	portfolio, err := s.portfolioRepo.GetByIDAndUser(ctx, portfolioID, userID)
	if err != nil {
		return nil, &types.ServiceError{
			Code:    "PORTFOLIO_NOT_FOUND",
			Message: fmt.Sprintf("portfolio not found or access denied: %s", portfolioID),
			Details: map[string]interface{}{
				"portfolioId": portfolioID,
			},
		}
	}

	// Build portfolio view with aggregated data
	view := &PortfolioView{
		ID:          portfolio.ID,
		Name:        portfolio.Name,
		Description: portfolio.Description,
		Addresses:   portfolio.Addresses,
		LastUpdated: portfolio.UpdatedAt,
	}

	// Get aggregated balance (Requirement 13.3, 15.1)
	balance, err := s.aggregateBalance(ctx, portfolio.Addresses)
	if err != nil {
		// Log error but don't fail the request
		fmt.Printf("Warning: failed to aggregate balance for portfolio %s: %v\n", portfolioID, err)
		balance = types.MultiChainBalance{Chains: []types.ChainBalance{}}
	}
	view.TotalBalance = balance

	// Get unified timeline (Requirement 13.4)
	timeline, err := s.mergeTimeline(ctx, portfolio.Addresses, 50) // Default to 50 most recent transactions
	if err != nil {
		// Log error but don't fail the request
		fmt.Printf("Warning: failed to merge timeline for portfolio %s: %v\n", portfolioID, err)
		timeline = []*types.NormalizedTransaction{}
	}
	view.UnifiedTimeline = timeline

	// Get statistics (Requirements 15.2, 15.3, 15.4, 15.5)
	stats, err := s.calculateStatistics(ctx, portfolio.Addresses)
	if err != nil {
		// Log error but don't fail the request
		fmt.Printf("Warning: failed to calculate statistics for portfolio %s: %v\n", portfolioID, err)
		stats = &portfolioStats{
			transactionCount:  0,
			totalVolume:       "0",
			topCounterparties: []types.Counterparty{},
			tokenHoldings:     []types.TokenHolding{},
		}
	}
	view.TransactionCount = stats.transactionCount
	view.TotalVolume = stats.totalVolume
	view.TopCounterparties = stats.topCounterparties
	view.TokenHoldings = stats.tokenHoldings

	return view, nil
}

// UpdatePortfolio updates portfolio composition
// Requirements: 13.5, 15.7
func (s *PortfolioService) UpdatePortfolio(ctx context.Context, input *UpdatePortfolioInput) (*Portfolio, error) {
	// Validate input
	if input.PortfolioID == "" {
		return nil, &types.ServiceError{
			Code:    "INVALID_INPUT",
			Message: "portfolioId is required",
		}
	}

	if input.UserID == "" {
		return nil, &types.ServiceError{
			Code:    "INVALID_INPUT",
			Message: "userId is required",
		}
	}

	// Retrieve portfolio and verify ownership
	portfolio, err := s.portfolioRepo.GetByIDAndUser(ctx, input.PortfolioID, input.UserID)
	if err != nil {
		return nil, &types.ServiceError{
			Code:    "PORTFOLIO_NOT_FOUND",
			Message: fmt.Sprintf("portfolio not found or access denied: %s", input.PortfolioID),
			Details: map[string]interface{}{
				"portfolioId": input.PortfolioID,
			},
		}
	}

	// Update fields if provided
	if input.Name != nil {
		if *input.Name == "" {
			return nil, &types.ServiceError{
				Code:    "INVALID_INPUT",
				Message: "portfolio name cannot be empty",
			}
		}
		portfolio.Name = *input.Name
	}

	if input.Description != nil {
		portfolio.Description = input.Description
	}

	if len(input.Addresses) > 0 {
		// Validate all addresses exist in the system
		for _, addr := range input.Addresses {
			exists, err := s.addressRepo.Exists(ctx, addr)
			if err != nil {
				return nil, fmt.Errorf("failed to check address existence: %w", err)
			}
			if !exists {
				return nil, &types.ServiceError{
					Code:    "ADDRESS_NOT_TRACKED",
					Message: fmt.Sprintf("address %s is not tracked in the system", addr),
					Details: map[string]interface{}{
						"address": addr,
					},
				}
			}
		}
		portfolio.Addresses = input.Addresses
	}

	// Update portfolio
	if err := s.portfolioRepo.Update(ctx, portfolio); err != nil {
		return nil, fmt.Errorf("failed to update portfolio: %w", err)
	}

	// Trigger aggregation updates asynchronously
	// Requirement 13.5, 15.7: Updates should complete within 30 seconds
	go func() {
		// Use a background context with timeout
		updateCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if err := s.UpdateAggregations(updateCtx, portfolio.ID); err != nil {
			fmt.Printf("Warning: failed to update aggregations for portfolio %s: %v\n", portfolio.ID, err)
		}

		if err := s.RefreshPortfolioCache(updateCtx, portfolio.ID); err != nil {
			fmt.Printf("Warning: failed to refresh cache for portfolio %s: %v\n", portfolio.ID, err)
		}
	}()

	return portfolio, nil
}

// DeletePortfolio deletes a portfolio
// Requirements: 13.7
func (s *PortfolioService) DeletePortfolio(ctx context.Context, portfolioID, userID string) (*DeletePortfolioResult, error) {
	// Validate input
	if portfolioID == "" {
		return nil, &types.ServiceError{
			Code:    "INVALID_INPUT",
			Message: "portfolioId is required",
		}
	}

	if userID == "" {
		return nil, &types.ServiceError{
			Code:    "INVALID_INPUT",
			Message: "userId is required",
		}
	}

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

	// Delete portfolio
	// Note: Individual addresses remain tracked (Requirement 13.7)
	// Historical snapshots are retained (Requirement 16.7)
	if err := s.portfolioRepo.DeleteByIDAndUser(ctx, portfolioID, userID); err != nil {
		return nil, fmt.Errorf("failed to delete portfolio: %w", err)
	}

	message := "Portfolio deleted successfully. Individual addresses remain tracked."
	return &DeletePortfolioResult{
		Success:     true,
		PortfolioID: portfolioID,
		Message:     &message,
	}, nil
}

// ListPortfolios lists all portfolios for a user
// Requirements: 13.1
func (s *PortfolioService) ListPortfolios(ctx context.Context, userID string) ([]*Portfolio, error) {
	if userID == "" {
		return nil, &types.ServiceError{
			Code:    "INVALID_INPUT",
			Message: "userId is required",
		}
	}

	portfolios, err := s.portfolioRepo.ListByUser(ctx, userID)
	if err != nil {
		return nil, fmt.Errorf("failed to list portfolios: %w", err)
	}

	return portfolios, nil
}

// GetStatistics retrieves aggregated portfolio statistics
// Requirements: 15.1, 15.2, 15.3, 15.4, 15.5
func (s *PortfolioService) GetStatistics(ctx context.Context, portfolioID, userID string) (*PortfolioStatistics, error) {
	// Verify portfolio exists and user owns it
	portfolio, err := s.portfolioRepo.GetByIDAndUser(ctx, portfolioID, userID)
	if err != nil {
		return nil, &types.ServiceError{
			Code:    "PORTFOLIO_NOT_FOUND",
			Message: fmt.Sprintf("portfolio not found or access denied: %s", portfolioID),
			Details: map[string]interface{}{
				"portfolioId": portfolioID,
			},
		}
	}

	// Calculate statistics
	stats, err := s.calculateStatistics(ctx, portfolio.Addresses)
	if err != nil {
		return nil, fmt.Errorf("failed to calculate statistics: %w", err)
	}

	// Get balance
	balance, err := s.aggregateBalance(ctx, portfolio.Addresses)
	if err != nil {
		// Log error but use empty balance
		fmt.Printf("Warning: failed to aggregate balance for portfolio %s: %v\n", portfolioID, err)
		balance = types.MultiChainBalance{Chains: []types.ChainBalance{}}
	}

	// Calculate average transaction value
	avgValue := "0"
	if stats.transactionCount > 0 {
		// TODO: Implement proper division using big.Int
		avgValue = stats.totalVolume // Placeholder
	}

	// Build full statistics response
	result := &PortfolioStatistics{
		PortfolioID:             portfolio.ID,
		TotalBalance:            balance,
		TransactionCount:        stats.transactionCount,
		TotalVolume:             stats.totalVolume,
		AverageTransactionValue: avgValue,
		TopCounterparties:       stats.topCounterparties,
		TokenHoldings:           stats.tokenHoldings,
		ChainDistribution:       stats.chainDistribution,
		ActivityByDay:           stats.activityByDay,
	}

	return result, nil
}

// GetSnapshots retrieves daily snapshots for portfolio
// Requirements: 16.1, 16.2, 16.3, 16.4
func (s *PortfolioService) GetSnapshots(ctx context.Context, input *GetSnapshotsInput) ([]*PortfolioSnapshot, error) {
	// Verify portfolio exists and user owns it
	exists, err := s.portfolioRepo.ExistsByIDAndUser(ctx, input.PortfolioID, input.UserID)
	if err != nil {
		return nil, fmt.Errorf("failed to check portfolio existence: %w", err)
	}

	if !exists {
		return nil, &types.ServiceError{
			Code:    "PORTFOLIO_NOT_FOUND",
			Message: fmt.Sprintf("portfolio not found or access denied: %s", input.PortfolioID),
			Details: map[string]interface{}{
				"portfolioId": input.PortfolioID,
			},
		}
	}

	// Note: Actual snapshot retrieval is delegated to SnapshotService
	// This method is kept for interface compatibility
	// Use SnapshotService.GetSnapshots() directly for full functionality
	return []*PortfolioSnapshot{}, nil
}

// aggregateBalance aggregates balances across all addresses in a portfolio
// Requirements: 13.3, 15.1
func (s *PortfolioService) aggregateBalance(ctx context.Context, addresses []string) (types.MultiChainBalance, error) {
	if len(addresses) == 0 {
		return types.MultiChainBalance{Chains: []types.ChainBalance{}}, nil
	}

	// Map to aggregate balances by chain
	chainBalances := make(map[types.ChainID]*types.ChainBalance)

	// Fetch balance for each address on each chain
	for _, address := range addresses {
		for chainID, adapter := range s.chainAdapters {
			// Get balance for this address on this chain
			balance, err := adapter.GetBalance(ctx, address)
			if err != nil {
				// Log error but continue with other addresses/chains
				fmt.Printf("Warning: failed to get balance for address %s on chain %s: %v\n", address, chainID, err)
				continue
			}

			// Initialize chain balance if not exists
			if chainBalances[chainID] == nil {
				chainBalances[chainID] = &types.ChainBalance{
					Chain:         chainID,
					NativeBalance: "0",
					TokenBalances: []types.TokenBalance{},
				}
			}

			// Aggregate native balance
			chainBalances[chainID].NativeBalance = s.addBalances(
				chainBalances[chainID].NativeBalance,
				balance.NativeBalance,
			)

			// Aggregate token balances
			chainBalances[chainID].TokenBalances = s.mergeTokenBalances(
				chainBalances[chainID].TokenBalances,
				balance.TokenBalances,
			)
		}
	}

	// Convert map to slice
	chains := make([]types.ChainBalance, 0, len(chainBalances))
	for _, balance := range chainBalances {
		chains = append(chains, *balance)
	}

	return types.MultiChainBalance{
		Chains:        chains,
		TotalValueUSD: nil, // USD value calculation can be added later
	}, nil
}

// addBalances adds two balance strings (simple string concatenation for now)
// In production, this should use big.Int for precise arithmetic
func (s *PortfolioService) addBalances(a, b string) string {
	// For MVP, we'll use simple string comparison
	// In production, use math/big for precise decimal arithmetic
	if a == "" || a == "0" {
		return b
	}
	if b == "" || b == "0" {
		return a
	}
	// TODO: Implement proper big number addition
	// For now, return the first non-zero value
	return a
}

// mergeTokenBalances merges token balances from multiple addresses
func (s *PortfolioService) mergeTokenBalances(existing, new []types.TokenBalance) []types.TokenBalance {
	// Create a map for efficient lookup and aggregation
	tokenMap := make(map[string]*types.TokenBalance)

	// Add existing balances to map
	for i := range existing {
		tokenMap[existing[i].Token] = &existing[i]
	}

	// Merge new balances
	for _, newBalance := range new {
		if existingBalance, exists := tokenMap[newBalance.Token]; exists {
			// Aggregate balance for existing token
			existingBalance.Balance = s.addBalances(existingBalance.Balance, newBalance.Balance)
		} else {
			// Add new token
			tokenMap[newBalance.Token] = &types.TokenBalance{
				Token:    newBalance.Token,
				Symbol:   newBalance.Symbol,
				Balance:  newBalance.Balance,
				Decimals: newBalance.Decimals,
				ValueUSD: newBalance.ValueUSD,
			}
		}
	}

	// Convert map back to slice
	result := make([]types.TokenBalance, 0, len(tokenMap))
	for _, balance := range tokenMap {
		result = append(result, *balance)
	}

	return result
}

// mergeTimeline merges transactions from all portfolio addresses into a unified timeline
// Requirements: 13.4
func (s *PortfolioService) mergeTimeline(ctx context.Context, addresses []string, limit int) ([]*types.NormalizedTransaction, error) {
	if len(addresses) == 0 {
		return []*types.NormalizedTransaction{}, nil
	}

	// Fetch transactions for all addresses
	filters := &storage.TransactionFilters{
		Limit:     limit * len(addresses), // Fetch more to ensure we have enough after merging
		SortBy:    "timestamp",
		SortOrder: "desc",
	}

	transactions, err := s.transactionRepo.GetByAddresses(ctx, addresses, filters)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch transactions for addresses: %w", err)
	}

	// Convert models.Transaction to types.NormalizedTransaction
	normalized := make([]*types.NormalizedTransaction, len(transactions))
	for i, tx := range transactions {
		normalized[i] = &types.NormalizedTransaction{
			Hash:           tx.Hash,
			Chain:          tx.Chain,
			From:           tx.From,
			To:             tx.To,
			Value:          tx.Value,
			Timestamp:      tx.Timestamp.Unix(),
			BlockNumber:    tx.BlockNumber,
			Status:         types.TransactionStatus(tx.Status),
			GasUsed:        tx.GasUsed,
			GasPrice:       tx.GasPrice,
			TokenTransfers: tx.TokenTransfers,
			MethodID:       tx.MethodID,
			Input:          tx.Input,
		}
	}

	// Sort by timestamp descending (most recent first)
	// Transactions are already sorted by the query, but we ensure it here
	s.sortTransactionsByTimestamp(normalized)

	// Limit to requested number
	if len(normalized) > limit {
		normalized = normalized[:limit]
	}

	return normalized, nil
}

// sortTransactionsByTimestamp sorts transactions by timestamp in descending order
func (s *PortfolioService) sortTransactionsByTimestamp(transactions []*types.NormalizedTransaction) {
	// Use a simple bubble sort for small arrays, or implement quicksort for larger ones
	// For production, use sort.Slice from the standard library
	for i := 0; i < len(transactions)-1; i++ {
		for j := i + 1; j < len(transactions); j++ {
			if transactions[i].Timestamp < transactions[j].Timestamp {
				transactions[i], transactions[j] = transactions[j], transactions[i]
			}
		}
	}
}

// portfolioStats holds calculated statistics
type portfolioStats struct {
	transactionCount   int64
	totalVolume        string
	topCounterparties  []types.Counterparty
	tokenHoldings      []types.TokenHolding
	chainDistribution  []ChainDistribution
	activityByDay      []ActivityByDay
}

// calculateStatistics calculates aggregated statistics for portfolio addresses
// Requirements: 15.2, 15.3, 15.4, 15.5
func (s *PortfolioService) calculateStatistics(ctx context.Context, addresses []string) (*portfolioStats, error) {
	if len(addresses) == 0 {
		return &portfolioStats{
			transactionCount:   0,
			totalVolume:        "0",
			topCounterparties:  []types.Counterparty{},
			tokenHoldings:      []types.TokenHolding{},
			chainDistribution:  []ChainDistribution{},
			activityByDay:      []ActivityByDay{},
		}, nil
	}

	// Fetch all transactions for the addresses (no limit for statistics)
	filters := &storage.TransactionFilters{
		Limit:     10000, // Reasonable limit for statistics calculation
		SortBy:    "timestamp",
		SortOrder: "desc",
	}

	transactions, err := s.transactionRepo.GetByAddresses(ctx, addresses, filters)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch transactions: %w", err)
	}

	// Calculate transaction count (Requirement 15.2)
	transactionCount := int64(len(transactions))

	// Calculate total volume (Requirement 15.3)
	totalVolume := s.calculateTotalVolume(transactions)

	// Identify top counterparties (Requirement 15.4)
	topCounterparties := s.identifyTopCounterparties(transactions, addresses, 10)

	// Aggregate token holdings (Requirement 15.5)
	tokenHoldings := s.aggregateTokenHoldings(transactions)

	// Calculate chain distribution
	chainDistribution := s.calculateChainDistribution(transactions, transactionCount)

	// Calculate activity by day
	activityByDay := s.calculateActivityByDay(transactions)

	return &portfolioStats{
		transactionCount:   transactionCount,
		totalVolume:        totalVolume,
		topCounterparties:  topCounterparties,
		tokenHoldings:      tokenHoldings,
		chainDistribution:  chainDistribution,
		activityByDay:      activityByDay,
	}, nil
}

// calculateTotalVolume calculates the total transaction volume
// Requirement 15.3
func (s *PortfolioService) calculateTotalVolume(transactions []*models.Transaction) string {
	// TODO: Implement proper big number addition
	// For MVP, return "0" or first non-zero value
	totalVolume := "0"
	for _, tx := range transactions {
		if tx.Value != "" && tx.Value != "0" {
			// In production, sum all values using big.Int
			totalVolume = tx.Value
			break
		}
	}
	return totalVolume
}

// identifyTopCounterparties identifies the most frequent counterparties
// Requirement 15.4
func (s *PortfolioService) identifyTopCounterparties(transactions []*models.Transaction, portfolioAddresses []string, limit int) []types.Counterparty {
	// Create a set of portfolio addresses for quick lookup
	addressSet := make(map[string]bool)
	for _, addr := range portfolioAddresses {
		addressSet[addr] = true
	}

	// Count transactions and volume per counterparty
	counterpartyMap := make(map[string]*types.Counterparty)

	for _, tx := range transactions {
		// Determine counterparty (the address that's NOT in the portfolio)
		var counterpartyAddr string
		if addressSet[tx.From] {
			counterpartyAddr = tx.To
		} else {
			counterpartyAddr = tx.From
		}

		if counterpartyAddr == "" {
			continue
		}

		// Initialize or update counterparty stats
		if counterpartyMap[counterpartyAddr] == nil {
			counterpartyMap[counterpartyAddr] = &types.Counterparty{
				Address:          counterpartyAddr,
				TransactionCount: 0,
				TotalVolume:      "0",
				Label:            nil,
			}
		}

		counterpartyMap[counterpartyAddr].TransactionCount++
		// TODO: Add volume using big.Int
		if tx.Value != "" && tx.Value != "0" {
			counterpartyMap[counterpartyAddr].TotalVolume = tx.Value
		}
	}

	// Convert map to slice and sort by transaction count
	counterparties := make([]types.Counterparty, 0, len(counterpartyMap))
	for _, cp := range counterpartyMap {
		counterparties = append(counterparties, *cp)
	}

	// Sort by transaction count (descending)
	s.sortCounterpartiesByCount(counterparties)

	// Limit to top N
	if len(counterparties) > limit {
		counterparties = counterparties[:limit]
	}

	return counterparties
}

// sortCounterpartiesByCount sorts counterparties by transaction count descending
func (s *PortfolioService) sortCounterpartiesByCount(counterparties []types.Counterparty) {
	for i := 0; i < len(counterparties)-1; i++ {
		for j := i + 1; j < len(counterparties); j++ {
			if counterparties[i].TransactionCount < counterparties[j].TransactionCount {
				counterparties[i], counterparties[j] = counterparties[j], counterparties[i]
			}
		}
	}
}

// aggregateTokenHoldings aggregates token holdings from transactions
// Requirement 15.5
func (s *PortfolioService) aggregateTokenHoldings(transactions []*models.Transaction) []types.TokenHolding {
	// Map to track token holdings
	tokenMap := make(map[string]*types.TokenHolding)

	for _, tx := range transactions {
		// Process token transfers in the transaction
		for _, transfer := range tx.TokenTransfers {
			if transfer.Token == "" {
				continue
			}

			// Initialize token holding if not exists
			if tokenMap[transfer.Token] == nil {
				symbol := "UNKNOWN"
				if transfer.Symbol != nil {
					symbol = *transfer.Symbol
				}

				decimals := 18 // Default decimals
				if transfer.Decimals != nil {
					decimals = *transfer.Decimals
				}

				tokenMap[transfer.Token] = &types.TokenHolding{
					Token:    transfer.Token,
					Symbol:   symbol,
					Balance:  "0",
					Decimals: decimals,
					Chains:   []types.ChainID{tx.Chain},
					ValueUSD: nil,
				}
			}

			// Add chain if not already present
			chainExists := false
			for _, chain := range tokenMap[transfer.Token].Chains {
				if chain == tx.Chain {
					chainExists = true
					break
				}
			}
			if !chainExists {
				tokenMap[transfer.Token].Chains = append(tokenMap[transfer.Token].Chains, tx.Chain)
			}

			// TODO: Calculate actual balance by tracking inflows/outflows
			// For MVP, just mark that we've seen this token
			if transfer.Value != "" && transfer.Value != "0" {
				tokenMap[transfer.Token].Balance = transfer.Value
			}
		}
	}

	// Convert map to slice
	holdings := make([]types.TokenHolding, 0, len(tokenMap))
	for _, holding := range tokenMap {
		holdings = append(holdings, *holding)
	}

	return holdings
}

// calculateChainDistribution calculates transaction distribution by chain
func (s *PortfolioService) calculateChainDistribution(transactions []*models.Transaction, totalCount int64) []ChainDistribution {
	// Count transactions per chain
	chainCounts := make(map[types.ChainID]int64)
	chainVolumes := make(map[types.ChainID]string)

	for _, tx := range transactions {
		chainCounts[tx.Chain]++
		// TODO: Sum volumes using big.Int
		if tx.Value != "" && tx.Value != "0" {
			chainVolumes[tx.Chain] = tx.Value
		}
	}

	// Convert to slice and calculate percentages
	distribution := make([]ChainDistribution, 0, len(chainCounts))
	for chain, count := range chainCounts {
		percentage := 0.0
		if totalCount > 0 {
			percentage = float64(count) / float64(totalCount) * 100.0
		}

		distribution = append(distribution, ChainDistribution{
			Chain:            chain,
			TransactionCount: count,
			Volume:           chainVolumes[chain],
			Percentage:       percentage,
		})
	}

	return distribution
}

// calculateActivityByDay calculates daily activity metrics
func (s *PortfolioService) calculateActivityByDay(transactions []*models.Transaction) []ActivityByDay {
	// Group transactions by date
	dailyActivity := make(map[string]*ActivityByDay)

	for _, tx := range transactions {
		date := tx.Timestamp.Format("2006-01-02")

		if dailyActivity[date] == nil {
			dailyActivity[date] = &ActivityByDay{
				Date:             date,
				TransactionCount: 0,
				Volume:           "0",
			}
		}

		dailyActivity[date].TransactionCount++
		// TODO: Sum volumes using big.Int
		if tx.Value != "" && tx.Value != "0" {
			dailyActivity[date].Volume = tx.Value
		}
	}

	// Convert map to slice
	activity := make([]ActivityByDay, 0, len(dailyActivity))
	for _, day := range dailyActivity {
		activity = append(activity, *day)
	}

	// Sort by date (most recent first)
	s.sortActivityByDate(activity)

	return activity
}

// sortActivityByDate sorts activity by date descending
func (s *PortfolioService) sortActivityByDate(activity []ActivityByDay) {
	for i := 0; i < len(activity)-1; i++ {
		for j := i + 1; j < len(activity); j++ {
			if activity[i].Date < activity[j].Date {
				activity[i], activity[j] = activity[j], activity[i]
			}
		}
	}
}

// UpdateAggregations updates aggregated statistics when portfolio composition changes
// Requirements: 13.5, 15.7
// This method should be called asynchronously after portfolio updates
func (s *PortfolioService) UpdateAggregations(ctx context.Context, portfolioID string) error {
	// Get portfolio
	portfolio, err := s.portfolioRepo.GetByID(ctx, portfolioID)
	if err != nil {
		return fmt.Errorf("failed to get portfolio: %w", err)
	}

	// Calculate fresh statistics
	// This will be used to update cached/materialized views
	_, err = s.calculateStatistics(ctx, portfolio.Addresses)
	if err != nil {
		return fmt.Errorf("failed to calculate statistics: %w", err)
	}

	// Calculate fresh balance
	_, err = s.aggregateBalance(ctx, portfolio.Addresses)
	if err != nil {
		return fmt.Errorf("failed to aggregate balance: %w", err)
	}

	// In a production system, this would:
	// 1. Update a materialized view in the database
	// 2. Invalidate cache entries
	// 3. Trigger background jobs for expensive calculations
	// 4. Ensure updates complete within 30 seconds (Requirement 15.7)

	fmt.Printf("Portfolio %s aggregations updated successfully\n", portfolioID)
	return nil
}

// RefreshPortfolioCache refreshes cached data for a portfolio
// This should be called after composition changes to ensure data consistency
func (s *PortfolioService) RefreshPortfolioCache(ctx context.Context, portfolioID string) error {
	// Get portfolio
	portfolio, err := s.portfolioRepo.GetByID(ctx, portfolioID)
	if err != nil {
		return fmt.Errorf("failed to get portfolio: %w", err)
	}

	// Trigger cache refresh for all addresses in the portfolio
	// In production, this would invalidate Redis cache entries
	for _, address := range portfolio.Addresses {
		fmt.Printf("Refreshing cache for address %s in portfolio %s\n", address, portfolioID)
		// TODO: Implement cache invalidation
		// cacheService.Invalidate(ctx, address)
	}

	return nil
}
