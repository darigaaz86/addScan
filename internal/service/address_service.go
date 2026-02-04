package service

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/address-scanner/internal/adapter"
	"github.com/address-scanner/internal/job"
	"github.com/address-scanner/internal/models"
	"github.com/address-scanner/internal/storage"
	"github.com/address-scanner/internal/types"
)

// AddressService handles address tracking and transaction queries
type AddressService struct {
	addressRepo   *storage.AddressRepository
	userRepo      *storage.UserRepository
	balanceRepo   *storage.BalanceRepository // For calculating balances from tx history
	chainAdapters map[types.ChainID]adapter.ChainAdapter
	backfillQueue job.BackfillJob
}

// NewAddressService creates a new address service
func NewAddressService(
	addressRepo *storage.AddressRepository,
	userRepo *storage.UserRepository,
	chainAdapters map[types.ChainID]adapter.ChainAdapter,
	backfillQueue job.BackfillJob,
) *AddressService {
	return &AddressService{
		addressRepo:   addressRepo,
		userRepo:      userRepo,
		chainAdapters: chainAdapters,
		backfillQueue: backfillQueue,
	}
}

// NewAddressServiceWithBalanceRepo creates address service with balance repository
// Use this for paid tier users who can calculate balances from transaction history
func NewAddressServiceWithBalanceRepo(
	addressRepo *storage.AddressRepository,
	userRepo *storage.UserRepository,
	balanceRepo *storage.BalanceRepository,
	chainAdapters map[types.ChainID]adapter.ChainAdapter,
	backfillQueue job.BackfillJob,
) *AddressService {
	return &AddressService{
		addressRepo:   addressRepo,
		userRepo:      userRepo,
		balanceRepo:   balanceRepo,
		chainAdapters: chainAdapters,
		backfillQueue: backfillQueue,
	}
}

// AddAddressInput represents input for adding an address
type AddAddressInput struct {
	UserID  string          `json:"userId"`
	Address string          `json:"address"`
	Chains  []types.ChainID `json:"chains,omitempty"` // Optional: specific chains to track
	Tier    types.UserTier  `json:"tier"`
}

// AddressTrackingResult represents the result of adding an address
type AddressTrackingResult struct {
	Success               bool            `json:"success"`
	Address               string          `json:"address"`
	Chains                []types.ChainID `json:"chains"`
	TrackingCreated       bool            `json:"trackingCreated"` // True if new tracking
	BackfillJobID         string          `json:"backfillJobId"`
	BackfillStatus        string          `json:"backfillStatus"` // queued, in_progress, completed
	EstimatedTransactions *int64          `json:"estimatedTransactions,omitempty"`
	Message               *string         `json:"message,omitempty"`
}

// AddAddress adds an address to the global tracking registry
// This is called internally when portfolios are created/updated
// Validates address format, creates tracking record, triggers backfill job, handles deduplication
// Requirements: 1.1, 1.2, 1.5, 1.7
func (s *AddressService) AddAddress(ctx context.Context, input *AddAddressInput) (*AddressTrackingResult, error) {
	// Validate address format using chain adapters
	if err := s.validateAddress(input.Address); err != nil {
		return nil, err
	}

	// Normalize address to lowercase
	input.Address = strings.ToLower(input.Address)

	// Verify user exists and get their tier
	user, err := s.userRepo.GetByID(ctx, input.UserID)
	if err != nil {
		return nil, &types.ServiceError{
			Code:    "USER_NOT_FOUND",
			Message: fmt.Sprintf("user not found: %s", input.UserID),
			Details: map[string]interface{}{
				"userId": input.UserID,
			},
		}
	}

	// Use user's tier if not explicitly provided
	if input.Tier == "" {
		input.Tier = user.Tier
	}

	// Determine chains to track (default to all enabled chains if not specified)
	chains := input.Chains
	if len(chains) == 0 {
		chains = s.getEnabledChains()
	}

	// Check if address exists in global registry (deduplication at system level)
	// Requirement 1.5: Deduplicate sync work and share cached data
	addressExists, err := s.addressRepo.Exists(ctx, input.Address)
	if err != nil {
		return nil, fmt.Errorf("failed to check address existence: %w", err)
	}

	trackingCreated := false
	var backfillJobID string
	backfillStatus := "completed"

	// If address doesn't exist in system, create records for each chain and trigger backfill
	if !addressExists {
		now := time.Now()

		// Create one address record per chain
		for _, chain := range chains {
			address := &models.Address{
				Address:          input.Address,
				Chain:            chain,
				Classification:   types.ClassificationNormal,
				TransactionCount: 0,
				FirstSeen:        now,
				LastActivity:     now,
				TotalVolume:      "0",
				Labels:           []string{},
				BackfillComplete: false,
				BackfillTier:     input.Tier,
				BackfillTxCount:  0,
				LastSyncedBlock:  0,
			}

			if err := s.addressRepo.Create(ctx, address); err != nil {
				return nil, fmt.Errorf("failed to create address for chain %s: %w", chain, err)
			}
		}

		trackingCreated = true

		// Trigger backfill job if backfill queue is available
		// Requirement 1.2: Trigger backfill job to fetch historical transactions
		if s.backfillQueue != nil {
			backfillInput := &job.BackfillJobInput{
				Address:  input.Address,
				Chains:   chains,
				Tier:     input.Tier,
				Priority: s.getBackfillPriority(input.Tier),
			}

			backfillResult, err := s.backfillQueue.Execute(ctx, backfillInput)
			if err != nil {
				// Log error but don't fail the address addition
				// The address is tracked, backfill can be retried later
				fmt.Printf("Warning: failed to start backfill job for address %s: %v\n", input.Address, err)
				backfillStatus = "failed"
			} else {
				backfillJobID = backfillResult.JobID
				backfillStatus = backfillResult.Status
			}
		} else {
			// No backfill queue available - worker will handle historical sync
			backfillStatus = "pending"
		}
	} else {
		// Address already exists in system - reuse existing (deduplication)
		trackingCreated = false
		backfillStatus = "completed" // Backfill already done for this address
	}

	return &AddressTrackingResult{
		Success:         true,
		Address:         input.Address,
		Chains:          chains,
		TrackingCreated: trackingCreated,
		BackfillJobID:   backfillJobID,
		BackfillStatus:  backfillStatus,
	}, nil
}

// validateAddress validates address format using chain adapters
// Requirement 1.6: Reject invalid address formats with descriptive error
func (s *AddressService) validateAddress(address string) error {
	if address == "" {
		return &types.ServiceError{
			Code:    "INVALID_ADDRESS",
			Message: "address cannot be empty",
			Details: map[string]interface{}{
				"address": address,
			},
		}
	}

	// If chain adapters are available, use them for validation
	if len(s.chainAdapters) > 0 {
		valid := false
		for _, chainAdapter := range s.chainAdapters {
			if chainAdapter.ValidateAddress(address) {
				valid = true
				break
			}
		}

		if !valid {
			return &types.ServiceError{
				Code:    "INVALID_ADDRESS_FORMAT",
				Message: fmt.Sprintf("invalid address format: %s (must be a valid blockchain address)", address),
				Details: map[string]interface{}{
					"address": address,
					"format":  "0x[a-fA-F0-9]{40}",
				},
			}
		}
	} else {
		// Fallback: Basic Ethereum address format validation
		// Ethereum addresses are 42 characters: 0x + 40 hex characters
		if len(address) != 42 || !strings.HasPrefix(address, "0x") {
			return &types.ServiceError{
				Code:    "INVALID_ADDRESS_FORMAT",
				Message: fmt.Sprintf("invalid address format: %s (must be 0x followed by 40 hex characters)", address),
				Details: map[string]interface{}{
					"address": address,
					"format":  "0x[a-fA-F0-9]{40}",
				},
			}
		}

		// Check if all characters after 0x are valid hex
		for _, c := range address[2:] {
			if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
				return &types.ServiceError{
					Code:    "INVALID_ADDRESS_FORMAT",
					Message: fmt.Sprintf("invalid address format: %s (contains non-hex characters)", address),
					Details: map[string]interface{}{
						"address": address,
						"format":  "0x[a-fA-F0-9]{40}",
					},
				}
			}
		}
	}

	return nil
}

// getEnabledChains returns all enabled chains from chain adapters
func (s *AddressService) getEnabledChains() []types.ChainID {
	chains := make([]types.ChainID, 0, len(s.chainAdapters))
	for chainID := range s.chainAdapters {
		chains = append(chains, chainID)
	}
	return chains
}

// getBackfillPriority returns priority for backfill jobs based on tier
// Paid tier gets higher priority
func (s *AddressService) getBackfillPriority(tier types.UserTier) *int {
	priority := 0
	if tier == types.TierPaid {
		priority = 10 // Higher priority for paid tier
	}
	return &priority
}

// ClassifyAddress classifies an address based on transaction count across all chains
// This should be called periodically or after significant transaction updates
func (s *AddressService) ClassifyAddress(ctx context.Context, address string, classifier *AddressClassifier) ([]*models.Address, error) {
	return classifier.ClassifyAllChains(ctx, address)
}

// GetBalanceInput represents input for getting address balance
type GetBalanceInput struct {
	Address string          `json:"address"`
	Chains  []types.ChainID `json:"chains,omitempty"` // Optional: specific chains to query
}

// GetBalanceResult represents the result of getting address balance
type GetBalanceResult struct {
	Address       string               `json:"address"`
	Balances      []types.ChainBalance `json:"balances"`
	TotalValueUSD *string              `json:"totalValueUsd,omitempty"`
	UpdatedAt     time.Time            `json:"updatedAt"`
}

// GetBalance retrieves the current balance for an address across chains
// For paid tier users with complete transaction history, balances are calculated
// from stored transactions (more accurate, no RPC calls needed)
// For free tier or incomplete history, falls back to RPC calls
func (s *AddressService) GetBalance(ctx context.Context, input *GetBalanceInput) (*GetBalanceResult, error) {
	// Validate address format
	if err := s.validateAddress(input.Address); err != nil {
		return nil, err
	}

	// Normalize address
	address := strings.ToLower(input.Address)

	// Determine which chains to query
	chains := input.Chains
	if len(chains) == 0 {
		chains = s.getEnabledChains()
	}

	// Try to use calculated balances from transaction history (paid tier)
	if s.balanceRepo != nil {
		balances, err := s.getCalculatedBalances(ctx, address, chains)
		if err == nil && len(balances) > 0 {
			return &GetBalanceResult{
				Address:   address,
				Balances:  balances,
				UpdatedAt: time.Now(),
			}, nil
		}
		// Fall through to RPC if calculated balances fail
	}

	// Fallback: fetch balance from RPC (free tier or no history)
	return s.getBalanceFromRPC(ctx, address, chains)
}

// getCalculatedBalances calculates balances from stored transaction history
// This is more accurate and doesn't consume RPC quota
func (s *AddressService) getCalculatedBalances(ctx context.Context, address string, chains []types.ChainID) ([]types.ChainBalance, error) {
	var balances []types.ChainBalance

	for _, chainID := range chains {
		// Check if we have complete history for this address/chain
		hasHistory, err := s.balanceRepo.HasCompleteHistory(ctx, address, chainID)
		if err != nil || !hasHistory {
			continue
		}

		// Get calculated balance
		addrBalance, err := s.balanceRepo.GetAddressBalance(ctx, address, chainID)
		if err != nil {
			continue
		}

		// Convert to ChainBalance format
		tokenBalances := make([]types.TokenBalance, len(addrBalance.TokenBalances))
		for i, tb := range addrBalance.TokenBalances {
			tokenBalances[i] = types.TokenBalance{
				Token:    tb.TokenAddress,
				Symbol:   tb.Symbol,
				Balance:  tb.Balance,
				Decimals: tb.Decimals,
			}
		}

		balances = append(balances, types.ChainBalance{
			Chain:         chainID,
			NativeBalance: addrBalance.NativeBalance,
			TokenBalances: tokenBalances,
		})
	}

	return balances, nil
}

// getBalanceFromRPC fetches balance from RPC providers (fallback method)
func (s *AddressService) getBalanceFromRPC(ctx context.Context, address string, chains []types.ChainID) (*GetBalanceResult, error) {
	// If no chain adapters available, return error
	if len(s.chainAdapters) == 0 {
		return nil, &types.ServiceError{
			Code:    "NO_CHAIN_ADAPTERS",
			Message: "no chain adapters configured to fetch balance",
		}
	}

	// Fetch balance from each chain
	var balances []types.ChainBalance
	for _, chainID := range chains {
		chainAdapter, ok := s.chainAdapters[chainID]
		if !ok {
			// Skip chains without adapters
			continue
		}

		balance, err := chainAdapter.GetBalance(ctx, address)
		if err != nil {
			// Log error but continue with other chains
			fmt.Printf("Warning: failed to get balance for %s on %s: %v\n", address, chainID, err)
			continue
		}

		balances = append(balances, *balance)
	}

	return &GetBalanceResult{
		Address:   address,
		Balances:  balances,
		UpdatedAt: time.Now(),
	}, nil
}
