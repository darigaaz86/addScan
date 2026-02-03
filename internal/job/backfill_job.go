package job

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/address-scanner/internal/adapter"
	"github.com/address-scanner/internal/models"
	"github.com/address-scanner/internal/storage"
	"github.com/address-scanner/internal/types"
	"github.com/google/uuid"
)

// BackfillJob handles historical transaction backfilling
type BackfillJob interface {
	// Execute starts backfill jobs for an address (one per chain)
	Execute(ctx context.Context, input *BackfillJobInput) (*BackfillResult, error)

	// GetProgress retrieves current job progress
	GetProgress(ctx context.Context, jobID string) (*BackfillProgress, error)

	// Cancel cancels a running backfill job
	Cancel(ctx context.Context, jobID string) (*CancelBackfillResult, error)
}

// BackfillJobInput represents input for a backfill job
type BackfillJobInput struct {
	Address  string          `json:"address"`
	Chains   []types.ChainID `json:"chains"`
	Tier     types.UserTier  `json:"tier"`
	Priority *int            `json:"priority,omitempty"`
}

// BackfillResult represents the result of starting backfill jobs
type BackfillResult struct {
	JobID               string              `json:"jobId"`  // First job ID (for backwards compat)
	JobIDs              []string            `json:"jobIds"` // All job IDs created
	Address             string              `json:"address"`
	Status              string              `json:"status"`
	TransactionsFetched int64               `json:"transactionsFetched"`
	StartedAt           time.Time           `json:"startedAt"`
	CompletedAt         *time.Time          `json:"completedAt,omitempty"`
	Error               *types.ServiceError `json:"error,omitempty"`
}

// BackfillProgress represents the progress of a backfill job
type BackfillProgress struct {
	JobID                  string        `json:"jobId"`
	Address                string        `json:"address"`
	Chain                  types.ChainID `json:"chain"`
	Status                 string        `json:"status"`
	PercentComplete        float64       `json:"percentComplete"`
	TransactionsFetched    int64         `json:"transactionsFetched"`
	EstimatedTimeRemaining *int          `json:"estimatedTimeRemaining,omitempty"`
	StartedAt              time.Time     `json:"startedAt"`
	LastUpdated            time.Time     `json:"lastUpdated"`
}

// CancelBackfillResult represents the result of canceling a backfill job
type CancelBackfillResult struct {
	Success bool    `json:"success"`
	JobID   string  `json:"jobId"`
	Message *string `json:"message,omitempty"`
}

// BackfillJobService implements the BackfillJob interface
type BackfillJobService struct {
	jobRepo         *storage.BackfillJobRepository
	chainAdapters   map[types.ChainID]adapter.ChainAdapter
	txRepo          *storage.TransactionRepository
	addressRepo     *storage.AddressRepository
	etherscanClient *adapter.EtherscanClient
}

// NewBackfillJobService creates a new backfill job service
func NewBackfillJobService(
	jobRepo *storage.BackfillJobRepository,
	chainAdapters map[types.ChainID]adapter.ChainAdapter,
	txRepo *storage.TransactionRepository,
	addressRepo *storage.AddressRepository,
) *BackfillJobService {
	return &BackfillJobService{
		jobRepo:       jobRepo,
		chainAdapters: chainAdapters,
		txRepo:        txRepo,
		addressRepo:   addressRepo,
	}
}

// NewBackfillJobServiceWithEtherscan creates a backfill job service with Etherscan support
// This enables fetching complete transaction data including gas, methodId, and funcName
func NewBackfillJobServiceWithEtherscan(
	jobRepo *storage.BackfillJobRepository,
	chainAdapters map[types.ChainID]adapter.ChainAdapter,
	txRepo *storage.TransactionRepository,
	addressRepo *storage.AddressRepository,
	etherscanAPIKey string,
) *BackfillJobService {
	var etherscanClient *adapter.EtherscanClient
	if etherscanAPIKey != "" {
		etherscanClient = adapter.NewEtherscanClient(etherscanAPIKey)
		log.Printf("[BackfillJob] Etherscan client initialized for complete transaction history")
	}

	return &BackfillJobService{
		jobRepo:         jobRepo,
		chainAdapters:   chainAdapters,
		txRepo:          txRepo,
		addressRepo:     addressRepo,
		etherscanClient: etherscanClient,
	}
}

// Execute starts backfill jobs for an address (one job per chain)
// Jobs are queued in the database and processed by the backfill worker
func (s *BackfillJobService) Execute(ctx context.Context, input *BackfillJobInput) (*BackfillResult, error) {
	priority := 0
	if input.Priority != nil {
		priority = *input.Priority
	}

	var jobIDs []string
	var jobs []*models.BackfillJobRecord
	startedAt := time.Now()

	// Create job records for batch insert
	for _, chain := range input.Chains {
		jobID := uuid.New().String()
		jobIDs = append(jobIDs, jobID)

		jobs = append(jobs, &models.BackfillJobRecord{
			JobID:               jobID,
			Address:             input.Address,
			Chain:               chain,
			Tier:                input.Tier,
			Status:              "queued",
			Priority:            priority,
			TransactionsFetched: 0,
			StartedAt:           startedAt,
			CompletedAt:         nil,
			Error:               nil,
			RetryCount:          0,
		})
	}

	// Batch insert all jobs in a single transaction
	if err := s.jobRepo.BatchCreate(ctx, jobs); err != nil {
		return nil, fmt.Errorf("failed to create backfill jobs: %w", err)
	}

	firstJobID := ""
	if len(jobIDs) > 0 {
		firstJobID = jobIDs[0]
	}

	log.Printf("[BackfillJob] Queued %d jobs for address %s (chains: %v)", len(jobIDs), input.Address, input.Chains)

	return &BackfillResult{
		JobID:               firstJobID,
		JobIDs:              jobIDs,
		Address:             input.Address,
		Status:              "queued",
		TransactionsFetched: 0,
		StartedAt:           startedAt,
	}, nil
}

// executeBackfill performs the actual backfill work for a single chain
func (s *BackfillJobService) executeBackfill(ctx context.Context, job *models.BackfillJobRecord) {
	// Update job status to in_progress
	job.Status = "in_progress"
	if err := s.jobRepo.Update(ctx, job); err != nil {
		fmt.Printf("Failed to update job status: %v\n", err)
		return
	}

	// Determine transaction limit based on tier
	var transactionLimit int64
	if job.Tier == types.TierFree {
		transactionLimit = 1000
		fmt.Printf("Backfill job %s: Free tier - limiting to %d transactions\n", job.JobID, transactionLimit)
	} else {
		transactionLimit = -1
		fmt.Printf("Backfill job %s: Paid tier - fetching all historical transactions\n", job.JobID)
	}

	chainAdapter, exists := s.chainAdapters[job.Chain]
	if !exists {
		fmt.Printf("Chain adapter not found for chain %s\n", job.Chain)
		job.Status = "failed"
		errorMsg := fmt.Sprintf("chain adapter not found for chain %s", job.Chain)
		job.Error = &errorMsg
		now := time.Now()
		job.CompletedAt = &now
		s.jobRepo.Update(ctx, job)
		return
	}

	// Fetch transactions
	transactions, err := s.fetchTransactionsForChain(ctx, chainAdapter, job.Address, transactionLimit)
	if err != nil {
		fmt.Printf("Failed to fetch transactions for chain %s: %v\n", job.Chain, err)
		job.Status = "failed"
		errorMsg := err.Error()
		job.Error = &errorMsg
		job.RetryCount++
		now := time.Now()
		job.CompletedAt = &now
		s.jobRepo.Update(ctx, job)
		return
	}

	// Store transactions in ClickHouse
	if len(transactions) > 0 {
		modelTransactions := make([]*models.Transaction, len(transactions))
		for i, tx := range transactions {
			modelTransactions[i] = models.FromNormalizedTransaction(tx, job.Address)
		}

		if err := s.txRepo.BatchInsert(ctx, modelTransactions); err != nil {
			fmt.Printf("Failed to store transactions for chain %s: %v\n", job.Chain, err)
			job.Status = "failed"
			errorMsg := err.Error()
			job.Error = &errorMsg
			job.RetryCount++
			now := time.Now()
			job.CompletedAt = &now
			s.jobRepo.Update(ctx, job)
			return
		}
		fmt.Printf("Stored %d transactions for address %s on chain %s\n", len(transactions), job.Address, job.Chain)
	}

	// Update sync status
	if err := s.updateSyncStatus(ctx, job.Address, job.Chain, true); err != nil {
		fmt.Printf("Failed to update sync status for chain %s: %v\n", job.Chain, err)
	}

	// Mark job as completed
	now := time.Now()
	job.Status = "completed"
	job.CompletedAt = &now
	job.TransactionsFetched = int64(len(transactions))

	fmt.Printf("Backfill job %s completed: fetched %d transactions (tier: %s)\n",
		job.JobID, len(transactions), job.Tier)

	if err := s.jobRepo.Update(ctx, job); err != nil {
		fmt.Printf("Failed to update job completion: %v\n", err)
	}
}

// updateSyncStatus updates the sync status in the addresses table
func (s *BackfillJobService) updateSyncStatus(ctx context.Context, address string, chain types.ChainID, backfillComplete bool) error {
	chainAdapter, exists := s.chainAdapters[chain]
	if !exists {
		return fmt.Errorf("chain adapter not found for chain %s", chain)
	}

	currentBlock, err := chainAdapter.GetCurrentBlock(ctx)
	if err != nil {
		return fmt.Errorf("failed to get current block: %w", err)
	}

	// Update the address record with sync status
	return s.addressRepo.UpdateSyncStatus(ctx, address, chain, currentBlock, backfillComplete)
}

// fetchTransactionsForChain fetches transactions using Etherscan first (complete data),
// then falls back to Alchemy if Etherscan fails or is not configured.
// Both free and paid tiers use the same flow - only the limit differs.
func (s *BackfillJobService) fetchTransactionsForChain(
	ctx context.Context,
	chainAdapter adapter.ChainAdapter,
	address string,
	limit int64,
) ([]*types.NormalizedTransaction, error) {
	var transactions []*types.NormalizedTransaction
	var etherscanErr error

	// Try Etherscan first (provides complete data: gas, methodId, funcName)
	if s.etherscanClient != nil {
		log.Printf("[BackfillJob] Fetching from Etherscan for %s on chain %s (limit: %d)", address, chainAdapter.GetChainID(), limit)
		txs, err := s.etherscanClient.FetchAllTransactions(ctx, address, chainAdapter.GetChainID())
		if err != nil {
			etherscanErr = err
			log.Printf("[BackfillJob] Warning: Etherscan fetch failed for chain %s: %v, will try fallback", chainAdapter.GetChainID(), err)
		} else {
			log.Printf("[BackfillJob] Etherscan returned %d transactions for %s on chain %s", len(txs), address, chainAdapter.GetChainID())
			transactions = txs
		}
	}

	// Fall back to Alchemy only if Etherscan failed (not just returned 0 results)
	// Empty results from Etherscan is valid - address may have no transactions
	if etherscanErr != nil || s.etherscanClient == nil {
		log.Printf("[BackfillJob] Fetching from Alchemy for %s on chain %s (limit: %d)", address, chainAdapter.GetChainID(), limit)
		txs, err := s.fetchFromAlchemyWithRetry(ctx, chainAdapter, address, limit)
		if err != nil {
			// If both Etherscan and Alchemy failed, return the error
			if etherscanErr != nil {
				return nil, fmt.Errorf("both providers failed - etherscan: %v, alchemy: %w", etherscanErr, err)
			}
			return nil, fmt.Errorf("failed to fetch transactions from Alchemy: %w", err)
		}
		transactions = txs
	}

	// Apply limit for free tier
	if limit > 0 && int64(len(transactions)) > limit {
		log.Printf("[BackfillJob] Applying limit: %d -> %d transactions", len(transactions), limit)
		return transactions[:limit], nil
	}

	return transactions, nil
}

// fetchFromAlchemyWithRetry fetches transactions from Alchemy with retry logic
func (s *BackfillJobService) fetchFromAlchemyWithRetry(
	ctx context.Context,
	chainAdapter adapter.ChainAdapter,
	address string,
	limit int64,
) ([]*types.NormalizedTransaction, error) {
	const maxRetries = 5
	const baseDelay = 1 * time.Second

	var lastErr error

	// For paid tier (limit = -1), pass -1 to enable pagination
	// For free tier, use the limit (max 1000)
	maxCount := int(limit)
	if limit > 1000 {
		maxCount = 1000 // Alchemy's per-request limit
	}

	for attempt := 0; attempt <= maxRetries; attempt++ {
		txs, err := chainAdapter.FetchTransactionHistory(ctx, address, maxCount)
		if err == nil {
			return txs, nil
		}

		lastErr = err
		log.Printf("[BackfillJob] Attempt %d/%d failed for address %s: %v",
			attempt+1, maxRetries+1, address, err)

		if attempt == maxRetries {
			break
		}

		delay := baseDelay * time.Duration(1<<uint(attempt))
		if delay > 60*time.Second {
			delay = 60 * time.Second
		}

		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, fmt.Errorf("context cancelled: %w", ctx.Err())
		}
	}

	return nil, fmt.Errorf("max retries exceeded: %w", lastErr)
}

// GetProgress retrieves current job progress
func (s *BackfillJobService) GetProgress(ctx context.Context, jobID string) (*BackfillProgress, error) {
	job, err := s.jobRepo.GetByID(ctx, jobID)
	if err != nil {
		return nil, fmt.Errorf("failed to get job: %w", err)
	}

	return &BackfillProgress{
		JobID:               job.JobID,
		Address:             job.Address,
		Chain:               job.Chain,
		Status:              job.Status,
		TransactionsFetched: job.TransactionsFetched,
		StartedAt:           job.StartedAt,
		LastUpdated:         time.Now(),
	}, nil
}

// Cancel cancels a running backfill job
func (s *BackfillJobService) Cancel(ctx context.Context, jobID string) (*CancelBackfillResult, error) {
	job, err := s.jobRepo.GetByID(ctx, jobID)
	if err != nil {
		return nil, fmt.Errorf("failed to get job: %w", err)
	}

	if job.Status == "completed" || job.Status == "failed" {
		message := fmt.Sprintf("Job already %s, cannot cancel", job.Status)
		return &CancelBackfillResult{
			Success: false,
			JobID:   jobID,
			Message: &message,
		}, nil
	}

	job.Status = "failed"
	errorMsg := "Job cancelled by user"
	job.Error = &errorMsg
	now := time.Now()
	job.CompletedAt = &now

	if err := s.jobRepo.Update(ctx, job); err != nil {
		return nil, fmt.Errorf("failed to cancel job: %w", err)
	}

	message := "Job cancelled successfully"
	return &CancelBackfillResult{
		Success: true,
		JobID:   jobID,
		Message: &message,
	}, nil
}
