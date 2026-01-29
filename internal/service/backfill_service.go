package service

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/address-scanner/internal/adapter"
	"github.com/address-scanner/internal/models"
	"github.com/address-scanner/internal/ratelimit"
	"github.com/address-scanner/internal/storage"
	"github.com/address-scanner/internal/types"
	"github.com/google/uuid"
)

// BackfillService handles historical transaction backfill
type BackfillService struct {
	backfillRepo    *storage.BackfillJobRepository
	txRepo          *storage.TransactionRepository
	chainAdapters   map[types.ChainID]adapter.ChainAdapter
	etherscanClient *adapter.EtherscanClient
	rateController  *ratelimit.BackfillRateController
}

// NewBackfillService creates a new backfill service
func NewBackfillService(
	backfillRepo *storage.BackfillJobRepository,
	txRepo *storage.TransactionRepository,
	chainAdapters map[types.ChainID]adapter.ChainAdapter,
) *BackfillService {
	return &BackfillService{
		backfillRepo:  backfillRepo,
		txRepo:        txRepo,
		chainAdapters: chainAdapters,
	}
}

// NewBackfillServiceWithEtherscan creates a backfill service with Etherscan support
func NewBackfillServiceWithEtherscan(
	backfillRepo *storage.BackfillJobRepository,
	txRepo *storage.TransactionRepository,
	chainAdapters map[types.ChainID]adapter.ChainAdapter,
	etherscanAPIKey string,
) *BackfillService {
	var etherscanClient *adapter.EtherscanClient
	if etherscanAPIKey != "" {
		etherscanClient = adapter.NewEtherscanClient(etherscanAPIKey)
		log.Printf("[Backfill] Etherscan client initialized for complete transaction history")
	}

	return &BackfillService{
		backfillRepo:    backfillRepo,
		txRepo:          txRepo,
		chainAdapters:   chainAdapters,
		etherscanClient: etherscanClient,
	}
}

// NewBackfillServiceWithRateController creates a backfill service with rate limiting support.
// The rate controller is optional - if nil, the service operates without rate limiting.
func NewBackfillServiceWithRateController(
	backfillRepo *storage.BackfillJobRepository,
	txRepo *storage.TransactionRepository,
	chainAdapters map[types.ChainID]adapter.ChainAdapter,
	etherscanAPIKey string,
	rateController *ratelimit.BackfillRateController,
) *BackfillService {
	var etherscanClient *adapter.EtherscanClient
	if etherscanAPIKey != "" {
		etherscanClient = adapter.NewEtherscanClient(etherscanAPIKey)
		log.Printf("[Backfill] Etherscan client initialized for complete transaction history")
	}

	if rateController != nil {
		log.Printf("[Backfill] Rate controller initialized for CU budget management")
	}

	return &BackfillService{
		backfillRepo:    backfillRepo,
		txRepo:          txRepo,
		chainAdapters:   chainAdapters,
		etherscanClient: etherscanClient,
		rateController:  rateController,
	}
}

// SetRateController sets the rate controller for the backfill service.
// This allows adding rate limiting to an existing service instance.
// Pass nil to disable rate limiting.
func (s *BackfillService) SetRateController(rateController *ratelimit.BackfillRateController) {
	s.rateController = rateController
	if rateController != nil {
		log.Printf("[Backfill] Rate controller enabled for CU budget management")
	} else {
		log.Printf("[Backfill] Rate controller disabled")
	}
}

// CreateBackfillJob creates backfill jobs (one per chain)
func (s *BackfillService) CreateBackfillJob(ctx context.Context, address string, chains []types.ChainID, tier types.UserTier) ([]string, error) {
	priority := 5
	if tier == types.TierPaid {
		priority = 10
	}

	var jobIDs []string
	for _, chain := range chains {
		jobID := uuid.New().String()
		job := &models.BackfillJobRecord{
			JobID:    jobID,
			Address:  address,
			Chain:    chain,
			Tier:     tier,
			Status:   string(types.BackfillStatusQueued),
			Priority: priority,
		}

		if err := s.backfillRepo.Create(ctx, job); err != nil {
			return nil, fmt.Errorf("failed to create backfill job for chain %s: %w", chain, err)
		}
		jobIDs = append(jobIDs, jobID)
	}

	return jobIDs, nil
}

// ProcessNextJob processes the next queued backfill job
func (s *BackfillService) ProcessNextJob(ctx context.Context) error {
	jobs, err := s.backfillRepo.GetQueuedJobs(ctx, 1)
	if err != nil {
		return fmt.Errorf("failed to get next job: %w", err)
	}

	if len(jobs) == 0 {
		return nil
	}

	job := jobs[0]

	log.Printf("[Backfill] Processing job %s for address %s on chain %s (tier: %s)",
		job.JobID, job.Address, job.Chain, job.Tier)

	job.Status = string(types.BackfillStatusInProgress)
	job.StartedAt = time.Now()
	if err := s.backfillRepo.Update(ctx, job); err != nil {
		return fmt.Errorf("failed to update job status: %w", err)
	}

	if err := s.processBackfill(ctx, job); err != nil {
		job.Status = string(types.BackfillStatusFailed)
		job.CompletedAt = timePtr(time.Now())
		job.Error = stringPtr(err.Error())
		s.backfillRepo.Update(ctx, job)
		return fmt.Errorf("backfill failed: %w", err)
	}

	job.Status = string(types.BackfillStatusCompleted)
	job.CompletedAt = timePtr(time.Now())
	if err := s.backfillRepo.Update(ctx, job); err != nil {
		return fmt.Errorf("failed to update job completion: %w", err)
	}

	log.Printf("[Backfill] Completed job %s - fetched %d transactions",
		job.JobID, job.TransactionsFetched)

	return nil
}

// processBackfill performs the actual backfill for a single chain job
func (s *BackfillService) processBackfill(ctx context.Context, job *models.BackfillJobRecord) error {
	limit := 1000
	if job.Tier == types.TierPaid {
		limit = -1
	}

	chainAdapter, ok := s.chainAdapters[job.Chain]
	if !ok {
		return fmt.Errorf("chain adapter not found for %s", job.Chain)
	}

	log.Printf("[Backfill] Fetching transactions for %s on chain %s", job.Address, job.Chain)

	transactions, err := s.fetchHistoricalTransactions(ctx, chainAdapter, job.Address, job.Chain, limit)
	if err != nil {
		return fmt.Errorf("failed to fetch transactions: %w", err)
	}

	if len(transactions) == 0 {
		log.Printf("[Backfill] No transactions found for %s on chain %s", job.Address, job.Chain)
		return nil
	}

	modelTransactions := make([]*models.Transaction, 0, len(transactions))
	for _, tx := range transactions {
		modelTx := models.FromNormalizedTransaction(tx, job.Address)
		modelTransactions = append(modelTransactions, modelTx)
	}

	if err := s.txRepo.BatchInsert(ctx, modelTransactions); err != nil {
		return fmt.Errorf("failed to store transactions: %w", err)
	}

	job.TransactionsFetched = int64(len(transactions))
	log.Printf("[Backfill] Stored %d transactions for %s on chain %s",
		len(transactions), job.Address, job.Chain)

	return nil
}

// fetchHistoricalTransactions fetches historical transactions for an address
// Strategy:
//   - Etherscan = source of truth (returns ALL transfers including multiple per tx)
//   - Each token transfer is stored as a separate record
//   - Normal transactions enriched with funcName from Etherscan
//   - Rate limiting is applied when rate controller is configured
func (s *BackfillService) fetchHistoricalTransactions(
	ctx context.Context,
	chainAdapter adapter.ChainAdapter,
	address string,
	chain types.ChainID,
	limit int,
) ([]*types.NormalizedTransaction, error) {
	log.Printf("[Backfill] Fetching transaction history for %s on chain %s (limit: %d)",
		address, chain, limit)

	// Check if we should pause due to high CU utilization (>90%)
	if s.rateController != nil {
		if s.rateController.ShouldPause(ctx) {
			log.Printf("[Backfill] Pausing due to high CU utilization (>90%%), waiting for budget...")
			// Wait for budget to become available before proceeding
			// Use a reasonable CU estimate for the upcoming operations
			// alchemy_getAssetTransfers costs 150 CU
			if err := s.rateController.WaitForBudget(ctx, ratelimit.CostAlchemyGetAssetTransfers); err != nil {
				return nil, fmt.Errorf("rate limit wait cancelled: %w", err)
			}
			log.Printf("[Backfill] Resuming after budget became available")
		}
	}

	// Fetch from Etherscan (complete transaction list - all 5 types)
	// This is the source of truth as it returns each transfer separately
	var transactions []*types.NormalizedTransaction
	if s.etherscanClient != nil {
		var err error
		transactions, err = s.etherscanClient.FetchAllTransactions(ctx, address, chain)
		if err != nil {
			log.Printf("[Backfill] Warning: Etherscan fetch failed: %v", err)
			transactions = []*types.NormalizedTransaction{}
		} else {
			log.Printf("[Backfill] Etherscan returned %d transactions for %s", len(transactions), address)
		}
	}

	// If Etherscan failed or returned nothing, fall back to Alchemy
	if len(transactions) == 0 {
		// Wait for CU budget before making Alchemy RPC call
		// alchemy_getAssetTransfers costs 150 CU
		if s.rateController != nil {
			log.Printf("[Backfill] Waiting for CU budget before Alchemy call...")
			if err := s.rateController.WaitForBudget(ctx, ratelimit.CostAlchemyGetAssetTransfers); err != nil {
				return nil, fmt.Errorf("rate limit wait cancelled: %w", err)
			}
		}

		alchemyTxs, err := chainAdapter.FetchTransactionHistory(ctx, address, limit)
		if err != nil {
			log.Printf("[Backfill] Warning: Alchemy fetch failed: %v", err)
			// Record failure for backoff if rate controller is configured
			if s.rateController != nil {
				s.rateController.RecordFailure()
			}
		} else {
			log.Printf("[Backfill] Alchemy returned %d transactions for %s", len(alchemyTxs), address)
			transactions = alchemyTxs
			// Record success to reset backoff
			if s.rateController != nil {
				s.rateController.RecordSuccess()
			}
		}
	}

	return transactions, nil
}

// GetJobStatus returns the status of a backfill job
func (s *BackfillService) GetJobStatus(ctx context.Context, jobID string) (*models.BackfillJobRecord, error) {
	return s.backfillRepo.GetByID(ctx, jobID)
}

func timePtr(t time.Time) *time.Time {
	return &t
}

func stringPtr(s string) *string {
	return &s
}
