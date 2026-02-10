package service

import (
	"context"
	"fmt"
	"log"
	"strings"
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
	duneClient      *adapter.DuneClient
	rateController  *ratelimit.BackfillRateController
	stopAutoRequeue chan struct{}
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

// NewBackfillServiceFull creates a backfill service with all data providers.
func NewBackfillServiceFull(
	backfillRepo *storage.BackfillJobRepository,
	txRepo *storage.TransactionRepository,
	chainAdapters map[types.ChainID]adapter.ChainAdapter,
	etherscanAPIKey string,
	duneAPIKey string,
	rateController *ratelimit.BackfillRateController,
) *BackfillService {
	var etherscanClient *adapter.EtherscanClient
	if etherscanAPIKey != "" {
		etherscanClient = adapter.NewEtherscanClient(etherscanAPIKey)
		log.Printf("[Backfill] Etherscan client initialized for complete transaction history")
	}

	var duneClient *adapter.DuneClient
	if duneAPIKey != "" {
		duneClient = adapter.NewDuneClient(duneAPIKey)
		log.Printf("[Backfill] Dune client initialized for BNB chain support")
	}

	if rateController != nil {
		log.Printf("[Backfill] Rate controller initialized for CU budget management")
	}

	return &BackfillService{
		backfillRepo:    backfillRepo,
		txRepo:          txRepo,
		chainAdapters:   chainAdapters,
		etherscanClient: etherscanClient,
		duneClient:      duneClient,
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

	return s.processJob(ctx, jobs[0])
}

// ProcessNextJobForChain processes the next queued backfill job for a specific chain
func (s *BackfillService) ProcessNextJobForChain(ctx context.Context, chain types.ChainID) error {
	jobs, err := s.backfillRepo.GetQueuedJobsForChain(ctx, chain, 1)
	if err != nil {
		return fmt.Errorf("failed to get next job for chain %s: %w", chain, err)
	}

	if len(jobs) == 0 {
		return nil
	}

	return s.processJob(ctx, jobs[0])
}

// processJob processes a single backfill job
func (s *BackfillService) processJob(ctx context.Context, job *models.BackfillJobRecord) error {
	log.Printf("[Backfill] Processing job %s for address %s on chain %s (tier: %s, retry: %d)",
		job.JobID, job.Address, job.Chain, job.Tier, job.RetryCount)

	job.Status = string(types.BackfillStatusInProgress)
	job.StartedAt = time.Now()
	if err := s.backfillRepo.Update(ctx, job); err != nil {
		return fmt.Errorf("failed to update job status: %w", err)
	}

	if err := s.processBackfill(ctx, job); err != nil {
		job.RetryCount++

		// Auto-retry up to 3 times, then mark as failed
		const maxRetries = 3
		if job.RetryCount < maxRetries {
			job.Status = string(types.BackfillStatusQueued) // Re-queue for retry
			job.Error = stringPtr(fmt.Sprintf("attempt %d failed: %s", job.RetryCount, err.Error()))
			log.Printf("[Backfill] Job %s failed (attempt %d/%d), re-queuing: %v",
				job.JobID, job.RetryCount, maxRetries, err)
		} else {
			job.Status = string(types.BackfillStatusFailed)
			job.CompletedAt = timePtr(time.Now())
			job.Error = stringPtr(fmt.Sprintf("failed after %d attempts: %s", job.RetryCount, err.Error()))
			log.Printf("[Backfill] Job %s failed permanently after %d attempts: %v",
				job.JobID, job.RetryCount, err)
		}

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

	// For L2 chains, fetch L1 fees from RPC
	if types.IsL2Chain(job.Chain) {
		if err := s.enrichL1Fees(ctx, chainAdapter, transactions); err != nil {
			log.Printf("[Backfill] Warning: failed to enrich L1 fees: %v", err)
			// Continue without L1 fees - better to have partial data than none
		}
	}

	modelTransactions := make([]*models.Transaction, 0, len(transactions))
	for _, tx := range transactions {
		modelTxs := models.FromNormalizedTransaction(tx, job.Address)
		modelTransactions = append(modelTransactions, modelTxs...)
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
	// Note: For BNB chain, use Dune Sim API (Etherscan free tier doesn't support BNB)
	var transactions []*types.NormalizedTransaction
	var fetchErr error

	// For BNB chain, use Dune Sim API
	if chain == types.ChainBNB && s.duneClient != nil {
		log.Printf("[Backfill] Using Dune for BNB chain")
		transactions, fetchErr = s.duneClient.FetchAllTransactions(ctx, address, chain)
		if fetchErr != nil {
			log.Printf("[Backfill] Warning: Dune fetch failed: %v", fetchErr)
		} else {
			log.Printf("[Backfill] Dune returned %d transactions for %s", len(transactions), address)
			return transactions, nil
		}
	}

	// For other chains (or BNB fallback), use Etherscan
	var etherscanErr error
	if s.etherscanClient != nil && chain != types.ChainBNB {
		transactions, etherscanErr = s.etherscanClient.FetchAllTransactions(ctx, address, chain)
		if etherscanErr != nil {
			log.Printf("[Backfill] Warning: Etherscan fetch failed: %v", etherscanErr)
		} else {
			log.Printf("[Backfill] Etherscan returned %d transactions for %s", len(transactions), address)
		}
	} else if chain == types.ChainBNB {
		// BNB without Dune - skip Etherscan (not supported on free tier)
		log.Printf("[Backfill] Skipping Etherscan for BNB chain (not supported on free tier)")
		etherscanErr = fmt.Errorf("BNB chain not supported on Etherscan free tier")
	}

	// Only fall back to Alchemy if Etherscan FAILED (not just returned 0 results)
	// 0 results from Etherscan is valid - address may have no transactions on this chain
	if etherscanErr != nil || s.etherscanClient == nil {
		// Retry loop with exponential backoff for 429 errors
		maxRetries := 3
		for attempt := 0; attempt < maxRetries; attempt++ {
			// Wait for CU budget before making Alchemy RPC call
			// alchemy_getAssetTransfers costs 300 CU (2 calls: outgoing + incoming)
			if s.rateController != nil {
				log.Printf("[Backfill] Waiting for CU budget before Alchemy call (attempt %d/%d)...", attempt+1, maxRetries)
				if err := s.rateController.WaitForBudget(ctx, ratelimit.CostAlchemyGetAssetTransfers); err != nil {
					return nil, fmt.Errorf("rate limit wait cancelled: %w", err)
				}
			}

			alchemyTxs, err := chainAdapter.FetchTransactionHistory(ctx, address, limit)
			if err != nil {
				// Check if it's a rate limit error (429)
				errStr := err.Error()
				is429 := strings.Contains(errStr, "429") || strings.Contains(errStr, "rate limit") || strings.Contains(errStr, "too many requests")

				if is429 && attempt < maxRetries-1 {
					// Record failure for backoff
					if s.rateController != nil {
						s.rateController.RecordFailure()
					}
					// Exponential backoff: 2s, 4s, 8s
					backoffDuration := time.Duration(2<<attempt) * time.Second
					log.Printf("[Backfill] Rate limited (429), backing off for %v before retry...", backoffDuration)
					select {
					case <-ctx.Done():
						return nil, ctx.Err()
					case <-time.After(backoffDuration):
						continue
					}
				}

				log.Printf("[Backfill] Alchemy fetch failed after %d attempts: %v", attempt+1, err)
				// Record failure for backoff if rate controller is configured
				if s.rateController != nil {
					s.rateController.RecordFailure()
				}
				break
			}

			log.Printf("[Backfill] Alchemy returned %d transactions for %s", len(alchemyTxs), address)
			transactions = alchemyTxs
			// Record success to reset backoff
			if s.rateController != nil {
				s.rateController.RecordSuccess()
			}
			break
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

// enrichL1Fees fetches L1 fees from RPC for L2 chain transactions
// Only fetches for transactions where the address is the sender (pays gas)
func (s *BackfillService) enrichL1Fees(ctx context.Context, chainAdapter adapter.ChainAdapter, transactions []*types.NormalizedTransaction) error {
	// Get the ethereum adapter to access RPC
	ethAdapter, ok := chainAdapter.(*adapter.EthereumAdapter)
	if !ok {
		return fmt.Errorf("chain adapter is not an EthereumAdapter")
	}

	// Collect unique tx hashes that need L1 fee lookup
	// Only for transactions where user is sender (they pay gas)
	txHashSet := make(map[string]bool)
	for _, tx := range transactions {
		if tx.GasUsed != nil && *tx.GasUsed != "" {
			txHashSet[tx.Hash] = true
		}
	}

	if len(txHashSet) == 0 {
		return nil
	}

	log.Printf("[Backfill] Fetching L1 fees for %d transactions", len(txHashSet))

	// Fetch L1 fees from RPC
	l1Fees, err := ethAdapter.FetchL1Fees(ctx, txHashSet)
	if err != nil {
		return fmt.Errorf("failed to fetch L1 fees: %w", err)
	}

	// Enrich transactions with L1 fees
	enriched := 0
	for _, tx := range transactions {
		if l1Fee, ok := l1Fees[tx.Hash]; ok {
			tx.L1Fee = &l1Fee
			enriched++
		}
	}

	log.Printf("[Backfill] Enriched %d transactions with L1 fees", enriched)
	return nil
}

// Auto re-queue configuration
const (
	autoRequeueInterval   = 15 * time.Minute // Check every 15 minutes
	autoRequeueCooldown   = 30 * time.Minute // Wait 30 min after failure before retry
	autoRequeueMaxRetries = 3                // Max auto-retry attempts
)

// StartAutoRequeue starts the background goroutine that automatically re-queues
// failed jobs with transient errors. Call StopAutoRequeue to stop it.
func (s *BackfillService) StartAutoRequeue(ctx context.Context) {
	s.stopAutoRequeue = make(chan struct{})

	go func() {
		ticker := time.NewTicker(autoRequeueInterval)
		defer ticker.Stop()

		log.Printf("[Backfill] Auto re-queue started (interval: %v, cooldown: %v, max retries: %d)",
			autoRequeueInterval, autoRequeueCooldown, autoRequeueMaxRetries)

		for {
			select {
			case <-ctx.Done():
				log.Printf("[Backfill] Auto re-queue stopped (context cancelled)")
				return
			case <-s.stopAutoRequeue:
				log.Printf("[Backfill] Auto re-queue stopped")
				return
			case <-ticker.C:
				s.runAutoRequeue(ctx)
			}
		}
	}()
}

// StopAutoRequeue stops the auto re-queue background goroutine
func (s *BackfillService) StopAutoRequeue() {
	if s.stopAutoRequeue != nil {
		close(s.stopAutoRequeue)
	}
}

// runAutoRequeue checks for and re-queues eligible failed jobs
func (s *BackfillService) runAutoRequeue(ctx context.Context) {
	count, err := s.backfillRepo.RequeueTransientFailures(ctx, autoRequeueMaxRetries, autoRequeueCooldown)
	if err != nil {
		log.Printf("[Backfill] Auto re-queue error: %v", err)
		return
	}

	if count > 0 {
		log.Printf("[Backfill] Auto re-queued %d failed jobs with transient errors", count)
	}
}

// RequeueFailedJobs manually re-queues all failed jobs for a specific chain
// This resets retry_count to 0, allowing full retry attempts
func (s *BackfillService) RequeueFailedJobs(ctx context.Context, chain string) (int64, error) {
	count, err := s.backfillRepo.RequeueFailedJobs(ctx, chain)
	if err != nil {
		return 0, fmt.Errorf("failed to re-queue jobs: %w", err)
	}

	if count > 0 {
		log.Printf("[Backfill] Manually re-queued %d failed jobs for chain: %s", count, chain)
	}

	return count, nil
}

// GetJobStats returns job counts by status
func (s *BackfillService) GetJobStats(ctx context.Context) (map[string]int64, error) {
	return s.backfillRepo.GetJobStats(ctx)
}

// CleanupOldJobs deletes old completed and failed jobs
func (s *BackfillService) CleanupOldJobs(ctx context.Context) (completed int64, failed int64, err error) {
	completed, err = s.backfillRepo.DeleteCompletedJobs(ctx, 7*24*time.Hour) // 7 days
	if err != nil {
		return 0, 0, fmt.Errorf("failed to cleanup completed jobs: %w", err)
	}

	failed, err = s.backfillRepo.DeleteOldFailedJobs(ctx, 30*24*time.Hour) // 30 days
	if err != nil {
		return completed, 0, fmt.Errorf("failed to cleanup failed jobs: %w", err)
	}

	if completed > 0 || failed > 0 {
		log.Printf("[Backfill] Cleanup: deleted %d completed jobs, %d failed jobs", completed, failed)
	}

	return completed, failed, nil
}
