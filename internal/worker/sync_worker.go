package worker

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/address-scanner/internal/adapter"
	"github.com/address-scanner/internal/models"
	"github.com/address-scanner/internal/ratelimit"
	"github.com/address-scanner/internal/storage"
	"github.com/address-scanner/internal/types"
)

// SyncWorker handles real-time blockchain synchronization
type SyncWorker struct {
	chain              types.ChainID
	chainAdapter       adapter.ChainAdapter
	txRepo             *storage.TransactionRepository
	syncStatusRepo     *storage.SyncStatusRepository
	addressRepo        *storage.AddressRepository
	portfolioRepo      *storage.PortfolioRepository
	userRepo           *storage.UserRepository
	cache              *storage.RedisCache
	pollInterval       time.Duration
	maxBlocksPerPoll   int
	lastBlockProcessed uint64
	running            bool
	mu                 sync.RWMutex
	stopCh             chan struct{}
	doneCh             chan struct{}
	lastPollTime       time.Time
	addressesTracked   int
	tierProvider       *TierAwareAddressProvider
	useTierPriority    bool
	// Rate limiting components (optional, for backward compatibility)
	rateLimitTracker  *ratelimit.CUBudgetTracker
	rateLimitRegistry *ratelimit.CUCostRegistry
}

// SyncWorkerConfig holds configuration for a sync worker
type SyncWorkerConfig struct {
	Chain            types.ChainID
	ChainAdapter     adapter.ChainAdapter
	TxRepo           *storage.TransactionRepository
	SyncStatusRepo   *storage.SyncStatusRepository
	AddressRepo      *storage.AddressRepository
	PortfolioRepo    *storage.PortfolioRepository
	UserRepo         *storage.UserRepository
	Cache            *storage.RedisCache
	PollInterval     time.Duration
	MaxBlocksPerPoll int  // Maximum blocks to process per poll cycle (default: 30)
	UseTierPriority  bool // Enable tier-based priority for address processing
	// Rate limiting components (optional, for backward compatibility)
	// If provided, the sync worker will use PriorityHigh for real-time sync operations
	// Requirements: 3.1, 3.2, 5.5
	RateLimitTracker  *ratelimit.CUBudgetTracker
	RateLimitRegistry *ratelimit.CUCostRegistry
}

// NewSyncWorker creates a new sync worker
func NewSyncWorker(cfg *SyncWorkerConfig) (*SyncWorker, error) {
	if cfg.ChainAdapter == nil {
		return nil, fmt.Errorf("chain adapter cannot be nil")
	}
	if cfg.TxRepo == nil {
		return nil, fmt.Errorf("transaction repository cannot be nil")
	}
	if cfg.SyncStatusRepo == nil {
		return nil, fmt.Errorf("sync status repository cannot be nil")
	}
	if cfg.AddressRepo == nil {
		return nil, fmt.Errorf("address repository cannot be nil")
	}
	if cfg.Cache == nil {
		return nil, fmt.Errorf("cache cannot be nil")
	}

	// Default poll interval: 15 seconds
	pollInterval := cfg.PollInterval
	if pollInterval == 0 {
		pollInterval = 15 * time.Second
	}

	// Validate poll interval is within acceptable range (10-30 seconds)
	if pollInterval < 10*time.Second || pollInterval > 30*time.Second {
		return nil, fmt.Errorf("poll interval must be between 10 and 30 seconds, got %v", pollInterval)
	}

	// Create tier-aware address provider if tier priority is enabled
	var tierProvider *TierAwareAddressProvider
	if cfg.UseTierPriority && cfg.PortfolioRepo != nil && cfg.UserRepo != nil {
		tierProvider = NewTierAwareAddressProvider(cfg.AddressRepo, cfg.PortfolioRepo, cfg.UserRepo)
	}

	// Configure rate limiting on the adapter if rate limiter components are provided
	// Requirements: 3.1, 3.2, 5.5 - Real-time sync uses PriorityHigh for reserved budget
	if cfg.RateLimitTracker != nil && cfg.RateLimitRegistry != nil {
		// Check if the adapter supports rate limiting (EthereumAdapter)
		if ethAdapter, ok := cfg.ChainAdapter.(*adapter.EthereumAdapter); ok {
			// Get the underlying client from the adapter
			underlyingClient := ethAdapter.GetUnderlyingClient()
			if underlyingClient != nil {
				// Create a rate-limited client with PriorityHigh for real-time sync
				rateLimitedClient, err := ratelimit.NewRateLimitedClient(&ratelimit.RateLimitedClientConfig{
					Client:       underlyingClient,
					Tracker:      cfg.RateLimitTracker,
					CostRegistry: cfg.RateLimitRegistry,
					Priority:     ratelimit.PriorityHigh, // Real-time sync gets priority access
				})
				if err != nil {
					log.Printf("[SyncWorker] Warning: failed to create rate-limited client: %v, continuing without rate limiting", err)
				} else {
					// Set the rate limiter on the adapter
					ethAdapter.SetRateLimiter(rateLimitedClient)
					ethAdapter.SetPriority(ratelimit.PriorityHigh)
					log.Printf("[SyncWorker] Chain %s: rate limiting enabled with PriorityHigh for real-time sync", cfg.Chain)
				}
			} else {
				log.Printf("[SyncWorker] Chain %s: adapter has no underlying client, continuing without rate limiting", cfg.Chain)
			}
		} else {
			log.Printf("[SyncWorker] Chain %s: adapter does not support rate limiting, continuing without", cfg.Chain)
		}
	}

	// Default maxBlocksPerPoll: 30 (based on Alchemy free tier 500 CU/s)
	maxBlocksPerPoll := cfg.MaxBlocksPerPoll
	if maxBlocksPerPoll <= 0 {
		maxBlocksPerPoll = 30
	}

	return &SyncWorker{
		chain:             cfg.Chain,
		chainAdapter:      cfg.ChainAdapter,
		txRepo:            cfg.TxRepo,
		syncStatusRepo:    cfg.SyncStatusRepo,
		addressRepo:       cfg.AddressRepo,
		portfolioRepo:     cfg.PortfolioRepo,
		userRepo:          cfg.UserRepo,
		cache:             cfg.Cache,
		pollInterval:      pollInterval,
		maxBlocksPerPoll:  maxBlocksPerPoll,
		stopCh:            make(chan struct{}),
		doneCh:            make(chan struct{}),
		tierProvider:      tierProvider,
		useTierPriority:   cfg.UseTierPriority,
		rateLimitTracker:  cfg.RateLimitTracker,
		rateLimitRegistry: cfg.RateLimitRegistry,
	}, nil
}

// Start begins the sync worker for a chain
func (w *SyncWorker) Start(ctx context.Context) error {
	w.mu.Lock()
	if w.running {
		w.mu.Unlock()
		return fmt.Errorf("sync worker for chain %s is already running", w.chain)
	}
	w.running = true
	w.mu.Unlock()

	log.Printf("[SyncWorker] Starting sync worker for chain %s with poll interval %v", w.chain, w.pollInterval)
	log.Printf("[SyncWorker] Chain %s: will log each block number during processing", w.chain)

	// Try to load last processed block from database
	lastBlock, err := w.loadLastProcessedBlock(ctx)
	if err != nil {
		log.Printf("[SyncWorker] Chain %s: failed to load last processed block: %v, starting from current", w.chain, err)
		lastBlock = 0
	}

	// If no saved progress, start from current block
	if lastBlock == 0 {
		currentBlock, err := w.chainAdapter.GetCurrentBlock(ctx)
		if err != nil {
			w.mu.Lock()
			w.running = false
			w.mu.Unlock()
			return fmt.Errorf("failed to get current block for chain %s: %w", w.chain, err)
		}
		lastBlock = currentBlock
		log.Printf("[SyncWorker] Chain %s initialized at current block %d", w.chain, currentBlock)
	} else {
		log.Printf("[SyncWorker] Chain %s resuming from saved block %d", w.chain, lastBlock)
	}

	w.mu.Lock()
	w.lastBlockProcessed = lastBlock
	w.mu.Unlock()

	// Start polling loop in goroutine
	go w.pollLoop(ctx)

	return nil
}

// Stop gracefully stops the sync worker
func (w *SyncWorker) Stop(ctx context.Context) error {
	w.mu.Lock()
	if !w.running {
		w.mu.Unlock()
		return fmt.Errorf("sync worker for chain %s is not running", w.chain)
	}
	w.mu.Unlock()

	log.Printf("[SyncWorker] Stopping sync worker for chain %s", w.chain)

	// Signal stop
	close(w.stopCh)

	// Wait for polling loop to finish with timeout
	select {
	case <-w.doneCh:
		log.Printf("[SyncWorker] Sync worker for chain %s stopped gracefully", w.chain)
	case <-ctx.Done():
		log.Printf("[SyncWorker] Sync worker for chain %s stop timed out", w.chain)
		return ctx.Err()
	case <-time.After(30 * time.Second):
		log.Printf("[SyncWorker] Sync worker for chain %s stop timed out after 30s", w.chain)
		return fmt.Errorf("stop timeout")
	}

	w.mu.Lock()
	w.running = false
	w.mu.Unlock()

	return nil
}

// pollLoop is the main polling loop that runs in a goroutine
func (w *SyncWorker) pollLoop(ctx context.Context) {
	defer close(w.doneCh)

	ticker := time.NewTicker(w.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("[SyncWorker] Chain %s: context cancelled", w.chain)
			return
		case <-w.stopCh:
			log.Printf("[SyncWorker] Chain %s: stop signal received", w.chain)
			return
		case <-ticker.C:
			// Update last poll time
			w.mu.Lock()
			w.lastPollTime = time.Now()
			w.mu.Unlock()

			// Poll chain for new blocks
			blocksProcessed, err := w.PollChain(ctx)
			if err != nil {
				log.Printf("[SyncWorker] Chain %s: poll error: %v", w.chain, err)
				// Continue polling despite errors
				continue
			}

			if blocksProcessed > 0 {
				log.Printf("[SyncWorker] Chain %s: processed %d new blocks", w.chain, blocksProcessed)
			}
		}
	}
}

// PollChain polls chain for new blocks
// Returns number of new blocks processed
// Limits processing to maxBlocksPerPoll to prevent long-running cycles
func (w *SyncWorker) PollChain(ctx context.Context) (int, error) {
	// Get current block number
	currentBlock, err := w.chainAdapter.GetCurrentBlock(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get current block: %w", err)
	}

	w.mu.RLock()
	lastBlock := w.lastBlockProcessed
	maxBlocks := w.maxBlocksPerPoll
	w.mu.RUnlock()

	// No new blocks
	if currentBlock <= lastBlock {
		return 0, nil
	}

	// Calculate blocks behind and apply batch limit
	blocksBehind := currentBlock - lastBlock
	targetBlock := currentBlock
	if blocksBehind > uint64(maxBlocks) {
		targetBlock = lastBlock + uint64(maxBlocks)
		log.Printf("[SyncWorker] Chain %s: %d blocks behind, processing %d blocks this cycle (will catch up over multiple cycles)",
			w.chain, blocksBehind, maxBlocks)
	}

	// Process new blocks (up to maxBlocksPerPoll)
	blocksProcessed := 0
	for blockNum := lastBlock + 1; blockNum <= targetBlock; blockNum++ {
		log.Printf("[SyncWorker] Chain %s: processing block %d", w.chain, blockNum)

		result, err := w.ProcessBlock(ctx, &ProcessBlockInput{
			Chain:       w.chain,
			BlockNumber: blockNum,
		})
		if err != nil {
			log.Printf("[SyncWorker] Chain %s: failed to process block %d: %v", w.chain, blockNum, err)
			// Continue with next block despite error
			continue
		}

		blocksProcessed++

		if result.TransactionsFound > 0 {
			log.Printf("[SyncWorker] Chain %s: block %d - found %d transactions for %d addresses",
				w.chain, blockNum, result.TransactionsFound, result.AddressesUpdated)
		} else {
			log.Printf("[SyncWorker] Chain %s: block %d - no transactions found", w.chain, blockNum)
		}
	}

	// Update last block processed in memory (to targetBlock, not currentBlock)
	w.mu.Lock()
	w.lastBlockProcessed = targetBlock
	w.mu.Unlock()

	// Persist progress to database
	log.Printf("[SyncWorker] Chain %s: saving progress at block %d", w.chain, targetBlock)
	if err := w.saveLastProcessedBlock(ctx, targetBlock); err != nil {
		log.Printf("[SyncWorker] Chain %s: failed to save progress: %v", w.chain, err)
		// Don't fail the operation, just log the error
	} else {
		log.Printf("[SyncWorker] Chain %s: progress saved successfully", w.chain)
	}

	// Log if still catching up
	if targetBlock < currentBlock {
		remaining := currentBlock - targetBlock
		log.Printf("[SyncWorker] Chain %s: still %d blocks behind, will continue catching up", w.chain, remaining)
	}

	return blocksProcessed, nil
}

// loadLastProcessedBlock loads the last processed block from database
func (w *SyncWorker) loadLastProcessedBlock(ctx context.Context) (uint64, error) {
	query := `SELECT last_processed_block FROM worker_progress WHERE chain = $1`

	var lastBlock int64
	err := w.syncStatusRepo.DB().Pool().QueryRow(ctx, query, string(w.chain)).Scan(&lastBlock)
	if err != nil {
		// If no record exists, return 0 (will start from current block)
		return 0, err
	}

	return uint64(lastBlock), nil
}

// saveLastProcessedBlock saves the last processed block to database
func (w *SyncWorker) saveLastProcessedBlock(ctx context.Context, blockNum uint64) error {
	query := `
		INSERT INTO worker_progress (chain, last_processed_block, updated_at)
		VALUES ($1, $2, NOW())
		ON CONFLICT (chain)
		DO UPDATE SET last_processed_block = $2, updated_at = NOW()
	`

	_, err := w.syncStatusRepo.DB().Pool().Exec(ctx, query, string(w.chain), int64(blockNum))
	return err
}

// ProcessBlock processes a new block
func (w *SyncWorker) ProcessBlock(ctx context.Context, input *ProcessBlockInput) (*ProcessBlockResult, error) {
	startTime := time.Now()

	// Get tracked addresses for this chain
	trackedAddresses, err := w.getTrackedAddresses(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get tracked addresses: %w", err)
	}

	// Update addresses tracked count
	w.mu.Lock()
	w.addressesTracked = len(trackedAddresses)
	w.mu.Unlock()

	// No addresses to track
	if len(trackedAddresses) == 0 {
		return &ProcessBlockResult{
			Chain:             input.Chain,
			BlockNumber:       input.BlockNumber,
			TransactionsFound: 0,
			AddressesUpdated:  0,
			ProcessingTimeMs:  time.Since(startTime).Milliseconds(),
		}, nil
	}

	// Fetch transactions for tracked addresses in this block
	transactions, err := w.chainAdapter.FetchTransactionsForBlock(ctx, input.BlockNumber, trackedAddresses)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch transactions for block %d: %w", input.BlockNumber, err)
	}

	// No transactions found
	if len(transactions) == 0 {
		return &ProcessBlockResult{
			Chain:             input.Chain,
			BlockNumber:       input.BlockNumber,
			TransactionsFound: 0,
			AddressesUpdated:  0,
			ProcessingTimeMs:  time.Since(startTime).Milliseconds(),
		}, nil
	}

	// Update addresses with new transactions
	updateResult, err := w.UpdateAddresses(ctx, transactions)
	if err != nil {
		return nil, fmt.Errorf("failed to update addresses: %w", err)
	}

	return &ProcessBlockResult{
		Chain:             input.Chain,
		BlockNumber:       input.BlockNumber,
		TransactionsFound: len(transactions),
		AddressesUpdated:  updateResult.AddressesUpdated,
		ProcessingTimeMs:  time.Since(startTime).Milliseconds(),
	}, nil
}

// UpdateAddresses updates tracked addresses with new transactions
func (w *SyncWorker) UpdateAddresses(ctx context.Context, transactions []*types.NormalizedTransaction) (*UpdateAddressesResult, error) {
	if len(transactions) == 0 {
		return &UpdateAddressesResult{
			AddressesUpdated:  0,
			TransactionsAdded: 0,
			CacheUpdated:      false,
		}, nil
	}

	// Get the list of tracked addresses to filter
	trackedAddresses, err := w.getTrackedAddresses(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get tracked addresses: %w", err)
	}
	trackedSet := make(map[string]bool)
	for _, addr := range trackedAddresses {
		trackedSet[strings.ToLower(addr)] = true
	}

	// Track unique TRACKED addresses affected (not all addresses in transactions)
	addressesAffected := make(map[string]bool)
	for _, tx := range transactions {
		fromLower := strings.ToLower(tx.From)
		toLower := strings.ToLower(tx.To)

		// Only track addresses that are in our tracked list
		if trackedSet[fromLower] {
			addressesAffected[fromLower] = true
		}
		if toLower != "" && trackedSet[toLower] {
			addressesAffected[toLower] = true
		}
	}

	// Convert NormalizedTransactions to models.Transaction for storage
	// Only store for tracked addresses
	var modelTransactions []*models.Transaction
	for address := range addressesAffected {
		for _, tx := range transactions {
			// Only store if this tracked address is involved in the transaction
			if strings.ToLower(tx.From) == address || strings.ToLower(tx.To) == address {
				modelTx := models.FromNormalizedTransaction(tx, address)
				modelTransactions = append(modelTransactions, modelTx)
			}
		}
	}

	// Store transactions in ClickHouse asynchronously
	go func() {
		bgCtx := context.Background()
		if err := w.txRepo.BatchInsert(bgCtx, modelTransactions); err != nil {
			log.Printf("[SyncWorker] Chain %s: failed to store transactions in ClickHouse: %v", w.chain, err)
		}
	}()

	// Update cache for affected tracked addresses only
	cacheUpdated := false
	for address := range addressesAffected {
		// Update cache with new transactions
		if err := w.cache.UpdateTransactions(ctx, address, w.chain, transactions); err != nil {
			log.Printf("[SyncWorker] Chain %s: failed to update cache for address %s: %v", w.chain, address, err)
			// Non-fatal: cache will be populated on next query
		} else {
			cacheUpdated = true
		}

		// Update sync status only for tracked addresses
		if err := w.updateSyncStatus(ctx, address); err != nil {
			log.Printf("[SyncWorker] Chain %s: failed to update sync status for address %s: %v", w.chain, address, err)
		}
	}

	return &UpdateAddressesResult{
		AddressesUpdated:  len(addressesAffected),
		TransactionsAdded: len(transactions),
		CacheUpdated:      cacheUpdated,
	}, nil
}

// GetStatus returns current worker status
func (w *SyncWorker) GetStatus() *SyncWorkerStatus {
	w.mu.RLock()
	defer w.mu.RUnlock()

	return &SyncWorkerStatus{
		Chain:               w.chain,
		Running:             w.running,
		LastPollTime:        w.lastPollTime,
		LastBlockProcessed:  w.lastBlockProcessed,
		CurrentBlock:        w.lastBlockProcessed, // Will be updated on next poll
		AddressesTracked:    w.addressesTracked,
		PollIntervalSeconds: int(w.pollInterval.Seconds()),
	}
}

// getTrackedAddresses retrieves all addresses being tracked for this chain
// If tier priority is enabled, returns addresses sorted by tier (paid first)
func (w *SyncWorker) getTrackedAddresses(ctx context.Context) ([]string, error) {
	// If tier priority is enabled, use the tier-aware provider
	if w.useTierPriority && w.tierProvider != nil {
		// Get addresses split by tier
		paidAddrs, freeAddrs, err := w.tierProvider.GetAddressesByTier(ctx, w.chain)
		if err != nil {
			return nil, fmt.Errorf("failed to get addresses by tier: %w", err)
		}

		// Concatenate paid tier first, then free tier
		allAddrs := make([]string, 0, len(paidAddrs)+len(freeAddrs))
		allAddrs = append(allAddrs, paidAddrs...)
		allAddrs = append(allAddrs, freeAddrs...)

		log.Printf("[SyncWorker] Chain %s: processing %d paid tier addresses, %d free tier addresses",
			w.chain, len(paidAddrs), len(freeAddrs))

		return allAddrs, nil
	}

	// Default behavior: get all addresses without priority
	chain := w.chain
	addresses, err := w.addressRepo.List(ctx, &storage.AddressFilters{
		Chain: &chain,
		Limit: 10000, // Reasonable limit for MVP
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list addresses: %w", err)
	}

	// Extract address strings
	addressList := make([]string, len(addresses))
	for i, addr := range addresses {
		addressList[i] = addr.Address
	}

	return addressList, nil
}

// updateSyncStatus updates the sync status for an address
func (w *SyncWorker) updateSyncStatus(ctx context.Context, address string) error {
	w.mu.RLock()
	currentBlock := w.lastBlockProcessed
	w.mu.RUnlock()

	// Update sync status in address table
	return w.addressRepo.UpdateLastSyncedBlock(ctx, address, w.chain, currentBlock)
}

// ProcessBlockInput defines input for processing a block
type ProcessBlockInput struct {
	Chain       types.ChainID
	BlockNumber uint64
}

// ProcessBlockResult defines result of processing a block
type ProcessBlockResult struct {
	Chain             types.ChainID
	BlockNumber       uint64
	TransactionsFound int
	AddressesUpdated  int
	ProcessingTimeMs  int64
}

// UpdateAddressesResult defines result of updating addresses
type UpdateAddressesResult struct {
	AddressesUpdated  int
	TransactionsAdded int
	CacheUpdated      bool
}

// SyncWorkerStatus represents the current status of a sync worker
type SyncWorkerStatus struct {
	Chain               types.ChainID
	Running             bool
	LastPollTime        time.Time
	LastBlockProcessed  uint64
	CurrentBlock        uint64
	AddressesTracked    int
	PollIntervalSeconds int
}
