// Package main provides the backfill worker entry point for the address scanner service.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/address-scanner/internal/adapter"
	"github.com/address-scanner/internal/config"
	"github.com/address-scanner/internal/ratelimit"
	"github.com/address-scanner/internal/service"
	"github.com/address-scanner/internal/storage"
	"github.com/address-scanner/internal/types"
)

func main() {
	fmt.Println("Address Scanner Backfill Worker")
	log.Println("Backfill worker starting...")

	// Load configuration
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Initialize database connections
	log.Println("Connecting to databases...")

	// Connect to Postgres
	postgres, err := storage.NewPostgresDB(&cfg.Database.Postgres)
	if err != nil {
		log.Fatalf("Failed to connect to Postgres: %v", err)
	}
	defer postgres.Close()

	// Connect to ClickHouse
	clickhouse, err := storage.NewClickHouseDB(&cfg.Database.ClickHouse)
	if err != nil {
		log.Fatalf("Failed to connect to ClickHouse: %v", err)
	}
	defer clickhouse.Close()

	// Connect to Redis (required for rate limiting coordination)
	redis, err := storage.NewRedisCache(&cfg.Database.Redis)
	if err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}
	defer redis.Close()

	log.Println("Database connections established")

	// Initialize repositories
	backfillRepo := storage.NewBackfillJobRepository(postgres)
	txRepo := storage.NewTransactionRepository(clickhouse)

	// Initialize token whitelist for spam detection
	whitelistRepo := storage.NewTokenWhitelistRepository(postgres)
	if err := whitelistRepo.LoadCache(context.Background()); err != nil {
		log.Printf("Warning: failed to load token whitelist: %v", err)
	} else {
		txRepo.SetTokenWhitelist(whitelistRepo)
		log.Println("Token whitelist loaded for spam detection")
	}

	// Initialize rate limiter components (optional - continues without if creation fails)
	// Requirements: 2.1, 4.1 - Redis-based CU budget tracking for cross-service coordination
	var rateController *ratelimit.BackfillRateController

	rateLimitCfg := ratelimit.LoadFromEnv()
	log.Printf("Rate limit configuration loaded: %s", rateLimitCfg)

	// Create CU Budget Tracker with Redis connection
	tracker, err := ratelimit.NewCUBudgetTracker(&ratelimit.CUBudgetTrackerConfig{
		Redis:          redis.Client(),
		TotalBudget:    rateLimitCfg.TotalCUPerSecond,
		ReservedBudget: rateLimitCfg.ReservedCU,
	})
	if err != nil {
		log.Printf("WARNING: Failed to create CU Budget Tracker: %v. Continuing without rate limiting.", err)
	} else {
		// Create BackfillRateController with the tracker
		controller, err := ratelimit.NewBackfillRateController(&ratelimit.BackfillRateControllerConfig{
			Tracker: tracker,
		})
		if err != nil {
			log.Printf("WARNING: Failed to create Backfill Rate Controller: %v. Continuing without rate limiting.", err)
		} else {
			rateController = controller
			log.Printf("Rate limiter initialized (Total: %d CU/s, Reserved: %d CU/s, Shared: %d CU/s)",
				rateLimitCfg.TotalCUPerSecond, rateLimitCfg.ReservedCU, rateLimitCfg.SharedCU)
		}
	}

	// Initialize chain adapters
	log.Println("Initializing chain adapters...")
	chainAdapters := make(map[types.ChainID]adapter.ChainAdapter)

	// Create adapters for each enabled chain
	for _, chainName := range cfg.Chains.Enabled {
		chainCfg, ok := cfg.Chains.Chains[chainName]
		if !ok {
			log.Printf("Skipping chain %s: no configuration found", chainName)
			continue
		}

		// Check if we have any RPC URLs configured
		if len(chainCfg.RPCURLs) == 0 && chainCfg.RPCPrimary == "" {
			log.Printf("Skipping chain %s: no RPC endpoint configured", chainName)
			continue
		}

		// Map chain name to ChainID
		var chainID types.ChainID
		switch chainName {
		case "ethereum":
			chainID = types.ChainEthereum
		case "polygon":
			chainID = types.ChainPolygon
		case "arbitrum":
			chainID = types.ChainArbitrum
		case "optimism":
			chainID = types.ChainOptimism
		case "base":
			chainID = types.ChainBase
		case "bnb":
			chainID = types.ChainBNB
		default:
			log.Printf("Skipping unknown chain: %s", chainName)
			continue
		}

		// Create data provider - prefer RPC pool if multiple URLs configured
		var provider adapter.DataProvider
		if len(chainCfg.RPCURLs) > 1 {
			// Use RPC pool for multiple accounts (failover on 429)
			pool, err := adapter.NewRPCPool(&adapter.RPCPoolConfig{
				Endpoints:    chainCfg.RPCURLs,
				CooldownTime: 60 * time.Second,
			})
			if err != nil {
				log.Printf("Failed to create RPC pool for chain %s: %v", chainName, err)
				continue
			}
			provider = adapter.NewPooledRPCProvider(pool)
			log.Printf("Chain %s: using RPC pool with %d endpoints", chainName, len(chainCfg.RPCURLs))
		} else {
			// Single endpoint - use legacy provider
			endpoint := chainCfg.RPCPrimary
			if len(chainCfg.RPCURLs) == 1 {
				endpoint = chainCfg.RPCURLs[0]
			}
			provider, err = adapter.NewRPCProvider(endpoint, chainCfg.RPCSecondary)
			if err != nil {
				log.Printf("Failed to create provider for chain %s: %v", chainName, err)
				continue
			}
			log.Printf("Chain %s: using single RPC endpoint", chainName)
		}

		// Create chain adapter
		chainAdapter, err := adapter.NewEthereumAdapter(chainID, provider)
		if err != nil {
			log.Printf("Failed to create adapter for chain %s: %v", chainName, err)
			continue
		}

		chainAdapters[chainID] = chainAdapter
		log.Printf("Chain adapter initialized: %s (RPC: %s)", chainName, chainCfg.RPCPrimary)
	}

	if len(chainAdapters) == 0 {
		log.Fatal("No chain adapters initialized. Please configure at least one chain with RPC endpoint.")
	}

	// Create backfill service with rate controller support
	// The rate controller is optional - if nil, the service operates without rate limiting
	backfillService := service.NewBackfillServiceFull(
		backfillRepo,
		txRepo,
		chainAdapters,
		cfg.Etherscan.APIKey,
		cfg.Dune.APIKey,
		rateController,
	)

	// Reset stale "in_progress" jobs on startup
	// These are jobs that were being processed when the worker crashed
	resetCtx, resetCancel := context.WithTimeout(context.Background(), 10*time.Second)
	staleCount, err := backfillRepo.ResetStaleInProgressJobs(resetCtx, 5*time.Minute)
	resetCancel()
	if err != nil {
		log.Printf("Warning: failed to reset stale jobs: %v", err)
	} else if staleCount > 0 {
		log.Printf("Reset %d stale in_progress jobs to queued", staleCount)
	}

	log.Printf("Backfill worker initialized with %d chain adapters", len(chainAdapters))
	if cfg.Etherscan.APIKey != "" {
		log.Println("Etherscan API enabled for complete transaction history (including approve, contract calls)")
	}
	if rateController != nil {
		log.Println("Rate limiting enabled for CU budget management")
	}
	log.Println("Starting job processing loop...")

	// Set up graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start auto re-queue background job
	backfillService.StartAutoRequeue(ctx)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	// Collect enabled chains for per-chain workers
	var enabledChains []types.ChainID
	for chainID := range chainAdapters {
		enabledChains = append(enabledChains, chainID)
	}

	// Start processing loop in goroutine
	doneCh := make(chan struct{})
	go func() {
		defer close(doneCh)
		processLoop(ctx, backfillService, enabledChains)
	}()

	// Wait for shutdown signal
	<-sigCh
	log.Println("Shutdown signal received, stopping backfill worker...")

	// Stop auto re-queue
	backfillService.StopAutoRequeue()

	cancel()

	// Wait for processing loop to finish
	select {
	case <-doneCh:
		log.Println("Backfill worker stopped gracefully")
	case <-time.After(30 * time.Second):
		log.Println("Backfill worker stop timed out")
	}

	log.Println("Goodbye!")
}

// processLoop starts per-chain workers that process jobs independently
// Each chain has its own worker so a slow/failing chain doesn't block others
// All workers share the same Etherscan client which has a global rate limiter (3 req/sec)
func processLoop(ctx context.Context, backfillService *service.BackfillService, chains []types.ChainID) {
	log.Printf("Starting %d per-chain backfill workers", len(chains))

	// Start a worker for each chain
	for _, chain := range chains {
		go chainWorker(ctx, backfillService, chain)
	}

	// Wait for context cancellation
	<-ctx.Done()
	log.Println("Processing loop stopped")
}

// chainWorker processes jobs for a specific chain
func chainWorker(ctx context.Context, backfillService *service.BackfillService, chain types.ChainID) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	log.Printf("[Worker-%s] Started", chain)

	for {
		select {
		case <-ctx.Done():
			log.Printf("[Worker-%s] Stopped", chain)
			return
		case <-ticker.C:
			if err := backfillService.ProcessNextJobForChain(ctx, chain); err != nil {
				log.Printf("[Worker-%s] Error: %v", chain, err)
			}
		}
	}
}
