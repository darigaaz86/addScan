// Package main provides the sync worker entry point for the address scanner service.
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
	"github.com/address-scanner/internal/storage"
	"github.com/address-scanner/internal/types"
	"github.com/address-scanner/internal/worker"
)

func main() {
	fmt.Println("Address Scanner Sync Worker")
	log.Println("Worker starting...")

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

	// Connect to Redis
	redis, err := storage.NewRedisCache(&cfg.Database.Redis)
	if err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}
	defer redis.Close()

	log.Println("Database connections established")

	// Initialize rate limiter components (optional - continues without if creation fails)
	// Requirements: 2.1, 5.1 - Redis-based CU budget tracking for cross-service coordination
	var rateLimitTracker *ratelimit.CUBudgetTracker
	var rateLimitRegistry *ratelimit.CUCostRegistry

	rateLimitCfg := ratelimit.LoadFromEnv()
	log.Printf("Rate limit configuration loaded: %s", rateLimitCfg)

	// Create CU Cost Registry with default costs
	rateLimitRegistry = ratelimit.NewCUCostRegistry(&ratelimit.CUCostRegistryConfig{
		DefaultCost: rateLimitCfg.DefaultMethodCost,
	})
	log.Println("CU Cost Registry initialized")

	// Create CU Budget Tracker with Redis connection
	tracker, err := ratelimit.NewCUBudgetTracker(&ratelimit.CUBudgetTrackerConfig{
		Redis:          redis.Client(),
		TotalBudget:    rateLimitCfg.TotalCUPerSecond,
		ReservedBudget: rateLimitCfg.ReservedCU,
	})
	if err != nil {
		log.Printf("WARNING: Failed to create CU Budget Tracker: %v. Continuing without rate limiting.", err)
		rateLimitRegistry = nil // Clear registry if tracker fails
	} else {
		rateLimitTracker = tracker
		log.Printf("CU Budget Tracker initialized (Total: %d CU/s, Reserved: %d CU/s, Shared: %d CU/s)",
			rateLimitCfg.TotalCUPerSecond, rateLimitCfg.ReservedCU, rateLimitCfg.SharedCU)
	}

	// Initialize repositories
	txRepo := storage.NewTransactionRepository(clickhouse)
	syncStatusRepo := storage.NewSyncStatusRepository(postgres)
	addressRepo := storage.NewAddressRepository(postgres)
	portfolioRepo := storage.NewPortfolioRepository(postgres)
	userRepo := storage.NewUserRepository(postgres)

	// Initialize chain adapters
	log.Println("Initializing chain adapters...")
	chainAdapters := make(map[types.ChainID]adapter.ChainAdapter)
	chainConfigs := make(map[types.ChainID]config.ChainConfig) // Store configs for later use

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
		chainConfigs[chainID] = chainCfg
		log.Printf("Chain adapter initialized: %s", chainName)
	}

	if len(chainAdapters) == 0 {
		log.Fatal("No chain adapters enabled. Please configure at least one chain with RPC endpoint.")
	}

	// Create sync workers for each enabled chain
	log.Println("Starting sync workers...")
	ctx := context.Background()
	workers := make([]*worker.SyncWorker, 0, len(chainAdapters))

	for chainID, chainAdapter := range chainAdapters {
		// Get config for this chain
		chainCfg := chainConfigs[chainID]
		pollInterval := chainCfg.PollInterval
		if pollInterval == 0 {
			pollInterval = 15 * time.Second
		}

		workerCfg := &worker.SyncWorkerConfig{
			Chain:             chainID,
			ChainAdapter:      chainAdapter,
			TxRepo:            txRepo,
			SyncStatusRepo:    syncStatusRepo,
			AddressRepo:       addressRepo,
			PortfolioRepo:     portfolioRepo,
			UserRepo:          userRepo,
			Cache:             redis,
			PollInterval:      pollInterval,
			MaxBlocksPerPoll:  cfg.Sync.MaxBlocksPerPoll,
			MaxBlocksPerBatch: cfg.Sync.MaxBlocksPerBatch, // Alchemy free tier: 10 blocks per eth_getLogs
			UseTierPriority:   true,                       // Enable tier-based priority
			UseBatchedPolling: chainCfg.UseBatchedPolling, // Use batched eth_getLogs for CU efficiency
			RateLimitTracker:  rateLimitTracker,
			RateLimitRegistry: rateLimitRegistry,
		}

		log.Printf("Chain %s: batched polling = %v", chainID, chainCfg.UseBatchedPolling)

		syncWorker, err := worker.NewSyncWorker(workerCfg)
		if err != nil {
			log.Fatalf("Failed to create sync worker for chain %s: %v", chainID, err)
		}

		if err := syncWorker.Start(ctx); err != nil {
			log.Fatalf("Failed to start sync worker for chain %s: %v", chainID, err)
		}

		workers = append(workers, syncWorker)
		log.Printf("Sync worker started for chain %s", chainID)
	}

	log.Printf("All sync workers started successfully (%d chains)", len(workers))

	// Set up graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	// Wait for shutdown signal
	<-sigCh
	log.Println("Shutdown signal received, stopping workers...")

	// Stop all workers gracefully
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for _, w := range workers {
		status := w.GetStatus()
		log.Printf("Stopping sync worker for chain %s...", status.Chain)
		if err := w.Stop(shutdownCtx); err != nil {
			log.Printf("Error stopping worker for chain %s: %v", status.Chain, err)
		} else {
			log.Printf("Sync worker for chain %s stopped", status.Chain)
		}
	}

	log.Println("All workers stopped. Goodbye!")
}
