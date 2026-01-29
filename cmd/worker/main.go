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

	// Create adapters for each enabled chain
	for _, chainName := range cfg.Chains.Enabled {
		chainCfg, ok := cfg.Chains.Chains[chainName]
		if !ok || chainCfg.RPCPrimary == "" {
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
		default:
			log.Printf("Skipping unknown chain: %s", chainName)
			continue
		}

		// Create data provider with failover
		endpoints := []string{chainCfg.RPCPrimary}
		if chainCfg.RPCSecondary != "" {
			endpoints = append(endpoints, chainCfg.RPCSecondary)
		}

		var provider adapter.DataProvider
		if len(endpoints) == 1 {
			provider, err = adapter.NewRPCProvider(endpoints[0], "")
		} else {
			provider, err = adapter.NewRPCProvider(endpoints[0], endpoints[1])
		}
		if err != nil {
			log.Printf("Failed to create provider for chain %s: %v", chainName, err)
			continue
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
		log.Fatal("No chain adapters enabled. Please configure at least one chain with RPC endpoint.")
	}

	// Create sync workers for each enabled chain
	log.Println("Starting sync workers...")
	ctx := context.Background()
	workers := make([]*worker.SyncWorker, 0, len(chainAdapters))

	for chainID, chainAdapter := range chainAdapters {
		// Get poll interval for this chain
		var pollInterval time.Duration
		for name, chainCfg := range cfg.Chains.Chains {
			var cid types.ChainID
			switch name {
			case "ethereum":
				cid = types.ChainEthereum
			case "polygon":
				cid = types.ChainPolygon
			case "arbitrum":
				cid = types.ChainArbitrum
			case "optimism":
				cid = types.ChainOptimism
			case "base":
				cid = types.ChainBase
			}
			if cid == chainID {
				pollInterval = chainCfg.PollInterval
				break
			}
		}
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
			UseTierPriority:   true, // Enable tier-based priority
			RateLimitTracker:  rateLimitTracker,
			RateLimitRegistry: rateLimitRegistry,
		}

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
