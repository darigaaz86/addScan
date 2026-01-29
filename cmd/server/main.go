// Package main provides the API server entry point for the address scanner service.
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
	"github.com/address-scanner/internal/api"
	"github.com/address-scanner/internal/config"
	"github.com/address-scanner/internal/job"
	"github.com/address-scanner/internal/logging"
	"github.com/address-scanner/internal/service"
	"github.com/address-scanner/internal/storage"
	"github.com/address-scanner/internal/types"
)

func main() {
	fmt.Println("Address Scanner API Server")
	log.Println("Server starting...")

	// Load configuration
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Initialize structured logging
	logLevel := logging.ParseLogLevel(cfg.Logging.Level)
	logFormat := logging.ParseLogFormat(cfg.Logging.Format)
	logging.InitGlobalLogger(logLevel, logFormat)

	logger := logging.GetGlobalLogger()
	logger.WithFields(map[string]interface{}{
		"level":  cfg.Logging.Level,
		"format": cfg.Logging.Format,
	}).Info("Structured logging initialized")

	// Initialize database connections
	logger.Info("Connecting to databases...")

	// Connect to Postgres
	postgres, err := storage.NewPostgresDB(&cfg.Database.Postgres)
	if err != nil {
		logger.WithError(err).Fatal("Failed to connect to Postgres")
	}
	defer postgres.Close()

	// Connect to ClickHouse
	clickhouse, err := storage.NewClickHouseDB(&cfg.Database.ClickHouse)
	if err != nil {
		logger.WithError(err).Fatal("Failed to connect to ClickHouse")
	}
	defer clickhouse.Close()

	// Connect to Redis
	redis, err := storage.NewRedisCache(&cfg.Database.Redis)
	if err != nil {
		logger.WithError(err).Fatal("Failed to connect to Redis")
	}
	defer redis.Close()

	logger.Info("Database connections established")

	// Initialize chain adapters
	logger.Info("Initializing chain adapters...")
	chainAdapters := make(map[types.ChainID]adapter.ChainAdapter)

	// Create adapters for each enabled chain
	for _, chainName := range cfg.Chains.Enabled {
		chainCfg, ok := cfg.Chains.Chains[chainName]
		if !ok || chainCfg.RPCPrimary == "" {
			logger.WithFields(map[string]interface{}{
				"chain": chainName,
			}).Warn("Skipping chain: no RPC endpoint configured")
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
			logger.WithFields(map[string]interface{}{
				"chain": chainName,
			}).Warn("Skipping unknown chain")
			continue
		}

		// Create data provider with failover
		var provider adapter.DataProvider
		if chainCfg.RPCSecondary != "" {
			provider, err = adapter.NewRPCProvider(chainCfg.RPCPrimary, chainCfg.RPCSecondary)
		} else {
			provider, err = adapter.NewRPCProvider(chainCfg.RPCPrimary, "")
		}
		if err != nil {
			logger.WithError(err).WithFields(map[string]interface{}{
				"chain": chainName,
			}).Warn("Failed to create provider for chain")
			continue
		}

		// Create chain adapter
		chainAdapter, err := adapter.NewEthereumAdapter(chainID, provider)
		if err != nil {
			logger.WithError(err).WithFields(map[string]interface{}{
				"chain": chainName,
			}).Warn("Failed to create adapter for chain")
			continue
		}

		chainAdapters[chainID] = chainAdapter
		logger.WithFields(map[string]interface{}{
			"chain": chainName,
			"rpc":   chainCfg.RPCPrimary,
		}).Info("Chain adapter initialized")
	}

	if len(chainAdapters) == 0 {
		logger.Warn("No chain adapters initialized - address validation will use fallback")
	}

	// Initialize repositories
	userRepo := storage.NewUserRepository(postgres)
	addressRepo := storage.NewAddressRepository(postgres)
	portfolioRepo := storage.NewPortfolioRepository(postgres)
	snapshotRepo := storage.NewSnapshotRepository(postgres.Pool())
	txRepo := storage.NewTransactionRepository(clickhouse)
	backfillJobRepo := storage.NewBackfillJobRepository(postgres)

	// Initialize cache service
	cacheService := storage.NewCacheService(redis, cfg.Cache.TTL)

	// Initialize unified timeline repository (for query service)
	unifiedTimelineRepo := storage.NewUnifiedTimelineRepository(clickhouse)

	// Initialize services
	logger.Info("Initializing services...")

	// Initialize backfill job service with Etherscan for complete transaction data
	etherscanAPIKey := os.Getenv("ETHERSCAN_API_KEY")
	backfillJobService := job.NewBackfillJobServiceWithEtherscan(
		backfillJobRepo,
		chainAdapters,
		txRepo,
		addressRepo,
		etherscanAPIKey,
	)

	// Address service (with chain adapters for validation and backfill job service)
	addressService := service.NewAddressService(
		addressRepo,
		userRepo,
		chainAdapters,      // Pass chain adapters for address validation
		backfillJobService, // Pass backfill job service to create jobs
	)

	// Query service
	queryService := service.NewQueryService(txRepo, unifiedTimelineRepo, cacheService)
	queryService.SetCacheWindowSize(cfg.Cache.TransactionWindow)

	// Portfolio service (with chain adapters for balance queries)
	portfolioService := service.NewPortfolioService(
		portfolioRepo,
		addressRepo,
		txRepo,
		chainAdapters,  // Pass chain adapters for balance queries
		addressService, // Pass address service for automatic address creation
	)

	// Snapshot service
	snapshotService := service.NewSnapshotService(
		snapshotRepo,
		portfolioRepo,
		userRepo,
		portfolioService,
	)

	logger.Info("Services initialized")

	// Create server configuration
	serverConfig := &api.ServerConfig{
		Host:            cfg.Server.Host,
		Port:            cfg.Server.Port,
		ReadTimeout:     15 * time.Second,
		WriteTimeout:    15 * time.Second,
		IdleTimeout:     60 * time.Second,
		ShutdownTimeout: 10 * time.Second,
		FreeTierRPS:     cfg.RateLimit.FreeTier,
		BasicTierRPS:    cfg.RateLimit.BasicTier,
		PremiumTierRPS:  cfg.RateLimit.PremiumTier,
	}

	server := api.NewServer(serverConfig, addressService, portfolioService, queryService, snapshotService, userRepo)

	// Start server in a goroutine
	go func() {
		if err := server.Start(); err != nil {
			logger.WithError(err).Fatal("Server failed to start")
		}
	}()

	logger.WithFields(map[string]interface{}{
		"host": cfg.Server.Host,
		"port": cfg.Server.Port,
	}).Info("Server started successfully")

	// Wait for interrupt signal to gracefully shutdown the server
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("Shutting down server...")

	// Create shutdown context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), serverConfig.ShutdownTimeout)
	defer cancel()

	// Attempt graceful shutdown
	if err := server.Shutdown(ctx); err != nil {
		logger.WithError(err).Fatal("Server forced to shutdown")
	}

	logger.Info("Server exited")
}
