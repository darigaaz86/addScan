// Package main provides the snapshot worker entry point
// This worker creates daily balance snapshots at 00:00 UTC
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/address-scanner/internal/config"
	"github.com/address-scanner/internal/logging"
	"github.com/address-scanner/internal/service"
	"github.com/address-scanner/internal/storage"
)

func main() {
	fmt.Println("Balance Snapshot Worker")
	log.Println("Worker starting...")

	// Load configuration
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Initialize logging
	logLevel := logging.ParseLogLevel(cfg.Logging.Level)
	logFormat := logging.ParseLogFormat(cfg.Logging.Format)
	logging.InitGlobalLogger(logLevel, logFormat)
	logger := logging.GetGlobalLogger()

	// Connect to databases
	logger.Info("Connecting to databases...")

	postgres, err := storage.NewPostgresDB(&cfg.Database.Postgres)
	if err != nil {
		logger.WithError(err).Fatal("Failed to connect to Postgres")
	}
	defer postgres.Close()

	clickhouse, err := storage.NewClickHouseDB(&cfg.Database.ClickHouse)
	if err != nil {
		logger.WithError(err).Fatal("Failed to connect to ClickHouse")
	}
	defer clickhouse.Close()

	logger.Info("Database connections established")

	// Initialize repositories
	addressRepo := storage.NewAddressRepository(postgres)
	portfolioRepo := storage.NewPortfolioRepository(postgres)
	balanceRepo := storage.NewBalanceRepository(clickhouse)
	snapshotRepo := storage.NewBalanceSnapshotRepository(clickhouse)

	// Initialize snapshot service
	snapshotService := service.NewBalanceSnapshotService(
		snapshotRepo,
		balanceRepo,
		addressRepo,
		portfolioRepo,
	)

	// Check for one-time run mode
	if len(os.Args) > 1 && os.Args[1] == "run" {
		// Run snapshot immediately
		logger.Info("Running snapshot immediately...")
		ctx := context.Background()
		today := time.Now().UTC().Truncate(24 * time.Hour)
		if err := snapshotService.CreateDailySnapshots(ctx, today); err != nil {
			logger.WithError(err).Fatal("Failed to create snapshots")
		}
		logger.Info("Snapshot complete")
		return
	}

	// Start scheduler
	logger.Info("Starting snapshot scheduler...")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Run scheduler in background
	go runScheduler(ctx, snapshotService, logger)

	// Wait for shutdown signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("Shutting down snapshot worker...")
	cancel()
	time.Sleep(time.Second) // Give time for cleanup
	logger.Info("Worker stopped")
}

// runScheduler runs the snapshot job at 00:00 UTC daily
func runScheduler(ctx context.Context, snapshotService *service.BalanceSnapshotService, logger *logging.Logger) {
	for {
		// Calculate time until next 00:00 UTC
		now := time.Now().UTC()
		next := time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, time.UTC)
		duration := next.Sub(now)

		logger.WithFields(map[string]interface{}{
			"next_run": next.Format(time.RFC3339),
			"wait":     duration.String(),
		}).Info("Waiting for next snapshot time")

		select {
		case <-ctx.Done():
			return
		case <-time.After(duration):
			// Run snapshot
			snapshotDate := time.Now().UTC().Truncate(24 * time.Hour)
			logger.WithFields(map[string]interface{}{
				"date": snapshotDate.Format("2006-01-02"),
			}).Info("Running daily snapshot")

			if err := snapshotService.CreateDailySnapshots(ctx, snapshotDate); err != nil {
				logger.WithError(err).Error("Failed to create daily snapshots")
			} else {
				logger.Info("Daily snapshot complete")
			}
		}
	}
}
