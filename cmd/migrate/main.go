// Package main provides a CLI tool for running database migrations.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/address-scanner/internal/config"
	"github.com/address-scanner/internal/storage"
)

func main() {
	var (
		action = flag.String("action", "up", "Migration action: up, down, version")
		dbType = flag.String("db", "postgres", "Database type: postgres, clickhouse")
	)
	flag.Parse()

	// Load configuration
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	switch *dbType {
	case "postgres":
		if err := runPostgresMigrations(cfg, *action); err != nil {
			log.Fatalf("Postgres migration failed: %v", err)
		}
	case "clickhouse":
		if err := runClickHouseMigrations(cfg, *action); err != nil {
			log.Fatalf("ClickHouse migration failed: %v", err)
		}
	default:
		log.Fatalf("Unknown database type: %s", *dbType)
	}
}

func runPostgresMigrations(cfg *config.Config, action string) error {
	databaseURL := fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s?sslmode=disable",
		cfg.Database.Postgres.User,
		cfg.Database.Postgres.Password,
		cfg.Database.Postgres.Host,
		cfg.Database.Postgres.Port,
		cfg.Database.Postgres.Database,
	)

	migrationsPath := "migrations/postgres"

	switch action {
	case "up":
		log.Println("Running Postgres migrations...")
		if err := storage.RunMigrations(databaseURL, migrationsPath); err != nil {
			return err
		}
		log.Println("Postgres migrations completed successfully")

	case "down":
		log.Println("Rolling back Postgres migration...")
		if err := storage.RollbackMigrations(databaseURL, migrationsPath); err != nil {
			return err
		}
		log.Println("Postgres migration rolled back successfully")

	case "version":
		version, dirty, err := storage.MigrationVersion(databaseURL, migrationsPath)
		if err != nil {
			return err
		}
		log.Printf("Current Postgres migration version: %d (dirty: %v)", version, dirty)

	default:
		return fmt.Errorf("unknown action: %s", action)
	}

	return nil
}

func runClickHouseMigrations(cfg *config.Config, action string) error {
	if action != "up" {
		return fmt.Errorf("ClickHouse migrations only support 'up' action")
	}

	log.Println("Connecting to ClickHouse...")
	db, err := storage.NewClickHouseDB(&cfg.Database.ClickHouse)
	if err != nil {
		return fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Printf("Error closing ClickHouse connection: %v", err)
		}
	}()

	migrationsPath := "migrations/clickhouse"
	if _, err := os.Stat(migrationsPath); os.IsNotExist(err) {
		return fmt.Errorf("migrations directory not found: %s", migrationsPath)
	}

	log.Println("Running ClickHouse migrations...")
	if err := storage.RunClickHouseMigrations(db, migrationsPath); err != nil {
		return err
	}

	log.Println("ClickHouse migrations completed successfully")
	return nil
}
