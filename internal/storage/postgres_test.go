package storage

import (
	"testing"

	"github.com/address-scanner/internal/config"
)

func TestNewPostgresDB(t *testing.T) {
	// Skip if not in integration test mode
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cfg := &config.PostgresConfig{
		Host:           "localhost",
		Port:           "5432",
		Database:       "address_scanner",
		User:           "scanner",
		Password:       "scanner_dev_password",
		MaxConnections: 10,
	}

	db, err := NewPostgresDB(cfg)
	if err != nil {
		t.Skipf("Skipping test - Postgres not available: %v", err)
		return
	}
	defer db.Close()

	// Test ping
	ctx := testContext(t)
	if err := db.Ping(ctx); err != nil {
		t.Errorf("Ping() error = %v", err)
	}
}

func TestPostgresDB_Pool(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cfg := &config.PostgresConfig{
		Host:           "localhost",
		Port:           "5432",
		Database:       "address_scanner",
		User:           "scanner",
		Password:       "scanner_dev_password",
		MaxConnections: 10,
	}

	db, err := NewPostgresDB(cfg)
	if err != nil {
		t.Skipf("Skipping test - Postgres not available: %v", err)
		return
	}
	defer db.Close()

	pool := db.Pool()
	if pool == nil {
		t.Error("Pool() returned nil")
	}
}
