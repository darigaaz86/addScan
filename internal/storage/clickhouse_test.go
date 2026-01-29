package storage

import (
	"testing"

	"github.com/address-scanner/internal/config"
)

func TestNewClickHouseDB(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cfg := &config.ClickHouseConfig{
		Host:     "localhost",
		Port:     "9000",
		Database: "address_scanner",
		User:     "default",
		Password: "clickhouse_dev_password",
	}

	db, err := NewClickHouseDB(cfg)
	if err != nil {
		t.Skipf("Skipping test - ClickHouse not available: %v", err)
		return
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	}()

	// Test ping
	ctx := testContext(t)
	if err := db.Ping(ctx); err != nil {
		t.Errorf("Ping() error = %v", err)
	}
}

func TestClickHouseDB_Conn(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cfg := &config.ClickHouseConfig{
		Host:     "localhost",
		Port:     "9000",
		Database: "address_scanner",
		User:     "default",
		Password: "clickhouse_dev_password",
	}

	db, err := NewClickHouseDB(cfg)
	if err != nil {
		t.Skipf("Skipping test - ClickHouse not available: %v", err)
		return
	}
	defer func() {
		_ = db.Close()
	}()

	conn := db.Conn()
	if conn == nil {
		t.Error("Conn() returned nil")
	}
}
