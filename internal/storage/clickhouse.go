package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/address-scanner/internal/config"
)

// ClickHouseDB wraps the ClickHouse connection
type ClickHouseDB struct {
	conn driver.Conn
}

// NewClickHouseDB creates a new ClickHouse database connection
func NewClickHouseDB(cfg *config.ClickHouseConfig) (*ClickHouseDB, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%s", cfg.Host, cfg.Port)},
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.User,
			Password: cfg.Password,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout:      10 * time.Second,
		MaxOpenConns:     10,
		MaxIdleConns:     5,
		ConnMaxLifetime:  time.Hour,
		ConnOpenStrategy: clickhouse.ConnOpenInOrder,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := conn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping ClickHouse: %w", err)
	}

	return &ClickHouseDB{conn: conn}, nil
}

// Close closes the ClickHouse connection
func (db *ClickHouseDB) Close() error {
	if db.conn != nil {
		return db.conn.Close()
	}
	return nil
}

// Conn returns the underlying ClickHouse connection
func (db *ClickHouseDB) Conn() driver.Conn {
	return db.conn
}

// Ping checks if the database is reachable
func (db *ClickHouseDB) Ping(ctx context.Context) error {
	return db.conn.Ping(ctx)
}

// Exec executes a query without returning rows
func (db *ClickHouseDB) Exec(ctx context.Context, query string, args ...interface{}) error {
	return db.conn.Exec(ctx, query, args...)
}
