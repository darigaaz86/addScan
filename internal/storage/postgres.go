// Package storage provides database connection and repository implementations.
package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/address-scanner/internal/config"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresDB wraps the pgxpool connection
type PostgresDB struct {
	pool *pgxpool.Pool
}

// NewPostgresDB creates a new Postgres database connection
func NewPostgresDB(cfg *config.PostgresConfig) (*PostgresDB, error) {
	connString := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=disable pool_max_conns=%d",
		cfg.Host,
		cfg.Port,
		cfg.User,
		cfg.Password,
		cfg.Database,
		cfg.MaxConnections,
	)

	poolConfig, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("unable to parse connection string: %w", err)
	}

	// Configure connection pool
	poolConfig.MaxConns = int32(cfg.MaxConnections) // #nosec G115 - MaxConnections is validated in config
	poolConfig.MinConns = 5
	poolConfig.MaxConnLifetime = time.Hour
	poolConfig.MaxConnIdleTime = 30 * time.Minute
	poolConfig.HealthCheckPeriod = time.Minute

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("unable to create connection pool: %w", err)
	}

	// Test connection
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("unable to ping database: %w", err)
	}

	return &PostgresDB{pool: pool}, nil
}

// Close closes the database connection pool
func (db *PostgresDB) Close() {
	if db.pool != nil {
		db.pool.Close()
	}
}

// Pool returns the underlying connection pool
func (db *PostgresDB) Pool() *pgxpool.Pool {
	return db.pool
}

// Ping checks if the database is reachable
func (db *PostgresDB) Ping(ctx context.Context) error {
	return db.pool.Ping(ctx)
}
