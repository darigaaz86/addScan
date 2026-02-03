package storage

import (
	"context"
	"fmt"
	"math/big"
	"time"
)

// GoldskyTrace represents a trace from Goldsky
type GoldskyTrace struct {
	ID              string
	TransactionHash string
	BlockNumber     uint64
	BlockTimestamp  time.Time
	FromAddress     string
	ToAddress       string
	Value           *big.Int
	CallType        string
	GasUsed         uint64
	Status          uint8
	Chain           string
}

// GoldskyLog represents a log from Goldsky
type GoldskyLog struct {
	ID              string
	TransactionHash string
	BlockNumber     uint64
	BlockTimestamp  time.Time
	ContractAddress string
	EventSignature  string
	FromAddress     string
	ToAddress       string
	Topics          string
	Data            string
	LogIndex        uint32
	Chain           string
}

// GoldskyRepository handles Goldsky data storage
type GoldskyRepository struct {
	db *ClickHouseDB
}

// NewGoldskyRepository creates a new Goldsky repository
func NewGoldskyRepository(db *ClickHouseDB) *GoldskyRepository {
	return &GoldskyRepository{db: db}
}

// InsertTraces inserts multiple traces into ClickHouse
func (r *GoldskyRepository) InsertTraces(ctx context.Context, traces []GoldskyTrace) error {
	if len(traces) == 0 {
		return nil
	}

	batch, err := r.db.conn.PrepareBatch(ctx, `
		INSERT INTO goldsky_traces (
			id, transaction_hash, block_number, block_timestamp,
			from_address, to_address, value, call_type, gas_used, status, chain
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	for _, t := range traces {
		// Convert big.Int to string for Decimal column
		valueStr := "0"
		if t.Value != nil {
			valueStr = t.Value.String()
		}

		chain := t.Chain
		if chain == "" {
			chain = "ethereum"
		}

		err := batch.Append(
			t.ID,
			t.TransactionHash,
			t.BlockNumber,
			t.BlockTimestamp,
			t.FromAddress,
			t.ToAddress,
			valueStr,
			t.CallType,
			t.GasUsed,
			t.Status,
			chain,
		)
		if err != nil {
			return fmt.Errorf("failed to append trace: %w", err)
		}
	}

	return batch.Send()
}

// InsertLogs inserts multiple logs into ClickHouse
func (r *GoldskyRepository) InsertLogs(ctx context.Context, logs []GoldskyLog) error {
	if len(logs) == 0 {
		return nil
	}

	batch, err := r.db.conn.PrepareBatch(ctx, `
		INSERT INTO goldsky_logs (
			id, transaction_hash, block_number, block_timestamp,
			contract_address, event_signature, from_address, to_address,
			topics, data, log_index, chain
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	for _, l := range logs {
		chain := l.Chain
		if chain == "" {
			chain = "ethereum"
		}

		err := batch.Append(
			l.ID,
			l.TransactionHash,
			l.BlockNumber,
			l.BlockTimestamp,
			l.ContractAddress,
			l.EventSignature,
			l.FromAddress,
			l.ToAddress,
			l.Topics,
			l.Data,
			l.LogIndex,
			chain,
		)
		if err != nil {
			return fmt.Errorf("failed to append log: %w", err)
		}
	}

	return batch.Send()
}

// GetTracesByAddress returns traces for a given address
func (r *GoldskyRepository) GetTracesByAddress(ctx context.Context, address string, limit int) ([]GoldskyTrace, error) {
	query := `
		SELECT id, transaction_hash, block_number, block_timestamp,
			   from_address, to_address, value, call_type, gas_used, status
		FROM goldsky_traces
		WHERE lower(from_address) = lower(?) OR lower(to_address) = lower(?)
		ORDER BY block_number DESC
		LIMIT ?
	`

	rows, err := r.db.conn.Query(ctx, query, address, address, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var traces []GoldskyTrace
	for rows.Next() {
		var t GoldskyTrace
		var value big.Int
		if err := rows.Scan(
			&t.ID, &t.TransactionHash, &t.BlockNumber, &t.BlockTimestamp,
			&t.FromAddress, &t.ToAddress, &value, &t.CallType, &t.GasUsed, &t.Status,
		); err != nil {
			return nil, err
		}
		t.Value = &value
		traces = append(traces, t)
	}
	return traces, nil
}

// GetLogsByAddress returns logs for a given address
func (r *GoldskyRepository) GetLogsByAddress(ctx context.Context, address string, limit int) ([]GoldskyLog, error) {
	query := `
		SELECT id, transaction_hash, block_number, block_timestamp,
			   contract_address, event_signature, from_address, to_address,
			   topics, data, log_index
		FROM goldsky_logs
		WHERE lower(contract_address) = lower(?) 
		   OR lower(from_address) = lower(?) 
		   OR lower(to_address) = lower(?)
		ORDER BY block_number DESC
		LIMIT ?
	`

	rows, err := r.db.conn.Query(ctx, query, address, address, address, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var logs []GoldskyLog
	for rows.Next() {
		var l GoldskyLog
		if err := rows.Scan(
			&l.ID, &l.TransactionHash, &l.BlockNumber, &l.BlockTimestamp,
			&l.ContractAddress, &l.EventSignature, &l.FromAddress, &l.ToAddress,
			&l.Topics, &l.Data, &l.LogIndex,
		); err != nil {
			return nil, err
		}
		logs = append(logs, l)
	}
	return logs, nil
}
