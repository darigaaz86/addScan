// Package api provides the HTTP API server implementation.
package api

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"math/big"
	"net/http"
	"time"

	"github.com/address-scanner/internal/storage"
)

// GoldskyTraceJSON represents a trace record from Goldsky webhook
type GoldskyTraceJSON struct {
	ID              string `json:"id"`
	TransactionHash string `json:"transaction_hash"`
	BlockNumber     int64  `json:"block_number"`
	BlockTimestamp  int64  `json:"block_timestamp"`
	FromAddress     string `json:"from_address"`
	ToAddress       string `json:"to_address"`
	Value           string `json:"value"`
	CallType        string `json:"call_type"`
	GasUsed         int64  `json:"gas_used"`
	Status          int    `json:"status"`
	Chain           string `json:"chain"`
	GsOp            string `json:"_gs_op"` // i=insert, u=update, d=delete
}

// GoldskyLogJSON represents a log record from Goldsky webhook
type GoldskyLogJSON struct {
	ID              string `json:"id"`
	TransactionHash string `json:"transaction_hash"`
	BlockNumber     int64  `json:"block_number"`
	BlockTimestamp  int64  `json:"block_timestamp"`
	ContractAddress string `json:"contract_address"`
	EventSignature  string `json:"event_signature"`
	FromAddress     string `json:"from_address"`
	ToAddress       string `json:"to_address"`
	Topics          string `json:"topics"`
	Data            string `json:"data"`
	LogIndex        int    `json:"log_index"`
	Chain           string `json:"chain"`
	GsOp            string `json:"_gs_op"`
}

// handleGoldskyTraces handles incoming trace data from Goldsky webhook
func (s *Server) handleGoldskyTraces(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("Error reading Goldsky traces body: %v", err)
		http.Error(w, "Failed to read body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var tracesJSON []GoldskyTraceJSON

	if err := json.Unmarshal(body, &tracesJSON); err != nil {
		var single GoldskyTraceJSON
		if err := json.Unmarshal(body, &single); err != nil {
			log.Printf("Error parsing Goldsky traces: %v, body: %s", err, string(body))
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}
		tracesJSON = []GoldskyTraceJSON{single}
	}

	// Convert to storage format
	traces := make([]storage.GoldskyTrace, 0, len(tracesJSON))
	for _, t := range tracesJSON {
		if t.GsOp == "d" {
			continue // Skip deletes for now
		}

		value := big.NewInt(0)
		if t.Value != "" {
			value.SetString(t.Value, 10)
		}

		traces = append(traces, storage.GoldskyTrace{
			ID:              t.ID,
			TransactionHash: t.TransactionHash,
			BlockNumber:     uint64(t.BlockNumber),
			BlockTimestamp:  time.Unix(t.BlockTimestamp, 0),
			FromAddress:     t.FromAddress,
			ToAddress:       t.ToAddress,
			Value:           value,
			CallType:        t.CallType,
			GasUsed:         uint64(t.GasUsed),
			Status:          uint8(t.Status),
			Chain:           t.Chain,
		})
	}

	log.Printf("Received %d traces from Goldsky", len(traces))

	// Store in ClickHouse if repository is available
	if s.goldskyRepo != nil && len(traces) > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := s.goldskyRepo.InsertTraces(ctx, traces); err != nil {
			log.Printf("Error storing traces: %v", err)
			// Don't fail the request - Goldsky will retry
		} else {
			log.Printf("Stored %d traces in ClickHouse", len(traces))
		}
	}

	w.WriteHeader(http.StatusOK)
}

// handleGoldskyLogs handles incoming log data from Goldsky webhook
func (s *Server) handleGoldskyLogs(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("Error reading Goldsky logs body: %v", err)
		http.Error(w, "Failed to read body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var logsJSON []GoldskyLogJSON

	if err := json.Unmarshal(body, &logsJSON); err != nil {
		var single GoldskyLogJSON
		if err := json.Unmarshal(body, &single); err != nil {
			log.Printf("Error parsing Goldsky logs: %v, body: %s", err, string(body))
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}
		logsJSON = []GoldskyLogJSON{single}
	}

	// Convert to storage format
	logs := make([]storage.GoldskyLog, 0, len(logsJSON))
	for _, l := range logsJSON {
		if l.GsOp == "d" {
			continue
		}

		logs = append(logs, storage.GoldskyLog{
			ID:              l.ID,
			TransactionHash: l.TransactionHash,
			BlockNumber:     uint64(l.BlockNumber),
			BlockTimestamp:  time.Unix(l.BlockTimestamp, 0),
			ContractAddress: l.ContractAddress,
			EventSignature:  l.EventSignature,
			FromAddress:     l.FromAddress,
			ToAddress:       l.ToAddress,
			Topics:          l.Topics,
			Data:            l.Data,
			LogIndex:        uint32(l.LogIndex),
			Chain:           l.Chain,
		})
	}

	log.Printf("Received %d logs from Goldsky", len(logs))

	if s.goldskyRepo != nil && len(logs) > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := s.goldskyRepo.InsertLogs(ctx, logs); err != nil {
			log.Printf("Error storing logs: %v", err)
		} else {
			log.Printf("Stored %d logs in ClickHouse", len(logs))
		}
	}

	w.WriteHeader(http.StatusOK)
}
