package adapter

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/address-scanner/internal/types"
)

// TestNodeRealFetchAllTransactions is an integration test that calls the real NodeReal API.
// Run with: go test ./internal/adapter/ -run TestNodeRealFetch -v
// Requires NODEREAL_BSC_RPC_URL env var to be set.
func TestNodeRealFetchAllTransactions(t *testing.T) {
	rpcURL := os.Getenv("NODEREAL_BSC_RPC_URL")
	if rpcURL == "" {
		t.Skip("NODEREAL_BSC_RPC_URL not set, skipping integration test")
	}

	client := NewNodeRealClient(rpcURL)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Use a tracked address - pick one likely to have BNB activity
	address := "0xa2aFEcFf0d7d0356aaA61cC73c976422cEfeFA58"

	txs, err := client.FetchAllTransactions(ctx, address, types.ChainBNB)
	if err != nil {
		t.Fatalf("FetchAllTransactions failed: %v", err)
	}

	t.Logf("Fetched %d normalized transactions", len(txs))

	// Print summary by category
	categories := map[string]int{}
	directions := map[string]int{}
	for _, tx := range txs {
		cat := "unknown"
		if tx.Category != nil {
			cat = *tx.Category
		}
		categories[cat]++
		directions[string(tx.Direction)]++
	}

	t.Logf("Categories: %v", categories)
	t.Logf("Directions: %v", directions)

	// Print first few transactions for inspection
	limit := 5
	if len(txs) < limit {
		limit = len(txs)
	}
	for i := 0; i < limit; i++ {
		tx := txs[i]
		cat := "unknown"
		if tx.Category != nil {
			cat = *tx.Category
		}
		t.Logf("  [%d] hash=%s cat=%s dir=%s value=%s from=%s to=%s internal=%v",
			i, tx.Hash[:16]+"...", cat, tx.Direction, tx.Value, tx.From[:10]+"...", tx.To[:10]+"...", tx.IsInternal)
		if tx.GasUsed != nil {
			t.Logf("       gas_used=%s gas_price=%s", *tx.GasUsed, *tx.GasPrice)
		}
		if len(tx.TokenTransfers) > 0 {
			tt := tx.TokenTransfers[0]
			t.Logf("       token=%s symbol=%v value=%s", tt.Token[:10]+"...", tt.Symbol, tt.Value)
		}
	}
}

// TestNodeRealDoRPC tests the raw RPC call to verify connectivity.
// Run with: go test ./internal/adapter/ -run TestNodeRealDoRPC -v
func TestNodeRealDoRPC(t *testing.T) {
	rpcURL := os.Getenv("NODEREAL_BSC_RPC_URL")
	if rpcURL == "" {
		t.Skip("NODEREAL_BSC_RPC_URL not set, skipping integration test")
	}

	client := NewNodeRealClient(rpcURL)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Simple connectivity test: fetch 1 page of transfers for a known address
	params := map[string]interface{}{
		"fromAddress": "0x0000000000000000000000000000000000001004",
		"category":    []string{"external"},
		"maxCount":    "0x5", // just 5
	}

	result, err := client.doRPC(ctx, "nr_getAssetTransfers", []interface{}{params})
	if err != nil {
		t.Fatalf("doRPC failed: %v", err)
	}

	t.Logf("Raw response: %s", string(result)[:min(500, len(result))])
}
