package worker

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/address-scanner/internal/types"
)

// --- Mock chain adapter ---

type mockChainAdapter struct {
	currentBlock       uint64
	blockRangeTxs      map[string][]*types.NormalizedTransaction
	blockTxs           map[uint64][]*types.NormalizedTransaction
	fetchRangeCalls    []fetchRangeCall
	fetchBlockCalls    []uint64
	getCurrentBlockErr error
	fetchRangeErr      error
	fetchRangeErrCount int32 // how many attempts should fail before succeeding
	fetchRangeAttempts atomic.Int32
	mu                 sync.Mutex
}

type fetchRangeCall struct {
	FromBlock uint64
	ToBlock   uint64
	Addresses []string
}

func (m *mockChainAdapter) GetCurrentBlock(ctx context.Context) (uint64, error) {
	if m.getCurrentBlockErr != nil {
		return 0, m.getCurrentBlockErr
	}
	return m.currentBlock, nil
}

func (m *mockChainAdapter) FetchTransactionsForBlockRange(ctx context.Context, fromBlock, toBlock uint64, addresses []string) ([]*types.NormalizedTransaction, error) {
	m.mu.Lock()
	m.fetchRangeCalls = append(m.fetchRangeCalls, fetchRangeCall{fromBlock, toBlock, addresses})
	m.mu.Unlock()

	attempt := m.fetchRangeAttempts.Add(1)
	if m.fetchRangeErr != nil && attempt <= int32(m.fetchRangeErrCount) {
		return nil, m.fetchRangeErr
	}

	key := fmt.Sprintf("%d-%d", fromBlock, toBlock)
	if txs, ok := m.blockRangeTxs[key]; ok {
		return txs, nil
	}
	return nil, nil
}

func (m *mockChainAdapter) FetchTransactionsForBlock(ctx context.Context, blockNum uint64, addresses []string) ([]*types.NormalizedTransaction, error) {
	m.mu.Lock()
	m.fetchBlockCalls = append(m.fetchBlockCalls, blockNum)
	m.mu.Unlock()
	if txs, ok := m.blockTxs[blockNum]; ok {
		return txs, nil
	}
	return nil, nil
}

func (m *mockChainAdapter) NormalizeTransaction(rawTx interface{}) (*types.NormalizedTransaction, error) {
	return nil, nil
}
func (m *mockChainAdapter) FetchTransactions(ctx context.Context, address string, fromBlock *uint64, toBlock *uint64) ([]*types.NormalizedTransaction, error) {
	return nil, nil
}
func (m *mockChainAdapter) FetchTransactionHistory(ctx context.Context, address string, maxCount int) ([]*types.NormalizedTransaction, error) {
	return nil, nil
}
func (m *mockChainAdapter) GetBlockByTimestamp(ctx context.Context, timestamp int64) (uint64, error) {
	return 0, nil
}
func (m *mockChainAdapter) GetBalance(ctx context.Context, address string) (*types.ChainBalance, error) {
	return nil, nil
}
func (m *mockChainAdapter) ValidateAddress(address string) bool { return true }
func (m *mockChainAdapter) GetChainID() types.ChainID           { return types.ChainBase }

// --- Test helper ---

func newTestWorker(adapter *mockChainAdapter, addresses []string) *SyncWorker {
	return &SyncWorker{
		chain:                    types.ChainBase,
		chainAdapter:             adapter,
		pollInterval:             15 * time.Second,
		maxBlocksPerPoll:         30,
		maxBlocksPerBatch:        10,
		stopCh:                   make(chan struct{}),
		doneCh:                   make(chan struct{}),
		useBatchedPolling:        true,
		trackedAddressesOverride: addresses,
	}
}

// ============================================================
// PollChainBatched tests
// ============================================================

func TestPollChainBatched_NoNewBlocks(t *testing.T) {
	adapter := &mockChainAdapter{currentBlock: 100}
	w := newTestWorker(adapter, []string{"0xabc"})
	w.lastBlockProcessed = 100

	count, err := w.PollChainBatched(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 blocks, got %d", count)
	}
	if len(adapter.fetchRangeCalls) != 0 {
		t.Errorf("expected no fetch calls, got %d", len(adapter.fetchRangeCalls))
	}
}

func TestPollChainBatched_CurrentBlockBehindLastProcessed(t *testing.T) {
	adapter := &mockChainAdapter{currentBlock: 50}
	w := newTestWorker(adapter, []string{"0xabc"})
	w.lastBlockProcessed = 100

	count, err := w.PollChainBatched(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 blocks, got %d", count)
	}
}

func TestPollChainBatched_GetCurrentBlockError(t *testing.T) {
	adapter := &mockChainAdapter{getCurrentBlockErr: fmt.Errorf("RPC unavailable")}
	w := newTestWorker(adapter, nil)

	_, err := w.PollChainBatched(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if got := err.Error(); got != "failed to get current block: RPC unavailable" {
		t.Errorf("unexpected error: %s", got)
	}
}

func TestPollChainBatched_NoTrackedAddresses_StillAdvancesBlock(t *testing.T) {
	adapter := &mockChainAdapter{currentBlock: 120}
	w := newTestWorker(adapter, []string{}) // empty address list
	w.lastBlockProcessed = 100

	count, err := w.PollChainBatched(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 20 {
		t.Errorf("expected 20 blocks, got %d", count)
	}
	// Block progress should advance even with no addresses
	if w.lastBlockProcessed != 120 {
		t.Errorf("expected lastBlockProcessed=120, got %d", w.lastBlockProcessed)
	}
	// No fetch calls should be made
	if len(adapter.fetchRangeCalls) != 0 {
		t.Errorf("expected no fetch calls with empty addresses, got %d", len(adapter.fetchRangeCalls))
	}
}

func TestPollChainBatched_BatchSplitting(t *testing.T) {
	adapter := &mockChainAdapter{
		currentBlock:  125,
		blockRangeTxs: make(map[string][]*types.NormalizedTransaction),
	}
	w := newTestWorker(adapter, []string{"0xabc", "0xdef"})
	w.lastBlockProcessed = 100
	w.maxBlocksPerPoll = 25
	w.maxBlocksPerBatch = 10

	count, err := w.PollChainBatched(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 25 {
		t.Errorf("expected 25 blocks, got %d", count)
	}

	// 25 blocks with batch size 10 = 3 batches: [101-110], [111-120], [121-125]
	if len(adapter.fetchRangeCalls) != 3 {
		t.Fatalf("expected 3 batch calls, got %d", len(adapter.fetchRangeCalls))
	}

	expected := []fetchRangeCall{
		{101, 110, []string{"0xabc", "0xdef"}},
		{111, 120, []string{"0xabc", "0xdef"}},
		{121, 125, []string{"0xabc", "0xdef"}},
	}
	for i, exp := range expected {
		got := adapter.fetchRangeCalls[i]
		if got.FromBlock != exp.FromBlock || got.ToBlock != exp.ToBlock {
			t.Errorf("batch %d: expected [%d-%d], got [%d-%d]", i, exp.FromBlock, exp.ToBlock, got.FromBlock, got.ToBlock)
		}
		if len(got.Addresses) != len(exp.Addresses) {
			t.Errorf("batch %d: expected %d addresses, got %d", i, len(exp.Addresses), len(got.Addresses))
		}
	}

	if w.lastBlockProcessed != 125 {
		t.Errorf("expected lastBlockProcessed=125, got %d", w.lastBlockProcessed)
	}
}

func TestPollChainBatched_MaxBlocksPerPollCap(t *testing.T) {
	adapter := &mockChainAdapter{
		currentBlock:  1000,
		blockRangeTxs: make(map[string][]*types.NormalizedTransaction),
	}
	w := newTestWorker(adapter, []string{"0xabc"})
	w.lastBlockProcessed = 900
	w.maxBlocksPerPoll = 30
	w.maxBlocksPerBatch = 10

	count, err := w.PollChainBatched(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Should cap at 30 blocks even though 100 behind
	if count != 30 {
		t.Errorf("expected 30 blocks (capped), got %d", count)
	}
	// Should process blocks 901-930, not 901-1000
	if w.lastBlockProcessed != 930 {
		t.Errorf("expected lastBlockProcessed=930, got %d", w.lastBlockProcessed)
	}
	// 30 blocks / 10 per batch = 3 batches
	if len(adapter.fetchRangeCalls) != 3 {
		t.Errorf("expected 3 batch calls, got %d", len(adapter.fetchRangeCalls))
	}
}

func TestPollChainBatched_TransactionsFoundAndProcessed(t *testing.T) {
	tx1 := &types.NormalizedTransaction{
		Hash:        "0xhash1",
		Chain:       types.ChainBase,
		From:        "0xabc",
		To:          "0xdef",
		Value:       "1000000000000000000",
		BlockNumber: 105,
		Timestamp:   time.Now().Unix(),
		Status:      types.StatusSuccess,
		Direction:   types.DirectionOut,
	}
	tx2 := &types.NormalizedTransaction{
		Hash:        "0xhash2",
		Chain:       types.ChainBase,
		From:        "0xother",
		To:          "0xabc",
		Value:       "500000000000000000",
		BlockNumber: 108,
		Timestamp:   time.Now().Unix(),
		Status:      types.StatusSuccess,
		Direction:   types.DirectionIn,
	}

	adapter := &mockChainAdapter{
		currentBlock: 110,
		blockRangeTxs: map[string][]*types.NormalizedTransaction{
			"101-110": {tx1, tx2},
		},
	}
	w := newTestWorker(adapter, []string{"0xabc"})
	w.lastBlockProcessed = 100
	w.maxBlocksPerPoll = 30
	w.maxBlocksPerBatch = 10

	count, err := w.PollChainBatched(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 10 {
		t.Errorf("expected 10 blocks, got %d", count)
	}
	if w.lastBlockProcessed != 110 {
		t.Errorf("expected lastBlockProcessed=110, got %d", w.lastBlockProcessed)
	}
	// Single batch for 10 blocks
	if len(adapter.fetchRangeCalls) != 1 {
		t.Errorf("expected 1 batch call, got %d", len(adapter.fetchRangeCalls))
	}
}

func TestPollChainBatched_TransientErrorRetry(t *testing.T) {
	adapter := &mockChainAdapter{
		currentBlock:       105,
		blockRangeTxs:      make(map[string][]*types.NormalizedTransaction),
		fetchRangeErr:      fmt.Errorf("503 service unavailable"),
		fetchRangeErrCount: 1, // fail first attempt, succeed on retry
	}
	w := newTestWorker(adapter, []string{"0xabc"})
	w.lastBlockProcessed = 100
	w.maxBlocksPerPoll = 5
	w.maxBlocksPerBatch = 5

	count, err := w.PollChainBatched(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 5 {
		t.Errorf("expected 5 blocks, got %d", count)
	}
	// Should have retried: 2 calls total (1 failed + 1 success)
	if len(adapter.fetchRangeCalls) != 2 {
		t.Errorf("expected 2 fetch calls (1 retry), got %d", len(adapter.fetchRangeCalls))
	}
}

func TestPollChainBatched_PermanentErrorContinues(t *testing.T) {
	adapter := &mockChainAdapter{
		currentBlock:       115,
		blockRangeTxs:      make(map[string][]*types.NormalizedTransaction),
		fetchRangeErr:      fmt.Errorf("invalid params"), // non-transient
		fetchRangeErrCount: 100,                          // always fail
	}
	w := newTestWorker(adapter, []string{"0xabc"})
	w.lastBlockProcessed = 100
	w.maxBlocksPerPoll = 15
	w.maxBlocksPerBatch = 5

	count, err := w.PollChainBatched(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Should still report blocks as processed (progress advances)
	if count != 15 {
		t.Errorf("expected 15 blocks, got %d", count)
	}
	// Block progress should still advance (batched polling continues past errors)
	if w.lastBlockProcessed != 115 {
		t.Errorf("expected lastBlockProcessed=115, got %d", w.lastBlockProcessed)
	}
}

func TestPollChainBatched_ContextCancellation(t *testing.T) {
	adapter := &mockChainAdapter{
		currentBlock:       200,
		blockRangeTxs:      make(map[string][]*types.NormalizedTransaction),
		fetchRangeErr:      fmt.Errorf("timeout"),
		fetchRangeErrCount: 100,
	}
	w := newTestWorker(adapter, []string{"0xabc"})
	w.lastBlockProcessed = 100
	w.maxBlocksPerPoll = 100
	w.maxBlocksPerBatch = 10

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	_, err := w.PollChainBatched(ctx)
	if err == nil {
		t.Fatal("expected context cancellation error")
	}
}

// ============================================================
// PollChain (non-batched) tests
// ============================================================

func TestPollChain_NoNewBlocks(t *testing.T) {
	adapter := &mockChainAdapter{currentBlock: 50}
	w := newTestWorker(adapter, []string{"0xabc"})
	w.lastBlockProcessed = 50

	count, err := w.PollChain(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 blocks, got %d", count)
	}
}

func TestPollChain_GetCurrentBlockError(t *testing.T) {
	adapter := &mockChainAdapter{getCurrentBlockErr: fmt.Errorf("connection refused")}
	w := newTestWorker(adapter, nil)

	_, err := w.PollChain(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestPollChain_ProcessesBlocksSequentially(t *testing.T) {
	tx := &types.NormalizedTransaction{
		Hash:        "0xhash1",
		Chain:       types.ChainBase,
		From:        "0xabc",
		To:          "0xdef",
		Value:       "1000",
		BlockNumber: 51,
		Timestamp:   time.Now().Unix(),
		Status:      types.StatusSuccess,
		Direction:   types.DirectionOut,
	}
	adapter := &mockChainAdapter{
		currentBlock: 53,
		blockTxs: map[uint64][]*types.NormalizedTransaction{
			51: {tx},
		},
	}
	w := newTestWorker(adapter, []string{"0xabc"})
	w.lastBlockProcessed = 50

	count, err := w.PollChain(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 3 {
		t.Errorf("expected 3 blocks, got %d", count)
	}
	// Should have called FetchTransactionsForBlock for each block
	if len(adapter.fetchBlockCalls) != 3 {
		t.Fatalf("expected 3 block calls, got %d", len(adapter.fetchBlockCalls))
	}
	for i, expected := range []uint64{51, 52, 53} {
		if adapter.fetchBlockCalls[i] != expected {
			t.Errorf("call %d: expected block %d, got %d", i, expected, adapter.fetchBlockCalls[i])
		}
	}
	if w.lastBlockProcessed != 53 {
		t.Errorf("expected lastBlockProcessed=53, got %d", w.lastBlockProcessed)
	}
}

func TestPollChain_MaxBlocksPerPollCap(t *testing.T) {
	adapter := &mockChainAdapter{
		currentBlock: 200,
		blockTxs:     make(map[uint64][]*types.NormalizedTransaction),
	}
	w := newTestWorker(adapter, []string{"0xabc"})
	w.lastBlockProcessed = 100
	w.maxBlocksPerPoll = 10

	count, err := w.PollChain(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if count != 10 {
		t.Errorf("expected 10 blocks (capped), got %d", count)
	}
	if w.lastBlockProcessed != 110 {
		t.Errorf("expected lastBlockProcessed=110, got %d", w.lastBlockProcessed)
	}
}

// ============================================================
// pollLoop tests
// ============================================================

func TestPollLoop_StopsOnSignal(t *testing.T) {
	adapter := &mockChainAdapter{currentBlock: 100}
	w := newTestWorker(adapter, []string{})
	w.lastBlockProcessed = 100
	w.pollInterval = 100 * time.Millisecond

	ctx := context.Background()
	go w.pollLoop(ctx)

	// Let it tick once
	time.Sleep(150 * time.Millisecond)

	// Send stop signal
	close(w.stopCh)

	// Wait for pollLoop to exit
	select {
	case <-w.doneCh:
		// success
	case <-time.After(2 * time.Second):
		t.Fatal("pollLoop did not stop within timeout")
	}
}

func TestPollLoop_StopsOnContextCancel(t *testing.T) {
	adapter := &mockChainAdapter{currentBlock: 100}
	w := newTestWorker(adapter, []string{})
	w.lastBlockProcessed = 100
	w.pollInterval = 100 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	go w.pollLoop(ctx)

	time.Sleep(150 * time.Millisecond)
	cancel()

	select {
	case <-w.doneCh:
		// success
	case <-time.After(2 * time.Second):
		t.Fatal("pollLoop did not stop on context cancel")
	}
}

func TestPollLoop_UsesBatchedPollingWhenEnabled(t *testing.T) {
	adapter := &mockChainAdapter{
		currentBlock:  105,
		blockRangeTxs: make(map[string][]*types.NormalizedTransaction),
	}
	w := newTestWorker(adapter, []string{"0xabc"})
	w.lastBlockProcessed = 100
	w.useBatchedPolling = true
	w.pollInterval = 100 * time.Millisecond
	w.maxBlocksPerBatch = 10

	ctx, cancel := context.WithCancel(context.Background())
	go w.pollLoop(ctx)

	// Wait for at least one poll cycle
	time.Sleep(250 * time.Millisecond)
	cancel()

	<-w.doneCh

	// Should have used FetchTransactionsForBlockRange (batched), not FetchTransactionsForBlock
	adapter.mu.Lock()
	rangeCalls := len(adapter.fetchRangeCalls)
	blockCalls := len(adapter.fetchBlockCalls)
	adapter.mu.Unlock()

	if rangeCalls == 0 {
		t.Error("expected batched polling to call FetchTransactionsForBlockRange, got 0 calls")
	}
	if blockCalls != 0 {
		t.Errorf("expected no per-block calls in batched mode, got %d", blockCalls)
	}
}

func TestPollLoop_UsesPerBlockPollingWhenBatchDisabled(t *testing.T) {
	adapter := &mockChainAdapter{
		currentBlock: 103,
		blockTxs:     make(map[uint64][]*types.NormalizedTransaction),
	}
	w := newTestWorker(adapter, []string{"0xabc"})
	w.lastBlockProcessed = 100
	w.useBatchedPolling = false
	w.pollInterval = 100 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	go w.pollLoop(ctx)

	time.Sleep(250 * time.Millisecond)
	cancel()

	<-w.doneCh

	adapter.mu.Lock()
	rangeCalls := len(adapter.fetchRangeCalls)
	blockCalls := len(adapter.fetchBlockCalls)
	adapter.mu.Unlock()

	if blockCalls == 0 {
		t.Error("expected per-block polling to call FetchTransactionsForBlock, got 0 calls")
	}
	if rangeCalls != 0 {
		t.Errorf("expected no range calls in per-block mode, got %d", rangeCalls)
	}
}
