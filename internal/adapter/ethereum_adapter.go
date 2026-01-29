package adapter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/big"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/address-scanner/internal/ratelimit"
	"github.com/address-scanner/internal/types"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

// EthereumAdapter implements ChainAdapter for Ethereum and EVM-compatible chains
type EthereumAdapter struct {
	chainID         types.ChainID
	client          *ethclient.Client
	rateLimitClient *ratelimit.RateLimitedClient // Optional rate-limited client
	provider        DataProvider
	priority        ratelimit.Priority // Priority level for rate limiting
}

// EthereumAdapterConfig holds configuration for creating an EthereumAdapter with rate limiting.
type EthereumAdapterConfig struct {
	// ChainID is the chain identifier. Required.
	ChainID types.ChainID

	// Provider is the data provider for RPC URLs. Required.
	Provider DataProvider

	// RateLimiter is the optional rate-limited client.
	// If nil, the adapter operates without rate limiting (backward compatible).
	RateLimiter *ratelimit.RateLimitedClient

	// Priority is the priority level for rate-limited requests.
	// Only used when RateLimiter is provided.
	// Default: PriorityHigh (for real-time sync)
	Priority ratelimit.Priority
}

// NewEthereumAdapter creates a new Ethereum chain adapter without rate limiting.
// This constructor maintains backward compatibility with existing code.
func NewEthereumAdapter(chainID types.ChainID, provider DataProvider) (*EthereumAdapter, error) {
	if provider == nil {
		return nil, fmt.Errorf("provider cannot be nil")
	}

	// Get primary RPC URL from provider
	rpcURL, err := provider.GetPrimaryURL()
	if err != nil {
		return nil, fmt.Errorf("failed to get primary RPC URL: %w", err)
	}

	// Create eth client
	client, err := ethclient.Dial(rpcURL)
	if err != nil {
		return nil, NewAdapterError(chainID, "NewEthereumAdapter", err, map[string]interface{}{
			"rpcURL": rpcURL,
		})
	}

	return &EthereumAdapter{
		chainID:  chainID,
		client:   client,
		provider: provider,
		priority: ratelimit.PriorityHigh, // Default to high priority
	}, nil
}

// NewEthereumAdapterWithRateLimiter creates a new Ethereum chain adapter with rate limiting.
// This constructor allows full configuration including rate limiting support.
// Requirements: 5.1, 5.5
func NewEthereumAdapterWithRateLimiter(cfg *EthereumAdapterConfig) (*EthereumAdapter, error) {
	if cfg == nil {
		return nil, fmt.Errorf("configuration cannot be nil")
	}

	if cfg.Provider == nil {
		return nil, fmt.Errorf("provider cannot be nil")
	}

	// Get primary RPC URL from provider
	rpcURL, err := cfg.Provider.GetPrimaryURL()
	if err != nil {
		return nil, fmt.Errorf("failed to get primary RPC URL: %w", err)
	}

	// Create eth client
	client, err := ethclient.Dial(rpcURL)
	if err != nil {
		return nil, NewAdapterError(cfg.ChainID, "NewEthereumAdapterWithRateLimiter", err, map[string]interface{}{
			"rpcURL": rpcURL,
		})
	}

	adapter := &EthereumAdapter{
		chainID:         cfg.ChainID,
		client:          client,
		rateLimitClient: cfg.RateLimiter,
		provider:        cfg.Provider,
		priority:        cfg.Priority,
	}

	log.Printf("[Adapter:%s] Created adapter with rate limiting enabled=%v, priority=%s",
		cfg.ChainID, cfg.RateLimiter != nil, cfg.Priority)

	return adapter, nil
}

// SetRateLimiter sets or updates the rate limiter for this adapter.
// This allows enabling rate limiting on an existing adapter.
// Pass nil to disable rate limiting.
func (a *EthereumAdapter) SetRateLimiter(rateLimiter *ratelimit.RateLimitedClient) {
	a.rateLimitClient = rateLimiter
	log.Printf("[Adapter:%s] Rate limiter updated, enabled=%v", a.chainID, rateLimiter != nil)
}

// SetPriority sets the priority level for rate-limited requests.
// This allows changing priority based on caller context.
// Requirements: 5.5
func (a *EthereumAdapter) SetPriority(priority ratelimit.Priority) {
	a.priority = priority
	log.Printf("[Adapter:%s] Priority set to %s", a.chainID, priority)
}

// GetPriority returns the current priority level for rate-limited requests.
func (a *EthereumAdapter) GetPriority() ratelimit.Priority {
	return a.priority
}

// IsRateLimited returns true if rate limiting is enabled for this adapter.
func (a *EthereumAdapter) IsRateLimited() bool {
	return a.rateLimitClient != nil
}

// GetUnderlyingClient returns the underlying ethclient.Client.
// This is useful for creating rate-limited wrappers around the client.
// Note: The returned client implements the ratelimit.EthClient interface.
func (a *EthereumAdapter) GetUnderlyingClient() *ethclient.Client {
	return a.client
}

// NormalizeTransaction converts Ethereum transaction to common format
func (a *EthereumAdapter) NormalizeTransaction(rawTx interface{}) (*types.NormalizedTransaction, error) {
	// Type assert to Ethereum transaction
	ethTx, ok := rawTx.(*ethtypes.Transaction)
	if !ok {
		return nil, NewAdapterError(a.chainID, "NormalizeTransaction", ErrInvalidTransaction, map[string]interface{}{
			"expectedType": "*ethtypes.Transaction",
			"actualType":   fmt.Sprintf("%T", rawTx),
		})
	}

	// Extract basic transaction data
	normalized := &types.NormalizedTransaction{
		Hash:        ethTx.Hash().Hex(),
		Chain:       a.chainID,
		Value:       ethTx.Value().String(),
		BlockNumber: 0, // Will be set from receipt
		Status:      types.StatusSuccess,
	}

	// Extract from address
	msg, err := ethtypes.Sender(ethtypes.LatestSignerForChainID(ethTx.ChainId()), ethTx)
	if err != nil {
		return nil, NewAdapterError(a.chainID, "NormalizeTransaction", err, map[string]interface{}{
			"txHash": ethTx.Hash().Hex(),
			"error":  "failed to extract sender",
		})
	}
	normalized.From = msg.Hex()

	// Extract to address
	if ethTx.To() != nil {
		normalized.To = ethTx.To().Hex()
	} else {
		// Contract creation transaction
		normalized.To = ""
	}

	// Extract gas data
	gasPrice := ethTx.GasPrice().String()
	normalized.GasPrice = &gasPrice

	// Extract method ID from input data
	if len(ethTx.Data()) >= 4 {
		methodID := fmt.Sprintf("0x%x", ethTx.Data()[:4])
		normalized.MethodID = &methodID
	}

	// Store full input data
	if len(ethTx.Data()) > 0 {
		input := fmt.Sprintf("0x%x", ethTx.Data())
		normalized.Input = &input
	}

	return normalized, nil
}

// NormalizeTransactionWithReceipt normalizes a transaction with its receipt
// This provides complete information including status, gas used, and logs
func (a *EthereumAdapter) NormalizeTransactionWithReceipt(ethTx *ethtypes.Transaction, receipt *ethtypes.Receipt, block *ethtypes.Block) (*types.NormalizedTransaction, error) {
	// Start with basic normalization
	normalized, err := a.NormalizeTransaction(ethTx)
	if err != nil {
		return nil, err
	}

	// Add receipt data
	normalized.BlockNumber = receipt.BlockNumber.Uint64()
	normalized.Timestamp = int64(block.Time())

	// Set transaction status
	if receipt.Status == ethtypes.ReceiptStatusSuccessful {
		normalized.Status = types.StatusSuccess
	} else {
		normalized.Status = types.StatusFailed
	}

	// Add gas used
	gasUsed := receipt.GasUsed
	gasUsedStr := fmt.Sprintf("%d", gasUsed)
	normalized.GasUsed = &gasUsedStr

	// Set native asset for the chain
	nativeAsset := GetNativeAsset(a.chainID)
	normalized.Asset = &nativeAsset

	// Set category based on transaction type
	category := "external"
	if normalized.MethodID != nil {
		switch *normalized.MethodID {
		case "0x095ea7b3":
			category = "approve"
		case "0xa9059cbb":
			category = "transfer"
		default:
			if len(ethTx.Data()) > 0 {
				category = "contract_call"
			}
		}
	} else if ethTx.To() == nil {
		category = "contract_creation"
	}
	normalized.Category = &category

	log.Printf("[Adapter:%s] Normalized tx %s: asset=%s, category=%s, methodID=%v",
		a.chainID, normalized.Hash, nativeAsset, category, normalized.MethodID)

	// Parse token transfers from logs
	tokenTransfers := a.parseTokenTransfers(receipt.Logs)
	if len(tokenTransfers) > 0 {
		normalized.TokenTransfers = tokenTransfers
	}

	return normalized, nil
}

// parseTokenTransfers extracts ERC20 token transfers from transaction logs
func (a *EthereumAdapter) parseTokenTransfers(logs []*ethtypes.Log) []types.TokenTransfer {
	// ERC20 Transfer event signature: Transfer(address,address,uint256)
	transferEventSignature := common.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")

	var transfers []types.TokenTransfer

	for _, log := range logs {
		// Check if this is a Transfer event
		if len(log.Topics) < 3 || log.Topics[0] != transferEventSignature {
			continue
		}

		// Extract from and to addresses from topics
		from := common.BytesToAddress(log.Topics[1].Bytes()).Hex()
		to := common.BytesToAddress(log.Topics[2].Bytes()).Hex()

		// Extract value from data
		value := new(big.Int).SetBytes(log.Data)

		transfers = append(transfers, types.TokenTransfer{
			Token: log.Address.Hex(),
			From:  from,
			To:    to,
			Value: value.String(),
		})
	}

	return transfers
}

// FetchTransactions retrieves transactions for an address from data provider
func (a *EthereumAdapter) FetchTransactions(ctx context.Context, address string, fromBlock *uint64, toBlock *uint64) ([]*types.NormalizedTransaction, error) {
	if !a.ValidateAddress(address) {
		return nil, NewAdapterError(a.chainID, "FetchTransactions", ErrInvalidAddress, map[string]interface{}{
			"address": address,
		})
	}

	// Convert address to common.Address
	addr := common.HexToAddress(address)

	// Build filter query
	query := ethereum.FilterQuery{
		Addresses: []common.Address{addr},
	}

	if fromBlock != nil {
		query.FromBlock = new(big.Int).SetUint64(*fromBlock)
	}

	if toBlock != nil {
		query.ToBlock = new(big.Int).SetUint64(*toBlock)
	}

	// Fetch logs using eth_getLogs (with rate limiting if available)
	var logs []ethtypes.Log
	var err error
	if a.rateLimitClient != nil {
		logs, err = a.rateLimitClient.FilterLogs(ctx, query)
	} else {
		logs, err = a.client.FilterLogs(ctx, query)
	}
	if err != nil {
		// Check if we should failover to secondary provider
		if a.shouldFailover(err) {
			return a.fetchTransactionsWithFailover(ctx, address, fromBlock, toBlock)
		}
		return nil, NewAdapterError(a.chainID, "FetchTransactions", err, map[string]interface{}{
			"address":   address,
			"fromBlock": fromBlock,
			"toBlock":   toBlock,
		})
	}

	// Extract unique transaction hashes from logs
	txHashes := make(map[common.Hash]bool)
	for _, log := range logs {
		txHashes[log.TxHash] = true
	}

	// Fetch full transaction details for each hash
	var transactions []*types.NormalizedTransaction
	for txHash := range txHashes {
		var tx *ethtypes.Transaction
		var isPending bool
		if a.rateLimitClient != nil {
			tx, isPending, err = a.rateLimitClient.TransactionByHash(ctx, txHash)
		} else {
			tx, isPending, err = a.client.TransactionByHash(ctx, txHash)
		}
		if err != nil {
			// Log error but continue with other transactions
			continue
		}
		if isPending {
			// Skip pending transactions
			continue
		}

		// Get transaction receipt
		var receipt *ethtypes.Receipt
		if a.rateLimitClient != nil {
			receipt, err = a.rateLimitClient.TransactionReceipt(ctx, txHash)
		} else {
			receipt, err = a.client.TransactionReceipt(ctx, txHash)
		}
		if err != nil {
			continue
		}

		// Get block for timestamp
		var block *ethtypes.Block
		if a.rateLimitClient != nil {
			block, err = a.rateLimitClient.BlockByNumber(ctx, receipt.BlockNumber)
		} else {
			block, err = a.client.BlockByNumber(ctx, receipt.BlockNumber)
		}
		if err != nil {
			continue
		}

		// Normalize transaction
		normalized, err := a.NormalizeTransactionWithReceipt(tx, receipt, block)
		if err != nil {
			continue
		}

		transactions = append(transactions, normalized)
	}

	return transactions, nil
}

// fetchTransactionsWithFailover attempts to fetch transactions using secondary provider
func (a *EthereumAdapter) fetchTransactionsWithFailover(ctx context.Context, address string, fromBlock *uint64, toBlock *uint64) ([]*types.NormalizedTransaction, error) {
	// Attempt failover
	if err := a.provider.Failover(); err != nil {
		return nil, NewAdapterError(a.chainID, "FetchTransactions", ErrProviderUnavailable, map[string]interface{}{
			"error": "all providers unavailable",
		})
	}

	// Get new RPC URL
	rpcURL, err := a.provider.GetCurrentURL()
	if err != nil {
		return nil, err
	}

	// Create new client with failover provider
	client, err := ethclient.Dial(rpcURL)
	if err != nil {
		return nil, err
	}

	// Update client
	a.client = client

	// Retry the request
	return a.FetchTransactions(ctx, address, fromBlock, toBlock)
}

// FetchTransactionsForBlock retrieves transactions for tracked addresses in a specific block
// Scans all transactions in the block and checks:
// 1. Native ETH transfers: from/to fields match tracked addresses
// 2. Token transfers: Transfer event logs where from/to match tracked addresses
func (a *EthereumAdapter) FetchTransactionsForBlock(ctx context.Context, blockNum uint64, addresses []string) ([]*types.NormalizedTransaction, error) {
	if len(addresses) == 0 {
		return []*types.NormalizedTransaction{}, nil
	}

	// Build address lookup map for O(1) checks
	addressMap := make(map[string]bool)
	for _, addr := range addresses {
		normalizedAddr := strings.ToLower(strings.TrimSpace(addr))
		if !strings.HasPrefix(normalizedAddr, "0x") {
			normalizedAddr = "0x" + normalizedAddr
		}
		if a.ValidateAddress(normalizedAddr) {
			addressMap[normalizedAddr] = true
		}
	}

	if len(addressMap) == 0 {
		return []*types.NormalizedTransaction{}, nil
	}

	// Fetch block with full transactions (with rate limiting if available)
	blockBig := new(big.Int).SetUint64(blockNum)
	log.Printf("[Adapter:%s] RPC Call: BlockByNumber for block %d (with full txs)", a.chainID, blockNum)
	var block *ethtypes.Block
	var err error
	if a.rateLimitClient != nil {
		block, err = a.rateLimitClient.BlockByNumber(ctx, blockBig)
	} else {
		block, err = a.client.BlockByNumber(ctx, blockBig)
	}
	if err != nil {
		if a.shouldFailover(err) {
			if failErr := a.provider.Failover(); failErr == nil {
				if rpcURL, urlErr := a.provider.GetCurrentURL(); urlErr == nil {
					if client, dialErr := ethclient.Dial(rpcURL); dialErr == nil {
						a.client = client
						return a.FetchTransactionsForBlock(ctx, blockNum, addresses)
					}
				}
			}
		}
		return nil, NewAdapterError(a.chainID, "FetchTransactionsForBlock", err, map[string]interface{}{
			"blockNum": blockNum,
			"error":    "failed to fetch block",
		})
	}

	// Use a map to track matched tx hashes (avoid duplicates)
	matchedTxMap := make(map[common.Hash]bool)
	nativeMatches := 0
	tokenMatches := 0

	// 1. Scan all transactions for native ETH transfers
	for _, tx := range block.Transactions() {
		// Check if 'to' address matches
		if tx.To() != nil {
			toAddr := strings.ToLower(tx.To().Hex())
			if addressMap[toAddr] {
				matchedTxMap[tx.Hash()] = true
				nativeMatches++
				continue
			}
		}

		// Check if 'from' address matches (need to derive sender)
		// Handle legacy transactions with chainID 0 by using the block's chain ID
		chainID := tx.ChainId()
		if chainID == nil || chainID.Sign() == 0 {
			// For legacy transactions, use chain ID 1 (mainnet) as default
			chainID = big.NewInt(1)
		}
		sender, err := ethtypes.Sender(ethtypes.LatestSignerForChainID(chainID), tx)
		if err == nil {
			fromAddr := strings.ToLower(sender.Hex())
			if addressMap[fromAddr] {
				matchedTxMap[tx.Hash()] = true
				nativeMatches++
			}
		}
	}

	// 2. Fetch logs for this block to find token transfers
	// ERC20/721 Transfer event: Transfer(address indexed from, address indexed to, ...)
	// ERC1155 TransferSingle/TransferBatch events also have from/to in topics
	transferEventSig := common.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")
	transferSingleSig := common.HexToHash("0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62")
	transferBatchSig := common.HexToHash("0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb")

	filterQuery := ethereum.FilterQuery{
		FromBlock: blockBig,
		ToBlock:   blockBig,
		Topics: [][]common.Hash{
			{transferEventSig, transferSingleSig, transferBatchSig}, // Match any of these events
		},
	}

	var logs []ethtypes.Log
	if a.rateLimitClient != nil {
		logs, err = a.rateLimitClient.FilterLogs(ctx, filterQuery)
	} else {
		logs, err = a.client.FilterLogs(ctx, filterQuery)
	}
	if err != nil {
		log.Printf("[Adapter:%s] Warning: failed to fetch logs for block %d: %v", a.chainID, blockNum, err)
		// Continue with native transfers only
	} else {
		// Check each log for matching addresses
		for _, logEntry := range logs {
			// ERC20/721 Transfer: topics[1] = from, topics[2] = to
			if logEntry.Topics[0] == transferEventSig && len(logEntry.Topics) >= 3 {
				from := strings.ToLower(common.BytesToAddress(logEntry.Topics[1].Bytes()).Hex())
				to := strings.ToLower(common.BytesToAddress(logEntry.Topics[2].Bytes()).Hex())
				if addressMap[from] || addressMap[to] {
					if !matchedTxMap[logEntry.TxHash] {
						matchedTxMap[logEntry.TxHash] = true
						tokenMatches++
					}
				}
			}
			// ERC1155 TransferSingle: topics[2] = from, topics[3] = to
			if logEntry.Topics[0] == transferSingleSig && len(logEntry.Topics) >= 4 {
				from := strings.ToLower(common.BytesToAddress(logEntry.Topics[2].Bytes()).Hex())
				to := strings.ToLower(common.BytesToAddress(logEntry.Topics[3].Bytes()).Hex())
				if addressMap[from] || addressMap[to] {
					if !matchedTxMap[logEntry.TxHash] {
						matchedTxMap[logEntry.TxHash] = true
						tokenMatches++
					}
				}
			}
			// ERC1155 TransferBatch: topics[2] = from, topics[3] = to
			if logEntry.Topics[0] == transferBatchSig && len(logEntry.Topics) >= 4 {
				from := strings.ToLower(common.BytesToAddress(logEntry.Topics[2].Bytes()).Hex())
				to := strings.ToLower(common.BytesToAddress(logEntry.Topics[3].Bytes()).Hex())
				if addressMap[from] || addressMap[to] {
					if !matchedTxMap[logEntry.TxHash] {
						matchedTxMap[logEntry.TxHash] = true
						tokenMatches++
					}
				}
			}
		}
	}

	log.Printf("[Adapter:%s] Block %d: scanned %d txs + %d logs, found %d native + %d token matches",
		a.chainID, blockNum, len(block.Transactions()), len(logs), nativeMatches, tokenMatches)

	if len(matchedTxMap) == 0 {
		return []*types.NormalizedTransaction{}, nil
	}

	// Fetch receipts and normalize matched transactions (with rate limiting if available)
	var transactions []*types.NormalizedTransaction
	for txHash := range matchedTxMap {
		var tx *ethtypes.Transaction
		var isPending bool
		if a.rateLimitClient != nil {
			tx, isPending, err = a.rateLimitClient.TransactionByHash(ctx, txHash)
		} else {
			tx, isPending, err = a.client.TransactionByHash(ctx, txHash)
		}
		if err != nil || isPending {
			continue
		}

		var receipt *ethtypes.Receipt
		if a.rateLimitClient != nil {
			receipt, err = a.rateLimitClient.TransactionReceipt(ctx, txHash)
		} else {
			receipt, err = a.client.TransactionReceipt(ctx, txHash)
		}
		if err != nil {
			continue
		}

		normalized, err := a.NormalizeTransactionWithReceipt(tx, receipt, block)
		if err != nil {
			continue
		}

		transactions = append(transactions, normalized)
	}

	return transactions, nil
}

// GetCurrentBlock returns the current block number for the chain
func (a *EthereumAdapter) GetCurrentBlock(ctx context.Context) (uint64, error) {
	log.Printf("[Adapter:%s] RPC Call: BlockNumber (getting current block)", a.chainID)
	var blockNum uint64
	var err error
	if a.rateLimitClient != nil {
		blockNum, err = a.rateLimitClient.BlockNumber(ctx)
	} else {
		blockNum, err = a.client.BlockNumber(ctx)
	}
	if err != nil {
		if a.shouldFailover(err) {
			if failErr := a.provider.Failover(); failErr == nil {
				if rpcURL, urlErr := a.provider.GetCurrentURL(); urlErr == nil {
					if client, dialErr := ethclient.Dial(rpcURL); dialErr == nil {
						a.client = client
						return a.GetCurrentBlock(ctx)
					}
				}
			}
		}
		return 0, NewAdapterError(a.chainID, "GetCurrentBlock", err, nil)
	}
	return blockNum, nil
}

// FetchTransactionHistory retrieves historical transactions using Alchemy's optimized API
// This is much more efficient than block scanning - single API call vs hundreds of FilterLogs calls
// If maxCount is -1, fetches ALL transactions using pagination
func (a *EthereumAdapter) FetchTransactionHistory(ctx context.Context, address string, maxCount int) ([]*types.NormalizedTransaction, error) {
	if !a.ValidateAddress(address) {
		return nil, NewAdapterError(a.chainID, "FetchTransactionHistory", ErrInvalidAddress, map[string]interface{}{
			"address": address,
		})
	}

	// Get RPC URL to make direct JSON-RPC call to Alchemy
	rpcURL, err := a.provider.GetCurrentURL()
	if err != nil {
		return nil, NewAdapterError(a.chainID, "FetchTransactionHistory", err, nil)
	}

	// Use Alchemy's alchemy_getAssetTransfers API
	// This is a specialized endpoint that returns transaction history efficiently

	// For unlimited fetching (paid tier), use pagination
	if maxCount < 0 {
		log.Printf("[Adapter:%s] Fetching ALL transaction history for %s using pagination",
			a.chainID, address)
		return FetchAlchemyAssetTransfersWithPagination(ctx, rpcURL, address, a.chainID)
	}

	log.Printf("[Adapter:%s] Fetching transaction history for %s (max: %d) using alchemy_getAssetTransfers",
		a.chainID, address, maxCount)

	// Make the API call using AlchemyClient
	transfers, err := FetchAlchemyAssetTransfers(ctx, rpcURL, address, maxCount, a.chainID)
	if err != nil {
		// Try failover if available
		if a.shouldFailover(err) {
			if failErr := a.provider.Failover(); failErr == nil {
				return a.FetchTransactionHistory(ctx, address, maxCount)
			}
		}
		return nil, NewAdapterError(a.chainID, "FetchTransactionHistory", err, map[string]interface{}{
			"address":  address,
			"maxCount": maxCount,
		})
	}

	log.Printf("[Adapter:%s] Fetched %d transactions for %s using asset transfer API",
		a.chainID, len(transfers), address)

	return transfers, nil
}

// GetBlockByTimestamp returns the block number closest to the given timestamp
func (a *EthereumAdapter) GetBlockByTimestamp(ctx context.Context, timestamp int64) (uint64, error) {
	// Binary search to find block closest to timestamp
	currentBlock, err := a.GetCurrentBlock(ctx)
	if err != nil {
		return 0, err
	}

	// Get current block timestamp (with rate limiting if available)
	var block *ethtypes.Block
	if a.rateLimitClient != nil {
		block, err = a.rateLimitClient.BlockByNumber(ctx, new(big.Int).SetUint64(currentBlock))
	} else {
		block, err = a.client.BlockByNumber(ctx, new(big.Int).SetUint64(currentBlock))
	}
	if err != nil {
		return 0, NewAdapterError(a.chainID, "GetBlockByTimestamp", err, map[string]interface{}{
			"timestamp": timestamp,
		})
	}

	// If target timestamp is in the future, return current block
	if timestamp >= int64(block.Time()) {
		return currentBlock, nil
	}

	// Binary search
	low := uint64(0)
	high := currentBlock

	for low < high {
		mid := (low + high) / 2

		if a.rateLimitClient != nil {
			block, err = a.rateLimitClient.BlockByNumber(ctx, new(big.Int).SetUint64(mid))
		} else {
			block, err = a.client.BlockByNumber(ctx, new(big.Int).SetUint64(mid))
		}
		if err != nil {
			return 0, err
		}

		blockTime := int64(block.Time())

		if blockTime < timestamp {
			low = mid + 1
		} else if blockTime > timestamp {
			high = mid
		} else {
			return mid, nil
		}
	}

	return low, nil
}

// GetBalance retrieves address balance on this chain
func (a *EthereumAdapter) GetBalance(ctx context.Context, address string) (*types.ChainBalance, error) {
	if !a.ValidateAddress(address) {
		return nil, NewAdapterError(a.chainID, "GetBalance", ErrInvalidAddress, map[string]interface{}{
			"address": address,
		})
	}

	addr := common.HexToAddress(address)

	// Get native balance
	balance, err := a.client.BalanceAt(ctx, addr, nil)
	if err != nil {
		if a.shouldFailover(err) {
			if failErr := a.provider.Failover(); failErr == nil {
				if rpcURL, urlErr := a.provider.GetCurrentURL(); urlErr == nil {
					if client, dialErr := ethclient.Dial(rpcURL); dialErr == nil {
						a.client = client
						return a.GetBalance(ctx, address)
					}
				}
			}
		}
		return nil, NewAdapterError(a.chainID, "GetBalance", err, map[string]interface{}{
			"address": address,
		})
	}

	return &types.ChainBalance{
		Chain:         a.chainID,
		NativeBalance: balance.String(),
		TokenBalances: []types.TokenBalance{}, // Token balances would require additional calls
	}, nil
}

// ValidateAddress checks if address format is valid for Ethereum
func (a *EthereumAdapter) ValidateAddress(address string) bool {
	// Ethereum addresses are 42 characters: 0x + 40 hex characters
	if len(address) != 42 {
		return false
	}

	// Check format: 0x followed by 40 hex characters
	matched, err := regexp.MatchString("^0x[a-fA-F0-9]{40}$", address)
	if err != nil {
		return false
	}

	return matched
}

// GetChainID returns the chain identifier
func (a *EthereumAdapter) GetChainID() types.ChainID {
	return a.chainID
}

// shouldFailover determines if an error warrants failing over to another provider
func (a *EthereumAdapter) shouldFailover(err error) bool {
	if err == nil {
		return false
	}

	errStr := strings.ToLower(err.Error())

	// Check for rate limit errors
	if strings.Contains(errStr, "rate limit") ||
		strings.Contains(errStr, "too many requests") ||
		strings.Contains(errStr, "429") {
		return true
	}

	// Check for timeout errors
	if strings.Contains(errStr, "timeout") ||
		strings.Contains(errStr, "deadline exceeded") {
		return true
	}

	// Check for connection errors
	if strings.Contains(errStr, "connection refused") ||
		strings.Contains(errStr, "connection reset") ||
		strings.Contains(errStr, "no such host") {
		return true
	}

	return false
}

// Close closes the Ethereum client connection
func (a *EthereumAdapter) Close() {
	if a.client != nil {
		a.client.Close()
	}
}

// AlchemyAssetTransfer represents a transfer from Alchemy's API
type AlchemyAssetTransfer struct {
	BlockNum    string      `json:"blockNum"`
	Hash        string      `json:"hash"`
	From        string      `json:"from"`
	To          *string     `json:"to"`
	Value       interface{} `json:"value"` // Can be string or number
	Asset       *string     `json:"asset"`
	Category    string      `json:"category"`
	RawContract struct {
		Value   interface{} `json:"value"` // Can be string or number
		Address *string     `json:"address"`
		Decimal *string     `json:"decimal"`
	} `json:"rawContract"`
	Metadata *struct {
		BlockTimestamp string `json:"blockTimestamp"` // ISO 8601 format
	} `json:"metadata"`
}

// AlchemyAssetTransfersResponse represents the response from alchemy_getAssetTransfers
type AlchemyAssetTransfersResponse struct {
	Transfers []AlchemyAssetTransfer `json:"transfers"`
	PageKey   *string                `json:"pageKey,omitempty"`
}

// FetchAlchemyAssetTransfers fetches transaction history using Alchemy's asset transfer API
// Fetches BOTH incoming (toAddress) and outgoing (fromAddress) transfers
func FetchAlchemyAssetTransfers(ctx context.Context, rpcURL string, address string, maxCount int, chainID types.ChainID) ([]*types.NormalizedTransaction, error) {
	// Fetch outgoing transfers (fromAddress)
	outgoing, err := fetchAlchemyTransfers(ctx, rpcURL, address, "fromAddress", maxCount/2, chainID)
	if err != nil {
		log.Printf("[Alchemy] Warning: failed to fetch outgoing transfers: %v", err)
		outgoing = []*types.NormalizedTransaction{}
	}

	// Fetch incoming transfers (toAddress)
	incoming, err := fetchAlchemyTransfers(ctx, rpcURL, address, "toAddress", maxCount/2, chainID)
	if err != nil {
		log.Printf("[Alchemy] Warning: failed to fetch incoming transfers: %v", err)
		incoming = []*types.NormalizedTransaction{}
	}

	// Merge and deduplicate by transaction hash
	txMap := make(map[string]*types.NormalizedTransaction)
	for _, tx := range outgoing {
		txMap[tx.Hash] = tx
	}
	for _, tx := range incoming {
		if _, exists := txMap[tx.Hash]; !exists {
			txMap[tx.Hash] = tx
		}
	}

	// Convert map to slice
	allTransactions := make([]*types.NormalizedTransaction, 0, len(txMap))
	for _, tx := range txMap {
		allTransactions = append(allTransactions, tx)
	}

	log.Printf("[Alchemy] Fetched %d unique transactions for %s (outgoing: %d, incoming: %d)",
		len(allTransactions), address, len(outgoing), len(incoming))

	return allTransactions, nil
}

// fetchAlchemyTransfers fetches transfers for a single direction
func fetchAlchemyTransfers(ctx context.Context, rpcURL string, address string, direction string, maxCount int, chainID types.ChainID) ([]*types.NormalizedTransaction, error) {
	// Prepare the JSON-RPC request
	requestBody := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "alchemy_getAssetTransfers",
		"params": []map[string]interface{}{
			{
				direction:      address,
				"category":     []string{"external", "internal", "erc20", "erc721", "erc1155"},
				"maxCount":     fmt.Sprintf("0x%x", maxCount), // Convert to hex
				"order":        "desc",
				"withMetadata": true,
			},
		},
	}

	// Marshal to JSON
	jsonData, err := json.Marshal(requestBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	// Create HTTP request
	req, err := http.NewRequestWithContext(ctx, "POST", rpcURL, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	// Make the request
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to make request: %w", err)
	}
	defer resp.Body.Close()

	// Read response
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	// Check status code
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d, body: %s", resp.StatusCode, string(body))
	}

	// Parse response
	var rpcResponse struct {
		JSONRPC string                        `json:"jsonrpc"`
		ID      int                           `json:"id"`
		Result  AlchemyAssetTransfersResponse `json:"result"`
		Error   *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}

	if err := json.Unmarshal(body, &rpcResponse); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	// Check for RPC error
	if rpcResponse.Error != nil {
		return nil, fmt.Errorf("RPC error %d: %s", rpcResponse.Error.Code, rpcResponse.Error.Message)
	}

	// Convert Alchemy transfers to NormalizedTransactions
	transactions := make([]*types.NormalizedTransaction, 0, len(rpcResponse.Result.Transfers))
	for _, transfer := range rpcResponse.Result.Transfers {
		tx := convertAlchemyTransferToTransaction(transfer, chainID)
		if tx != nil {
			transactions = append(transactions, tx)
		}
	}

	return transactions, nil
}

// FetchAlchemyAssetTransfersWithPagination fetches ALL transactions using pagination
// This is used for paid tier to get complete transaction history
// Fetches both incoming (toAddress) and outgoing (fromAddress) transfers
func FetchAlchemyAssetTransfersWithPagination(ctx context.Context, rpcURL string, address string, chainID types.ChainID) ([]*types.NormalizedTransaction, error) {
	// Fetch outgoing transfers (fromAddress)
	outgoing, err := fetchTransfersWithPagination(ctx, rpcURL, address, "fromAddress", chainID)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch outgoing transfers: %w", err)
	}

	// Fetch incoming transfers (toAddress)
	incoming, err := fetchTransfersWithPagination(ctx, rpcURL, address, "toAddress", chainID)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch incoming transfers: %w", err)
	}

	// Merge and deduplicate by transaction hash
	txMap := make(map[string]*types.NormalizedTransaction)
	for _, tx := range outgoing {
		txMap[tx.Hash] = tx
	}
	for _, tx := range incoming {
		if _, exists := txMap[tx.Hash]; !exists {
			txMap[tx.Hash] = tx
		}
	}

	// Convert map to slice
	allTransactions := make([]*types.NormalizedTransaction, 0, len(txMap))
	for _, tx := range txMap {
		allTransactions = append(allTransactions, tx)
	}

	log.Printf("[Alchemy] Total unique transactions for %s: %d (outgoing: %d, incoming: %d)",
		address, len(allTransactions), len(outgoing), len(incoming))

	return allTransactions, nil
}

// fetchTransfersWithPagination fetches transfers with pagination for a specific direction
func fetchTransfersWithPagination(ctx context.Context, rpcURL string, address string, direction string, chainID types.ChainID) ([]*types.NormalizedTransaction, error) {
	var allTransactions []*types.NormalizedTransaction
	var pageKey *string
	pageNum := 1
	maxPages := 100 // Safety limit to prevent infinite loops

	for pageNum <= maxPages {
		log.Printf("[Alchemy] Fetching %s page %d for %s", direction, pageNum, address)

		// Build request params
		params := map[string]interface{}{
			direction:      address,
			"category":     []string{"external", "internal", "erc20", "erc721", "erc1155"},
			"maxCount":     "0x3e8", // 1000 in hex (max allowed)
			"order":        "desc",
			"withMetadata": true,
		}

		// Add pageKey if we have one from previous request
		if pageKey != nil {
			params["pageKey"] = *pageKey
		}

		requestBody := map[string]interface{}{
			"jsonrpc": "2.0",
			"id":      pageNum,
			"method":  "alchemy_getAssetTransfers",
			"params":  []map[string]interface{}{params},
		}

		jsonData, err := json.Marshal(requestBody)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal request: %w", err)
		}

		req, err := http.NewRequestWithContext(ctx, "POST", rpcURL, bytes.NewBuffer(jsonData))
		if err != nil {
			return nil, fmt.Errorf("failed to create request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")

		client := &http.Client{Timeout: 30 * time.Second}
		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("failed to make request: %w", err)
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("failed to read response: %w", err)
		}

		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("unexpected status code: %d, body: %s", resp.StatusCode, string(body))
		}

		var rpcResponse struct {
			JSONRPC string                        `json:"jsonrpc"`
			ID      int                           `json:"id"`
			Result  AlchemyAssetTransfersResponse `json:"result"`
			Error   *struct {
				Code    int    `json:"code"`
				Message string `json:"message"`
			} `json:"error"`
		}

		if err := json.Unmarshal(body, &rpcResponse); err != nil {
			return nil, fmt.Errorf("failed to parse response: %w", err)
		}

		if rpcResponse.Error != nil {
			return nil, fmt.Errorf("RPC error %d: %s", rpcResponse.Error.Code, rpcResponse.Error.Message)
		}

		// Convert transfers to transactions
		for _, transfer := range rpcResponse.Result.Transfers {
			tx := convertAlchemyTransferToTransaction(transfer, chainID)
			if tx != nil {
				allTransactions = append(allTransactions, tx)
			}
		}

		log.Printf("[Alchemy] %s page %d: fetched %d transfers, total so far: %d",
			direction, pageNum, len(rpcResponse.Result.Transfers), len(allTransactions))

		// Check if there are more pages
		if rpcResponse.Result.PageKey == nil || *rpcResponse.Result.PageKey == "" {
			break
		}

		pageKey = rpcResponse.Result.PageKey
		pageNum++

		// Small delay to avoid rate limiting
		time.Sleep(200 * time.Millisecond)
	}

	return allTransactions, nil
}

// convertAlchemyTransferToTransaction converts an Alchemy transfer to a NormalizedTransaction
func convertAlchemyTransferToTransaction(transfer AlchemyAssetTransfer, chainID types.ChainID) *types.NormalizedTransaction {
	// Parse block number from hex
	blockNum, err := strconv.ParseUint(strings.TrimPrefix(transfer.BlockNum, "0x"), 16, 64)
	if err != nil {
		log.Printf("[Alchemy] Failed to parse block number %s: %v", transfer.BlockNum, err)
		return nil
	}

	// Parse timestamp from metadata
	var timestamp int64
	if transfer.Metadata != nil && transfer.Metadata.BlockTimestamp != "" {
		t, err := time.Parse(time.RFC3339, transfer.Metadata.BlockTimestamp)
		if err != nil {
			log.Printf("[Alchemy] Failed to parse timestamp %s: %v", transfer.Metadata.BlockTimestamp, err)
			timestamp = time.Now().Unix()
		} else {
			timestamp = t.Unix()
		}
	} else {
		timestamp = time.Now().Unix()
	}

	tx := &types.NormalizedTransaction{
		Hash:        transfer.Hash,
		Chain:       chainID,
		From:        transfer.From,
		BlockNumber: blockNum,
		Status:      types.StatusSuccess,
		Timestamp:   timestamp,
	}

	if transfer.Asset != nil {
		tx.Asset = transfer.Asset
	}
	tx.Category = &transfer.Category

	if transfer.To != nil {
		tx.To = *transfer.To
	}

	// Set value
	valueStr := "0"
	if transfer.Value != nil {
		switch v := transfer.Value.(type) {
		case string:
			valueStr = v
		case float64:
			valueStr = fmt.Sprintf("%.18f", v) // Preserve precision for ETH values
		case int:
			valueStr = fmt.Sprintf("%d", v)
		}
	} else if transfer.RawContract.Value != nil {
		switch v := transfer.RawContract.Value.(type) {
		case string:
			valueStr = v
		case float64:
			valueStr = fmt.Sprintf("%.18f", v)
		case int:
			valueStr = fmt.Sprintf("%d", v)
		}
	}
	tx.Value = valueStr

	// Handle token transfers
	if transfer.Category == "erc20" || transfer.Category == "erc721" || transfer.Category == "erc1155" {
		if transfer.RawContract.Address != nil {
			tokenTransfer := types.TokenTransfer{
				Token: *transfer.RawContract.Address,
				From:  transfer.From,
				Value: valueStr,
			}
			if transfer.To != nil {
				tokenTransfer.To = *transfer.To
			}
			if transfer.Asset != nil {
				tokenTransfer.Symbol = transfer.Asset
			}
			if transfer.RawContract.Decimal != nil {
				decimalHex := strings.TrimPrefix(*transfer.RawContract.Decimal, "0x")
				if decimal, err := strconv.ParseInt(decimalHex, 16, 32); err == nil {
					d := int(decimal)
					tokenTransfer.Decimals = &d
				}
			}
			tx.TokenTransfers = []types.TokenTransfer{tokenTransfer}
		}
	}

	return tx
}
