package adapter

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/address-scanner/internal/types"
)

// EtherscanClient fetches complete transaction history from Etherscan API
// This captures ALL transactions including approve, contract calls, token transfers, NFTs
type EtherscanClient struct {
	apiKey      string
	baseURL     string
	client      *http.Client
	rateLimiter *rateLimiter // single global rate limiter (3 req/sec for free tier)
}

// rateLimiter implements a simple token bucket rate limiter
type rateLimiter struct {
	tokens     float64
	maxTokens  float64
	refillRate float64 // tokens per second
	lastRefill time.Time
	mu         sync.Mutex
}

func newRateLimiter(requestsPerSecond float64) *rateLimiter {
	return &rateLimiter{
		tokens:     requestsPerSecond, // start full
		maxTokens:  requestsPerSecond,
		refillRate: requestsPerSecond,
		lastRefill: time.Now(),
	}
}

func (r *rateLimiter) wait() {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Refill tokens based on elapsed time
	now := time.Now()
	elapsed := now.Sub(r.lastRefill).Seconds()
	r.tokens += elapsed * r.refillRate
	if r.tokens > r.maxTokens {
		r.tokens = r.maxTokens
	}
	r.lastRefill = now

	// If no tokens available, wait
	if r.tokens < 1 {
		waitTime := time.Duration((1 - r.tokens) / r.refillRate * float64(time.Second))
		r.mu.Unlock()
		time.Sleep(waitTime)
		r.mu.Lock()
		r.tokens = 0
		r.lastRefill = time.Now()
	} else {
		r.tokens--
	}
}

// EtherscanTransaction represents a normal/internal transaction from Etherscan API
type EtherscanTransaction struct {
	Hash            string `json:"hash"`
	BlockNumber     string `json:"blockNumber"`
	TimeStamp       string `json:"timeStamp"`
	From            string `json:"from"`
	To              string `json:"to"`
	Value           string `json:"value"`
	Gas             string `json:"gas"`
	GasPrice        string `json:"gasPrice"`
	GasUsed         string `json:"gasUsed"`
	IsError         string `json:"isError"`
	TxReceiptStatus string `json:"txreceipt_status"`
	Input           string `json:"input"`
	MethodId        string `json:"methodId"`
	FunctionName    string `json:"functionName"`
	ContractAddress string `json:"contractAddress"`
	Nonce           string `json:"nonce"`
	Confirmations   string `json:"confirmations"`
}

// EtherscanTokenTransfer represents an ERC20 token transfer
type EtherscanTokenTransfer struct {
	Hash            string `json:"hash"`
	BlockNumber     string `json:"blockNumber"`
	TimeStamp       string `json:"timeStamp"`
	From            string `json:"from"`
	To              string `json:"to"`
	Value           string `json:"value"`
	ContractAddress string `json:"contractAddress"`
	TokenName       string `json:"tokenName"`
	TokenSymbol     string `json:"tokenSymbol"`
	TokenDecimal    string `json:"tokenDecimal"`
	Gas             string `json:"gas"`
	GasPrice        string `json:"gasPrice"`
	GasUsed         string `json:"gasUsed"`
	MethodId        string `json:"methodId"`
	FunctionName    string `json:"functionName"`
	Nonce           string `json:"nonce"`
	Confirmations   string `json:"confirmations"`
}

// EtherscanNFTTransfer represents an ERC721 NFT transfer
type EtherscanNFTTransfer struct {
	Hash            string `json:"hash"`
	BlockNumber     string `json:"blockNumber"`
	TimeStamp       string `json:"timeStamp"`
	From            string `json:"from"`
	To              string `json:"to"`
	ContractAddress string `json:"contractAddress"`
	TokenID         string `json:"tokenID"`
	TokenName       string `json:"tokenName"`
	TokenSymbol     string `json:"tokenSymbol"`
	Gas             string `json:"gas"`
	GasPrice        string `json:"gasPrice"`
	GasUsed         string `json:"gasUsed"`
	MethodId        string `json:"methodId"`
	FunctionName    string `json:"functionName"`
	Nonce           string `json:"nonce"`
	Confirmations   string `json:"confirmations"`
}

// EtherscanERC1155Transfer represents an ERC1155 multi-token transfer
type EtherscanERC1155Transfer struct {
	Hash            string `json:"hash"`
	BlockNumber     string `json:"blockNumber"`
	TimeStamp       string `json:"timeStamp"`
	From            string `json:"from"`
	To              string `json:"to"`
	ContractAddress string `json:"contractAddress"`
	TokenID         string `json:"tokenID"`
	TokenValue      string `json:"tokenValue"`
	TokenName       string `json:"tokenName"`
	TokenSymbol     string `json:"tokenSymbol"`
	Gas             string `json:"gas"`
	GasPrice        string `json:"gasPrice"`
	GasUsed         string `json:"gasUsed"`
	MethodId        string `json:"methodId"`
	FunctionName    string `json:"functionName"`
	Nonce           string `json:"nonce"`
	Confirmations   string `json:"confirmations"`
}

// EtherscanResponse represents the API response for normal transactions
type EtherscanResponse struct {
	Status  string                 `json:"status"`
	Message string                 `json:"message"`
	Result  []EtherscanTransaction `json:"result"`
}

// EtherscanTokenResponse represents the API response for token transfers
type EtherscanTokenResponse struct {
	Status  string                   `json:"status"`
	Message string                   `json:"message"`
	Result  []EtherscanTokenTransfer `json:"result"`
}

// EtherscanNFTResponse represents the API response for NFT transfers
type EtherscanNFTResponse struct {
	Status  string                 `json:"status"`
	Message string                 `json:"message"`
	Result  []EtherscanNFTTransfer `json:"result"`
}

// EtherscanERC1155Response represents the API response for ERC1155 transfers
type EtherscanERC1155Response struct {
	Status  string                     `json:"status"`
	Message string                     `json:"message"`
	Result  []EtherscanERC1155Transfer `json:"result"`
}

// GetNativeAsset returns the native asset symbol for a chain
func GetNativeAsset(chain types.ChainID) string {
	switch chain {
	case types.ChainEthereum:
		return "ETH"
	case types.ChainPolygon:
		return "MATIC"
	case types.ChainArbitrum:
		return "ETH" // Arbitrum uses ETH
	case types.ChainOptimism:
		return "ETH" // Optimism uses ETH
	case types.ChainBase:
		return "ETH" // Base uses ETH
	case types.ChainBNB:
		return "BNB"
	default:
		return "ETH"
	}
}

// NewEtherscanClient creates a new Etherscan API client
func NewEtherscanClient(apiKey string) *EtherscanClient {
	// Free tier: 3 requests per second (global, not per chain)
	const requestsPerSecond = 3.0

	return &EtherscanClient{
		apiKey:      apiKey,
		baseURL:     "https://api.etherscan.io/v2/api",
		client:      &http.Client{Timeout: 30 * time.Second},
		rateLimiter: newRateLimiter(requestsPerSecond),
	}
}

// GetChainID returns the Etherscan chain ID for a given chain
func (c *EtherscanClient) GetChainID(chain types.ChainID) int {
	switch chain {
	case types.ChainEthereum:
		return 1
	case types.ChainPolygon:
		return 137
	case types.ChainArbitrum:
		return 42161
	case types.ChainOptimism:
		return 10
	case types.ChainBase:
		return 8453
	case types.ChainBNB:
		return 56
	default:
		return 1
	}
}

// throttle waits for rate limiter before making request
func (c *EtherscanClient) throttle(chainID int) {
	c.rateLimiter.wait()
}

// FetchAllTransactions fetches ALL transaction types for an address
// Includes: normal, internal, ERC20, ERC721, ERC1155
// Each transfer is returned as a separate record (no deduplication by hash)
func (c *EtherscanClient) FetchAllTransactions(ctx context.Context, address string, chain types.ChainID) ([]*types.NormalizedTransaction, error) {
	if c.apiKey == "" {
		return nil, fmt.Errorf("etherscan API key not configured")
	}

	chainID := c.GetChainID(chain)

	// 1. Fetch normal transactions (external)
	normalTxs, err := c.fetchTransactionList(ctx, address, chain, chainID, "txlist")
	if err != nil {
		log.Printf("[Etherscan] Warning: failed to fetch normal transactions: %v", err)
		normalTxs = []*types.NormalizedTransaction{}
	}

	// 2. Fetch internal transactions
	internalTxs, err := c.fetchTransactionList(ctx, address, chain, chainID, "txlistinternal")
	if err != nil {
		log.Printf("[Etherscan] Warning: failed to fetch internal transactions: %v", err)
		internalTxs = []*types.NormalizedTransaction{}
	}

	// 3. Fetch ERC20 token transfers
	erc20Txs, err := c.fetchTokenTransfers(ctx, address, chain, chainID)
	if err != nil {
		log.Printf("[Etherscan] Warning: failed to fetch ERC20 transfers: %v", err)
		erc20Txs = []*types.NormalizedTransaction{}
	}

	// 4. Fetch ERC721 NFT transfers
	erc721Txs, err := c.fetchNFTTransfers(ctx, address, chain, chainID)
	if err != nil {
		log.Printf("[Etherscan] Warning: failed to fetch ERC721 transfers: %v", err)
		erc721Txs = []*types.NormalizedTransaction{}
	}

	// 5. Fetch ERC1155 multi-token transfers
	erc1155Txs, err := c.fetchERC1155Transfers(ctx, address, chain, chainID)
	if err != nil {
		log.Printf("[Etherscan] Warning: failed to fetch ERC1155 transfers: %v", err)
		erc1155Txs = []*types.NormalizedTransaction{}
	}

	// Combine all transactions - NO deduplication
	// Each transfer is stored as a separate record
	var result []*types.NormalizedTransaction
	result = append(result, normalTxs...)
	result = append(result, internalTxs...)
	result = append(result, erc20Txs...)
	result = append(result, erc721Txs...)
	result = append(result, erc1155Txs...)

	log.Printf("[Etherscan] Fetched %d total transactions for %s (normal: %d, internal: %d, erc20: %d, erc721: %d, erc1155: %d)",
		len(result), address, len(normalTxs), len(internalTxs), len(erc20Txs), len(erc721Txs), len(erc1155Txs))

	return result, nil
}

// fetchTransactionList fetches normal/internal transactions using Etherscan API
func (c *EtherscanClient) fetchTransactionList(ctx context.Context, address string, chain types.ChainID, chainID int, action string) ([]*types.NormalizedTransaction, error) {
	url := fmt.Sprintf("%s?chainid=%d&module=account&action=%s&address=%s&sort=desc&apikey=%s",
		c.baseURL, chainID, action, address, c.apiKey)

	// Retry loop for flaky free tier access (Base, BNB, OP)
	// Retry forever since we know it will eventually succeed
	baseDelay := 1 * time.Second
	maxDelay := 30 * time.Second

	var rawResp struct {
		Status  string          `json:"status"`
		Message string          `json:"message"`
		Result  json.RawMessage `json:"result"`
	}

	attempt := 0
	for {
		// Throttle requests per chain to avoid 429
		c.throttle(chainID)

		body, err := c.doRequest(ctx, url, chainID)
		if err != nil {
			return nil, err
		}

		if err := json.Unmarshal(body, &rawResp); err != nil {
			return nil, fmt.Errorf("failed to parse response: %w", err)
		}

		// If status is "1", success - break out of retry loop
		if rawResp.Status == "1" {
			break
		}

		// Log the actual response for debugging (only first few attempts)
		if attempt < 3 {
			log.Printf("[Etherscan] %s response for %s: status=%s, message=%s, result=%s",
				action, address, rawResp.Status, rawResp.Message, string(rawResp.Result[:min(100, len(rawResp.Result))]))
		}

		if rawResp.Message == "No transactions found" || rawResp.Message == "No records found" {
			return []*types.NormalizedTransaction{}, nil
		}
		// NOTOK with "No record found" in result is valid empty response
		if rawResp.Message == "NOTOK" && strings.Contains(string(rawResp.Result), "No record") {
			return []*types.NormalizedTransaction{}, nil
		}
		// NOTOK with "Free API access" - retry forever for flaky free tier
		if rawResp.Message == "NOTOK" && strings.Contains(string(rawResp.Result), "Free API access") {
			attempt++
			delay := baseDelay * time.Duration(1<<uint(min(attempt, 5))) // Cap exponential at 32s
			if delay > maxDelay {
				delay = maxDelay
			}
			log.Printf("[Etherscan] Free tier flaky for %s %s (attempt %d), retrying in %v",
				action, address, attempt, delay)
			select {
			case <-time.After(delay):
				continue
			case <-ctx.Done():
				return nil, fmt.Errorf("context cancelled: %w", ctx.Err())
			}
		}
		// Other NOTOK responses - also retry forever (could be temporary)
		if rawResp.Message == "NOTOK" {
			attempt++
			delay := baseDelay * time.Duration(1<<uint(min(attempt, 5)))
			if delay > maxDelay {
				delay = maxDelay
			}
			log.Printf("[Etherscan] NOTOK on chain %d (attempt %d), retrying in %v", chainID, attempt, delay)
			select {
			case <-time.After(delay):
				continue
			case <-ctx.Done():
				return nil, fmt.Errorf("context cancelled: %w", ctx.Err())
			}
		}
		return nil, fmt.Errorf("etherscan API error: %s", rawResp.Message)
	}

	// Check if result is a string (some APIs return string on empty)
	if len(rawResp.Result) > 0 && rawResp.Result[0] == '"' {
		return []*types.NormalizedTransaction{}, nil
	}

	var txList []EtherscanTransaction
	if err := json.Unmarshal(rawResp.Result, &txList); err != nil {
		return nil, fmt.Errorf("failed to parse transactions: %w", err)
	}

	isInternal := action == "txlistinternal"
	transactions := make([]*types.NormalizedTransaction, 0, len(txList))

	// Track trace index per tx hash for internal transactions
	traceIndexMap := make(map[string]uint32)

	for _, tx := range txList {
		normalized := c.convertTransaction(tx, chain)
		if normalized != nil {
			if isInternal {
				normalized.IsInternal = true
				// Assign trace index for internal transactions with same hash
				normalized.TraceIndex = traceIndexMap[tx.Hash]
				traceIndexMap[tx.Hash]++
			}
			transactions = append(transactions, normalized)
		}
	}

	return transactions, nil
}

// fetchTokenTransfers fetches ERC20 token transfers
func (c *EtherscanClient) fetchTokenTransfers(ctx context.Context, address string, chain types.ChainID, chainID int) ([]*types.NormalizedTransaction, error) {
	// Throttle requests per chain to avoid 429
	c.throttle(chainID)

	url := fmt.Sprintf("%s?chainid=%d&module=account&action=tokentx&address=%s&sort=desc&apikey=%s",
		c.baseURL, chainID, address, c.apiKey)

	log.Printf("[Etherscan] Fetching ERC20 transfers for %s on chain %d", address, chainID)

	body, err := c.doRequest(ctx, url, chainID)
	if err != nil {
		log.Printf("[Etherscan] ERC20 request failed for %s: %v", address, err)
		return nil, err
	}

	// Check if result is a string (error message like "No transactions found")
	var rawResp struct {
		Status  string          `json:"status"`
		Message string          `json:"message"`
		Result  json.RawMessage `json:"result"`
	}
	if err := json.Unmarshal(body, &rawResp); err != nil {
		log.Printf("[Etherscan] ERC20 parse failed for %s: %v", address, err)
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	log.Printf("[Etherscan] ERC20 response for %s: status=%s, message=%s, result_len=%d", address, rawResp.Status, rawResp.Message, len(rawResp.Result))

	if rawResp.Status != "1" {
		if rawResp.Message == "No transactions found" || rawResp.Message == "No records found" || rawResp.Message == "NOTOK" {
			log.Printf("[Etherscan] ERC20 no records for %s: %s", address, rawResp.Message)
			return []*types.NormalizedTransaction{}, nil
		}
		return nil, fmt.Errorf("etherscan API error: %s", rawResp.Message)
	}

	if len(rawResp.Result) > 0 && rawResp.Result[0] == '"' {
		log.Printf("[Etherscan] ERC20 result is string for %s", address)
		return []*types.NormalizedTransaction{}, nil
	}

	var txList []EtherscanTokenTransfer
	if err := json.Unmarshal(rawResp.Result, &txList); err != nil {
		log.Printf("[Etherscan] ERC20 unmarshal failed for %s: %v, body: %s", address, err, string(rawResp.Result[:min(200, len(rawResp.Result))]))
		return nil, fmt.Errorf("failed to parse token transfers: %w", err)
	}

	log.Printf("[Etherscan] ERC20 parsed %d transfers for %s", len(txList), address)

	transactions := make([]*types.NormalizedTransaction, 0, len(txList))
	for _, tx := range txList {
		normalized := c.convertTokenTransfer(tx, chain)
		if normalized != nil {
			transactions = append(transactions, normalized)
		}
	}

	return transactions, nil
}

// fetchNFTTransfers fetches ERC721 NFT transfers
func (c *EtherscanClient) fetchNFTTransfers(ctx context.Context, address string, chain types.ChainID, chainID int) ([]*types.NormalizedTransaction, error) {
	// Throttle requests per chain to avoid 429
	c.throttle(chainID)

	url := fmt.Sprintf("%s?chainid=%d&module=account&action=tokennfttx&address=%s&sort=desc&apikey=%s",
		c.baseURL, chainID, address, c.apiKey)

	body, err := c.doRequest(ctx, url, chainID)
	if err != nil {
		return nil, err
	}

	// Check if result is a string (error message like "No transactions found")
	var rawResp struct {
		Status  string          `json:"status"`
		Message string          `json:"message"`
		Result  json.RawMessage `json:"result"`
	}
	if err := json.Unmarshal(body, &rawResp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	if rawResp.Status != "1" {
		if rawResp.Message == "No transactions found" || rawResp.Message == "No records found" || rawResp.Message == "NOTOK" {
			return []*types.NormalizedTransaction{}, nil
		}
		return nil, fmt.Errorf("etherscan API error: %s", rawResp.Message)
	}

	if len(rawResp.Result) > 0 && rawResp.Result[0] == '"' {
		return []*types.NormalizedTransaction{}, nil
	}

	var txList []EtherscanNFTTransfer
	if err := json.Unmarshal(rawResp.Result, &txList); err != nil {
		return nil, fmt.Errorf("failed to parse NFT transfers: %w", err)
	}

	transactions := make([]*types.NormalizedTransaction, 0, len(txList))
	for _, tx := range txList {
		normalized := c.convertNFTTransfer(tx, chain)
		if normalized != nil {
			transactions = append(transactions, normalized)
		}
	}

	return transactions, nil
}

// fetchERC1155Transfers fetches ERC1155 multi-token transfers
func (c *EtherscanClient) fetchERC1155Transfers(ctx context.Context, address string, chain types.ChainID, chainID int) ([]*types.NormalizedTransaction, error) {
	// Throttle requests per chain to avoid 429
	c.throttle(chainID)

	url := fmt.Sprintf("%s?chainid=%d&module=account&action=token1155tx&address=%s&sort=desc&apikey=%s",
		c.baseURL, chainID, address, c.apiKey)

	body, err := c.doRequest(ctx, url, chainID)
	if err != nil {
		return nil, err
	}

	// Check if result is a string (error message like "No transactions found")
	var rawResp struct {
		Status  string          `json:"status"`
		Message string          `json:"message"`
		Result  json.RawMessage `json:"result"`
	}
	if err := json.Unmarshal(body, &rawResp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	if rawResp.Status != "1" {
		if rawResp.Message == "No transactions found" || rawResp.Message == "No records found" || rawResp.Message == "NOTOK" {
			return []*types.NormalizedTransaction{}, nil
		}
		return nil, fmt.Errorf("etherscan API error: %s", rawResp.Message)
	}

	if len(rawResp.Result) > 0 && rawResp.Result[0] == '"' {
		return []*types.NormalizedTransaction{}, nil
	}

	var txList []EtherscanERC1155Transfer
	if err := json.Unmarshal(rawResp.Result, &txList); err != nil {
		return nil, fmt.Errorf("failed to parse ERC1155 transfers: %w", err)
	}

	transactions := make([]*types.NormalizedTransaction, 0, len(txList))
	for _, tx := range txList {
		normalized := c.convertERC1155Transfer(tx, chain)
		if normalized != nil {
			transactions = append(transactions, normalized)
		}
	}

	return transactions, nil
}

// isRetryableNOTOK checks if a NOTOK response should be retried
// Some chains (Base, BNB, OP) have flaky free tier access
func (c *EtherscanClient) isRetryableNOTOK(body []byte) bool {
	var resp struct {
		Status  string `json:"status"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return false
	}
	// Retry NOTOK responses that aren't "No records found" type
	if resp.Status == "0" && resp.Message == "NOTOK" {
		return true
	}
	return false
}

// doRequest performs HTTP request with retry logic for rate limiting (429)
func (c *EtherscanClient) doRequest(ctx context.Context, url string, chainID int) ([]byte, error) {
	const maxRetries = 5
	baseDelay := 1 * time.Second

	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create request: %w", err)
		}

		resp, err := c.client.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("failed to make request: %w", err)
			// Network error - retry with backoff
			if attempt < maxRetries {
				delay := baseDelay * time.Duration(1<<uint(attempt))
				if delay > 30*time.Second {
					delay = 30 * time.Second
				}
				log.Printf("[Etherscan] Request failed (attempt %d/%d): %v, retrying in %v", attempt+1, maxRetries+1, err, delay)
				select {
				case <-time.After(delay):
					continue
				case <-ctx.Done():
					return nil, fmt.Errorf("context cancelled: %w", ctx.Err())
				}
			}
			continue
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("failed to read response: %w", err)
		}

		// Handle rate limiting (429)
		if resp.StatusCode == http.StatusTooManyRequests {
			lastErr = fmt.Errorf("rate limited (429)")
			if attempt < maxRetries {
				// Use Retry-After header if present, otherwise exponential backoff
				delay := baseDelay * time.Duration(1<<uint(attempt))
				if retryAfter := resp.Header.Get("Retry-After"); retryAfter != "" {
					if seconds, err := strconv.Atoi(retryAfter); err == nil {
						delay = time.Duration(seconds) * time.Second
					}
				}
				if delay > 60*time.Second {
					delay = 60 * time.Second
				}
				log.Printf("[Etherscan] Rate limited on chain %d (attempt %d/%d), retrying in %v", chainID, attempt+1, maxRetries+1, delay)

				select {
				case <-time.After(delay):
					continue
				case <-ctx.Done():
					return nil, fmt.Errorf("context cancelled: %w", ctx.Err())
				}
			}
			continue
		}

		// Handle other HTTP errors
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("HTTP error: %d - %s", resp.StatusCode, string(body))
		}

		return body, nil
	}

	return nil, fmt.Errorf("max retries exceeded: %w", lastErr)
}

// convertTransaction converts Etherscan transaction to NormalizedTransaction
func (c *EtherscanClient) convertTransaction(tx EtherscanTransaction, chain types.ChainID) *types.NormalizedTransaction {
	blockNum, _ := strconv.ParseUint(tx.BlockNumber, 10, 64)
	timestamp, _ := strconv.ParseInt(tx.TimeStamp, 10, 64)

	// Set native asset as default for normal transactions
	nativeAsset := GetNativeAsset(chain)

	normalized := &types.NormalizedTransaction{
		Hash:        tx.Hash,
		Chain:       chain,
		From:        tx.From,
		To:          tx.To,
		Value:       tx.Value,
		Asset:       &nativeAsset,
		BlockNumber: blockNum,
		Timestamp:   timestamp,
	}

	// Set status
	// For normal transactions: IsError="0" AND TxReceiptStatus="1" means success
	// For internal transactions: TxReceiptStatus is empty, so only check IsError
	// Internal transactions don't have their own receipt - they succeed if IsError="0"
	if tx.IsError == "0" {
		// For normal transactions, also verify receipt status
		// For internal transactions (TxReceiptStatus is empty), IsError="0" is sufficient
		if tx.TxReceiptStatus == "" || tx.TxReceiptStatus == "1" {
			normalized.Status = types.StatusSuccess
		} else {
			normalized.Status = types.StatusFailed
		}
	} else {
		normalized.Status = types.StatusFailed
	}

	// Set gas info
	if tx.GasUsed != "" {
		normalized.GasUsed = &tx.GasUsed
	}
	if tx.GasPrice != "" {
		normalized.GasPrice = &tx.GasPrice
	}

	// Set method ID, function name, and input
	if tx.MethodId != "" {
		normalized.MethodID = &tx.MethodId
	}
	if tx.FunctionName != "" {
		normalized.FuncName = &tx.FunctionName
	}
	if tx.Input != "" && tx.Input != "0x" {
		normalized.Input = &tx.Input
	}

	// Set category based on method
	category := "external"
	if tx.MethodId == "0x095ea7b3" {
		category = "approve"
	} else if tx.Input != "" && tx.Input != "0x" {
		category = "contract_call"
	}
	normalized.Category = &category

	return normalized
}

// convertTokenTransfer converts ERC20 token transfer to NormalizedTransaction
func (c *EtherscanClient) convertTokenTransfer(tx EtherscanTokenTransfer, chain types.ChainID) *types.NormalizedTransaction {
	blockNum, _ := strconv.ParseUint(tx.BlockNumber, 10, 64)
	timestamp, _ := strconv.ParseInt(tx.TimeStamp, 10, 64)
	decimals, _ := strconv.Atoi(tx.TokenDecimal)

	category := "erc20"
	symbol := tx.TokenSymbol

	normalized := &types.NormalizedTransaction{
		Hash:        tx.Hash,
		Chain:       chain,
		From:        tx.From,
		To:          tx.To,
		Value:       "0", // Native ETH value is 0 for token transfers
		Asset:       &symbol,
		Category:    &category,
		BlockNumber: blockNum,
		Timestamp:   timestamp,
		Status:      types.StatusSuccess,
		TokenTransfers: []types.TokenTransfer{
			{
				Token:    tx.ContractAddress,
				From:     tx.From,
				To:       tx.To,
				Value:    tx.Value,
				Symbol:   &tx.TokenSymbol,
				Decimals: &decimals,
			},
		},
	}

	if tx.GasUsed != "" {
		normalized.GasUsed = &tx.GasUsed
	}
	if tx.GasPrice != "" {
		normalized.GasPrice = &tx.GasPrice
	}
	if tx.MethodId != "" {
		normalized.MethodID = &tx.MethodId
	}
	if tx.FunctionName != "" {
		normalized.FuncName = &tx.FunctionName
	}

	return normalized
}

// convertNFTTransfer converts ERC721 NFT transfer to NormalizedTransaction
func (c *EtherscanClient) convertNFTTransfer(tx EtherscanNFTTransfer, chain types.ChainID) *types.NormalizedTransaction {
	blockNum, _ := strconv.ParseUint(tx.BlockNumber, 10, 64)
	timestamp, _ := strconv.ParseInt(tx.TimeStamp, 10, 64)

	category := "erc721"
	symbol := tx.TokenSymbol
	tokenID := tx.TokenID

	normalized := &types.NormalizedTransaction{
		Hash:        tx.Hash,
		Chain:       chain,
		From:        tx.From,
		To:          tx.To,
		Value:       "0", // Native ETH value is 0 for NFT transfers
		Asset:       &symbol,
		Category:    &category,
		BlockNumber: blockNum,
		Timestamp:   timestamp,
		Status:      types.StatusSuccess,
		TokenTransfers: []types.TokenTransfer{
			{
				Token:   tx.ContractAddress,
				From:    tx.From,
				To:      tx.To,
				Value:   "1", // NFT quantity is always 1
				Symbol:  &tx.TokenSymbol,
				TokenID: &tokenID, // Store token ID for NFTs
			},
		},
	}

	if tx.GasUsed != "" {
		normalized.GasUsed = &tx.GasUsed
	}
	if tx.GasPrice != "" {
		normalized.GasPrice = &tx.GasPrice
	}
	if tx.MethodId != "" {
		normalized.MethodID = &tx.MethodId
	}
	if tx.FunctionName != "" {
		normalized.FuncName = &tx.FunctionName
	}

	return normalized
}

// convertERC1155Transfer converts ERC1155 transfer to NormalizedTransaction
func (c *EtherscanClient) convertERC1155Transfer(tx EtherscanERC1155Transfer, chain types.ChainID) *types.NormalizedTransaction {
	blockNum, _ := strconv.ParseUint(tx.BlockNumber, 10, 64)
	timestamp, _ := strconv.ParseInt(tx.TimeStamp, 10, 64)

	category := "erc1155"
	symbol := tx.TokenSymbol
	tokenID := tx.TokenID

	normalized := &types.NormalizedTransaction{
		Hash:        tx.Hash,
		Chain:       chain,
		From:        tx.From,
		To:          tx.To,
		Value:       "0", // Native ETH value is 0 for ERC1155 transfers
		Asset:       &symbol,
		Category:    &category,
		BlockNumber: blockNum,
		Timestamp:   timestamp,
		Status:      types.StatusSuccess,
		TokenTransfers: []types.TokenTransfer{
			{
				Token:   tx.ContractAddress,
				From:    tx.From,
				To:      tx.To,
				Value:   tx.TokenValue,
				Symbol:  &tx.TokenSymbol,
				TokenID: &tokenID, // Store token ID for ERC1155
			},
		},
	}

	if tx.GasUsed != "" {
		normalized.GasUsed = &tx.GasUsed
	}
	if tx.GasPrice != "" {
		normalized.GasPrice = &tx.GasPrice
	}
	if tx.MethodId != "" {
		normalized.MethodID = &tx.MethodId
	}
	if tx.FunctionName != "" {
		normalized.FuncName = &tx.FunctionName
	}

	return normalized
}

// FetchTransactionsInRange fetches transactions within a block range
func (c *EtherscanClient) FetchTransactionsInRange(ctx context.Context, address string, chain types.ChainID, startBlock, endBlock uint64) ([]*types.NormalizedTransaction, error) {
	if c.apiKey == "" {
		return nil, fmt.Errorf("etherscan API key not configured")
	}

	chainID := c.GetChainID(chain)

	// Throttle requests per chain to avoid 429
	c.throttle(chainID)

	url := fmt.Sprintf("%s?chainid=%d&module=account&action=txlist&address=%s&startblock=%d&endblock=%d&sort=asc&apikey=%s",
		c.baseURL, chainID, address, startBlock, endBlock, c.apiKey)

	body, err := c.doRequest(ctx, url, chainID)
	if err != nil {
		return nil, err
	}

	var ethResp EtherscanResponse
	if err := json.Unmarshal(body, &ethResp); err != nil {
		return nil, err
	}

	if ethResp.Status != "1" && ethResp.Message != "No transactions found" {
		return nil, fmt.Errorf("etherscan error: %s", ethResp.Message)
	}

	transactions := make([]*types.NormalizedTransaction, 0, len(ethResp.Result))
	for _, tx := range ethResp.Result {
		normalized := c.convertTransaction(tx, chain)
		if normalized != nil {
			transactions = append(transactions, normalized)
		}
	}

	return transactions, nil
}
