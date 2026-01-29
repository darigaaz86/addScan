package adapter

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/address-scanner/internal/types"
)

// EtherscanClient fetches complete transaction history from Etherscan API
// This captures ALL transactions including approve, contract calls, token transfers, NFTs
type EtherscanClient struct {
	apiKey  string
	baseURL string
	client  *http.Client
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
	default:
		return "ETH"
	}
}

// NewEtherscanClient creates a new Etherscan API client
func NewEtherscanClient(apiKey string) *EtherscanClient {
	return &EtherscanClient{
		apiKey:  apiKey,
		baseURL: "https://api.etherscan.io/v2/api",
		client:  &http.Client{Timeout: 30 * time.Second},
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
	default:
		return 1
	}
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
	normalTxs, err := c.fetchTransactionList(ctx, address, chainID, "txlist")
	if err != nil {
		log.Printf("[Etherscan] Warning: failed to fetch normal transactions: %v", err)
		normalTxs = []*types.NormalizedTransaction{}
	}

	// 2. Fetch internal transactions
	internalTxs, err := c.fetchTransactionList(ctx, address, chainID, "txlistinternal")
	if err != nil {
		log.Printf("[Etherscan] Warning: failed to fetch internal transactions: %v", err)
		internalTxs = []*types.NormalizedTransaction{}
	}

	// 3. Fetch ERC20 token transfers
	erc20Txs, err := c.fetchTokenTransfers(ctx, address, chainID)
	if err != nil {
		log.Printf("[Etherscan] Warning: failed to fetch ERC20 transfers: %v", err)
		erc20Txs = []*types.NormalizedTransaction{}
	}

	// 4. Fetch ERC721 NFT transfers
	erc721Txs, err := c.fetchNFTTransfers(ctx, address, chainID)
	if err != nil {
		log.Printf("[Etherscan] Warning: failed to fetch ERC721 transfers: %v", err)
		erc721Txs = []*types.NormalizedTransaction{}
	}

	// 5. Fetch ERC1155 multi-token transfers
	erc1155Txs, err := c.fetchERC1155Transfers(ctx, address, chainID)
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
func (c *EtherscanClient) fetchTransactionList(ctx context.Context, address string, chainID int, action string) ([]*types.NormalizedTransaction, error) {
	url := fmt.Sprintf("%s?chainid=%d&module=account&action=%s&address=%s&sort=desc&apikey=%s",
		c.baseURL, chainID, action, address, c.apiKey)

	body, err := c.doRequest(ctx, url)
	if err != nil {
		return nil, err
	}

	var ethResp EtherscanResponse
	if err := json.Unmarshal(body, &ethResp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	if ethResp.Status != "1" {
		if ethResp.Message == "No transactions found" {
			return []*types.NormalizedTransaction{}, nil
		}
		return nil, fmt.Errorf("etherscan API error: %s", ethResp.Message)
	}

	transactions := make([]*types.NormalizedTransaction, 0, len(ethResp.Result))
	for _, tx := range ethResp.Result {
		normalized := c.convertTransaction(tx, types.ChainID(fmt.Sprintf("%d", chainID)))
		if normalized != nil {
			transactions = append(transactions, normalized)
		}
	}

	return transactions, nil
}

// fetchTokenTransfers fetches ERC20 token transfers
func (c *EtherscanClient) fetchTokenTransfers(ctx context.Context, address string, chainID int) ([]*types.NormalizedTransaction, error) {
	url := fmt.Sprintf("%s?chainid=%d&module=account&action=tokentx&address=%s&sort=desc&apikey=%s",
		c.baseURL, chainID, address, c.apiKey)

	body, err := c.doRequest(ctx, url)
	if err != nil {
		return nil, err
	}

	var resp EtherscanTokenResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("failed to parse token response: %w", err)
	}

	if resp.Status != "1" {
		if resp.Message == "No transactions found" {
			return []*types.NormalizedTransaction{}, nil
		}
		return nil, fmt.Errorf("etherscan API error: %s", resp.Message)
	}

	transactions := make([]*types.NormalizedTransaction, 0, len(resp.Result))
	for _, tx := range resp.Result {
		normalized := c.convertTokenTransfer(tx, types.ChainID(fmt.Sprintf("%d", chainID)))
		if normalized != nil {
			transactions = append(transactions, normalized)
		}
	}

	return transactions, nil
}

// fetchNFTTransfers fetches ERC721 NFT transfers
func (c *EtherscanClient) fetchNFTTransfers(ctx context.Context, address string, chainID int) ([]*types.NormalizedTransaction, error) {
	url := fmt.Sprintf("%s?chainid=%d&module=account&action=tokennfttx&address=%s&sort=desc&apikey=%s",
		c.baseURL, chainID, address, c.apiKey)

	body, err := c.doRequest(ctx, url)
	if err != nil {
		return nil, err
	}

	var resp EtherscanNFTResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("failed to parse NFT response: %w", err)
	}

	if resp.Status != "1" {
		if resp.Message == "No transactions found" {
			return []*types.NormalizedTransaction{}, nil
		}
		return nil, fmt.Errorf("etherscan API error: %s", resp.Message)
	}

	transactions := make([]*types.NormalizedTransaction, 0, len(resp.Result))
	for _, tx := range resp.Result {
		normalized := c.convertNFTTransfer(tx, types.ChainID(fmt.Sprintf("%d", chainID)))
		if normalized != nil {
			transactions = append(transactions, normalized)
		}
	}

	return transactions, nil
}

// fetchERC1155Transfers fetches ERC1155 multi-token transfers
func (c *EtherscanClient) fetchERC1155Transfers(ctx context.Context, address string, chainID int) ([]*types.NormalizedTransaction, error) {
	url := fmt.Sprintf("%s?chainid=%d&module=account&action=token1155tx&address=%s&sort=desc&apikey=%s",
		c.baseURL, chainID, address, c.apiKey)

	body, err := c.doRequest(ctx, url)
	if err != nil {
		return nil, err
	}

	var resp EtherscanERC1155Response
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("failed to parse ERC1155 response: %w", err)
	}

	if resp.Status != "1" {
		if resp.Message == "No transactions found" {
			return []*types.NormalizedTransaction{}, nil
		}
		return nil, fmt.Errorf("etherscan API error: %s", resp.Message)
	}

	transactions := make([]*types.NormalizedTransaction, 0, len(resp.Result))
	for _, tx := range resp.Result {
		normalized := c.convertERC1155Transfer(tx, types.ChainID(fmt.Sprintf("%d", chainID)))
		if normalized != nil {
			transactions = append(transactions, normalized)
		}
	}

	return transactions, nil
}

// doRequest performs HTTP request and returns response body
func (c *EtherscanClient) doRequest(ctx context.Context, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to make request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	return body, nil
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
	if tx.IsError == "0" && tx.TxReceiptStatus == "1" {
		normalized.Status = types.StatusSuccess
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
		Value:       tx.Value,
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

	normalized := &types.NormalizedTransaction{
		Hash:        tx.Hash,
		Chain:       chain,
		From:        tx.From,
		To:          tx.To,
		Value:       "1", // NFT is always 1
		Asset:       &symbol,
		Category:    &category,
		BlockNumber: blockNum,
		Timestamp:   timestamp,
		Status:      types.StatusSuccess,
		TokenTransfers: []types.TokenTransfer{
			{
				Token:  tx.ContractAddress,
				From:   tx.From,
				To:     tx.To,
				Value:  tx.TokenID, // Store tokenID as value for NFTs
				Symbol: &tx.TokenSymbol,
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

	normalized := &types.NormalizedTransaction{
		Hash:        tx.Hash,
		Chain:       chain,
		From:        tx.From,
		To:          tx.To,
		Value:       tx.TokenValue,
		Asset:       &symbol,
		Category:    &category,
		BlockNumber: blockNum,
		Timestamp:   timestamp,
		Status:      types.StatusSuccess,
		TokenTransfers: []types.TokenTransfer{
			{
				Token:  tx.ContractAddress,
				From:   tx.From,
				To:     tx.To,
				Value:  tx.TokenValue,
				Symbol: &tx.TokenSymbol,
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
	url := fmt.Sprintf("%s?chainid=%d&module=account&action=txlist&address=%s&startblock=%d&endblock=%d&sort=asc&apikey=%s",
		c.baseURL, chainID, address, startBlock, endBlock, c.apiKey)

	body, err := c.doRequest(ctx, url)
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
