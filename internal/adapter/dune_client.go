// Package adapter provides blockchain data adapters for the address scanner.
package adapter

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/address-scanner/internal/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

// DuneClient handles API calls to Dune Sim for blockchain data
type DuneClient struct {
	apiKey     string
	httpClient *http.Client
	baseURL    string
	rpcURL     string // RPC URL for fetching gas data
}

// NewDuneClient creates a new Dune Sim API client
func NewDuneClient(apiKey string) *DuneClient {
	return &DuneClient{
		apiKey:     apiKey,
		httpClient: &http.Client{Timeout: 30 * time.Second},
		baseURL:    "https://api.sim.dune.com/v1/evm",
	}
}

// SetRPCURL sets the RPC URL for fetching gas data
func (c *DuneClient) SetRPCURL(rpcURL string) {
	c.rpcURL = rpcURL
}

// DuneActivityResponse represents the activity API response
type DuneActivityResponse struct {
	NextOffset string         `json:"next_offset"`
	Activity   []DuneActivity `json:"activity"`
}

// DuneActivity represents a single activity from Dune
type DuneActivity struct {
	ChainID      int            `json:"chain_id"`
	BlockNumber  int64          `json:"block_number"`
	BlockTime    string         `json:"block_time"`
	TxHash       string         `json:"tx_hash"`
	TxFrom       string         `json:"tx_from"`
	TxTo         *string        `json:"tx_to"`
	TxValue      string         `json:"tx_value"`
	Type         string         `json:"type"`       // send, receive, mint, burn, swap
	AssetType    string         `json:"asset_type"` // native, erc20, erc721, erc1155
	TokenAddress string         `json:"token_address"`
	From         string         `json:"from"`
	To           string         `json:"to"`
	Value        string         `json:"value"`
	ID           string         `json:"id"` // NFT token ID
	TokenMeta    *DuneTokenMeta `json:"token_metadata"`
	NFTMeta      *DuneNFTMeta   `json:"nft_metadata"`
	// Swap-specific fields
	FromTokenAddress  string         `json:"from_token_address"`
	FromTokenValue    string         `json:"from_token_value"`
	FromTokenMetadata *DuneTokenMeta `json:"from_token_metadata"`
	ToTokenAddress    string         `json:"to_token_address"`
	ToTokenValue      string         `json:"to_token_value"`
	ToTokenMetadata   *DuneTokenMeta `json:"to_token_metadata"`
}

// DuneTokenMeta represents token metadata
type DuneTokenMeta struct {
	Symbol   string `json:"symbol"`
	Name     string `json:"name"`
	Decimals int    `json:"decimals"`
	Logo     string `json:"logo"`
}

// DuneNFTMeta represents NFT metadata
type DuneNFTMeta struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

// chainToDuneChainID converts ChainID to Dune chain ID
func chainToDuneChainID(chain types.ChainID) int {
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

// FetchAllTransactions fetches complete transaction history for an address
func (c *DuneClient) FetchAllTransactions(ctx context.Context, address string, chain types.ChainID) ([]*types.NormalizedTransaction, error) {
	chainID := chainToDuneChainID(chain)
	var allTransactions []*types.NormalizedTransaction
	offset := ""
	page := 0

	// Track tx hashes that already have a native gas record to avoid duplicates
	// This must be outside the loop to track across all pages
	gasRecordCreated := make(map[string]bool)

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		// Build URL
		reqURL := fmt.Sprintf("%s/activity/%s?chain_ids=%d&limit=100", c.baseURL, address, chainID)
		if offset != "" {
			reqURL += "&offset=" + url.QueryEscape(offset)
		}

		req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create request: %w", err)
		}
		req.Header.Set("Accept", "application/json")
		req.Header.Set("X-Sim-Api-Key", c.apiKey)

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch from Dune: %w", err)
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("Dune API error: status=%d, body=%s", resp.StatusCode, string(body))
		}

		var activityResp DuneActivityResponse
		if err := json.Unmarshal(body, &activityResp); err != nil {
			return nil, fmt.Errorf("failed to parse Dune response: %w", err)
		}

		log.Printf("[Dune] Page %d: got %d activities for %s", page, len(activityResp.Activity), address)

		// Convert to normalized transactions
		for _, act := range activityResp.Activity {
			normalized := c.convertToNormalized(act, address, chain, gasRecordCreated)
			if normalized != nil {
				allTransactions = append(allTransactions, normalized...)
			}
		}

		// Check if done
		if activityResp.NextOffset == "" || len(activityResp.Activity) == 0 {
			break
		}

		offset = activityResp.NextOffset
		page++

		// Rate limit - Dune has generous limits but be nice
		time.Sleep(50 * time.Millisecond)
	}

	log.Printf("[Dune] Total: fetched %d normalized transactions for %s on %s",
		len(allTransactions), address, chain)

	// Enrich transactions with gas data from RPC
	if c.rpcURL != "" && len(allTransactions) > 0 {
		if err := c.enrichGasData(ctx, allTransactions); err != nil {
			log.Printf("[Dune] Warning: failed to enrich gas data: %v", err)
			// Continue without gas data - better to have partial data than none
		}
	}

	return allTransactions, nil
}

// convertToNormalized converts a Dune activity to normalized format
func (c *DuneClient) convertToNormalized(act DuneActivity, address string, chain types.ChainID, gasRecordCreated map[string]bool) []*types.NormalizedTransaction {
	addressLower := strings.ToLower(address)

	// Parse timestamp
	timestamp := c.parseTimestamp(act.BlockTime)

	// Handle swap type specially - it generates 2 transactions (out + in)
	if act.Type == "swap" {
		// Mark gas record as created for swap (swap OUT already includes gas)
		gasRecordCreated[act.TxHash] = true
		return c.convertSwapToNormalized(act, addressLower, chain, timestamp)
	}

	// Handle approve type - skip for now (no value transfer)
	// But we still need to create a gas record if user initiated it
	if act.Type == "approve" {
		// Create gas record for approve if not already created
		if !gasRecordCreated[act.TxHash] && strings.EqualFold(act.TxFrom, addressLower) {
			gasRecordCreated[act.TxHash] = true
			nativeCategory := "native"
			txTo := ""
			if act.TxTo != nil {
				txTo = *act.TxTo
			}
			return []*types.NormalizedTransaction{{
				Hash:        act.TxHash,
				Chain:       chain,
				From:        addressLower,
				To:          txTo,
				Value:       "0",
				Direction:   types.DirectionOut,
				Timestamp:   timestamp,
				BlockNumber: uint64(act.BlockNumber),
				Status:      types.StatusSuccess,
				Category:    &nativeCategory,
			}}
		}
		return nil
	}

	// Handle call type - contract calls where user pays gas but no value transfer
	if act.Type == "call" {
		// Create gas record for call if not already created and user initiated it
		if !gasRecordCreated[act.TxHash] && strings.EqualFold(act.TxFrom, addressLower) {
			gasRecordCreated[act.TxHash] = true
			nativeCategory := "native"
			txTo := ""
			if act.TxTo != nil {
				txTo = *act.TxTo
			}
			return []*types.NormalizedTransaction{{
				Hash:        act.TxHash,
				Chain:       chain,
				From:        addressLower,
				To:          txTo,
				Value:       "0",
				Direction:   types.DirectionOut,
				Timestamp:   timestamp,
				BlockNumber: uint64(act.BlockNumber),
				Status:      types.StatusSuccess,
				Category:    &nativeCategory,
			}}
		}
		return nil
	}

	// Determine direction for non-swap types
	var direction types.TransactionDirection
	switch act.Type {
	case "receive", "mint":
		direction = types.DirectionIn
	case "send", "burn":
		direction = types.DirectionOut
	default:
		// Skip unknown types
		return nil
	}

	// Determine transfer type and category
	var transferType types.TransferType
	var category string
	switch act.AssetType {
	case "native":
		transferType = types.TransferTypeNative
		category = "native"
		// Native OUT already captures gas
		if direction == types.DirectionOut {
			gasRecordCreated[act.TxHash] = true
		}
	case "erc20":
		transferType = types.TransferTypeERC20
		category = "erc20"
	case "erc721":
		transferType = types.TransferTypeERC721
		category = "erc721"
	case "erc1155":
		transferType = types.TransferTypeERC1155
		category = "erc1155"
	default:
		return nil
	}

	// Get token metadata
	var symbol *string
	var decimals *int
	if act.TokenMeta != nil {
		if act.TokenMeta.Symbol != "" {
			symbol = &act.TokenMeta.Symbol
		}
		if act.TokenMeta.Decimals > 0 {
			decimals = &act.TokenMeta.Decimals
		}
	}
	if act.NFTMeta != nil && symbol == nil {
		if act.NFTMeta.Name != "" {
			symbol = &act.NFTMeta.Name
		}
	}

	// Build from/to based on direction
	var from, to string
	if direction == types.DirectionIn {
		from = act.From
		to = addressLower
	} else {
		from = addressLower
		to = act.To
	}

	var results []*types.NormalizedTransaction

	// For any transaction where user is the initiator (tx_from == address), create a native OUT record for gas
	// This is needed because Dune doesn't return separate native activity for gas payment
	// Applies to:
	// - Token OUT transfers (user sends tokens)
	// - Token IN transfers where user initiated (e.g., Venus Mint - user deposits USDT, receives vUSDT)
	// Only create once per tx_hash to avoid duplicates
	isUserInitiated := strings.EqualFold(act.TxFrom, addressLower)
	if transferType != types.TransferTypeNative && isUserInitiated && !gasRecordCreated[act.TxHash] {
		gasRecordCreated[act.TxHash] = true
		nativeCategory := "native"
		txTo := ""
		if act.TxTo != nil {
			txTo = *act.TxTo
		}
		nativeGasTx := &types.NormalizedTransaction{
			Hash:        act.TxHash,
			Chain:       chain,
			From:        addressLower,
			To:          txTo, // Contract address
			Value:       "0",  // No native value, just gas
			Direction:   types.DirectionOut,
			Timestamp:   timestamp,
			BlockNumber: uint64(act.BlockNumber),
			Status:      types.StatusSuccess,
			Category:    &nativeCategory,
			// Gas will be enriched later by enrichGasData
		}
		results = append(results, nativeGasTx)
	}

	normalized := &types.NormalizedTransaction{
		Hash:        act.TxHash,
		Chain:       chain,
		From:        from,
		To:          to,
		Value:       act.Value,
		Asset:       symbol,
		Direction:   direction,
		Timestamp:   timestamp,
		BlockNumber: uint64(act.BlockNumber),
		Status:      types.StatusSuccess,
		Category:    &category,
	}

	// Add token transfer details for non-native
	if transferType != types.TransferTypeNative {
		tt := types.TokenTransfer{
			Token:    act.TokenAddress,
			From:     from,
			To:       to,
			Value:    act.Value,
			Symbol:   symbol,
			Decimals: decimals,
		}
		if act.ID != "" {
			tt.TokenID = &act.ID
		}
		normalized.TokenTransfers = []types.TokenTransfer{tt}
	}

	results = append(results, normalized)
	return results
}

// convertSwapToNormalized converts a swap activity to normalized transactions
// A swap generates 2 transactions: one OUT (what user sent) and one IN (what user received)
func (c *DuneClient) convertSwapToNormalized(act DuneActivity, address string, chain types.ChainID, timestamp int64) []*types.NormalizedTransaction {
	var results []*types.NormalizedTransaction

	// Get the router/contract address
	routerAddr := ""
	if act.TxTo != nil {
		routerAddr = *act.TxTo
	}

	// OUT transaction - what user sent to swap
	if act.FromTokenValue != "" && act.FromTokenValue != "0" {
		isNative := act.FromTokenAddress == "native" || act.FromTokenAddress == ""
		var category string
		var symbol *string

		if isNative {
			category = "native"
		} else {
			category = "erc20"
		}

		if act.FromTokenMetadata != nil && act.FromTokenMetadata.Symbol != "" {
			symbol = &act.FromTokenMetadata.Symbol
		}

		outTx := &types.NormalizedTransaction{
			Hash:        act.TxHash,
			Chain:       chain,
			From:        address,
			To:          routerAddr,
			Value:       act.FromTokenValue,
			Asset:       symbol,
			Direction:   types.DirectionOut,
			Timestamp:   timestamp,
			BlockNumber: uint64(act.BlockNumber),
			Status:      types.StatusSuccess,
			Category:    &category,
		}

		// Add token transfer for ERC20
		if !isNative {
			var decimals *int
			if act.FromTokenMetadata != nil && act.FromTokenMetadata.Decimals > 0 {
				decimals = &act.FromTokenMetadata.Decimals
			}
			outTx.TokenTransfers = []types.TokenTransfer{{
				Token:    act.FromTokenAddress,
				From:     address,
				To:       routerAddr,
				Value:    act.FromTokenValue,
				Symbol:   symbol,
				Decimals: decimals,
			}}
		}

		results = append(results, outTx)
	}

	// IN transaction - what user received from swap
	if act.ToTokenValue != "" && act.ToTokenValue != "0" {
		isNative := act.ToTokenAddress == "native" || act.ToTokenAddress == ""
		var category string
		var symbol *string

		if isNative {
			category = "native"
		} else {
			category = "erc20"
		}

		if act.ToTokenMetadata != nil && act.ToTokenMetadata.Symbol != "" {
			symbol = &act.ToTokenMetadata.Symbol
		}

		inTx := &types.NormalizedTransaction{
			Hash:        act.TxHash,
			Chain:       chain,
			From:        routerAddr,
			To:          address,
			Value:       act.ToTokenValue,
			Asset:       symbol,
			Direction:   types.DirectionIn,
			Timestamp:   timestamp,
			BlockNumber: uint64(act.BlockNumber),
			Status:      types.StatusSuccess,
			Category:    &category,
		}

		// Add token transfer for ERC20
		if !isNative {
			var decimals *int
			if act.ToTokenMetadata != nil && act.ToTokenMetadata.Decimals > 0 {
				decimals = &act.ToTokenMetadata.Decimals
			}
			inTx.TokenTransfers = []types.TokenTransfer{{
				Token:    act.ToTokenAddress,
				From:     routerAddr,
				To:       address,
				Value:    act.ToTokenValue,
				Symbol:   symbol,
				Decimals: decimals,
			}}
		}

		results = append(results, inTx)
	}

	return results
}

func (c *DuneClient) parseTimestamp(ts string) int64 {
	// Dune format: "2025-09-08T06:20:45+00:00"
	t, err := time.Parse(time.RFC3339, ts)
	if err != nil {
		return 0
	}
	return t.Unix()
}

// GetChainID returns the Dune chain ID for a given chain
func (c *DuneClient) GetChainID(chain types.ChainID) int {
	return chainToDuneChainID(chain)
}

// enrichGasData fetches gas data from RPC for transactions where the address is the sender
func (c *DuneClient) enrichGasData(ctx context.Context, transactions []*types.NormalizedTransaction) error {
	if c.rpcURL == "" {
		return fmt.Errorf("no RPC URL configured")
	}

	client, err := ethclient.Dial(c.rpcURL)
	if err != nil {
		return fmt.Errorf("failed to connect to RPC: %w", err)
	}
	defer client.Close()

	// Collect unique tx hashes for outgoing transactions (sender pays gas)
	txHashSet := make(map[string]bool)
	for _, tx := range transactions {
		if tx.Direction == types.DirectionOut {
			txHashSet[tx.Hash] = true
		}
	}

	if len(txHashSet) == 0 {
		return nil
	}

	log.Printf("[Dune] Fetching gas data for %d outgoing transactions", len(txHashSet))

	// Fetch receipts in batches
	gasData := make(map[string]struct {
		gasUsed  string
		gasPrice string
	})

	processed := 0
	for hash := range txHashSet {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		receipt, err := client.TransactionReceipt(ctx, stringToHash(hash))
		if err != nil {
			log.Printf("[Dune] Warning: failed to get receipt for %s: %v", hash[:10], err)
			continue
		}

		tx, _, err := client.TransactionByHash(ctx, stringToHash(hash))
		if err != nil {
			log.Printf("[Dune] Warning: failed to get tx for %s: %v", hash[:10], err)
			continue
		}

		gasData[hash] = struct {
			gasUsed  string
			gasPrice string
		}{
			gasUsed:  fmt.Sprintf("%d", receipt.GasUsed),
			gasPrice: tx.GasPrice().String(),
		}

		processed++

		// Rate limit RPC calls
		if processed%10 == 0 {
			time.Sleep(100 * time.Millisecond)
		}
	}

	// Enrich transactions with gas data
	enriched := 0
	for _, tx := range transactions {
		if data, ok := gasData[tx.Hash]; ok {
			tx.GasUsed = &data.gasUsed
			tx.GasPrice = &data.gasPrice
			enriched++
		}
	}

	log.Printf("[Dune] Enriched %d transactions with gas data", enriched)
	return nil
}

// stringToHash converts a hex string to common.Hash
func stringToHash(s string) [32]byte {
	var hash [32]byte
	// Remove 0x prefix if present
	s = strings.TrimPrefix(s, "0x")
	// Pad to 64 chars (32 bytes)
	if len(s) < 64 {
		s = strings.Repeat("0", 64-len(s)) + s
	}
	// Decode hex
	for i := 0; i < 32; i++ {
		fmt.Sscanf(s[i*2:i*2+2], "%02x", &hash[i])
	}
	return hash
}
