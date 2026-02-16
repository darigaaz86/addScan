// Package adapter provides blockchain data adapters for the address scanner.
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
	"strings"
	"time"

	"github.com/address-scanner/internal/types"
)

// NodeRealClient handles API calls to NodeReal MegaNode for BNB chain data
type NodeRealClient struct {
	rpcURL     string
	httpClient *http.Client
}

// NodeReal JSON-RPC types
type nodeRealRequest struct {
	JSONRPC string      `json:"jsonrpc"`
	Method  string      `json:"method"`
	Params  interface{} `json:"params"`
	ID      int         `json:"id"`
}

type nodeRealResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      int             `json:"id"`
	Result  json.RawMessage `json:"result"`
	Error   *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

type nodeRealTransferResult struct {
	PageKey   string             `json:"pageKey"`
	Transfers []nodeRealTransfer `json:"transfers"`
}

type nodeRealTransfer struct {
	Category        string `json:"category"` // external, internal, 20, 721, 1155
	BlockNum        string `json:"blockNum"` // hex
	From            string `json:"from"`
	To              string `json:"to"`
	Value           string `json:"value"` // hex wei for native, hex amount for tokens
	Asset           string `json:"asset"`
	Hash            string `json:"hash"`
	BlockTimeStamp  int64  `json:"blockTimeStamp"` // unix timestamp
	GasPrice        int64  `json:"gasPrice"`
	GasUsed         int64  `json:"gasUsed"`
	ReceiptsStatus  int    `json:"receiptsStatus"` // 1 = success
	Input           string `json:"input"`
	LogIndex        int    `json:"logIndex"`
	TraceIndex      int    `json:"traceIndex"`
	ContractAddress string `json:"contractAddress"`
}

// NewNodeRealClient creates a new NodeReal MegaNode client
func NewNodeRealClient(rpcURL string) *NodeRealClient {
	return &NodeRealClient{
		rpcURL:     rpcURL,
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}
}

// FetchAllTransactions fetches complete transaction history for an address on BNB chain
func (c *NodeRealClient) FetchAllTransactions(ctx context.Context, address string, chain types.ChainID) ([]*types.NormalizedTransaction, error) {
	addressLower := strings.ToLower(address)
	var allTransactions []*types.NormalizedTransaction

	// Track tx hashes that already have a native gas record to avoid duplicates
	gasRecordCreated := make(map[string]bool)

	// Fetch both directions: fromAddress and toAddress
	for _, direction := range []string{"from", "to"} {
		pageKey := ""
		page := 0

		for {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
			}

			params := map[string]interface{}{
				"category": []string{"external", "internal", "20", "721", "1155"},
				"maxCount": "0x64", // 100 per page
			}
			if direction == "from" {
				params["fromAddress"] = addressLower
			} else {
				params["toAddress"] = addressLower
			}
			if pageKey != "" {
				params["pageKey"] = pageKey
			}

			result, err := c.doRPC(ctx, "nr_getAssetTransfers", []interface{}{params})
			if err != nil {
				return nil, fmt.Errorf("failed to fetch %s transfers: %w", direction, err)
			}

			var transferResult nodeRealTransferResult
			if err := json.Unmarshal(result, &transferResult); err != nil {
				return nil, fmt.Errorf("failed to parse transfer result: %w", err)
			}

			log.Printf("[NodeReal] Page %d (%s): got %d transfers for %s",
				page, direction, len(transferResult.Transfers), address)

			for _, t := range transferResult.Transfers {
				normalized := c.convertToNormalized(t, addressLower, chain, gasRecordCreated)
				allTransactions = append(allTransactions, normalized...)
			}

			if transferResult.PageKey == "" || len(transferResult.Transfers) == 0 {
				break
			}

			pageKey = transferResult.PageKey
			page++
			time.Sleep(50 * time.Millisecond)
		}
	}

	// Deduplicate: transactions where address is both sender and receiver appear in both queries
	allTransactions = c.deduplicateTransactions(allTransactions)

	log.Printf("[NodeReal] Total: fetched %d normalized transactions for %s on %s",
		len(allTransactions), address, chain)

	return allTransactions, nil
}

// convertToNormalized converts a NodeReal transfer to normalized format
func (c *NodeRealClient) convertToNormalized(t nodeRealTransfer, address string, chain types.ChainID, gasRecordCreated map[string]bool) []*types.NormalizedTransaction {
	var results []*types.NormalizedTransaction

	blockNumber := hexToUint64(t.BlockNum)
	status := types.StatusSuccess
	if t.ReceiptsStatus != 1 {
		status = types.StatusFailed
	}

	from := strings.ToLower(t.From)
	to := strings.ToLower(t.To)

	// Determine direction relative to tracked address
	direction := types.DirectionIn
	if strings.EqualFold(from, address) {
		direction = types.DirectionOut
	}

	// Extract method ID from input (first 4 bytes / 10 chars including 0x)
	var methodID *string
	if len(t.Input) >= 10 && t.Input != "0x" {
		m := t.Input[:10]
		methodID = &m
	}

	gasUsed := fmt.Sprintf("%d", t.GasUsed)
	gasPrice := fmt.Sprintf("%d", t.GasPrice)

	switch t.Category {
	case "external":
		// Native value transfer or contract call
		value := hexToBigIntString(t.Value)
		category := "native"

		// For outgoing external txs, always create native record (captures gas)
		if direction == types.DirectionOut {
			gasRecordCreated[t.Hash] = true
		}

		tx := &types.NormalizedTransaction{
			Hash:        t.Hash,
			Chain:       chain,
			From:        from,
			To:          to,
			Value:       value,
			Direction:   direction,
			Timestamp:   t.BlockTimeStamp,
			BlockNumber: blockNumber,
			Status:      status,
			Category:    &category,
			MethodID:    methodID,
			Input:       &t.Input,
		}
		if direction == types.DirectionOut {
			tx.GasUsed = &gasUsed
			tx.GasPrice = &gasPrice
		}
		results = append(results, tx)

	case "internal":
		// Internal (trace) transfer
		value := hexToBigIntString(t.Value)
		category := "native"

		tx := &types.NormalizedTransaction{
			Hash:        t.Hash,
			Chain:       chain,
			From:        from,
			To:          to,
			Value:       value,
			Direction:   direction,
			Timestamp:   t.BlockTimeStamp,
			BlockNumber: blockNumber,
			Status:      status,
			Category:    &category,
			IsInternal:  true,
			TraceIndex:  uint32(t.TraceIndex),
		}
		// Internal txs don't have gas (gas is on the outer tx)
		results = append(results, tx)

		// If user initiated the outer tx and no gas record yet, create one
		// (internal txs don't carry gas, but the outer tx does)
		// We skip this since the external tx query should already capture it

	case "20":
		// ERC20 token transfer
		value := hexToBigIntString(t.Value)
		category := "erc20"
		symbol := t.Asset
		contractAddr := strings.ToLower(t.ContractAddress)

		// Create native record for this token transfer if not yet created.
		// Gas is only attributed if the "external" category already confirmed
		// the tracked address as the actual tx sender (gasRecordCreated[hash] == true).
		// We never set gas based on the ERC20 transfer's from field alone,
		// because phishing txs can spoof the from address in token events.
		alreadyConfirmedSender := gasRecordCreated[t.Hash]
		if !alreadyConfirmedSender {
			gasRecordCreated[t.Hash] = true
			nativeCat := "native"
			gasTx := &types.NormalizedTransaction{
				Hash:        t.Hash,
				Chain:       chain,
				From:        address,
				To:          contractAddr,
				Value:       "0",
				Direction:   types.DirectionOut,
				Timestamp:   t.BlockTimeStamp,
				BlockNumber: blockNumber,
				Status:      status,
				Category:    &nativeCat,
				MethodID:    methodID,
			}
			// No gas attribution here — only the "external" case sets gas
			results = append(results, gasTx)
		}

		tx := &types.NormalizedTransaction{
			Hash:        t.Hash,
			Chain:       chain,
			From:        from,
			To:          to,
			Value:       value,
			Asset:       &symbol,
			Direction:   direction,
			Timestamp:   t.BlockTimeStamp,
			BlockNumber: blockNumber,
			Status:      status,
			Category:    &category,
			TokenTransfers: []types.TokenTransfer{{
				Token:  contractAddr,
				From:   from,
				To:     to,
				Value:  value,
				Symbol: &symbol,
			}},
		}
		results = append(results, tx)

	case "721":
		category := "erc721"
		symbol := t.Asset
		contractAddr := strings.ToLower(t.ContractAddress)
		tokenID := hexToBigIntString(t.Value) // For 721, value is token ID

		tx := &types.NormalizedTransaction{
			Hash:        t.Hash,
			Chain:       chain,
			From:        from,
			To:          to,
			Value:       "1",
			Asset:       &symbol,
			Direction:   direction,
			Timestamp:   t.BlockTimeStamp,
			BlockNumber: blockNumber,
			Status:      status,
			Category:    &category,
			TokenTransfers: []types.TokenTransfer{{
				Token:   contractAddr,
				From:    from,
				To:      to,
				Value:   "1",
				Symbol:  &symbol,
				TokenID: &tokenID,
			}},
		}
		results = append(results, tx)

	case "1155":
		category := "erc1155"
		symbol := t.Asset
		contractAddr := strings.ToLower(t.ContractAddress)
		value := hexToBigIntString(t.Value)

		tx := &types.NormalizedTransaction{
			Hash:        t.Hash,
			Chain:       chain,
			From:        from,
			To:          to,
			Value:       value,
			Asset:       &symbol,
			Direction:   direction,
			Timestamp:   t.BlockTimeStamp,
			BlockNumber: blockNumber,
			Status:      status,
			Category:    &category,
			TokenTransfers: []types.TokenTransfer{{
				Token:  contractAddr,
				From:   from,
				To:     to,
				Value:  value,
				Symbol: &symbol,
			}},
		}
		results = append(results, tx)
	}

	return results
}

// isUserInitiatedToken checks if the user initiated a token transfer tx
// For token transfers fetched via toAddress, the "from" in the transfer is the token sender,
// but the tx initiator might still be our tracked address
func (c *NodeRealClient) isUserInitiatedToken(t nodeRealTransfer, address string) bool {
	// If gasUsed > 0 and the transfer is incoming, the user might still be the tx initiator
	// We can't know for sure without the tx_from field, so we conservatively return false
	// The external tx query should already capture the gas record
	return false
}

// deduplicateTransactions removes duplicate transactions that appear in both from/to queries
func (c *NodeRealClient) deduplicateTransactions(txs []*types.NormalizedTransaction) []*types.NormalizedTransaction {
	seen := make(map[string]bool)
	var result []*types.NormalizedTransaction

	for _, tx := range txs {
		// Key: hash + direction + category + value (to distinguish multiple transfers in same tx)
		cat := ""
		if tx.Category != nil {
			cat = *tx.Category
		}
		key := fmt.Sprintf("%s:%s:%s:%s:%d", tx.Hash, tx.Direction, cat, tx.Value, tx.TraceIndex)
		if tx.IsInternal {
			key += ":internal"
		}
		if len(tx.TokenTransfers) > 0 {
			key += ":" + tx.TokenTransfers[0].Token
		}
		if !seen[key] {
			seen[key] = true
			result = append(result, tx)
		}
	}

	if len(txs) != len(result) {
		log.Printf("[NodeReal] Deduplicated %d -> %d transactions", len(txs), len(result))
	}

	return result
}

// doRPC makes a JSON-RPC call to NodeReal
func (c *NodeRealClient) doRPC(ctx context.Context, method string, params interface{}) (json.RawMessage, error) {
	req := nodeRealRequest{
		JSONRPC: "2.0",
		Method:  method,
		Params:  params,
		ID:      1,
	}

	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST", c.rpcURL, bytes.NewBuffer(body))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP error %d: %s", resp.StatusCode, string(respBody))
	}

	var rpcResp nodeRealResponse
	if err := json.Unmarshal(respBody, &rpcResp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	if rpcResp.Error != nil {
		return nil, fmt.Errorf("RPC error %d: %s", rpcResp.Error.Code, rpcResp.Error.Message)
	}

	return rpcResp.Result, nil
}

// hexToUint64 converts a hex string to uint64
func hexToUint64(hex string) uint64 {
	hex = strings.TrimPrefix(hex, "0x")
	if hex == "" {
		return 0
	}
	n := new(big.Int)
	n.SetString(hex, 16)
	return n.Uint64()
}

// hexToBigIntString converts a hex string to decimal string
func hexToBigIntString(hex string) string {
	hex = strings.TrimPrefix(hex, "0x")
	if hex == "" || hex == "0" {
		return "0"
	}
	n := new(big.Int)
	n.SetString(hex, 16)
	return n.String()
}
