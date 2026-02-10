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
)

// DuneClient handles API calls to Dune Sim for blockchain data
type DuneClient struct {
	apiKey     string
	httpClient *http.Client
	baseURL    string
}

// NewDuneClient creates a new Dune Sim API client
func NewDuneClient(apiKey string) *DuneClient {
	return &DuneClient{
		apiKey:     apiKey,
		httpClient: &http.Client{Timeout: 30 * time.Second},
		baseURL:    "https://api.sim.dune.com/v1/evm",
	}
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
	Type         string         `json:"type"`       // send, receive, mint, burn
	AssetType    string         `json:"asset_type"` // native, erc20, erc721, erc1155
	TokenAddress string         `json:"token_address"`
	From         string         `json:"from"`
	To           string         `json:"to"`
	Value        string         `json:"value"`
	ID           string         `json:"id"` // NFT token ID
	TokenMeta    *DuneTokenMeta `json:"token_metadata"`
	NFTMeta      *DuneNFTMeta   `json:"nft_metadata"`
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
			normalized := c.convertToNormalized(act, address, chain)
			if normalized != nil {
				allTransactions = append(allTransactions, normalized)
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

	return allTransactions, nil
}

// convertToNormalized converts a Dune activity to normalized format
func (c *DuneClient) convertToNormalized(act DuneActivity, address string, chain types.ChainID) *types.NormalizedTransaction {
	addressLower := strings.ToLower(address)

	// Parse timestamp
	timestamp := c.parseTimestamp(act.BlockTime)

	// Determine direction
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

	return normalized
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
