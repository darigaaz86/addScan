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

		// Decode amount based on event type
		amount := decodeEventAmount(l.EventSignature, l.Data)

		logs = append(logs, storage.GoldskyLog{
			ID:              l.ID,
			TransactionHash: l.TransactionHash,
			BlockNumber:     uint64(l.BlockNumber),
			BlockTimestamp:  time.Unix(l.BlockTimestamp, 0),
			ContractAddress: l.ContractAddress,
			EventSignature:  l.EventSignature,
			FromAddress:     l.FromAddress,
			ToAddress:       l.ToAddress,
			Amount:          amount,
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

// Event signatures for common DeFi events (first 10 chars of keccak256 hash)
const (
	// ERC20 Transfer(address indexed from, address indexed to, uint256 value)
	EventTransfer = "0xddf252ad"
	// ERC20 Approval(address indexed owner, address indexed spender, uint256 value)
	EventApproval = "0x8c5be1e5"

	// WETH Deposit(address indexed dst, uint256 wad)
	EventWETHDeposit = "0xe1fffcc4"
	// WETH Withdrawal(address indexed src, uint256 wad)
	EventWETHWithdrawal = "0x7fcf532c"

	// Uniswap V3 Swap(address indexed sender, address indexed recipient, int256 amount0, int256 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick)
	EventUniswapV3Swap = "0xc42079f9"
	// Uniswap V2 Swap(address indexed sender, uint256 amount0In, uint256 amount1In, uint256 amount0Out, uint256 amount1Out, address indexed to)
	EventUniswapV2Swap = "0xd78ad95f"
	// Uniswap V2/V3 Sync(uint112 reserve0, uint112 reserve1)
	EventSync = "0x1c411e9a"

	// Permit2 Approval(address indexed owner, address indexed token, address indexed spender, uint160 amount, uint48 expiration)
	EventPermit2Approval = "0xc6a377bf"

	// Aave V3 Supply(address indexed reserve, address user, address indexed onBehalfOf, uint256 amount, uint16 indexed referralCode)
	EventAaveSupply = "0x2b627736"
	// Aave V3 Withdraw(address indexed reserve, address indexed user, address indexed to, uint256 amount)
	EventAaveWithdraw = "0x3115d144"
	// Aave V2 Deposit(address indexed reserve, address user, address indexed onBehalfOf, uint256 amount, uint16 indexed referralCode)
	EventAaveDeposit = "0xde6857219"

	// Compound cToken Mint(address minter, uint256 mintAmount, uint256 mintTokens)
	EventCompoundMint = "0x4c209b5f"
	// Compound cToken Redeem(address redeemer, uint256 redeemAmount, uint256 redeemTokens)
	EventCompoundRedeem = "0xe5b754fb"

	// 1inch/Aggregator Swapped(address sender, IERC20 srcToken, IERC20 dstToken, address dstReceiver, uint256 spentAmount, uint256 returnAmount)
	EventSwapped = "0xd6d4f518"
)

// decodeEventAmount decodes the amount from event data based on event type
// Returns the relevant amount for balance calculation
func decodeEventAmount(eventSig, data string) string {
	if len(eventSig) < 10 || data == "" || data == "0x" {
		return "0"
	}

	sig := eventSig[:10]

	switch sig {
	case EventTransfer:
		// Transfer: data = amount (uint256)
		if len(data) >= 66 {
			return decodeHexToDecimal(data[0:66])
		}

	case EventApproval:
		// Approval: data = amount (uint256)
		if len(data) >= 66 {
			return decodeHexToDecimal(data[0:66])
		}

	case EventWETHDeposit, EventWETHWithdrawal:
		// WETH Deposit/Withdrawal: data = wad (uint256)
		if len(data) >= 66 {
			return decodeHexToDecimal(data[0:66])
		}

	case EventUniswapV3Swap:
		// Uniswap V3 Swap: data = amount0 (int256) + amount1 (int256) + ...
		// amount0 and amount1 can be negative (tokens going out)
		// Return the positive amount (what user received)
		if len(data) >= 130 { // 0x + 64 + 64
			amount0 := decodeHexToSignedDecimal(data[2:66])
			amount1 := decodeHexToSignedDecimal(data[66:130])
			// Return the positive amount (what user received)
			if amount0 != "0" && len(amount0) > 0 && amount0[0] != '-' {
				return amount0
			}
			if amount1 != "0" && len(amount1) > 0 && amount1[0] != '-' {
				return amount1
			}
			// If both negative, return absolute of first non-zero
			if amount0 != "0" {
				return trimNegative(amount0)
			}
			return trimNegative(amount1)
		}

	case EventUniswapV2Swap:
		// Uniswap V2 Swap: data = amount0In + amount1In + amount0Out + amount1Out
		// Return the non-zero output amount
		if len(data) >= 258 { // 0x + 64*4
			amount0Out := decodeHexToDecimal("0x" + data[130:194])
			amount1Out := decodeHexToDecimal("0x" + data[194:258])
			if amount0Out != "0" {
				return amount0Out
			}
			return amount1Out
		}

	case EventPermit2Approval:
		// Permit2 Approval: data = owner (address, indexed) + amount (uint160) + expiration (uint48) + nonce (uint48)
		// The amount is uint160, padded to 32 bytes (second word in data)
		if len(data) >= 130 { // 0x + 64 + 64
			return decodeHexToDecimal("0x" + data[66:130])
		}

	case EventAaveSupply, EventAaveDeposit:
		// Aave Supply/Deposit: data contains amount
		// Layout varies by version, but amount is typically first non-indexed param
		if len(data) >= 66 {
			return decodeHexToDecimal(data[0:66])
		}

	case EventAaveWithdraw:
		// Aave Withdraw: data = amount (uint256)
		if len(data) >= 66 {
			return decodeHexToDecimal(data[0:66])
		}

	case EventCompoundMint:
		// Compound Mint: data = mintAmount (uint256) + mintTokens (uint256)
		if len(data) >= 66 {
			return decodeHexToDecimal(data[0:66])
		}

	case EventCompoundRedeem:
		// Compound Redeem: data = redeemAmount (uint256) + redeemTokens (uint256)
		if len(data) >= 66 {
			return decodeHexToDecimal(data[0:66])
		}

	case EventSwapped:
		// 1inch Swapped: data = spentAmount (uint256) + returnAmount (uint256)
		// Return the returnAmount (what user received)
		if len(data) >= 130 {
			return decodeHexToDecimal("0x" + data[66:130])
		}

	case EventSync:
		// Sync event doesn't have a meaningful amount for balance calculation
		// It's just reserve updates
		return "0"

	default:
		// For unknown events, try to decode first uint256 if data is standard length
		if len(data) >= 66 {
			return decodeHexToDecimal(data[0:66])
		}
	}

	return "0"
}

// decodeHexToDecimal converts a hex string (0x...) to decimal string
// Used for decoding uint256 values (big-endian, as used by Ethereum)
func decodeHexToDecimal(hexStr string) string {
	if hexStr == "" || hexStr == "0x" {
		return "0"
	}

	// Remove 0x prefix
	if len(hexStr) >= 2 && hexStr[:2] == "0x" {
		hexStr = hexStr[2:]
	}

	// Parse as big.Int (big-endian hex string)
	value := new(big.Int)
	_, ok := value.SetString(hexStr, 16)
	if !ok {
		return "0"
	}

	return value.String()
}

// decodeHexToSignedDecimal converts a hex string to signed decimal (int256)
// Handles two's complement for negative numbers
func decodeHexToSignedDecimal(hexStr string) string {
	if hexStr == "" {
		return "0"
	}

	// Parse as unsigned first
	value := new(big.Int)
	value.SetString(hexStr, 16)

	// Check if negative (high bit set) - for 256-bit numbers
	// If first hex char >= 8, it's negative in two's complement
	if len(hexStr) == 64 && hexStr[0] >= '8' {
		// Two's complement: subtract 2^256
		maxUint256 := new(big.Int)
		maxUint256.SetString("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", 16)
		maxUint256.Add(maxUint256, big.NewInt(1)) // 2^256

		value.Sub(value, maxUint256)
	}

	return value.String()
}

// trimNegative removes the negative sign from a string
func trimNegative(s string) string {
	if len(s) > 0 && s[0] == '-' {
		return s[1:]
	}
	return s
}
