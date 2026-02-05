package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"

	"github.com/address-scanner/internal/types"
)

// BalanceRepository calculates address balances from transaction history
// This is used for paid tier users who have full transaction history
// Includes both regular transactions AND Goldsky internal transactions
type BalanceRepository struct {
	db *ClickHouseDB
}

// NewBalanceRepository creates a new balance repository
func NewBalanceRepository(db *ClickHouseDB) *BalanceRepository {
	return &BalanceRepository{db: db}
}

// AddressBalance represents calculated balance for an address on a chain
type AddressBalance struct {
	Address       string               `json:"address"`
	Chain         types.ChainID        `json:"chain"`
	NativeBalance string               `json:"nativeBalance"` // Wei as string
	TokenBalances []TokenBalanceResult `json:"tokenBalances"`
	TxCountIn     int64                `json:"txCountIn"`
	TxCountOut    int64                `json:"txCountOut"`
	FirstTxTime   int64                `json:"firstTxTime"`
	LastTxTime    int64                `json:"lastTxTime"`
	LastBlock     uint64               `json:"lastBlock"`
}

// TokenBalanceResult represents a token balance
type TokenBalanceResult struct {
	TokenAddress string `json:"tokenAddress"`
	Symbol       string `json:"symbol"`
	Decimals     int    `json:"decimals"`
	Balance      string `json:"balance"` // Raw balance as string
}

// GetNativeBalance calculates native token balance for an address on a chain
// Combines: regular transactions + Goldsky internal transactions (traces)
// Formula: sum(in values) - sum(out values) - sum(gas fees)
func (r *BalanceRepository) GetNativeBalance(ctx context.Context, address string, chain types.ChainID) (*big.Int, error) {
	address = strings.ToLower(address)

	// 1. Calculate from regular transactions table
	regularBalance, err := r.getNativeBalanceFromTransactions(ctx, address, chain)
	if err != nil {
		return nil, fmt.Errorf("failed to get balance from transactions: %w", err)
	}

	// 2. Calculate from Goldsky traces (internal transactions)
	// goldsky_traces has `value` as Decimal(38,0) which is the ETH value
	internalBalance, err := r.getNativeBalanceFromGoldskyTraces(ctx, address, chain)
	if err != nil {
		// Log but don't fail - Goldsky data might not exist yet
		fmt.Printf("Warning: failed to get internal tx balance for %s on %s: %v\n", address, chain, err)
		internalBalance = big.NewInt(0)
	}

	// Total balance = regular + internal
	totalBalance := new(big.Int).Add(regularBalance, internalBalance)

	return totalBalance, nil
}

// getNativeBalanceFromTransactions calculates balance from regular transactions
func (r *BalanceRepository) getNativeBalanceFromTransactions(ctx context.Context, address string, chain types.ChainID) (*big.Int, error) {
	query := `
		SELECT 
			direction,
			value,
			gas_used,
			gas_price
		FROM transactions
		WHERE address = ? 
		  AND chain = ?
		  AND status = 'success'
	`

	rows, err := r.db.Conn().Query(ctx, query, address, string(chain))
	if err != nil {
		return nil, fmt.Errorf("failed to query transactions: %w", err)
	}
	defer rows.Close()

	totalIn := big.NewInt(0)
	totalOut := big.NewInt(0)
	totalGas := big.NewInt(0)

	for rows.Next() {
		var direction string
		var value string
		var gasUsed *string
		var gasPrice *string

		if err := rows.Scan(&direction, &value, &gasUsed, &gasPrice); err != nil {
			return nil, fmt.Errorf("failed to scan transaction: %w", err)
		}

		// Parse value
		valueBig := new(big.Int)
		if value != "" && value != "0" {
			valueBig.SetString(value, 10)
		}

		// Add to appropriate total based on direction
		if direction == "in" {
			totalIn.Add(totalIn, valueBig)
		} else if direction == "out" {
			totalOut.Add(totalOut, valueBig)

			// For outgoing transactions, also subtract gas fees
			if gasUsed != nil && gasPrice != nil && *gasUsed != "" && *gasPrice != "" {
				gasUsedBig := new(big.Int)
				gasPriceBig := new(big.Int)
				gasUsedBig.SetString(*gasUsed, 10)
				gasPriceBig.SetString(*gasPrice, 10)

				gasFee := new(big.Int).Mul(gasUsedBig, gasPriceBig)
				totalGas.Add(totalGas, gasFee)
			}
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating transactions: %w", err)
	}

	// Balance = totalIn - totalOut - totalGas
	balance := new(big.Int).Sub(totalIn, totalOut)
	balance.Sub(balance, totalGas)

	return balance, nil
}

// getNativeBalanceFromGoldskyTraces calculates balance from internal transactions
// Internal transactions are contract-to-contract or contract-to-EOA transfers
// that don't appear in regular transaction lists
func (r *BalanceRepository) getNativeBalanceFromGoldskyTraces(ctx context.Context, address string, chain types.ChainID) (*big.Int, error) {
	// Query incoming internal transactions (to_address = our address)
	inQuery := `
		SELECT sum(value)
		FROM goldsky_traces
		WHERE lower(to_address) = ?
		  AND chain = ?
		  AND status = 1
	`

	var totalIn big.Int
	row := r.db.Conn().QueryRow(ctx, inQuery, address, string(chain))
	var inValue interface{}
	if err := row.Scan(&inValue); err != nil {
		// No data is fine
		totalIn.SetInt64(0)
	} else if inValue != nil {
		// Handle Decimal type from ClickHouse
		switch v := inValue.(type) {
		case *big.Int:
			totalIn.Set(v)
		case int64:
			totalIn.SetInt64(v)
		case string:
			totalIn.SetString(v, 10)
		}
	}

	// Query outgoing internal transactions (from_address = our address)
	outQuery := `
		SELECT sum(value)
		FROM goldsky_traces
		WHERE lower(from_address) = ?
		  AND chain = ?
		  AND status = 1
	`

	var totalOut big.Int
	row = r.db.Conn().QueryRow(ctx, outQuery, address, string(chain))
	var outValue interface{}
	if err := row.Scan(&outValue); err != nil {
		totalOut.SetInt64(0)
	} else if outValue != nil {
		switch v := outValue.(type) {
		case *big.Int:
			totalOut.Set(v)
		case int64:
			totalOut.SetInt64(v)
		case string:
			totalOut.SetString(v, 10)
		}
	}

	// Internal balance = incoming - outgoing
	balance := new(big.Int).Sub(&totalIn, &totalOut)

	return balance, nil
}

// GetTokenBalances calculates ERC20 token balances for an address on a chain
// Sources:
// 1. token_transfers JSON from regular transactions (parsed by Etherscan/Alchemy)
// 2. goldsky_logs Transfer events - data field contains hex-encoded amount
//
// Note: goldsky_logs.data is the raw hex value for ERC20 Transfer events
func (r *BalanceRepository) GetTokenBalances(ctx context.Context, address string, chain types.ChainID) ([]TokenBalanceResult, error) {
	address = strings.ToLower(address)

	// Map to track token balances: tokenAddress -> balance info
	tokenBalances := make(map[string]*tokenBalanceTracker)

	// 1. Get token transfers from regular transactions (most reliable - already parsed)
	if err := r.getTokenBalancesFromTransactions(ctx, address, chain, tokenBalances); err != nil {
		return nil, fmt.Errorf("failed to get token balances from transactions: %w", err)
	}

	// 2. Get token transfers from Goldsky logs (ERC20 Transfer events)
	// The `data` field contains the hex-encoded uint256 amount
	if err := r.getTokenBalancesFromGoldskyLogs(ctx, address, chain, tokenBalances); err != nil {
		// Log but don't fail - Goldsky data might not exist yet
		fmt.Printf("Warning: failed to get token balances from Goldsky logs: %v\n", err)
	}

	// Convert to result slice
	results := make([]TokenBalanceResult, 0, len(tokenBalances))
	for _, tracker := range tokenBalances {
		// Only include tokens with non-zero balance
		if tracker.balance.Sign() > 0 {
			results = append(results, TokenBalanceResult{
				TokenAddress: tracker.tokenAddress,
				Symbol:       tracker.symbol,
				Decimals:     tracker.decimals,
				Balance:      tracker.balance.String(),
			})
		}
	}

	return results, nil
}

// getTokenBalancesFromTransactions extracts token balances from transaction token_transfers
func (r *BalanceRepository) getTokenBalancesFromTransactions(ctx context.Context, address string, chain types.ChainID, tokenBalances map[string]*tokenBalanceTracker) error {
	query := `
		SELECT token_transfers
		FROM transactions
		WHERE address = ?
		  AND chain = ?
		  AND status = 'success'
		  AND token_transfers != '[]'
		  AND is_spam = 0
	`

	rows, err := r.db.Conn().Query(ctx, query, address, string(chain))
	if err != nil {
		return fmt.Errorf("failed to query token transfers: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var tokenTransfersJSON string
		if err := rows.Scan(&tokenTransfersJSON); err != nil {
			return fmt.Errorf("failed to scan token transfers: %w", err)
		}

		var transfers []types.TokenTransfer
		if err := json.Unmarshal([]byte(tokenTransfersJSON), &transfers); err != nil {
			continue // Skip malformed JSON
		}

		for _, transfer := range transfers {
			r.processTokenTransfer(address, transfer, tokenBalances)
		}
	}

	return rows.Err()
}

// getTokenBalancesFromGoldskyLogs extracts token balances from Goldsky Transfer events
// ERC20 Transfer event signature: 0xddf252ad (Transfer(address,address,uint256))
// The `amount` column contains the decoded decimal value
func (r *BalanceRepository) getTokenBalancesFromGoldskyLogs(ctx context.Context, address string, chain types.ChainID, tokenBalances map[string]*tokenBalanceTracker) error {
	// ERC20 Transfer event signature
	transferSig := "0xddf252ad"

	// Query incoming token transfers (to_address = our address)
	inQuery := `
		SELECT 
			contract_address,
			amount
		FROM goldsky_logs
		WHERE lower(to_address) = ?
		  AND chain = ?
		  AND startsWith(event_signature, ?)
		  AND amount != ''
		  AND amount != '0'
	`

	rows, err := r.db.Conn().Query(ctx, inQuery, address, string(chain), transferSig)
	if err != nil {
		return fmt.Errorf("failed to query incoming token transfers: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var contractAddr, amount string
		if err := rows.Scan(&contractAddr, &amount); err != nil {
			continue
		}

		tokenAddr := strings.ToLower(contractAddr)
		if tokenBalances[tokenAddr] == nil {
			tokenBalances[tokenAddr] = &tokenBalanceTracker{
				tokenAddress: tokenAddr,
				symbol:       "UNKNOWN",
				decimals:     18,
				balance:      big.NewInt(0),
			}
		}

		// Parse amount (already decoded to decimal string)
		valueBig := new(big.Int)
		valueBig.SetString(amount, 10)
		tokenBalances[tokenAddr].balance.Add(tokenBalances[tokenAddr].balance, valueBig)
	}

	// Query outgoing token transfers (from_address = our address)
	outQuery := `
		SELECT 
			contract_address,
			amount
		FROM goldsky_logs
		WHERE lower(from_address) = ?
		  AND chain = ?
		  AND startsWith(event_signature, ?)
		  AND amount != ''
		  AND amount != '0'
	`

	rows2, err := r.db.Conn().Query(ctx, outQuery, address, string(chain), transferSig)
	if err != nil {
		return fmt.Errorf("failed to query outgoing token transfers: %w", err)
	}
	defer rows2.Close()

	for rows2.Next() {
		var contractAddr, amount string
		if err := rows2.Scan(&contractAddr, &amount); err != nil {
			continue
		}

		tokenAddr := strings.ToLower(contractAddr)
		if tokenBalances[tokenAddr] == nil {
			tokenBalances[tokenAddr] = &tokenBalanceTracker{
				tokenAddress: tokenAddr,
				symbol:       "UNKNOWN",
				decimals:     18,
				balance:      big.NewInt(0),
			}
		}

		// Parse amount (already decoded to decimal string)
		valueBig := new(big.Int)
		valueBig.SetString(amount, 10)
		tokenBalances[tokenAddr].balance.Sub(tokenBalances[tokenAddr].balance, valueBig)
	}

	return nil
}

// parseHexValue parses a hex-encoded uint256 value from log data
// Handles: "0x..." prefix, empty strings, and padded values
func (r *BalanceRepository) parseHexValue(hexData string) *big.Int {
	valueBig := new(big.Int)

	if hexData == "" || hexData == "0x" {
		return valueBig
	}

	// Remove 0x prefix if present
	hexData = strings.TrimPrefix(hexData, "0x")

	// Remove leading zeros (but keep at least one digit)
	hexData = strings.TrimLeft(hexData, "0")
	if hexData == "" {
		return valueBig
	}

	valueBig.SetString(hexData, 16)
	return valueBig
}

// processTokenTransfer processes a single token transfer and updates balances
func (r *BalanceRepository) processTokenTransfer(address string, transfer types.TokenTransfer, tokenBalances map[string]*tokenBalanceTracker) {
	if transfer.Token == "" {
		return
	}

	tokenAddr := strings.ToLower(transfer.Token)

	// Initialize tracker if not exists
	if tokenBalances[tokenAddr] == nil {
		decimals := 18
		if transfer.Decimals != nil {
			decimals = *transfer.Decimals
		}
		symbol := "UNKNOWN"
		if transfer.Symbol != nil {
			symbol = *transfer.Symbol
		}

		tokenBalances[tokenAddr] = &tokenBalanceTracker{
			tokenAddress: tokenAddr,
			symbol:       symbol,
			decimals:     decimals,
			balance:      big.NewInt(0),
		}
	}

	// Parse transfer value
	valueBig := new(big.Int)
	if transfer.Value != "" && transfer.Value != "0" {
		valueBig.SetString(transfer.Value, 10)
	}

	// Determine direction: if 'to' matches our address, it's incoming
	if strings.EqualFold(transfer.To, address) {
		tokenBalances[tokenAddr].balance.Add(tokenBalances[tokenAddr].balance, valueBig)
	} else if strings.EqualFold(transfer.From, address) {
		tokenBalances[tokenAddr].balance.Sub(tokenBalances[tokenAddr].balance, valueBig)
	}
}

// tokenBalanceTracker tracks token balance during calculation
type tokenBalanceTracker struct {
	tokenAddress string
	symbol       string
	decimals     int
	balance      *big.Int
}

// GetAddressBalance gets complete balance info for an address on a chain
// Uses pre-aggregated materialized views for fast queries
func (r *BalanceRepository) GetAddressBalance(ctx context.Context, address string, chain types.ChainID) (*AddressBalance, error) {
	address = strings.ToLower(address)

	// Query native balance from aggregation table (single query)
	nativeQuery := `
		SELECT 
			toString(sum(balance_in) - sum(balance_out) - sum(gas_spent)) as balance,
			sum(tx_count_in) as tx_count_in,
			sum(tx_count_out) as tx_count_out,
			min(first_seen) as first_seen,
			max(last_seen) as last_seen
		FROM native_balances_agg
		WHERE address = ? AND chain = ?
	`

	var nativeBalance string
	var txCountIn, txCountOut uint64
	var firstSeen, lastSeen int64

	row := r.db.Conn().QueryRow(ctx, nativeQuery, address, string(chain))
	if err := row.Scan(&nativeBalance, &txCountIn, &txCountOut, &firstSeen, &lastSeen); err != nil {
		nativeBalance = "0"
		txCountIn, txCountOut = 0, 0
	}

	// Query token balances from aggregation table (single query)
	tokenQuery := `
		SELECT 
			token_address,
			any(token_symbol) as symbol,
			any(token_decimals) as decimals,
			toString(sum(balance_in) - sum(balance_out)) as balance
		FROM token_balances_agg
		WHERE address = ? AND chain = ?
		GROUP BY token_address
		HAVING sum(balance_in) - sum(balance_out) > 0
	`

	rows, err := r.db.Conn().Query(ctx, tokenQuery, address, string(chain))
	if err != nil {
		return nil, fmt.Errorf("failed to query token balances: %w", err)
	}
	defer rows.Close()

	tokenBalances := make([]TokenBalanceResult, 0)
	for rows.Next() {
		var tb TokenBalanceResult
		if err := rows.Scan(&tb.TokenAddress, &tb.Symbol, &tb.Decimals, &tb.Balance); err != nil {
			continue
		}
		if tb.Symbol == "" {
			tb.Symbol = "UNKNOWN"
		}
		tokenBalances = append(tokenBalances, tb)
	}

	// If no data at all, return zero balance
	if txCountIn == 0 && txCountOut == 0 && len(tokenBalances) == 0 {
		return &AddressBalance{
			Address:       address,
			Chain:         chain,
			NativeBalance: "0",
			TokenBalances: []TokenBalanceResult{},
		}, nil
	}

	return &AddressBalance{
		Address:       address,
		Chain:         chain,
		NativeBalance: nativeBalance,
		TokenBalances: tokenBalances,
		TxCountIn:     int64(txCountIn),
		TxCountOut:    int64(txCountOut),
		FirstTxTime:   firstSeen,
		LastTxTime:    lastSeen,
	}, nil
}

// GetMultiChainBalance gets balances for an address across all chains
func (r *BalanceRepository) GetMultiChainBalance(ctx context.Context, address string, chains []types.ChainID) ([]AddressBalance, error) {
	address = strings.ToLower(address)

	results := make([]AddressBalance, 0, len(chains))
	for _, chain := range chains {
		balance, err := r.GetAddressBalance(ctx, address, chain)
		if err != nil {
			// Log error but continue with other chains
			continue
		}
		results = append(results, *balance)
	}

	return results, nil
}

// GetBalanceSummary gets a quick summary of balances for multiple addresses
// This is optimized for portfolio views - uses batch queries
func (r *BalanceRepository) GetBalanceSummary(ctx context.Context, addresses []string) (map[string]map[types.ChainID]*AddressBalance, error) {
	if len(addresses) == 0 {
		return make(map[string]map[types.ChainID]*AddressBalance), nil
	}

	// Normalize addresses
	normalizedAddrs := make([]string, len(addresses))
	for i, addr := range addresses {
		normalizedAddrs[i] = strings.ToLower(addr)
	}

	// Build result map
	result := make(map[string]map[types.ChainID]*AddressBalance)

	// Batch query native balances from aggregation table
	placeholders := strings.Repeat("?,", len(normalizedAddrs)-1) + "?"
	nativeQuery := `
		SELECT 
			address,
			chain,
			toString(sum(balance_in) - sum(balance_out) - sum(gas_spent)) as balance,
			sum(tx_count_in) as tx_count_in,
			sum(tx_count_out) as tx_count_out,
			min(first_seen) as first_seen,
			max(last_seen) as last_seen
		FROM native_balances_agg
		WHERE address IN (` + placeholders + `)
		GROUP BY address, chain
	`

	args := make([]any, len(normalizedAddrs))
	for i, addr := range normalizedAddrs {
		args[i] = addr
	}

	rows, err := r.db.Conn().Query(ctx, nativeQuery, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query native balances: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var address, chainStr, balance string
		var txCountIn, txCountOut uint64
		var firstSeen, lastSeen int64

		if err := rows.Scan(&address, &chainStr, &balance, &txCountIn, &txCountOut, &firstSeen, &lastSeen); err != nil {
			continue
		}

		chain := types.ChainID(chainStr)
		if result[address] == nil {
			result[address] = make(map[types.ChainID]*AddressBalance)
		}

		result[address][chain] = &AddressBalance{
			Address:       address,
			Chain:         chain,
			NativeBalance: balance,
			TokenBalances: []TokenBalanceResult{},
			TxCountIn:     int64(txCountIn),
			TxCountOut:    int64(txCountOut),
			FirstTxTime:   firstSeen,
			LastTxTime:    lastSeen,
		}
	}

	// Batch query token balances
	tokenQuery := `
		SELECT 
			address,
			chain,
			token_address,
			any(token_symbol) as symbol,
			any(token_decimals) as decimals,
			toString(sum(balance_in) - sum(balance_out)) as balance
		FROM token_balances_agg
		WHERE address IN (` + placeholders + `)
		GROUP BY address, chain, token_address
		HAVING sum(balance_in) - sum(balance_out) > 0
	`

	rows2, err := r.db.Conn().Query(ctx, tokenQuery, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query token balances: %w", err)
	}
	defer rows2.Close()

	for rows2.Next() {
		var address, chainStr string
		var tb TokenBalanceResult
		if err := rows2.Scan(&address, &chainStr, &tb.TokenAddress, &tb.Symbol, &tb.Decimals, &tb.Balance); err != nil {
			continue
		}

		chain := types.ChainID(chainStr)
		if tb.Symbol == "" {
			tb.Symbol = "UNKNOWN"
		}

		// Ensure address/chain entry exists
		if result[address] == nil {
			result[address] = make(map[types.ChainID]*AddressBalance)
		}
		if result[address][chain] == nil {
			result[address][chain] = &AddressBalance{
				Address:       address,
				Chain:         chain,
				NativeBalance: "0",
				TokenBalances: []TokenBalanceResult{},
			}
		}

		result[address][chain].TokenBalances = append(result[address][chain].TokenBalances, tb)
	}

	return result, nil
}

// HasCompleteHistory checks if an address has complete transaction history
// (i.e., backfill is complete)
func (r *BalanceRepository) HasCompleteHistory(ctx context.Context, address string, chain types.ChainID) (bool, error) {
	// This would check against the backfill_jobs table or sync_status
	// For now, we assume if there are any transactions, history exists
	query := `SELECT count() FROM transactions WHERE address = ? AND chain = ? LIMIT 1`

	var count uint64
	row := r.db.Conn().QueryRow(ctx, query, strings.ToLower(address), string(chain))
	if err := row.Scan(&count); err != nil {
		return false, err
	}

	return count > 0, nil
}
