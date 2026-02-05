package storage

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/address-scanner/internal/models"
	"github.com/address-scanner/internal/types"
)

// TransactionRepository handles transaction data persistence in ClickHouse
type TransactionRepository struct {
	db             *ClickHouseDB
	optimizer      *QueryOptimizer
	tokenWhitelist *TokenWhitelistRepository
}

// NewTransactionRepository creates a new transaction repository
func NewTransactionRepository(db *ClickHouseDB) *TransactionRepository {
	return &TransactionRepository{
		db:        db,
		optimizer: NewQueryOptimizer(db),
	}
}

// SetTokenWhitelist sets the token whitelist repository for spam detection
func (r *TransactionRepository) SetTokenWhitelist(whitelist *TokenWhitelistRepository) {
	r.tokenWhitelist = whitelist
}

// MarkSpamTokens marks token transactions as spam if not in whitelist
func (r *TransactionRepository) MarkSpamTokens(transactions []*models.Transaction) {
	if r.tokenWhitelist == nil {
		return
	}

	for _, tx := range transactions {
		// Only check token transfers (not native)
		if tx.TransferType == types.TransferTypeNative {
			continue
		}
		if tx.TokenAddress == "" {
			continue
		}

		// Mark as spam if token is not whitelisted
		if !r.tokenWhitelist.IsWhitelisted(string(tx.Chain), tx.TokenAddress) {
			tx.IsSpam = 1
		}
	}
}

// Insert inserts a single transaction record
func (r *TransactionRepository) Insert(ctx context.Context, tx *models.Transaction) error {
	if err := ValidateAddress(tx.Address); err != nil {
		return err
	}

	chain := string(types.NormalizeChainID(string(tx.Chain)))

	query := `
		INSERT INTO transactions (
			tx_hash, log_index, chain, address,
			tx_from, tx_to,
			transfer_type, transfer_from, transfer_to, value, direction,
			token_address, token_symbol, token_decimals, token_id,
			block_number, timestamp, status, gas_used, gas_price, method_id, func_name, is_spam
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`

	return r.db.Conn().Exec(ctx, query,
		tx.TxHash,
		tx.LogIndex,
		chain,
		strings.ToLower(tx.Address),
		strings.ToLower(tx.TxFrom),
		strings.ToLower(tx.TxTo),
		string(tx.TransferType),
		strings.ToLower(tx.TransferFrom),
		strings.ToLower(tx.TransferTo),
		tx.Value,
		string(tx.Direction),
		strings.ToLower(tx.TokenAddress),
		tx.TokenSymbol,
		tx.TokenDecimals,
		tx.TokenID,
		tx.BlockNumber,
		tx.Timestamp,
		tx.Status,
		tx.GasUsed,
		tx.GasPrice,
		tx.MethodID,
		tx.FuncName,
		tx.IsSpam,
	)
}

// BatchInsert inserts multiple transaction records
func (r *TransactionRepository) BatchInsert(ctx context.Context, transactions []*models.Transaction) error {
	if len(transactions) == 0 {
		return nil
	}

	// Mark spam tokens before inserting
	r.MarkSpamTokens(transactions)

	batch, err := r.db.Conn().PrepareBatch(ctx, `
		INSERT INTO transactions (
			tx_hash, log_index, chain, address,
			tx_from, tx_to,
			transfer_type, transfer_from, transfer_to, value, direction,
			token_address, token_symbol, token_decimals, token_id,
			block_number, timestamp, status, gas_used, gas_price, method_id, func_name, is_spam
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	for _, tx := range transactions {
		if err := ValidateAddress(tx.Address); err != nil {
			return fmt.Errorf("invalid address %s: %w", tx.Address, err)
		}

		chain := string(types.NormalizeChainID(string(tx.Chain)))

		err = batch.Append(
			tx.TxHash,
			tx.LogIndex,
			chain,
			strings.ToLower(tx.Address),
			strings.ToLower(tx.TxFrom),
			strings.ToLower(tx.TxTo),
			string(tx.TransferType),
			strings.ToLower(tx.TransferFrom),
			strings.ToLower(tx.TransferTo),
			tx.Value,
			string(tx.Direction),
			strings.ToLower(tx.TokenAddress),
			tx.TokenSymbol,
			tx.TokenDecimals,
			tx.TokenID,
			tx.BlockNumber,
			tx.Timestamp,
			tx.Status,
			tx.GasUsed,
			tx.GasPrice,
			tx.MethodID,
			tx.FuncName,
			tx.IsSpam,
		)
		if err != nil {
			return fmt.Errorf("failed to append tx %s: %w", tx.TxHash, err)
		}
	}

	return batch.Send()
}

// BatchInsertFromNormalized converts and inserts normalized transactions
func (r *TransactionRepository) BatchInsertFromNormalized(ctx context.Context, normalized []*types.NormalizedTransaction, address string) error {
	var records []*models.Transaction
	for _, nt := range normalized {
		records = append(records, models.FromNormalizedTransaction(nt, address)...)
	}
	return r.BatchInsert(ctx, records)
}

// GetByAddress retrieves transactions for an address with filters
func (r *TransactionRepository) GetByAddress(ctx context.Context, address string, filters *TransactionFilters) ([]*models.Transaction, error) {
	if err := ValidateAddress(address); err != nil {
		return nil, err
	}
	address = strings.ToLower(address)

	query := `
		SELECT tx_hash, log_index, chain, address,
			   tx_from, tx_to,
			   transfer_type, transfer_from, transfer_to, value, direction,
			   token_address, token_symbol, token_decimals, token_id,
			   block_number, timestamp, status, gas_used, gas_price, method_id, func_name
		FROM transactions
		WHERE address = ?
	`
	args := []any{address}

	// Apply filters
	if filters != nil {
		if len(filters.Chains) > 0 {
			placeholders := strings.Repeat("?,", len(filters.Chains)-1) + "?"
			query += " AND chain IN (" + placeholders + ")"
			for _, chain := range filters.Chains {
				args = append(args, string(chain))
			}
		}

		if filters.TransferType != nil {
			query += " AND transfer_type = ?"
			args = append(args, string(*filters.TransferType))
		}

		if filters.TokenAddress != nil {
			query += " AND token_address = ?"
			args = append(args, strings.ToLower(*filters.TokenAddress))
		}

		if filters.Direction != nil {
			query += " AND direction = ?"
			args = append(args, string(*filters.Direction))
		}

		if filters.DateFrom != nil {
			query += " AND timestamp >= ?"
			args = append(args, *filters.DateFrom)
		}

		if filters.DateTo != nil {
			query += " AND timestamp <= ?"
			args = append(args, *filters.DateTo)
		}

		if filters.Status != nil {
			query += " AND status = ?"
			args = append(args, string(*filters.Status))
		}
	}

	// Ordering
	orderBy := "timestamp DESC, log_index ASC"
	if filters != nil && filters.SortBy != "" {
		switch filters.SortBy {
		case "timestamp":
			orderBy = "timestamp"
		case "block_number":
			orderBy = "block_number"
		}
		if filters.SortOrder == "asc" {
			orderBy += " ASC"
		} else {
			orderBy += " DESC"
		}
	}
	query += " ORDER BY " + orderBy

	// Pagination
	if filters != nil {
		if filters.Limit > 0 {
			query += fmt.Sprintf(" LIMIT %d", filters.Limit)
		}
		if filters.Offset > 0 {
			query += fmt.Sprintf(" OFFSET %d", filters.Offset)
		}
	}

	rows, err := r.db.Conn().Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query transactions: %w", err)
	}
	defer rows.Close()

	return r.scanTransactions(rows)
}

// GetByHash retrieves all transfer records for a transaction hash
func (r *TransactionRepository) GetByHash(ctx context.Context, hash string) ([]*models.Transaction, error) {
	query := `
		SELECT tx_hash, log_index, chain, address,
			   tx_from, tx_to,
			   transfer_type, transfer_from, transfer_to, value, direction,
			   token_address, token_symbol, token_decimals, token_id,
			   block_number, timestamp, status, gas_used, gas_price, method_id, func_name
		FROM transactions
		WHERE tx_hash = ?
		ORDER BY log_index ASC
	`

	rows, err := r.db.Conn().Query(ctx, query, hash)
	if err != nil {
		return nil, fmt.Errorf("failed to query by hash: %w", err)
	}
	defer rows.Close()

	return r.scanTransactions(rows)
}

// GetNativeBalance calculates native token balance for an address on a chain
func (r *TransactionRepository) GetNativeBalance(ctx context.Context, address string, chain types.ChainID) (string, error) {
	address = strings.ToLower(address)

	query := `
		SELECT 
			toString(
				sumIf(toInt256OrZero(value), direction = 'in') - 
				sumIf(toInt256OrZero(value), direction = 'out') -
				sumIf(toInt256OrZero(gas_used) * toInt256OrZero(gas_price), direction = 'out' AND tx_from = address)
			) as balance
		FROM transactions
		WHERE address = ? AND chain = ? AND transfer_type = 'native' AND status = 'success'
	`

	var balance string
	row := r.db.Conn().QueryRow(ctx, query, address, string(chain))
	if err := row.Scan(&balance); err != nil {
		return "0", err
	}
	if balance == "" {
		return "0", nil
	}
	return balance, nil
}

// GetTokenBalances calculates token balances for an address on a chain
func (r *TransactionRepository) GetTokenBalances(ctx context.Context, address string, chain types.ChainID) ([]TokenBalanceResult, error) {
	address = strings.ToLower(address)

	query := `
		SELECT 
			token_address,
			any(token_symbol) as symbol,
			any(token_decimals) as decimals,
			toString(sumIf(toInt256OrZero(value), direction = 'in') - sumIf(toInt256OrZero(value), direction = 'out')) as balance
		FROM transactions
		WHERE address = ? AND chain = ? AND transfer_type IN ('erc20', 'erc721', 'erc1155') AND status = 'success'
		GROUP BY token_address
		HAVING balance != '0'
	`

	rows, err := r.db.Conn().Query(ctx, query, address, string(chain))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var balances []TokenBalanceResult
	for rows.Next() {
		var b TokenBalanceResult
		if err := rows.Scan(&b.TokenAddress, &b.Symbol, &b.Decimals, &b.Balance); err != nil {
			return nil, err
		}
		balances = append(balances, b)
	}
	return balances, nil
}

// CountByAddress counts transactions for an address
func (r *TransactionRepository) CountByAddress(ctx context.Context, address string, filters *TransactionFilters) (int64, error) {
	if err := ValidateAddress(address); err != nil {
		return 0, err
	}
	address = strings.ToLower(address)

	query := `SELECT COUNT(*) FROM transactions WHERE address = ?`
	args := []any{address}

	if filters != nil {
		if len(filters.Chains) > 0 {
			placeholders := strings.Repeat("?,", len(filters.Chains)-1) + "?"
			query += " AND chain IN (" + placeholders + ")"
			for _, chain := range filters.Chains {
				args = append(args, string(chain))
			}
		}
		if filters.TransferType != nil {
			query += " AND transfer_type = ?"
			args = append(args, string(*filters.TransferType))
		}
	}

	var count uint64
	if err := r.db.Conn().QueryRow(ctx, query, args...).Scan(&count); err != nil {
		return 0, err
	}
	return int64(count), nil
}

// CountByAddressAndChain counts transactions for an address on a specific chain
func (r *TransactionRepository) CountByAddressAndChain(ctx context.Context, address string, chain types.ChainID) (int64, error) {
	if err := ValidateAddress(address); err != nil {
		return 0, err
	}
	address = strings.ToLower(address)

	var count uint64
	err := r.db.Conn().QueryRow(ctx,
		`SELECT COUNT(*) FROM transactions WHERE address = ? AND chain = ?`,
		address, string(chain),
	).Scan(&count)
	return int64(count), err
}

// DeleteByAddress deletes all transactions for an address
func (r *TransactionRepository) DeleteByAddress(ctx context.Context, address string) error {
	if err := ValidateAddress(address); err != nil {
		return err
	}
	return r.db.Conn().Exec(ctx, `ALTER TABLE transactions DELETE WHERE address = ?`, strings.ToLower(address))
}

// GetByAddresses retrieves transactions for multiple addresses with filters
func (r *TransactionRepository) GetByAddresses(ctx context.Context, addresses []string, filters *TransactionFilters) ([]*models.Transaction, error) {
	if len(addresses) == 0 {
		return []*models.Transaction{}, nil
	}

	// Normalize addresses
	normalizedAddrs := make([]string, len(addresses))
	for i, addr := range addresses {
		if err := ValidateAddress(addr); err != nil {
			return nil, err
		}
		normalizedAddrs[i] = strings.ToLower(addr)
	}

	placeholders := strings.Repeat("?,", len(normalizedAddrs)-1) + "?"
	query := fmt.Sprintf(`
		SELECT tx_hash, log_index, chain, address,
			   tx_from, tx_to,
			   transfer_type, transfer_from, transfer_to, value, direction,
			   token_address, token_symbol, token_decimals, token_id,
			   block_number, timestamp, status, gas_used, gas_price, method_id, func_name
		FROM transactions
		WHERE address IN (%s)
	`, placeholders)

	args := make([]any, len(normalizedAddrs))
	for i, addr := range normalizedAddrs {
		args[i] = addr
	}

	// Apply filters
	if filters != nil {
		if len(filters.Chains) > 0 {
			chainPlaceholders := strings.Repeat("?,", len(filters.Chains)-1) + "?"
			query += " AND chain IN (" + chainPlaceholders + ")"
			for _, chain := range filters.Chains {
				args = append(args, string(chain))
			}
		}

		if filters.TransferType != nil {
			query += " AND transfer_type = ?"
			args = append(args, string(*filters.TransferType))
		}

		if filters.DateFrom != nil {
			query += " AND timestamp >= ?"
			args = append(args, *filters.DateFrom)
		}

		if filters.DateTo != nil {
			query += " AND timestamp <= ?"
			args = append(args, *filters.DateTo)
		}

		if filters.Status != nil {
			query += " AND status = ?"
			args = append(args, string(*filters.Status))
		}
	}

	// Ordering
	orderBy := "timestamp DESC, log_index ASC"
	if filters != nil && filters.SortBy != "" {
		switch filters.SortBy {
		case "timestamp":
			orderBy = "timestamp"
		case "block_number":
			orderBy = "block_number"
		}
		if filters.SortOrder == "asc" {
			orderBy += " ASC"
		} else {
			orderBy += " DESC"
		}
	}
	query += " ORDER BY " + orderBy

	// Pagination
	if filters != nil {
		if filters.Limit > 0 {
			query += fmt.Sprintf(" LIMIT %d", filters.Limit)
		}
		if filters.Offset > 0 {
			query += fmt.Sprintf(" OFFSET %d", filters.Offset)
		}
	}

	rows, err := r.db.Conn().Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query transactions: %w", err)
	}
	defer rows.Close()

	return r.scanTransactions(rows)
}

// GetOptimizer returns the query optimizer
func (r *TransactionRepository) GetOptimizer() *QueryOptimizer {
	return r.optimizer
}

func (r *TransactionRepository) scanTransactions(rows interface {
	Next() bool
	Scan(dest ...any) error
}) ([]*models.Transaction, error) {
	var transactions []*models.Transaction
	for rows.Next() {
		var tx models.Transaction
		var chainStr, transferTypeStr, directionStr string

		err := rows.Scan(
			&tx.TxHash, &tx.LogIndex, &chainStr, &tx.Address,
			&tx.TxFrom, &tx.TxTo,
			&transferTypeStr, &tx.TransferFrom, &tx.TransferTo, &tx.Value, &directionStr,
			&tx.TokenAddress, &tx.TokenSymbol, &tx.TokenDecimals, &tx.TokenID,
			&tx.BlockNumber, &tx.Timestamp, &tx.Status, &tx.GasUsed, &tx.GasPrice, &tx.MethodID, &tx.FuncName,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan: %w", err)
		}

		tx.Chain = types.ChainID(chainStr)
		tx.TransferType = types.TransferType(transferTypeStr)
		tx.Direction = types.TransactionDirection(directionStr)

		transactions = append(transactions, &tx)
	}
	return transactions, nil
}

// TransactionFilters defines filters for querying transactions
type TransactionFilters struct {
	Chains       []types.ChainID
	TransferType *types.TransferType
	TokenAddress *string
	Direction    *types.TransactionDirection
	DateFrom     *time.Time
	DateTo       *time.Time
	Status       *types.TransactionStatus
	SortBy       string
	SortOrder    string
	Limit        int
	Offset       int
}
