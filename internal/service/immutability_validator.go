package service

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/address-scanner/internal/models"
	"github.com/address-scanner/internal/storage"
	"github.com/address-scanner/internal/types"
)

// ImmutabilityValidator ensures confirmed transactions never change
// Requirement 12.4: Ensure transaction data immutability after blockchain confirmation
type ImmutabilityValidator struct {
	transactionRepo *storage.TransactionRepository
	// Track confirmed transactions to detect any changes
	confirmedTxCache sync.Map // map[string]*models.Transaction
	// Minimum confirmations before considering a transaction immutable
	minConfirmations uint64
}

// NewImmutabilityValidator creates a new immutability validator
func NewImmutabilityValidator(
	transactionRepo *storage.TransactionRepository,
	minConfirmations uint64,
) *ImmutabilityValidator {
	return &ImmutabilityValidator{
		transactionRepo:  transactionRepo,
		minConfirmations: minConfirmations,
	}
}

// ValidationResult represents the result of an immutability validation
type ValidationResult struct {
	TransactionHash string    `json:"transactionHash"`
	Valid           bool      `json:"valid"`
	Violations      []string  `json:"violations,omitempty"`
	CheckedAt       time.Time `json:"checkedAt"`
	Confirmed       bool      `json:"confirmed"`
	Confirmations   uint64    `json:"confirmations"`
}

// ValidateTransaction checks if a transaction has remained immutable
// Returns error if transaction has changed after confirmation
func (iv *ImmutabilityValidator) ValidateTransaction(ctx context.Context, hash string, currentBlock uint64) (*ValidationResult, error) {
	result := &ValidationResult{
		TransactionHash: hash,
		CheckedAt:       time.Now(),
		Valid:           true,
	}

	// Fetch current transaction from database
	currentTxs, err := iv.transactionRepo.GetByHash(ctx, hash)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch transaction: %w", err)
	}
	if len(currentTxs) == 0 {
		return nil, fmt.Errorf("transaction not found: %s", hash)
	}
	currentTx := currentTxs[0]

	// Calculate confirmations
	result.Confirmations = currentBlock - currentTx.BlockNumber
	result.Confirmed = result.Confirmations >= iv.minConfirmations

	// If not yet confirmed, skip immutability check
	if !result.Confirmed {
		return result, nil
	}

	// Check if we have a cached version of this confirmed transaction
	cachedValue, exists := iv.confirmedTxCache.Load(hash)
	if !exists {
		// First time seeing this confirmed transaction - cache it
		iv.confirmedTxCache.Store(hash, currentTx)
		return result, nil
	}

	// Compare with cached version
	cachedTx := cachedValue.(*models.Transaction)
	violations := iv.compareTransactions(cachedTx, currentTx)

	if len(violations) > 0 {
		result.Valid = false
		result.Violations = violations
		log.Printf("IMMUTABILITY VIOLATION: Transaction %s has changed after confirmation: %v", hash, violations)
	}

	return result, nil
}

// compareTransactions compares two transaction records and returns any differences
func (iv *ImmutabilityValidator) compareTransactions(original, current *models.Transaction) []string {
	var violations []string

	// Critical fields that must never change
	if original.TxHash != current.TxHash {
		violations = append(violations, fmt.Sprintf("hash changed: %s -> %s", original.TxHash, current.TxHash))
	}

	if original.Chain != current.Chain {
		violations = append(violations, fmt.Sprintf("chain changed: %s -> %s", original.Chain, current.Chain))
	}

	if original.TxFrom != current.TxFrom {
		violations = append(violations, fmt.Sprintf("from changed: %s -> %s", original.TxFrom, current.TxFrom))
	}

	if original.TxTo != current.TxTo {
		violations = append(violations, fmt.Sprintf("to changed: %s -> %s", original.TxTo, current.TxTo))
	}

	if original.Value != current.Value {
		violations = append(violations, fmt.Sprintf("value changed: %s -> %s", original.Value, current.Value))
	}

	if !original.Timestamp.Equal(current.Timestamp) {
		violations = append(violations, fmt.Sprintf("timestamp changed: %v -> %v", original.Timestamp, current.Timestamp))
	}

	if original.BlockNumber != current.BlockNumber {
		violations = append(violations, fmt.Sprintf("block number changed: %d -> %d", original.BlockNumber, current.BlockNumber))
	}

	if original.Status != current.Status {
		violations = append(violations, fmt.Sprintf("status changed: %s -> %s", original.Status, current.Status))
	}

	// Gas fields should also be immutable
	if original.GasUsed != current.GasUsed {
		violations = append(violations, fmt.Sprintf("gas used changed: %s -> %s", original.GasUsed, current.GasUsed))
	}

	if original.GasPrice != current.GasPrice {
		violations = append(violations, fmt.Sprintf("gas price changed: %s -> %s", original.GasPrice, current.GasPrice))
	}

	// Method ID should be immutable
	if original.MethodID != current.MethodID {
		violations = append(violations, fmt.Sprintf("method ID changed: %s -> %s", original.MethodID, current.MethodID))
	}

	return violations
}

// ValidateTransactionBatch validates multiple transactions for immutability
func (iv *ImmutabilityValidator) ValidateTransactionBatch(ctx context.Context, hashes []string, currentBlock uint64) ([]*ValidationResult, error) {
	results := make([]*ValidationResult, 0, len(hashes))

	for _, hash := range hashes {
		result, err := iv.ValidateTransaction(ctx, hash, currentBlock)
		if err != nil {
			log.Printf("Failed to validate transaction %s: %v", hash, err)
			continue
		}
		results = append(results, result)
	}

	return results, nil
}

// ValidateAddressTransactions validates all confirmed transactions for an address
func (iv *ImmutabilityValidator) ValidateAddressTransactions(ctx context.Context, address string, chain types.ChainID, currentBlock uint64) ([]*ValidationResult, error) {
	// Fetch all transactions for this address
	filters := &storage.TransactionFilters{
		Chains: []types.ChainID{chain},
	}
	transactions, err := iv.transactionRepo.GetByAddress(ctx, address, filters)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch transactions: %w", err)
	}

	// Validate each confirmed transaction
	results := make([]*ValidationResult, 0)
	for _, tx := range transactions {
		confirmations := currentBlock - tx.BlockNumber
		if confirmations >= iv.minConfirmations {
			result, err := iv.ValidateTransaction(ctx, tx.TxHash, currentBlock)
			if err != nil {
				log.Printf("Failed to validate transaction %s: %v", tx.TxHash, err)
				continue
			}
			results = append(results, result)
		}
	}

	return results, nil
}

// PreventTransactionUpdate validates that a transaction update is allowed
// This should be called before any transaction update operation
func (iv *ImmutabilityValidator) PreventTransactionUpdate(ctx context.Context, hash string, currentBlock uint64) error {
	// Fetch the transaction
	txs, err := iv.transactionRepo.GetByHash(ctx, hash)
	if err != nil {
		return fmt.Errorf("failed to fetch transaction: %w", err)
	}
	if len(txs) == 0 {
		return fmt.Errorf("transaction not found: %s", hash)
	}
	tx := txs[0]

	// Calculate confirmations
	confirmations := currentBlock - tx.BlockNumber

	// If transaction is confirmed, prevent updates
	if confirmations >= iv.minConfirmations {
		return &types.ServiceError{
			Code:    "IMMUTABLE_TRANSACTION",
			Message: fmt.Sprintf("cannot update confirmed transaction %s (confirmations: %d)", hash, confirmations),
			Details: map[string]interface{}{
				"hash":          hash,
				"confirmations": confirmations,
				"blockNumber":   tx.BlockNumber,
			},
		}
	}

	return nil
}

// StartPeriodicValidation starts a background goroutine that periodically validates transaction immutability
func (iv *ImmutabilityValidator) StartPeriodicValidation(ctx context.Context, addresses []string, chains []types.ChainID, interval time.Duration, getCurrentBlock func(types.ChainID) uint64) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	log.Printf("Starting periodic immutability validation every %v", interval)

	for {
		select {
		case <-ctx.Done():
			log.Println("Stopping periodic immutability validation")
			return
		case <-ticker.C:
			log.Println("Running periodic immutability validation")

			violationCount := 0
			for _, address := range addresses {
				for _, chain := range chains {
					currentBlock := getCurrentBlock(chain)
					results, err := iv.ValidateAddressTransactions(ctx, address, chain, currentBlock)
					if err != nil {
						log.Printf("Error validating transactions for %s on %s: %v", address, chain, err)
						continue
					}

					for _, result := range results {
						if !result.Valid {
							violationCount++
							log.Printf("IMMUTABILITY VIOLATION: %s - %v", result.TransactionHash, result.Violations)
						}
					}
				}
			}

			if violationCount > 0 {
				log.Printf("Found %d immutability violations during periodic check", violationCount)
			} else {
				log.Println("No immutability violations found during periodic check")
			}
		}
	}
}

// ClearCache clears the confirmed transaction cache
// This should be used carefully, typically only for testing or maintenance
func (iv *ImmutabilityValidator) ClearCache() {
	iv.confirmedTxCache = sync.Map{}
	log.Println("Cleared immutability validator cache")
}

// GetCacheSize returns the number of confirmed transactions being tracked
func (iv *ImmutabilityValidator) GetCacheSize() int {
	count := 0
	iv.confirmedTxCache.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	return count
}

// ImmutabilityStats provides statistics about immutability validation
type ImmutabilityStats struct {
	TotalValidations   int       `json:"totalValidations"`
	ValidTransactions  int       `json:"validTransactions"`
	Violations         int       `json:"violations"`
	LastCheckTime      time.Time `json:"lastCheckTime"`
	CachedTransactions int       `json:"cachedTransactions"`
}

// GetStats returns statistics about immutability validation
func (iv *ImmutabilityValidator) GetStats() *ImmutabilityStats {
	return &ImmutabilityStats{
		TotalValidations:   0, // Would need to track this in production
		ValidTransactions:  0,
		Violations:         0,
		LastCheckTime:      time.Now(),
		CachedTransactions: iv.GetCacheSize(),
	}
}

// ValidateBeforeInsert ensures a transaction being inserted doesn't conflict with existing confirmed transactions
func (iv *ImmutabilityValidator) ValidateBeforeInsert(ctx context.Context, tx *models.Transaction, currentBlock uint64) error {
	// Check if this transaction hash already exists
	existingTxs, err := iv.transactionRepo.GetByHash(ctx, tx.TxHash)
	if err != nil || len(existingTxs) == 0 {
		// Transaction doesn't exist, safe to insert
		return nil
	}
	existingTx := existingTxs[0]

	// Transaction exists - check if it's confirmed
	confirmations := currentBlock - existingTx.BlockNumber
	if confirmations >= iv.minConfirmations {
		// Existing transaction is confirmed - verify new data matches
		violations := iv.compareTransactions(existingTx, tx)
		if len(violations) > 0 {
			return &types.ServiceError{
				Code:    "IMMUTABLE_TRANSACTION_CONFLICT",
				Message: fmt.Sprintf("cannot insert transaction %s: conflicts with confirmed transaction", tx.TxHash),
				Details: map[string]interface{}{
					"hash":          tx.TxHash,
					"confirmations": confirmations,
					"violations":    violations,
				},
			}
		}
	}

	return nil
}
