package service

import (
	"context"
	"fmt"

	"github.com/address-scanner/internal/models"
	"github.com/address-scanner/internal/storage"
	"github.com/address-scanner/internal/types"
)

// AddressClassifier handles address classification based on transaction count
type AddressClassifier struct {
	addressRepo *storage.AddressRepository
	txRepo      *storage.TransactionRepository
	// Threshold for classifying an address as "super"
	// Addresses with transaction count >= threshold are classified as "super"
	// Default: 10000 transactions
	superAddressThreshold int64
}

// NewAddressClassifier creates a new address classifier
func NewAddressClassifier(
	addressRepo *storage.AddressRepository,
	txRepo *storage.TransactionRepository,
	superAddressThreshold int64,
) *AddressClassifier {
	if superAddressThreshold <= 0 {
		superAddressThreshold = 10000 // Default threshold
	}
	return &AddressClassifier{
		addressRepo:           addressRepo,
		txRepo:                txRepo,
		superAddressThreshold: superAddressThreshold,
	}
}

// ClassifyAddress classifies an address on a specific chain based on transaction count
// Returns the updated address with classification
func (c *AddressClassifier) ClassifyAddress(ctx context.Context, address string, chain types.ChainID) (*models.Address, error) {
	// Get address record for this chain
	addr, err := c.addressRepo.Get(ctx, address, chain)
	if err != nil {
		return nil, fmt.Errorf("failed to get address: %w", err)
	}
	if addr == nil {
		return nil, fmt.Errorf("address not found: %s on chain %s", address, chain)
	}

	// Get transaction count for this chain
	count, err := c.txRepo.CountByAddressAndChain(ctx, address, chain)
	if err != nil {
		return nil, fmt.Errorf("failed to count transactions: %w", err)
	}

	// Update transaction count
	addr.TransactionCount = count

	// Apply threshold-based classification
	oldClassification := addr.Classification
	newClassification := c.classifyByThreshold(count)
	addr.Classification = newClassification

	// Update address in database
	if err := c.addressRepo.Update(ctx, addr); err != nil {
		return nil, fmt.Errorf("failed to update address: %w", err)
	}

	if oldClassification != newClassification {
		fmt.Printf("Address %s on %s classification changed: %s -> %s (txs: %d, threshold: %d)\n",
			address, chain, oldClassification, newClassification, count, c.superAddressThreshold)
	}

	return addr, nil
}

// ClassifyAllChains classifies an address across all chains it's tracked on
func (c *AddressClassifier) ClassifyAllChains(ctx context.Context, address string) ([]*models.Address, error) {
	// Get all chain records for this address
	addresses, err := c.addressRepo.GetByAddress(ctx, address)
	if err != nil {
		return nil, fmt.Errorf("failed to get addresses: %w", err)
	}

	var results []*models.Address
	for _, addr := range addresses {
		classified, err := c.ClassifyAddress(ctx, address, addr.Chain)
		if err != nil {
			fmt.Printf("Warning: failed to classify address %s on chain %s: %v\n", address, addr.Chain, err)
			continue
		}
		results = append(results, classified)
	}

	return results, nil
}

// classifyByThreshold classifies an address based on transaction count threshold
func (c *AddressClassifier) classifyByThreshold(transactionCount int64) types.AddressClassification {
	if transactionCount >= c.superAddressThreshold {
		return types.ClassificationSuper
	}
	return types.ClassificationNormal
}

// GetSuperAddressThreshold returns the current threshold for super address classification
func (c *AddressClassifier) GetSuperAddressThreshold() int64 {
	return c.superAddressThreshold
}

// SetSuperAddressThreshold updates the threshold for super address classification
func (c *AddressClassifier) SetSuperAddressThreshold(threshold int64) {
	if threshold > 0 {
		c.superAddressThreshold = threshold
		fmt.Printf("Super address threshold updated to: %d\n", threshold)
	}
}

// ReclassifyAllAddresses reclassifies all addresses in the system
func (c *AddressClassifier) ReclassifyAllAddresses(ctx context.Context) error {
	addresses, err := c.addressRepo.List(ctx, &storage.AddressFilters{
		Limit: 10000,
	})
	if err != nil {
		return fmt.Errorf("failed to list addresses: %w", err)
	}

	reclassified := 0
	for _, addr := range addresses {
		_, err := c.ClassifyAddress(ctx, addr.Address, addr.Chain)
		if err != nil {
			fmt.Printf("Warning: failed to reclassify address %s on %s: %v\n", addr.Address, addr.Chain, err)
			continue
		}
		reclassified++
	}

	fmt.Printf("Reclassified %d address-chain records\n", reclassified)
	return nil
}

// GetClassificationStats returns statistics about address classifications
func (c *AddressClassifier) GetClassificationStats(ctx context.Context) (map[types.AddressClassification]int64, error) {
	stats := make(map[types.AddressClassification]int64)

	normalClass := types.ClassificationNormal
	normalAddresses, err := c.addressRepo.List(ctx, &storage.AddressFilters{
		Classification: &normalClass,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to count normal addresses: %w", err)
	}
	stats[types.ClassificationNormal] = int64(len(normalAddresses))

	superClass := types.ClassificationSuper
	superAddresses, err := c.addressRepo.List(ctx, &storage.AddressFilters{
		Classification: &superClass,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to count super addresses: %w", err)
	}
	stats[types.ClassificationSuper] = int64(len(superAddresses))

	return stats, nil
}
