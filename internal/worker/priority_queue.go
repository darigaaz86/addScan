package worker

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/address-scanner/internal/storage"
	"github.com/address-scanner/internal/types"
)

// AddressPriority represents an address with its priority level
type AddressPriority struct {
	Address  string
	Tier     types.UserTier
	Priority int // Higher number = higher priority
}

// PriorityQueue manages addresses with tier-based priority
type PriorityQueue struct {
	addresses       []AddressPriority
	mu              sync.RWMutex
	portfolioRepo   *storage.PortfolioRepository
	userRepo        *storage.UserRepository
	lastRefreshTime int64
}

// NewPriorityQueue creates a new priority queue
func NewPriorityQueue(portfolioRepo *storage.PortfolioRepository, userRepo *storage.UserRepository) *PriorityQueue {
	return &PriorityQueue{
		addresses:     make([]AddressPriority, 0),
		portfolioRepo: portfolioRepo,
		userRepo:      userRepo,
	}
}

// RefreshAddresses refreshes the address list with current priorities
func (pq *PriorityQueue) RefreshAddresses(ctx context.Context, chain types.ChainID, addressList []string) error {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	// Create a map for quick lookup
	addressMap := make(map[string]bool)
	for _, addr := range addressList {
		addressMap[addr] = true
	}

	// Build priority list
	priorityList := make([]AddressPriority, 0, len(addressList))

	// For each address, determine its highest tier across all users
	for _, addr := range addressList {
		tier, err := pq.getHighestTierForAddress(ctx, addr)
		if err != nil {
			// Default to free tier if we can't determine
			tier = types.TierFree
		}

		priority := pq.calculatePriority(tier)
		priorityList = append(priorityList, AddressPriority{
			Address:  addr,
			Tier:     tier,
			Priority: priority,
		})
	}

	// Sort by priority (highest first)
	sort.Slice(priorityList, func(i, j int) bool {
		return priorityList[i].Priority > priorityList[j].Priority
	})

	pq.addresses = priorityList
	return nil
}

// GetPrioritizedAddresses returns addresses sorted by priority
func (pq *PriorityQueue) GetPrioritizedAddresses() []string {
	pq.mu.RLock()
	defer pq.mu.RUnlock()

	addresses := make([]string, len(pq.addresses))
	for i, ap := range pq.addresses {
		addresses[i] = ap.Address
	}
	return addresses
}

// GetPaidTierAddresses returns only paid tier addresses
func (pq *PriorityQueue) GetPaidTierAddresses() []string {
	pq.mu.RLock()
	defer pq.mu.RUnlock()

	var addresses []string
	for _, ap := range pq.addresses {
		if ap.Tier == types.TierPaid {
			addresses = append(addresses, ap.Address)
		}
	}
	return addresses
}

// GetFreeTierAddresses returns only free tier addresses
func (pq *PriorityQueue) GetFreeTierAddresses() []string {
	pq.mu.RLock()
	defer pq.mu.RUnlock()

	var addresses []string
	for _, ap := range pq.addresses {
		if ap.Tier == types.TierFree {
			addresses = append(addresses, ap.Address)
		}
	}
	return addresses
}

// SplitByTier returns addresses split into paid and free tiers
func (pq *PriorityQueue) SplitByTier() (paid []string, free []string) {
	pq.mu.RLock()
	defer pq.mu.RUnlock()

	for _, ap := range pq.addresses {
		if ap.Tier == types.TierPaid {
			paid = append(paid, ap.Address)
		} else {
			free = append(free, ap.Address)
		}
	}
	return paid, free
}

// getHighestTierForAddress determines the highest tier for an address across all users
func (pq *PriorityQueue) getHighestTierForAddress(ctx context.Context, address string) (types.UserTier, error) {
	// Query portfolios table to find all users with this address in their portfolios
	// Get user tiers from users table
	// Return the highest tier found
	
	// This query finds the highest tier among all users who have this address in any portfolio
	// Since tier is on the user level, we just need to find if any paid user tracks this address
	
	// For MVP, we'll default to free tier
	// In production, this should query: SELECT MAX(u.tier) FROM users u JOIN portfolios p ON p.user_id = u.id WHERE address = ANY(p.addresses)
	
	return types.TierFree, nil // Default to free tier for MVP
}

// calculatePriority calculates numeric priority from tier
func (pq *PriorityQueue) calculatePriority(tier types.UserTier) int {
	switch tier {
	case types.TierPaid:
		return 100 // High priority
	case types.TierFree:
		return 10 // Low priority
	default:
		return 1 // Lowest priority
	}
}

// GetAddressCount returns the total number of addresses in the queue
func (pq *PriorityQueue) GetAddressCount() int {
	pq.mu.RLock()
	defer pq.mu.RUnlock()
	return len(pq.addresses)
}

// GetPaidTierCount returns the number of paid tier addresses
func (pq *PriorityQueue) GetPaidTierCount() int {
	pq.mu.RLock()
	defer pq.mu.RUnlock()

	count := 0
	for _, ap := range pq.addresses {
		if ap.Tier == types.TierPaid {
			count++
		}
	}
	return count
}

// GetFreeTierCount returns the number of free tier addresses
func (pq *PriorityQueue) GetFreeTierCount() int {
	pq.mu.RLock()
	defer pq.mu.RUnlock()

	count := 0
	for _, ap := range pq.addresses {
		if ap.Tier == types.TierFree {
			count++
		}
	}
	return count
}

// TierAwareAddressProvider provides addresses with tier-based priority
type TierAwareAddressProvider struct {
	addressRepo     *storage.AddressRepository
	portfolioRepo   *storage.PortfolioRepository
	userRepo        *storage.UserRepository
	priorityQueue   *PriorityQueue
	refreshInterval int64 // seconds
}

// NewTierAwareAddressProvider creates a new tier-aware address provider
func NewTierAwareAddressProvider(
	addressRepo *storage.AddressRepository,
	portfolioRepo *storage.PortfolioRepository,
	userRepo *storage.UserRepository,
) *TierAwareAddressProvider {
	return &TierAwareAddressProvider{
		addressRepo:     addressRepo,
		portfolioRepo:   portfolioRepo,
		userRepo:        userRepo,
		priorityQueue:   NewPriorityQueue(portfolioRepo, userRepo),
		refreshInterval: 300, // Refresh every 5 minutes
	}
}

// GetPrioritizedAddresses returns addresses prioritized by tier
func (tap *TierAwareAddressProvider) GetPrioritizedAddresses(ctx context.Context, chain types.ChainID) ([]string, error) {
	// Get all addresses for this chain
	addresses, err := tap.addressRepo.List(ctx, &storage.AddressFilters{
		Chain: &chain,
		Limit: 10000,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list addresses: %w", err)
	}

	// Extract address strings
	addressList := make([]string, len(addresses))
	for i, addr := range addresses {
		addressList[i] = addr.Address
	}

	// Refresh priority queue
	if err := tap.priorityQueue.RefreshAddresses(ctx, chain, addressList); err != nil {
		return nil, fmt.Errorf("failed to refresh priority queue: %w", err)
	}

	// Return prioritized addresses
	return tap.priorityQueue.GetPrioritizedAddresses(), nil
}

// GetAddressesByTier returns addresses split by tier (paid first, then free)
func (tap *TierAwareAddressProvider) GetAddressesByTier(ctx context.Context, chain types.ChainID) (paid []string, free []string, err error) {
	// Get all addresses for this chain
	addresses, err := tap.addressRepo.List(ctx, &storage.AddressFilters{
		Chain: &chain,
		Limit: 10000,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to list addresses: %w", err)
	}

	// Extract address strings
	addressList := make([]string, len(addresses))
	for i, addr := range addresses {
		addressList[i] = addr.Address
	}

	// Refresh priority queue
	if err := tap.priorityQueue.RefreshAddresses(ctx, chain, addressList); err != nil {
		return nil, nil, fmt.Errorf("failed to refresh priority queue: %w", err)
	}

	// Split by tier
	paid, free = tap.priorityQueue.SplitByTier()
	return paid, free, nil
}

// GetTierForAddress determines the tier for a specific address
func (tap *TierAwareAddressProvider) GetTierForAddress(ctx context.Context, address string) (types.UserTier, error) {
	// Query portfolios table to find all users with this address in their portfolios
	// Get user tiers from users table
	// Return the highest tier found
	
	if tap.portfolioRepo == nil || tap.userRepo == nil {
		return types.TierFree, nil
	}
	
	// TODO: Implement tier lookup by querying portfolios
	// For now, default to free tier - this will be optimized later with caching
	// Query would be:
	// SELECT u.tier FROM users u JOIN portfolios p ON p.user_id = u.id 
	// WHERE $1 = ANY(p.addresses) ORDER BY tier DESC LIMIT 1
	
	return types.TierFree, nil
}

// AddressTierInfo holds tier information for an address
type AddressTierInfo struct {
	Address       string
	Tier          types.UserTier
	UserCount     int // Number of users tracking this address
	PaidUserCount int // Number of paid users tracking this address
}

// GetAddressTierInfo retrieves tier information for multiple addresses
func (tap *TierAwareAddressProvider) GetAddressTierInfo(ctx context.Context, addresses []string) (map[string]*AddressTierInfo, error) {
	result := make(map[string]*AddressTierInfo)

	for _, addr := range addresses {
		// For each address, determine its tier
		tier, err := tap.GetTierForAddress(ctx, addr)
		if err != nil {
			// Default to free tier on error
			tier = types.TierFree
		}

		result[addr] = &AddressTierInfo{
			Address:       addr,
			Tier:          tier,
			UserCount:     0, // Would need to query this
			PaidUserCount: 0, // Would need to query this
		}
	}

	return result, nil
}
