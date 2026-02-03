// Package ratelimit provides CU (Compute Unit) rate limiting for Alchemy RPC calls.
package ratelimit

import (
	"sync"
)

// Default CU costs for known RPC methods based on Alchemy pricing.
const (
	DefaultCUCost = 20 // Default cost for unknown methods

	// Known method costs
	CostEthBlockNumber           = 10
	CostEthGetBlockByNumber      = 16
	CostEthGetLogs               = 75
	CostEthGetTransactionByHash  = 15
	CostEthGetTransactionReceipt = 15
	// alchemy_getAssetTransfers: 150 CU per call, but FetchAlchemyAssetTransfers
	// makes 2 calls (outgoing + incoming), so total cost is 300 CU
	CostAlchemyGetAssetTransfers = 300
)

// RPC method names
const (
	MethodEthBlockNumber           = "eth_blockNumber"
	MethodEthGetBlockByNumber      = "eth_getBlockByNumber"
	MethodEthGetLogs               = "eth_getLogs"
	MethodEthGetTransactionByHash  = "eth_getTransactionByHash"
	MethodEthGetTransactionReceipt = "eth_getTransactionReceipt"
	MethodAlchemyGetAssetTransfers = "alchemy_getAssetTransfers"
)

// CUCostRegistry maps RPC methods to their CU costs.
// It is safe for concurrent use.
type CUCostRegistry struct {
	mu          sync.RWMutex
	costs       map[string]int
	defaultCost int
}

// CUCostRegistryConfig holds configuration for the registry.
type CUCostRegistryConfig struct {
	// DefaultCost is the CU cost for unknown RPC methods.
	// If zero, uses the package default (20 CU).
	DefaultCost int

	// Overrides allows custom CU costs for specific methods.
	// These override the built-in defaults.
	Overrides map[string]int
}

// NewCUCostRegistry creates a new registry with default Alchemy costs.
// If cfg is nil, default configuration is used.
func NewCUCostRegistry(cfg *CUCostRegistryConfig) *CUCostRegistry {
	// Initialize with default costs
	costs := map[string]int{
		MethodEthBlockNumber:           CostEthBlockNumber,
		MethodEthGetBlockByNumber:      CostEthGetBlockByNumber,
		MethodEthGetLogs:               CostEthGetLogs,
		MethodEthGetTransactionByHash:  CostEthGetTransactionByHash,
		MethodEthGetTransactionReceipt: CostEthGetTransactionReceipt,
		MethodAlchemyGetAssetTransfers: CostAlchemyGetAssetTransfers,
	}

	defaultCost := DefaultCUCost

	// Apply configuration if provided
	if cfg != nil {
		// Use configured default cost if specified
		if cfg.DefaultCost > 0 {
			defaultCost = cfg.DefaultCost
		}

		// Apply overrides
		for method, cost := range cfg.Overrides {
			if cost > 0 {
				costs[method] = cost
			}
		}
	}

	return &CUCostRegistry{
		costs:       costs,
		defaultCost: defaultCost,
	}
}

// GetCost returns the CU cost for an RPC method.
// If the method is not known, returns the configured default cost.
func (r *CUCostRegistry) GetCost(method string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if cost, ok := r.costs[method]; ok {
		return cost
	}
	return r.defaultCost
}

// SetCost allows runtime cost updates for a specific method.
// This is useful for testing or tuning costs based on observed behavior.
// The cost must be positive; zero or negative values are ignored.
func (r *CUCostRegistry) SetCost(method string, cost int) {
	if cost <= 0 {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.costs[method] = cost
}

// GetDefaultCost returns the configured default cost for unknown methods.
func (r *CUCostRegistry) GetDefaultCost() int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.defaultCost
}

// KnownMethods returns a list of all known RPC method names.
func (r *CUCostRegistry) KnownMethods() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	methods := make([]string, 0, len(r.costs))
	for method := range r.costs {
		methods = append(methods, method)
	}
	return methods
}
