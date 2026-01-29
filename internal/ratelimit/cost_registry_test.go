package ratelimit

import (
	"sync"
	"testing"
)

func TestNewCUCostRegistry_DefaultConfig(t *testing.T) {
	registry := NewCUCostRegistry(nil)

	if registry == nil {
		t.Fatal("expected non-nil registry")
	}

	// Verify default cost
	if got := registry.GetDefaultCost(); got != DefaultCUCost {
		t.Errorf("GetDefaultCost() = %d, want %d", got, DefaultCUCost)
	}
}

func TestNewCUCostRegistry_CustomDefaultCost(t *testing.T) {
	cfg := &CUCostRegistryConfig{
		DefaultCost: 50,
	}
	registry := NewCUCostRegistry(cfg)

	if got := registry.GetDefaultCost(); got != 50 {
		t.Errorf("GetDefaultCost() = %d, want 50", got)
	}
}

func TestNewCUCostRegistry_WithOverrides(t *testing.T) {
	cfg := &CUCostRegistryConfig{
		Overrides: map[string]int{
			MethodEthBlockNumber: 100, // Override existing
			"custom_method":      25,  // Add new
		},
	}
	registry := NewCUCostRegistry(cfg)

	// Check override of existing method
	if got := registry.GetCost(MethodEthBlockNumber); got != 100 {
		t.Errorf("GetCost(%s) = %d, want 100", MethodEthBlockNumber, got)
	}

	// Check new method added via override
	if got := registry.GetCost("custom_method"); got != 25 {
		t.Errorf("GetCost(custom_method) = %d, want 25", got)
	}
}

func TestCUCostRegistry_GetCost_KnownMethods(t *testing.T) {
	registry := NewCUCostRegistry(nil)

	tests := []struct {
		method   string
		expected int
	}{
		{MethodEthBlockNumber, CostEthBlockNumber},
		{MethodEthGetBlockByNumber, CostEthGetBlockByNumber},
		{MethodEthGetLogs, CostEthGetLogs},
		{MethodEthGetTransactionByHash, CostEthGetTransactionByHash},
		{MethodEthGetTransactionReceipt, CostEthGetTransactionReceipt},
		{MethodAlchemyGetAssetTransfers, CostAlchemyGetAssetTransfers},
	}

	for _, tt := range tests {
		t.Run(tt.method, func(t *testing.T) {
			if got := registry.GetCost(tt.method); got != tt.expected {
				t.Errorf("GetCost(%s) = %d, want %d", tt.method, got, tt.expected)
			}
		})
	}
}

func TestCUCostRegistry_GetCost_UnknownMethod(t *testing.T) {
	registry := NewCUCostRegistry(nil)

	unknownMethods := []string{
		"unknown_method",
		"eth_unknownMethod",
		"",
		"some_random_string",
	}

	for _, method := range unknownMethods {
		t.Run(method, func(t *testing.T) {
			if got := registry.GetCost(method); got != DefaultCUCost {
				t.Errorf("GetCost(%q) = %d, want %d (default)", method, got, DefaultCUCost)
			}
		})
	}
}

func TestCUCostRegistry_GetCost_UnknownMethodWithCustomDefault(t *testing.T) {
	cfg := &CUCostRegistryConfig{
		DefaultCost: 42,
	}
	registry := NewCUCostRegistry(cfg)

	if got := registry.GetCost("unknown_method"); got != 42 {
		t.Errorf("GetCost(unknown_method) = %d, want 42", got)
	}
}

func TestCUCostRegistry_SetCost(t *testing.T) {
	registry := NewCUCostRegistry(nil)

	// Set cost for a new method
	registry.SetCost("new_method", 99)
	if got := registry.GetCost("new_method"); got != 99 {
		t.Errorf("GetCost(new_method) = %d, want 99", got)
	}

	// Update cost for an existing method
	registry.SetCost(MethodEthBlockNumber, 200)
	if got := registry.GetCost(MethodEthBlockNumber); got != 200 {
		t.Errorf("GetCost(%s) = %d, want 200", MethodEthBlockNumber, got)
	}
}

func TestCUCostRegistry_SetCost_InvalidValues(t *testing.T) {
	registry := NewCUCostRegistry(nil)

	originalCost := registry.GetCost(MethodEthBlockNumber)

	// Zero cost should be ignored
	registry.SetCost(MethodEthBlockNumber, 0)
	if got := registry.GetCost(MethodEthBlockNumber); got != originalCost {
		t.Errorf("SetCost with 0 should be ignored, got %d, want %d", got, originalCost)
	}

	// Negative cost should be ignored
	registry.SetCost(MethodEthBlockNumber, -10)
	if got := registry.GetCost(MethodEthBlockNumber); got != originalCost {
		t.Errorf("SetCost with negative should be ignored, got %d, want %d", got, originalCost)
	}
}

func TestCUCostRegistry_KnownMethods(t *testing.T) {
	registry := NewCUCostRegistry(nil)

	methods := registry.KnownMethods()

	expectedMethods := map[string]bool{
		MethodEthBlockNumber:           true,
		MethodEthGetBlockByNumber:      true,
		MethodEthGetLogs:               true,
		MethodEthGetTransactionByHash:  true,
		MethodEthGetTransactionReceipt: true,
		MethodAlchemyGetAssetTransfers: true,
	}

	if len(methods) != len(expectedMethods) {
		t.Errorf("KnownMethods() returned %d methods, want %d", len(methods), len(expectedMethods))
	}

	for _, method := range methods {
		if !expectedMethods[method] {
			t.Errorf("unexpected method in KnownMethods(): %s", method)
		}
	}
}

func TestCUCostRegistry_ConcurrentAccess(t *testing.T) {
	registry := NewCUCostRegistry(nil)

	var wg sync.WaitGroup
	iterations := 100

	// Concurrent reads
	for i := 0; i < iterations; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = registry.GetCost(MethodEthBlockNumber)
			_ = registry.GetCost("unknown")
			_ = registry.GetDefaultCost()
			_ = registry.KnownMethods()
		}()
	}

	// Concurrent writes
	for i := 0; i < iterations; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			registry.SetCost("concurrent_method", i+1)
		}(i)
	}

	wg.Wait()

	// Verify registry is still functional
	if got := registry.GetCost(MethodEthBlockNumber); got != CostEthBlockNumber {
		t.Errorf("after concurrent access, GetCost(%s) = %d, want %d",
			MethodEthBlockNumber, got, CostEthBlockNumber)
	}
}

func TestCUCostRegistry_OverrideWithZeroIgnored(t *testing.T) {
	cfg := &CUCostRegistryConfig{
		Overrides: map[string]int{
			MethodEthBlockNumber: 0, // Should be ignored
		},
	}
	registry := NewCUCostRegistry(cfg)

	// Original cost should be preserved
	if got := registry.GetCost(MethodEthBlockNumber); got != CostEthBlockNumber {
		t.Errorf("GetCost(%s) = %d, want %d (zero override should be ignored)",
			MethodEthBlockNumber, got, CostEthBlockNumber)
	}
}

func TestCUCostRegistry_ZeroDefaultCostIgnored(t *testing.T) {
	cfg := &CUCostRegistryConfig{
		DefaultCost: 0, // Should use package default
	}
	registry := NewCUCostRegistry(cfg)

	if got := registry.GetDefaultCost(); got != DefaultCUCost {
		t.Errorf("GetDefaultCost() = %d, want %d (zero should use package default)",
			got, DefaultCUCost)
	}
}
