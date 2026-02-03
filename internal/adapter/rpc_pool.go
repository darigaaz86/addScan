package adapter

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/ethclient"
)

// RPCPool manages multiple RPC endpoints with failover on rate limiting (429)
// Strategy: Stick to current account until 429, then switch to next
type RPCPool struct {
	endpoints    []string
	clients      []*ethclient.Client
	currentIndex int
	mu           sync.RWMutex
	cooldowns    map[int]time.Time // Track when each endpoint was rate limited
	cooldownTime time.Duration     // How long to wait before retrying a rate-limited endpoint
}

// RPCPoolConfig holds configuration for creating an RPC pool
type RPCPoolConfig struct {
	// Endpoints is a list of RPC URLs (e.g., multiple Alchemy keys)
	Endpoints []string
	// CooldownTime is how long to wait before retrying a rate-limited endpoint
	// Default: 60 seconds
	CooldownTime time.Duration
}

// NewRPCPool creates a new RPC pool from multiple endpoints
func NewRPCPool(cfg *RPCPoolConfig) (*RPCPool, error) {
	if cfg == nil || len(cfg.Endpoints) == 0 {
		return nil, fmt.Errorf("at least one RPC endpoint is required")
	}

	cooldownTime := cfg.CooldownTime
	if cooldownTime == 0 {
		cooldownTime = 60 * time.Second
	}

	pool := &RPCPool{
		endpoints:    cfg.Endpoints,
		clients:      make([]*ethclient.Client, len(cfg.Endpoints)),
		currentIndex: 0,
		cooldowns:    make(map[int]time.Time),
		cooldownTime: cooldownTime,
	}

	// Connect to first endpoint only (lazy connect others)
	client, err := ethclient.Dial(cfg.Endpoints[0])
	if err != nil {
		return nil, fmt.Errorf("failed to connect to primary RPC endpoint: %w", err)
	}
	pool.clients[0] = client

	log.Printf("[RPCPool] Initialized with %d endpoints, starting with endpoint 0", len(cfg.Endpoints))

	return pool, nil
}

// NewRPCPoolFromURLs creates an RPC pool from comma-separated URLs
func NewRPCPoolFromURLs(urls string) (*RPCPool, error) {
	endpoints := strings.Split(urls, ",")
	// Trim whitespace from each endpoint
	for i, ep := range endpoints {
		endpoints[i] = strings.TrimSpace(ep)
	}
	// Filter out empty strings
	var validEndpoints []string
	for _, ep := range endpoints {
		if ep != "" {
			validEndpoints = append(validEndpoints, ep)
		}
	}

	return NewRPCPool(&RPCPoolConfig{
		Endpoints: validEndpoints,
	})
}

// GetClient returns the current active client
func (p *RPCPool) GetClient() *ethclient.Client {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.clients[p.currentIndex]
}

// GetCurrentURL returns the current active RPC URL
func (p *RPCPool) GetCurrentURL() string {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.endpoints[p.currentIndex]
}

// GetCurrentIndex returns the current endpoint index
func (p *RPCPool) GetCurrentIndex() int {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.currentIndex
}

// EndpointCount returns the number of endpoints in the pool
func (p *RPCPool) EndpointCount() int {
	return len(p.endpoints)
}

// OnRateLimited should be called when a 429 response is received
// It switches to the next available endpoint
// Returns error if all endpoints are rate limited
func (p *RPCPool) OnRateLimited(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Mark current endpoint as rate limited
	p.cooldowns[p.currentIndex] = time.Now()
	log.Printf("[RPCPool] Endpoint %d rate limited, marking cooldown", p.currentIndex)

	// Find next available endpoint
	startIndex := p.currentIndex
	for i := 0; i < len(p.endpoints); i++ {
		nextIndex := (p.currentIndex + 1 + i) % len(p.endpoints)

		// Check if this endpoint is still in cooldown
		if cooldownTime, exists := p.cooldowns[nextIndex]; exists {
			if time.Since(cooldownTime) < p.cooldownTime {
				log.Printf("[RPCPool] Endpoint %d still in cooldown (%v remaining)",
					nextIndex, p.cooldownTime-time.Since(cooldownTime))
				continue
			}
			// Cooldown expired, remove from map
			delete(p.cooldowns, nextIndex)
		}

		// Found an available endpoint
		if err := p.switchToEndpoint(nextIndex); err != nil {
			log.Printf("[RPCPool] Failed to switch to endpoint %d: %v", nextIndex, err)
			continue
		}

		log.Printf("[RPCPool] Switched from endpoint %d to endpoint %d", startIndex, nextIndex)
		return nil
	}

	return fmt.Errorf("all %d RPC endpoints are rate limited", len(p.endpoints))
}

// switchToEndpoint switches to a specific endpoint (must hold lock)
func (p *RPCPool) switchToEndpoint(index int) error {
	// Lazy connect if not already connected
	if p.clients[index] == nil {
		client, err := ethclient.Dial(p.endpoints[index])
		if err != nil {
			return fmt.Errorf("failed to connect to endpoint %d: %w", index, err)
		}
		p.clients[index] = client
	}

	p.currentIndex = index
	return nil
}

// TryResetToPrimary attempts to switch back to the primary endpoint (index 0)
// if its cooldown has expired. Call this periodically to prefer the primary.
func (p *RPCPool) TryResetToPrimary() bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.currentIndex == 0 {
		return true // Already on primary
	}

	// Check if primary is still in cooldown
	if cooldownTime, exists := p.cooldowns[0]; exists {
		if time.Since(cooldownTime) < p.cooldownTime {
			return false // Still in cooldown
		}
		delete(p.cooldowns, 0)
	}

	// Try to switch back to primary
	if err := p.switchToEndpoint(0); err != nil {
		log.Printf("[RPCPool] Failed to reset to primary endpoint: %v", err)
		return false
	}

	log.Printf("[RPCPool] Reset to primary endpoint (index 0)")
	return true
}

// IsRateLimitError checks if an error indicates rate limiting (429)
func IsRateLimitError(err error) bool {
	if err == nil {
		return false
	}
	errStr := strings.ToLower(err.Error())
	return strings.Contains(errStr, "429") ||
		strings.Contains(errStr, "rate limit") ||
		strings.Contains(errStr, "too many requests") ||
		strings.Contains(errStr, "exceeded") ||
		strings.Contains(errStr, "throttl")
}

// Close closes all client connections
func (p *RPCPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for i, client := range p.clients {
		if client != nil {
			client.Close()
			p.clients[i] = nil
		}
	}
}

// Status returns the current status of the pool
func (p *RPCPool) Status() *RPCPoolStatus {
	p.mu.RLock()
	defer p.mu.RUnlock()

	status := &RPCPoolStatus{
		TotalEndpoints: len(p.endpoints),
		CurrentIndex:   p.currentIndex,
		EndpointStatus: make([]EndpointStatus, len(p.endpoints)),
	}

	for i := range p.endpoints {
		es := EndpointStatus{
			Index:     i,
			Connected: p.clients[i] != nil,
			IsCurrent: i == p.currentIndex,
		}

		if cooldownTime, exists := p.cooldowns[i]; exists {
			remaining := p.cooldownTime - time.Since(cooldownTime)
			if remaining > 0 {
				es.InCooldown = true
				es.CooldownRemaining = remaining
			}
		}

		status.EndpointStatus[i] = es
	}

	return status
}

// RPCPoolStatus represents the current status of the RPC pool
type RPCPoolStatus struct {
	TotalEndpoints int
	CurrentIndex   int
	EndpointStatus []EndpointStatus
}

// EndpointStatus represents the status of a single endpoint
type EndpointStatus struct {
	Index             int
	Connected         bool
	IsCurrent         bool
	InCooldown        bool
	CooldownRemaining time.Duration
}

// PooledRPCProvider implements DataProvider interface using RPCPool
// This allows the pool to be used with existing adapter code
type PooledRPCProvider struct {
	pool *RPCPool
	mu   sync.RWMutex

	// Health tracking
	totalRequests    int64
	successfulReqs   int64
	failedReqs       int64
	totalLatency     time.Duration
	lastSuccess      time.Time
	lastFailure      time.Time
	consecutiveFails int
}

// NewPooledRPCProvider creates a new pooled RPC provider
func NewPooledRPCProvider(pool *RPCPool) *PooledRPCProvider {
	return &PooledRPCProvider{
		pool: pool,
	}
}

// NewPooledRPCProviderFromURLs creates a pooled provider from comma-separated URLs
func NewPooledRPCProviderFromURLs(urls string) (*PooledRPCProvider, error) {
	pool, err := NewRPCPoolFromURLs(urls)
	if err != nil {
		return nil, err
	}
	return NewPooledRPCProvider(pool), nil
}

// GetPrimaryURL returns the first endpoint URL
func (p *PooledRPCProvider) GetPrimaryURL() (string, error) {
	if p.pool.EndpointCount() == 0 {
		return "", fmt.Errorf("no endpoints configured")
	}
	return p.pool.endpoints[0], nil
}

// GetCurrentURL returns the currently active RPC endpoint URL
func (p *PooledRPCProvider) GetCurrentURL() (string, error) {
	return p.pool.GetCurrentURL(), nil
}

// Failover switches to the next available endpoint on rate limit
func (p *PooledRPCProvider) Failover() error {
	return p.pool.OnRateLimited(context.Background())
}

// RecordSuccess records a successful request
func (p *PooledRPCProvider) RecordSuccess(duration time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.totalRequests++
	p.successfulReqs++
	p.totalLatency += duration
	p.lastSuccess = time.Now()
	p.consecutiveFails = 0
}

// RecordFailure records a failed request
func (p *PooledRPCProvider) RecordFailure(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.totalRequests++
	p.failedReqs++
	p.lastFailure = time.Now()
	p.consecutiveFails++

	// Auto-failover on rate limit errors
	if IsRateLimitError(err) {
		go func() {
			if failErr := p.pool.OnRateLimited(context.Background()); failErr != nil {
				log.Printf("[PooledRPCProvider] Failed to failover: %v", failErr)
			}
		}()
	}
}

// GetHealth returns the current health status
func (p *PooledRPCProvider) GetHealth() *ProviderHealth {
	p.mu.RLock()
	defer p.mu.RUnlock()

	var successRate float64
	if p.totalRequests > 0 {
		successRate = float64(p.successfulReqs) / float64(p.totalRequests)
	}

	var avgLatency time.Duration
	if p.successfulReqs > 0 {
		avgLatency = p.totalLatency / time.Duration(p.successfulReqs)
	}

	return &ProviderHealth{
		CurrentURL:       p.pool.GetCurrentURL(),
		TotalRequests:    p.totalRequests,
		SuccessfulReqs:   p.successfulReqs,
		FailedReqs:       p.failedReqs,
		SuccessRate:      successRate,
		AverageLatency:   avgLatency,
		LastSuccess:      p.lastSuccess,
		LastFailure:      p.lastFailure,
		ConsecutiveFails: p.consecutiveFails,
		IsHealthy:        p.consecutiveFails < 5,
	}
}

// IsHealthy returns true if the provider is healthy
func (p *PooledRPCProvider) IsHealthy() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.consecutiveFails < 5
}

// Reset tries to reset to the primary endpoint
func (p *PooledRPCProvider) Reset() {
	p.pool.TryResetToPrimary()
	p.mu.Lock()
	p.consecutiveFails = 0
	p.mu.Unlock()
}

// GetPool returns the underlying RPC pool
func (p *PooledRPCProvider) GetPool() *RPCPool {
	return p.pool
}

// GetClient returns the current active ethclient
func (p *PooledRPCProvider) GetClient() *ethclient.Client {
	return p.pool.GetClient()
}

// Close closes all connections
func (p *PooledRPCProvider) Close() {
	p.pool.Close()
}
