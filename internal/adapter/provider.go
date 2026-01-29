package adapter

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// DataProvider defines the interface for blockchain data providers
type DataProvider interface {
	// GetPrimaryURL returns the primary RPC endpoint URL
	GetPrimaryURL() (string, error)

	// GetCurrentURL returns the currently active RPC endpoint URL
	GetCurrentURL() (string, error)

	// Failover switches to the next available provider
	// Returns error if no providers are available
	Failover() error

	// RecordSuccess records a successful request for health tracking
	RecordSuccess(duration time.Duration)

	// RecordFailure records a failed request for health tracking
	RecordFailure(err error)

	// GetHealth returns the current health status of the provider
	GetHealth() *ProviderHealth

	// IsHealthy returns true if the provider is considered healthy
	IsHealthy() bool

	// Reset resets the provider to use the primary endpoint
	Reset()
}

// ProviderHealth represents the health status of a data provider
type ProviderHealth struct {
	CurrentURL       string        `json:"currentUrl"`
	TotalRequests    int64         `json:"totalRequests"`
	SuccessfulReqs   int64         `json:"successfulRequests"`
	FailedReqs       int64         `json:"failedRequests"`
	SuccessRate      float64       `json:"successRate"`
	AverageLatency   time.Duration `json:"averageLatency"`
	LastSuccess      time.Time     `json:"lastSuccess"`
	LastFailure      time.Time     `json:"lastFailure"`
	ConsecutiveFails int           `json:"consecutiveFails"`
	IsHealthy        bool          `json:"isHealthy"`
}

// RPCProvider implements DataProvider for RPC-based blockchain providers
type RPCProvider struct {
	mu sync.RWMutex

	// Provider configuration
	primaryURL   string
	secondaryURL string
	currentURL   string

	// Health tracking
	totalRequests    int64
	successfulReqs   int64
	failedReqs       int64
	totalLatency     time.Duration
	lastSuccess      time.Time
	lastFailure      time.Time
	consecutiveFails int

	// Health thresholds
	maxConsecutiveFails int     // Max consecutive failures before marking unhealthy
	minSuccessRate      float64 // Minimum success rate to be considered healthy
}

// NewRPCProvider creates a new RPC provider with primary and optional secondary URLs
func NewRPCProvider(primaryURL, secondaryURL string) (*RPCProvider, error) {
	if primaryURL == "" {
		return nil, fmt.Errorf("primary URL cannot be empty")
	}

	return &RPCProvider{
		primaryURL:          primaryURL,
		secondaryURL:        secondaryURL,
		currentURL:          primaryURL,
		maxConsecutiveFails: 5,
		minSuccessRate:      0.5, // 50% success rate threshold
	}, nil
}

// GetPrimaryURL returns the primary RPC endpoint URL
func (p *RPCProvider) GetPrimaryURL() (string, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.primaryURL == "" {
		return "", fmt.Errorf("primary URL not configured")
	}

	return p.primaryURL, nil
}

// GetCurrentURL returns the currently active RPC endpoint URL
func (p *RPCProvider) GetCurrentURL() (string, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.currentURL == "" {
		return "", fmt.Errorf("no active URL configured")
	}

	return p.currentURL, nil
}

// Failover switches to the next available provider
func (p *RPCProvider) Failover() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// If currently on primary, try secondary
	if p.currentURL == p.primaryURL {
		if p.secondaryURL == "" {
			return fmt.Errorf("no secondary provider configured")
		}
		p.currentURL = p.secondaryURL
		return nil
	}

	// If currently on secondary, try primary
	if p.currentURL == p.secondaryURL {
		if p.primaryURL == "" {
			return fmt.Errorf("no primary provider configured")
		}
		p.currentURL = p.primaryURL
		return nil
	}

	return fmt.Errorf("no available providers for failover")
}

// RecordSuccess records a successful request for health tracking
func (p *RPCProvider) RecordSuccess(duration time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.totalRequests++
	p.successfulReqs++
	p.totalLatency += duration
	p.lastSuccess = time.Now()
	p.consecutiveFails = 0 // Reset consecutive failures on success
}

// RecordFailure records a failed request for health tracking
func (p *RPCProvider) RecordFailure(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.totalRequests++
	p.failedReqs++
	p.lastFailure = time.Now()
	p.consecutiveFails++
}

// GetHealth returns the current health status of the provider
func (p *RPCProvider) GetHealth() *ProviderHealth {
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
		CurrentURL:       p.currentURL,
		TotalRequests:    p.totalRequests,
		SuccessfulReqs:   p.successfulReqs,
		FailedReqs:       p.failedReqs,
		SuccessRate:      successRate,
		AverageLatency:   avgLatency,
		LastSuccess:      p.lastSuccess,
		LastFailure:      p.lastFailure,
		ConsecutiveFails: p.consecutiveFails,
		IsHealthy:        p.isHealthyLocked(),
	}
}

// IsHealthy returns true if the provider is considered healthy
func (p *RPCProvider) IsHealthy() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.isHealthyLocked()
}

// isHealthyLocked checks health status (must be called with lock held)
func (p *RPCProvider) isHealthyLocked() bool {
	// Check consecutive failures
	if p.consecutiveFails >= p.maxConsecutiveFails {
		return false
	}

	// Check success rate (only if we have enough data)
	if p.totalRequests >= 10 {
		successRate := float64(p.successfulReqs) / float64(p.totalRequests)
		if successRate < p.minSuccessRate {
			return false
		}
	}

	return true
}

// Reset resets the provider to use the primary endpoint
func (p *RPCProvider) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.currentURL = p.primaryURL
	p.consecutiveFails = 0
}

// ResetStats resets all health statistics
func (p *RPCProvider) ResetStats() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.totalRequests = 0
	p.successfulReqs = 0
	p.failedReqs = 0
	p.totalLatency = 0
	p.consecutiveFails = 0
	p.lastSuccess = time.Time{}
	p.lastFailure = time.Time{}
}

// SetHealthThresholds configures health check thresholds
func (p *RPCProvider) SetHealthThresholds(maxConsecutiveFails int, minSuccessRate float64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if maxConsecutiveFails > 0 {
		p.maxConsecutiveFails = maxConsecutiveFails
	}

	if minSuccessRate > 0 && minSuccessRate <= 1.0 {
		p.minSuccessRate = minSuccessRate
	}
}

// MultiProviderPool manages multiple providers with automatic failover
type MultiProviderPool struct {
	mu sync.RWMutex

	providers     []DataProvider
	currentIndex  int
	healthChecker *HealthChecker
}

// NewMultiProviderPool creates a new provider pool
func NewMultiProviderPool(providers []DataProvider) (*MultiProviderPool, error) {
	if len(providers) == 0 {
		return nil, fmt.Errorf("at least one provider is required")
	}

	pool := &MultiProviderPool{
		providers:    providers,
		currentIndex: 0,
	}

	// Start health checker
	pool.healthChecker = NewHealthChecker(pool, 30*time.Second)
	pool.healthChecker.Start()

	return pool, nil
}

// GetPrimaryURL returns the primary provider's URL
func (p *MultiProviderPool) GetPrimaryURL() (string, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if len(p.providers) == 0 {
		return "", fmt.Errorf("no providers available")
	}

	return p.providers[0].GetPrimaryURL()
}

// GetCurrentURL returns the current provider's URL
func (p *MultiProviderPool) GetCurrentURL() (string, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.currentIndex >= len(p.providers) {
		return "", fmt.Errorf("invalid provider index")
	}

	return p.providers[p.currentIndex].GetCurrentURL()
}

// Failover switches to the next available healthy provider
func (p *MultiProviderPool) Failover() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Try each provider in sequence
	startIndex := p.currentIndex
	for i := 0; i < len(p.providers); i++ {
		nextIndex := (startIndex + i + 1) % len(p.providers)
		if p.providers[nextIndex].IsHealthy() {
			p.currentIndex = nextIndex
			return nil
		}
	}

	return fmt.Errorf("no healthy providers available")
}

// RecordSuccess records a successful request
func (p *MultiProviderPool) RecordSuccess(duration time.Duration) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.currentIndex < len(p.providers) {
		p.providers[p.currentIndex].RecordSuccess(duration)
	}
}

// RecordFailure records a failed request
func (p *MultiProviderPool) RecordFailure(err error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.currentIndex < len(p.providers) {
		p.providers[p.currentIndex].RecordFailure(err)
	}
}

// GetHealth returns health status of current provider
func (p *MultiProviderPool) GetHealth() *ProviderHealth {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.currentIndex < len(p.providers) {
		return p.providers[p.currentIndex].GetHealth()
	}

	return nil
}

// GetAllHealth returns health status of all providers
func (p *MultiProviderPool) GetAllHealth() []*ProviderHealth {
	p.mu.RLock()
	defer p.mu.RUnlock()

	health := make([]*ProviderHealth, len(p.providers))
	for i, provider := range p.providers {
		health[i] = provider.GetHealth()
	}

	return health
}

// IsHealthy returns true if current provider is healthy
func (p *MultiProviderPool) IsHealthy() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.currentIndex < len(p.providers) {
		return p.providers[p.currentIndex].IsHealthy()
	}

	return false
}

// Reset resets to the first healthy provider
func (p *MultiProviderPool) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Find first healthy provider
	for i, provider := range p.providers {
		if provider.IsHealthy() {
			p.currentIndex = i
			provider.Reset()
			return
		}
	}

	// If no healthy providers, reset to first
	p.currentIndex = 0
	p.providers[0].Reset()
}

// Stop stops the health checker
func (p *MultiProviderPool) Stop() {
	if p.healthChecker != nil {
		p.healthChecker.Stop()
	}
}

// HealthChecker periodically checks provider health and triggers failover if needed
type HealthChecker struct {
	pool     *MultiProviderPool
	interval time.Duration
	stopCh   chan struct{}
	stopped  bool
	mu       sync.Mutex
}

// NewHealthChecker creates a new health checker
func NewHealthChecker(pool *MultiProviderPool, interval time.Duration) *HealthChecker {
	return &HealthChecker{
		pool:     pool,
		interval: interval,
		stopCh:   make(chan struct{}),
	}
}

// Start begins health checking
func (h *HealthChecker) Start() {
	go h.run()
}

// run is the main health checking loop
func (h *HealthChecker) run() {
	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			h.checkHealth()
		case <-h.stopCh:
			return
		}
	}
}

// checkHealth checks current provider health and triggers failover if unhealthy
func (h *HealthChecker) checkHealth() {
	if !h.pool.IsHealthy() {
		// Current provider is unhealthy, attempt failover
		if err := h.pool.Failover(); err != nil {
			// Log error - all providers unhealthy
			// In production, this would trigger alerts
		}
	}
}

// Stop stops the health checker
func (h *HealthChecker) Stop() {
	h.mu.Lock()
	defer h.mu.Unlock()

	if !h.stopped {
		close(h.stopCh)
		h.stopped = true
	}
}

// ProviderWithRetry wraps a provider with exponential backoff retry logic
type ProviderWithRetry struct {
	provider    DataProvider
	maxRetries  int
	baseDelay   time.Duration
	maxDelay    time.Duration
	backoffFunc func(attempt int, baseDelay time.Duration) time.Duration
}

// NewProviderWithRetry creates a provider with retry logic
func NewProviderWithRetry(provider DataProvider, maxRetries int) *ProviderWithRetry {
	return &ProviderWithRetry{
		provider:   provider,
		maxRetries: maxRetries,
		baseDelay:  1 * time.Second,
		maxDelay:   60 * time.Second,
		backoffFunc: func(attempt int, baseDelay time.Duration) time.Duration {
			// Exponential backoff: 1s, 2s, 4s, 8s, 16s, 32s, 60s (capped)
			delay := baseDelay * time.Duration(1<<uint(attempt))
			if delay > 60*time.Second {
				delay = 60 * time.Second
			}
			return delay
		},
	}
}

// ExecuteWithRetry executes a function with exponential backoff retry
func (p *ProviderWithRetry) ExecuteWithRetry(ctx context.Context, fn func() error) error {
	var lastErr error

	for attempt := 0; attempt <= p.maxRetries; attempt++ {
		// Execute function
		err := fn()
		if err == nil {
			return nil
		}

		lastErr = err

		// Check if we should retry
		if attempt < p.maxRetries {
			// Calculate backoff delay
			delay := p.backoffFunc(attempt, p.baseDelay)

			// Wait with context cancellation support
			select {
			case <-time.After(delay):
				// Continue to next attempt
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	return fmt.Errorf("max retries exceeded: %w", lastErr)
}

// Delegate methods to underlying provider
func (p *ProviderWithRetry) GetPrimaryURL() (string, error) {
	return p.provider.GetPrimaryURL()
}

func (p *ProviderWithRetry) GetCurrentURL() (string, error) {
	return p.provider.GetCurrentURL()
}

func (p *ProviderWithRetry) Failover() error {
	return p.provider.Failover()
}

func (p *ProviderWithRetry) RecordSuccess(duration time.Duration) {
	p.provider.RecordSuccess(duration)
}

func (p *ProviderWithRetry) RecordFailure(err error) {
	p.provider.RecordFailure(err)
}

func (p *ProviderWithRetry) GetHealth() *ProviderHealth {
	return p.provider.GetHealth()
}

func (p *ProviderWithRetry) IsHealthy() bool {
	return p.provider.IsHealthy()
}

func (p *ProviderWithRetry) Reset() {
	p.provider.Reset()
}
