package service

import (
	"context"
	"time"

	"github.com/address-scanner/internal/circuitbreaker"
	"github.com/address-scanner/internal/errors"
	"github.com/address-scanner/internal/logging"
	"github.com/address-scanner/internal/retry"
	"github.com/address-scanner/internal/types"
)

// ProviderClient demonstrates error handling, retry, and circuit breaker integration
type ProviderClient struct {
	providerName    string
	circuitBreaker  *circuitbreaker.CircuitBreaker
	retryConfig     *retry.RetryConfig
	logger          *logging.Logger
}

// NewProviderClient creates a new provider client with error handling
func NewProviderClient(providerName string, cbManager *circuitbreaker.CircuitBreakerManager) *ProviderClient {
	// Get or create circuit breaker for this provider
	cbConfig := &circuitbreaker.Config{
		Name:             providerName,
		MaxFailures:      10,
		FailureThreshold: 0.5, // 50% failure rate triggers open
		Timeout:          30 * time.Second,
		HalfOpenMaxCalls: 3,
	}
	cb := cbManager.GetOrCreate(providerName, cbConfig)

	// Configure retry with exponential backoff
	retryConfig := &retry.RetryConfig{
		MaxAttempts:  5,
		InitialDelay: 1 * time.Second,
		MaxDelay:     60 * time.Second,
		Multiplier:   2.0,
	}

	return &ProviderClient{
		providerName:   providerName,
		circuitBreaker: cb,
		retryConfig:    retryConfig,
		logger:         logging.GetGlobalLogger().WithField("provider", providerName),
	}
}

// FetchTransactions demonstrates the complete error handling flow
func (pc *ProviderClient) FetchTransactions(ctx context.Context, address string) ([]*types.NormalizedTransaction, error) {
	logger := pc.logger.WithFields(map[string]interface{}{
		"operation": "FetchTransactions",
		"address":   address,
	})

	logger.Info("Fetching transactions from provider")

	var transactions []*types.NormalizedTransaction
	var lastErr error

	// Execute with circuit breaker protection
	err := pc.circuitBreaker.Execute(ctx, func() error {
		// Execute with retry logic
		result := retry.WithExponentialBackoff(ctx, pc.retryConfig, func(ctx context.Context, attempt int) error {
			logger.WithField("attempt", attempt).Debug("Attempting to fetch transactions")

			// Simulate provider call (replace with actual provider logic)
			txs, err := pc.callProvider(ctx, address)
			if err != nil {
				// Categorize the error
				catErr := errors.Categorize(err)
				
				logger.WithFields(map[string]interface{}{
					"attempt":  attempt,
					"category": catErr.Category,
					"code":     catErr.Code,
				}).Warn("Provider call failed")

				// Check if error is retryable
				if !errors.IsRetryable(err) {
					logger.WithError(err).Error("Non-retryable error encountered")
					return err // Don't retry
				}

				return err // Retry
			}

			transactions = txs
			return nil
		})

		if !result.Success {
			lastErr = result.LastError
			return result.LastError
		}

		return nil
	})

	if err != nil {
		// Circuit breaker is open
		if err == circuitbreaker.ErrCircuitOpen {
			logger.Warn("Circuit breaker is open, provider unavailable")
			return nil, errors.NewServiceUnavailableError(pc.providerName)
		}

		// Categorize and log the error
		catErr := errors.Categorize(lastErr)
		logger.WithFields(map[string]interface{}{
			"category":   catErr.Category,
			"statusCode": catErr.StatusCode,
			"code":       catErr.Code,
		}).Error("Failed to fetch transactions after all retries")

		return nil, catErr
	}

	logger.WithField("transactionCount", len(transactions)).Info("Successfully fetched transactions")
	return transactions, nil
}

// callProvider simulates calling an external provider
// In a real implementation, this would make HTTP requests to Alchemy, Infura, etc.
func (pc *ProviderClient) callProvider(ctx context.Context, address string) ([]*types.NormalizedTransaction, error) {
	// Simulate provider logic
	// This is where you would make actual HTTP requests
	
	// Example error scenarios:
	// 1. Provider timeout
	// return nil, errors.NewProviderTimeoutError(pc.providerName)
	
	// 2. Provider rate limit
	// return nil, errors.NewProviderRateLimitError(pc.providerName)
	
	// 3. Provider error
	// return nil, errors.NewProviderError(pc.providerName, fmt.Errorf("connection refused"))
	
	// Success case
	return []*types.NormalizedTransaction{}, nil
}

// GetProviderStatus returns the current status of the provider
func (pc *ProviderClient) GetProviderStatus() *ProviderStatus {
	stats := pc.circuitBreaker.GetStats()
	
	return &ProviderStatus{
		Name:             pc.providerName,
		Available:        stats.State == circuitbreaker.StateClosed,
		CircuitState:     string(stats.State),
		TotalCalls:       stats.TotalCalls,
		Failures:         stats.Failures,
		Successes:        stats.Successes,
		FailureRate:      stats.FailureRate,
		LastFailureTime:  stats.LastFailureTime,
		LastStateChange:  stats.LastStateChange,
	}
}

// ProviderStatus represents the status of a provider
type ProviderStatus struct {
	Name             string    `json:"name"`
	Available        bool      `json:"available"`
	CircuitState     string    `json:"circuitState"`
	TotalCalls       int       `json:"totalCalls"`
	Failures         int       `json:"failures"`
	Successes        int       `json:"successes"`
	FailureRate      float64   `json:"failureRate"`
	LastFailureTime  time.Time `json:"lastFailureTime"`
	LastStateChange  time.Time `json:"lastStateChange"`
}

// Example: Using the provider client with failover
type MultiProviderClient struct {
	primaryProvider   *ProviderClient
	secondaryProvider *ProviderClient
	logger            *logging.Logger
}

// NewMultiProviderClient creates a client with primary and secondary providers
func NewMultiProviderClient(cbManager *circuitbreaker.CircuitBreakerManager) *MultiProviderClient {
	return &MultiProviderClient{
		primaryProvider:   NewProviderClient("alchemy", cbManager),
		secondaryProvider: NewProviderClient("infura", cbManager),
		logger:            logging.GetGlobalLogger().WithField("component", "MultiProviderClient"),
	}
}

// FetchTransactionsWithFailover demonstrates provider failover
func (mpc *MultiProviderClient) FetchTransactionsWithFailover(ctx context.Context, address string) ([]*types.NormalizedTransaction, error) {
	mpc.logger.Info("Attempting to fetch transactions with failover")

	// Try primary provider
	txs, err := mpc.primaryProvider.FetchTransactions(ctx, address)
	if err == nil {
		mpc.logger.Info("Successfully fetched from primary provider")
		return txs, nil
	}

	// Log primary failure
	mpc.logger.WithError(err).Warn("Primary provider failed, attempting failover to secondary")

	// Try secondary provider
	txs, err = mpc.secondaryProvider.FetchTransactions(ctx, address)
	if err == nil {
		mpc.logger.Info("Successfully fetched from secondary provider")
		return txs, nil
	}

	// Both providers failed
	mpc.logger.WithError(err).Error("All providers failed")
	return nil, errors.NewServiceUnavailableError("all data providers")
}

// GetAllProviderStatuses returns status for all providers
func (mpc *MultiProviderClient) GetAllProviderStatuses() map[string]*ProviderStatus {
	return map[string]*ProviderStatus{
		"primary":   mpc.primaryProvider.GetProviderStatus(),
		"secondary": mpc.secondaryProvider.GetProviderStatus(),
	}
}

// Example: Error handling in a service method
func ExampleServiceMethodWithErrorHandling(ctx context.Context, address string) error {
	logger := logging.FromContext(ctx).WithFields(map[string]interface{}{
		"operation": "ExampleServiceMethod",
		"address":   address,
	})

	logger.Info("Starting operation")

	// Validate input
	if address == "" {
		err := errors.NewInvalidParameterError("address", "cannot be empty")
		logger.WithError(err).Warn("Validation failed")
		return err
	}

	// Simulate database operation
	err := performDatabaseOperation(ctx, address)
	if err != nil {
		// Categorize the error
		catErr := errors.Categorize(err)
		
		// Log based on category
		if errors.IsUserError(err) {
			logger.WithFields(map[string]interface{}{
				"category": catErr.Category,
				"code":     catErr.Code,
			}).Warn("User error encountered")
		} else if errors.IsSystemError(err) {
			logger.WithFields(map[string]interface{}{
				"category": catErr.Category,
				"code":     catErr.Code,
			}).Error("System error encountered")
		}

		return catErr
	}

	logger.Info("Operation completed successfully")
	return nil
}

// performDatabaseOperation simulates a database operation
func performDatabaseOperation(ctx context.Context, address string) error {
	// Simulate database logic
	// This would be replaced with actual database calls
	return nil
}

// Example: Graceful degradation when cache is unavailable
func ExampleGracefulDegradation(ctx context.Context, address string, cacheAvailable bool) ([]*types.NormalizedTransaction, error) {
	logger := logging.FromContext(ctx).WithField("operation", "ExampleGracefulDegradation")

	if cacheAvailable {
		// Try cache first
		logger.Debug("Attempting to fetch from cache")
		// txs, err := fetchFromCache(ctx, address)
		// if err == nil {
		//     return txs, nil
		// }
		// logger.WithError(err).Warn("Cache unavailable, falling back to database")
	}

	// Fallback to database
	logger.Info("Fetching from database (cache unavailable)")
	// return fetchFromDatabase(ctx, address)
	return nil, nil
}
