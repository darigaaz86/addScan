package circuitbreaker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/address-scanner/internal/logging"
)

// State represents the circuit breaker state
type State string

const (
	// StateClosed means the circuit is closed and requests are allowed
	StateClosed State = "closed"
	// StateOpen means the circuit is open and requests are blocked
	StateOpen State = "open"
	// StateHalfOpen means the circuit is testing if the service has recovered
	StateHalfOpen State = "half_open"
)

// CircuitBreaker implements the circuit breaker pattern
type CircuitBreaker struct {
	name             string
	maxFailures      int           // Number of failures before opening
	failureThreshold float64       // Percentage of failures to trigger open (0.0-1.0)
	timeout          time.Duration // Time to wait before attempting half-open
	halfOpenMaxCalls int           // Max calls allowed in half-open state

	mu               sync.RWMutex
	state            State
	failures         int
	successes        int
	totalCalls       int
	lastFailureTime  time.Time
	lastStateChange  time.Time
	consecutiveFails int
}

// Config configures a circuit breaker
type Config struct {
	Name             string
	MaxFailures      int
	FailureThreshold float64
	Timeout          time.Duration
	HalfOpenMaxCalls int
}

// DefaultConfig returns a default circuit breaker configuration
func DefaultConfig(name string) *Config {
	return &Config{
		Name:             name,
		MaxFailures:      10,
		FailureThreshold: 0.5, // 50% failure rate
		Timeout:          30 * time.Second,
		HalfOpenMaxCalls: 3,
	}
}

// NewCircuitBreaker creates a new circuit breaker
func NewCircuitBreaker(config *Config) *CircuitBreaker {
	return &CircuitBreaker{
		name:             config.Name,
		maxFailures:      config.MaxFailures,
		failureThreshold: config.FailureThreshold,
		timeout:          config.Timeout,
		halfOpenMaxCalls: config.HalfOpenMaxCalls,
		state:            StateClosed,
		lastStateChange:  time.Now(),
	}
}

// ErrCircuitOpen is returned when the circuit breaker is open
var ErrCircuitOpen = errors.New("circuit breaker is open")

// ErrTooManyRequests is returned when too many requests are made in half-open state
var ErrTooManyRequests = errors.New("too many requests in half-open state")

// Execute executes a function with circuit breaker protection
func (cb *CircuitBreaker) Execute(ctx context.Context, fn func() error) error {
	// Check if we can execute
	if err := cb.beforeRequest(); err != nil {
		return err
	}

	// Execute the function
	err := fn()

	// Record the result
	cb.afterRequest(err)

	return err
}

// beforeRequest checks if a request can be executed
func (cb *CircuitBreaker) beforeRequest() error {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case StateClosed:
		// Allow request
		return nil

	case StateOpen:
		// Check if timeout has elapsed
		if time.Since(cb.lastStateChange) > cb.timeout {
			// Transition to half-open
			cb.setState(StateHalfOpen)
			logging.WithFields(map[string]interface{}{
				"circuitBreaker": cb.name,
				"state":          StateHalfOpen,
			}).Info("Circuit breaker transitioning to half-open")
			return nil
		}
		// Circuit is still open
		return ErrCircuitOpen

	case StateHalfOpen:
		// Allow limited requests in half-open state
		if cb.totalCalls >= cb.halfOpenMaxCalls {
			return ErrTooManyRequests
		}
		return nil

	default:
		return nil
	}
}

// afterRequest records the result of a request
func (cb *CircuitBreaker) afterRequest(err error) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.totalCalls++

	if err != nil {
		cb.onFailure()
	} else {
		cb.onSuccess()
	}
}

// onSuccess handles a successful request
func (cb *CircuitBreaker) onSuccess() {
	cb.successes++
	cb.consecutiveFails = 0

	switch cb.state {
	case StateHalfOpen:
		// If we've had enough successful calls in half-open, close the circuit
		if cb.successes >= cb.halfOpenMaxCalls {
			cb.setState(StateClosed)
			cb.reset()
			logging.WithFields(map[string]interface{}{
				"circuitBreaker": cb.name,
				"state":          StateClosed,
			}).Info("Circuit breaker closed after successful recovery")
		}
	}
}

// onFailure handles a failed request
func (cb *CircuitBreaker) onFailure() {
	cb.failures++
	cb.consecutiveFails++
	cb.lastFailureTime = time.Now()

	switch cb.state {
	case StateClosed:
		// Check if we should open the circuit
		if cb.shouldOpen() {
			cb.setState(StateOpen)
			logging.WithFields(map[string]interface{}{
				"circuitBreaker":     cb.name,
				"state":              StateOpen,
				"failures":           cb.failures,
				"totalCalls":         cb.totalCalls,
				"failureRate":        cb.getFailureRate(),
				"consecutiveFails":   cb.consecutiveFails,
			}).Warn("Circuit breaker opened due to failures")
		}

	case StateHalfOpen:
		// Any failure in half-open state reopens the circuit
		cb.setState(StateOpen)
		logging.WithFields(map[string]interface{}{
			"circuitBreaker": cb.name,
			"state":          StateOpen,
		}).Warn("Circuit breaker reopened after failure in half-open state")
	}
}

// shouldOpen determines if the circuit should open
func (cb *CircuitBreaker) shouldOpen() bool {
	// Need minimum number of calls to make a decision
	if cb.totalCalls < cb.maxFailures {
		return false
	}

	// Check failure rate
	failureRate := cb.getFailureRate()
	if failureRate >= cb.failureThreshold {
		return true
	}

	// Check consecutive failures
	if cb.consecutiveFails >= cb.maxFailures {
		return true
	}

	return false
}

// getFailureRate calculates the current failure rate
func (cb *CircuitBreaker) getFailureRate() float64 {
	if cb.totalCalls == 0 {
		return 0.0
	}
	return float64(cb.failures) / float64(cb.totalCalls)
}

// setState changes the circuit breaker state
func (cb *CircuitBreaker) setState(state State) {
	cb.state = state
	cb.lastStateChange = time.Now()
}

// reset resets the circuit breaker counters
func (cb *CircuitBreaker) reset() {
	cb.failures = 0
	cb.successes = 0
	cb.totalCalls = 0
	cb.consecutiveFails = 0
}

// GetState returns the current state of the circuit breaker
func (cb *CircuitBreaker) GetState() State {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state
}

// GetStats returns statistics about the circuit breaker
func (cb *CircuitBreaker) GetStats() *Stats {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	return &Stats{
		Name:             cb.name,
		State:            cb.state,
		Failures:         cb.failures,
		Successes:        cb.successes,
		TotalCalls:       cb.totalCalls,
		ConsecutiveFails: cb.consecutiveFails,
		FailureRate:      cb.getFailureRate(),
		LastFailureTime:  cb.lastFailureTime,
		LastStateChange:  cb.lastStateChange,
	}
}

// Stats represents circuit breaker statistics
type Stats struct {
	Name             string    `json:"name"`
	State            State     `json:"state"`
	Failures         int       `json:"failures"`
	Successes        int       `json:"successes"`
	TotalCalls       int       `json:"totalCalls"`
	ConsecutiveFails int       `json:"consecutiveFails"`
	FailureRate      float64   `json:"failureRate"`
	LastFailureTime  time.Time `json:"lastFailureTime"`
	LastStateChange  time.Time `json:"lastStateChange"`
}

// Reset manually resets the circuit breaker to closed state
func (cb *CircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.setState(StateClosed)
	cb.reset()

	logging.WithField("circuitBreaker", cb.name).Info("Circuit breaker manually reset")
}

// ForceOpen manually forces the circuit breaker to open state
func (cb *CircuitBreaker) ForceOpen() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.setState(StateOpen)

	logging.WithField("circuitBreaker", cb.name).Warn("Circuit breaker manually forced open")
}

// CircuitBreakerManager manages multiple circuit breakers
type CircuitBreakerManager struct {
	breakers map[string]*CircuitBreaker
	mu       sync.RWMutex
}

// NewCircuitBreakerManager creates a new circuit breaker manager
func NewCircuitBreakerManager() *CircuitBreakerManager {
	return &CircuitBreakerManager{
		breakers: make(map[string]*CircuitBreaker),
	}
}

// GetOrCreate gets an existing circuit breaker or creates a new one
func (cbm *CircuitBreakerManager) GetOrCreate(name string, config *Config) *CircuitBreaker {
	cbm.mu.Lock()
	defer cbm.mu.Unlock()

	if cb, exists := cbm.breakers[name]; exists {
		return cb
	}

	if config == nil {
		config = DefaultConfig(name)
	}

	cb := NewCircuitBreaker(config)
	cbm.breakers[name] = cb

	return cb
}

// Get retrieves a circuit breaker by name
func (cbm *CircuitBreakerManager) Get(name string) (*CircuitBreaker, error) {
	cbm.mu.RLock()
	defer cbm.mu.RUnlock()

	if cb, exists := cbm.breakers[name]; exists {
		return cb, nil
	}

	return nil, fmt.Errorf("circuit breaker '%s' not found", name)
}

// GetAll returns all circuit breakers
func (cbm *CircuitBreakerManager) GetAll() map[string]*CircuitBreaker {
	cbm.mu.RLock()
	defer cbm.mu.RUnlock()

	result := make(map[string]*CircuitBreaker)
	for name, cb := range cbm.breakers {
		result[name] = cb
	}

	return result
}

// GetAllStats returns statistics for all circuit breakers
func (cbm *CircuitBreakerManager) GetAllStats() map[string]*Stats {
	cbm.mu.RLock()
	defer cbm.mu.RUnlock()

	result := make(map[string]*Stats)
	for name, cb := range cbm.breakers {
		result[name] = cb.GetStats()
	}

	return result
}

// ResetAll resets all circuit breakers
func (cbm *CircuitBreakerManager) ResetAll() {
	cbm.mu.RLock()
	defer cbm.mu.RUnlock()

	for _, cb := range cbm.breakers {
		cb.Reset()
	}

	logging.Info("All circuit breakers reset")
}

// Remove removes a circuit breaker
func (cbm *CircuitBreakerManager) Remove(name string) {
	cbm.mu.Lock()
	defer cbm.mu.Unlock()

	delete(cbm.breakers, name)
}
