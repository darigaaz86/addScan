package errors

import (
	"fmt"
	"net/http"

	"github.com/address-scanner/internal/types"
)

// ErrorCategory represents the category of an error
type ErrorCategory string

const (
	// CategoryUserInput represents user input errors (4xx)
	CategoryUserInput ErrorCategory = "user_input"
	// CategorySystem represents system errors (5xx)
	CategorySystem ErrorCategory = "system"
	// CategoryProvider represents data provider errors
	CategoryProvider ErrorCategory = "provider"
	// CategoryDatabase represents database errors
	CategoryDatabase ErrorCategory = "database"
	// CategoryCache represents cache errors
	CategoryCache ErrorCategory = "cache"
	// CategoryValidation represents validation errors
	CategoryValidation ErrorCategory = "validation"
	// CategoryAuthorization represents authorization errors
	CategoryAuthorization ErrorCategory = "authorization"
	// CategoryNotFound represents not found errors
	CategoryNotFound ErrorCategory = "not_found"
	// CategoryConflict represents conflict errors
	CategoryConflict ErrorCategory = "conflict"
	// CategoryRateLimit represents rate limit errors
	CategoryRateLimit ErrorCategory = "rate_limit"
)

// CategorizedError represents an error with category and HTTP status code
type CategorizedError struct {
	Category   ErrorCategory
	StatusCode int
	Code       string
	Message    string
	Details    map[string]interface{}
	Cause      error
}

// Error implements the error interface
func (e *CategorizedError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s: %s (caused by: %v)", e.Code, e.Message, e.Cause)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// Unwrap returns the underlying cause
func (e *CategorizedError) Unwrap() error {
	return e.Cause
}

// ToServiceError converts to a ServiceError
func (e *CategorizedError) ToServiceError() *types.ServiceError {
	return &types.ServiceError{
		Code:    e.Code,
		Message: e.Message,
		Details: e.Details,
	}
}

// User Input Errors (4xx)

// NewInvalidAddressError creates an invalid address error
func NewInvalidAddressError(address string) *CategorizedError {
	return &CategorizedError{
		Category:   CategoryUserInput,
		StatusCode: http.StatusBadRequest,
		Code:       "INVALID_ADDRESS",
		Message:    fmt.Sprintf("invalid address format: %s", address),
		Details: map[string]interface{}{
			"address": address,
		},
	}
}

// NewInvalidParameterError creates an invalid parameter error
func NewInvalidParameterError(param string, reason string) *CategorizedError {
	return &CategorizedError{
		Category:   CategoryValidation,
		StatusCode: http.StatusBadRequest,
		Code:       "INVALID_PARAMETER",
		Message:    fmt.Sprintf("invalid parameter '%s': %s", param, reason),
		Details: map[string]interface{}{
			"parameter": param,
			"reason":    reason,
		},
	}
}

// NewUnauthorizedError creates an unauthorized error
func NewUnauthorizedError(message string) *CategorizedError {
	return &CategorizedError{
		Category:   CategoryAuthorization,
		StatusCode: http.StatusUnauthorized,
		Code:       "UNAUTHORIZED",
		Message:    message,
	}
}

// NewForbiddenError creates a forbidden error
func NewForbiddenError(message string) *CategorizedError {
	return &CategorizedError{
		Category:   CategoryAuthorization,
		StatusCode: http.StatusForbidden,
		Code:       "FORBIDDEN",
		Message:    message,
	}
}

// NewNotFoundError creates a not found error
func NewNotFoundError(resource string, id string) *CategorizedError {
	return &CategorizedError{
		Category:   CategoryNotFound,
		StatusCode: http.StatusNotFound,
		Code:       "NOT_FOUND",
		Message:    fmt.Sprintf("%s not found: %s", resource, id),
		Details: map[string]interface{}{
			"resource": resource,
			"id":       id,
		},
	}
}

// NewConflictError creates a conflict error
func NewConflictError(message string) *CategorizedError {
	return &CategorizedError{
		Category:   CategoryConflict,
		StatusCode: http.StatusConflict,
		Code:       "CONFLICT",
		Message:    message,
	}
}

// NewTierLimitExceededError creates a tier limit exceeded error
func NewTierLimitExceededError(tier string, limit int) *CategorizedError {
	return &CategorizedError{
		Category:   CategoryRateLimit,
		StatusCode: http.StatusForbidden,
		Code:       "TIER_LIMIT_EXCEEDED",
		Message:    fmt.Sprintf("tier limit exceeded for %s tier (limit: %d)", tier, limit),
		Details: map[string]interface{}{
			"tier":  tier,
			"limit": limit,
		},
	}
}

// NewRateLimitError creates a rate limit error
func NewRateLimitError(retryAfter int) *CategorizedError {
	return &CategorizedError{
		Category:   CategoryRateLimit,
		StatusCode: http.StatusTooManyRequests,
		Code:       "RATE_LIMIT_EXCEEDED",
		Message:    "rate limit exceeded",
		Details: map[string]interface{}{
			"retryAfter": retryAfter,
		},
	}
}

// System Errors (5xx)

// NewInternalError creates an internal server error
func NewInternalError(message string, cause error) *CategorizedError {
	return &CategorizedError{
		Category:   CategorySystem,
		StatusCode: http.StatusInternalServerError,
		Code:       "INTERNAL_ERROR",
		Message:    message,
		Cause:      cause,
	}
}

// NewDatabaseError creates a database error
func NewDatabaseError(operation string, cause error) *CategorizedError {
	return &CategorizedError{
		Category:   CategoryDatabase,
		StatusCode: http.StatusInternalServerError,
		Code:       "DATABASE_ERROR",
		Message:    fmt.Sprintf("database error during %s", operation),
		Cause:      cause,
		Details: map[string]interface{}{
			"operation": operation,
		},
	}
}

// NewCacheError creates a cache error
func NewCacheError(operation string, cause error) *CategorizedError {
	return &CategorizedError{
		Category:   CategoryCache,
		StatusCode: http.StatusInternalServerError,
		Code:       "CACHE_ERROR",
		Message:    fmt.Sprintf("cache error during %s", operation),
		Cause:      cause,
		Details: map[string]interface{}{
			"operation": operation,
		},
	}
}

// NewServiceUnavailableError creates a service unavailable error
func NewServiceUnavailableError(service string) *CategorizedError {
	return &CategorizedError{
		Category:   CategorySystem,
		StatusCode: http.StatusServiceUnavailable,
		Code:       "SERVICE_UNAVAILABLE",
		Message:    fmt.Sprintf("service unavailable: %s", service),
		Details: map[string]interface{}{
			"service": service,
		},
	}
}

// Data Provider Errors

// NewProviderError creates a data provider error
func NewProviderError(provider string, cause error) *CategorizedError {
	return &CategorizedError{
		Category:   CategoryProvider,
		StatusCode: http.StatusBadGateway,
		Code:       "PROVIDER_ERROR",
		Message:    fmt.Sprintf("data provider error: %s", provider),
		Cause:      cause,
		Details: map[string]interface{}{
			"provider": provider,
		},
	}
}

// NewProviderTimeoutError creates a provider timeout error
func NewProviderTimeoutError(provider string) *CategorizedError {
	return &CategorizedError{
		Category:   CategoryProvider,
		StatusCode: http.StatusGatewayTimeout,
		Code:       "PROVIDER_TIMEOUT",
		Message:    fmt.Sprintf("data provider timeout: %s", provider),
		Details: map[string]interface{}{
			"provider": provider,
		},
	}
}

// NewProviderRateLimitError creates a provider rate limit error
func NewProviderRateLimitError(provider string) *CategorizedError {
	return &CategorizedError{
		Category:   CategoryProvider,
		StatusCode: http.StatusTooManyRequests,
		Code:       "PROVIDER_RATE_LIMIT",
		Message:    fmt.Sprintf("data provider rate limit exceeded: %s", provider),
		Details: map[string]interface{}{
			"provider": provider,
		},
	}
}

// Categorize categorizes an existing error
func Categorize(err error) *CategorizedError {
	if err == nil {
		return nil
	}

	// If already categorized, return as-is
	if catErr, ok := err.(*CategorizedError); ok {
		return catErr
	}

	// If it's a ServiceError, convert it
	if svcErr, ok := err.(*types.ServiceError); ok {
		return categorizeServiceError(svcErr)
	}

	// Default to internal error
	return NewInternalError("unexpected error", err)
}

// categorizeServiceError categorizes a ServiceError
func categorizeServiceError(err *types.ServiceError) *CategorizedError {
	switch err.Code {
	case "INVALID_ADDRESS", "INVALID_ADDRESS_FORMAT":
		return &CategorizedError{
			Category:   CategoryUserInput,
			StatusCode: http.StatusBadRequest,
			Code:       err.Code,
			Message:    err.Message,
			Details:    err.Details,
		}
	case "USER_NOT_FOUND", "ADDRESS_NOT_FOUND", "PORTFOLIO_NOT_FOUND":
		return &CategorizedError{
			Category:   CategoryNotFound,
			StatusCode: http.StatusNotFound,
			Code:       err.Code,
			Message:    err.Message,
			Details:    err.Details,
		}
	case "TIER_LIMIT_EXCEEDED":
		return &CategorizedError{
			Category:   CategoryRateLimit,
			StatusCode: http.StatusForbidden,
			Code:       err.Code,
			Message:    err.Message,
			Details:    err.Details,
		}
	case "UNAUTHORIZED":
		return &CategorizedError{
			Category:   CategoryAuthorization,
			StatusCode: http.StatusUnauthorized,
			Code:       err.Code,
			Message:    err.Message,
			Details:    err.Details,
		}
	case "FORBIDDEN":
		return &CategorizedError{
			Category:   CategoryAuthorization,
			StatusCode: http.StatusForbidden,
			Code:       err.Code,
			Message:    err.Message,
			Details:    err.Details,
		}
	case "IMMUTABLE_TRANSACTION", "IMMUTABLE_TRANSACTION_CONFLICT":
		return &CategorizedError{
			Category:   CategoryConflict,
			StatusCode: http.StatusConflict,
			Code:       err.Code,
			Message:    err.Message,
			Details:    err.Details,
		}
	default:
		return &CategorizedError{
			Category:   CategorySystem,
			StatusCode: http.StatusInternalServerError,
			Code:       err.Code,
			Message:    err.Message,
			Details:    err.Details,
		}
	}
}

// GetHTTPStatusCode returns the HTTP status code for an error
func GetHTTPStatusCode(err error) int {
	if catErr := Categorize(err); catErr != nil {
		return catErr.StatusCode
	}
	return http.StatusInternalServerError
}

// IsRetryable determines if an error is retryable
func IsRetryable(err error) bool {
	catErr := Categorize(err)
	if catErr == nil {
		return false
	}

	// Retryable categories
	switch catErr.Category {
	case CategoryProvider, CategoryDatabase, CategoryCache:
		return true
	case CategorySystem:
		// Some system errors are retryable
		return catErr.StatusCode == http.StatusServiceUnavailable ||
			catErr.StatusCode == http.StatusGatewayTimeout
	default:
		return false
	}
}

// IsUserError determines if an error is a user error (4xx)
func IsUserError(err error) bool {
	catErr := Categorize(err)
	if catErr == nil {
		return false
	}

	return catErr.StatusCode >= 400 && catErr.StatusCode < 500
}

// IsSystemError determines if an error is a system error (5xx)
func IsSystemError(err error) bool {
	catErr := Categorize(err)
	if catErr == nil {
		return false
	}

	return catErr.StatusCode >= 500
}
