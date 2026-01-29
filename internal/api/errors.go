package api

import (
	"encoding/json"
	"net/http"

	"github.com/address-scanner/internal/types"
)

// ErrorResponse represents an API error response.
type ErrorResponse struct {
	Error types.ServiceError `json:"error"`
}

// respondError sends an error response.
func respondError(w http.ResponseWriter, statusCode int, code, message string, details map[string]interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)

	response := ErrorResponse{
		Error: types.ServiceError{
			Code:    code,
			Message: message,
			Details: details,
		},
	}

	json.NewEncoder(w).Encode(response)
}

// respondJSON sends a JSON response.
func respondJSON(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)

	if data != nil {
		json.NewEncoder(w).Encode(data)
	}
}

// parseJSONBody parses JSON request body.
func parseJSONBody(r *http.Request, v interface{}) error {
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	return decoder.Decode(v)
}

// Common error codes
const (
	ErrCodeInvalidInput       = "INVALID_INPUT"
	ErrCodeNotFound           = "NOT_FOUND"
	ErrCodeUnauthorized       = "UNAUTHORIZED"
	ErrCodeForbidden          = "FORBIDDEN"
	ErrCodeTierLimitExceeded  = "TIER_LIMIT_EXCEEDED"
	ErrCodeInternalError      = "INTERNAL_ERROR"
	ErrCodeServiceUnavailable = "SERVICE_UNAVAILABLE"
)

// mapServiceError maps service errors to HTTP status codes.
func mapServiceError(err error) (int, string, string) {
	if serviceErr, ok := err.(*types.ServiceError); ok {
		switch serviceErr.Code {
		case "INVALID_ADDRESS", "INVALID_ADDRESS_FORMAT":
			return http.StatusBadRequest, ErrCodeInvalidInput, serviceErr.Message
		case "ADDRESS_NOT_FOUND":
			return http.StatusNotFound, ErrCodeNotFound, serviceErr.Message
		case "PORTFOLIO_NOT_FOUND":
			return http.StatusNotFound, ErrCodeNotFound, serviceErr.Message
		case "TRANSACTION_NOT_FOUND":
			return http.StatusNotFound, ErrCodeNotFound, serviceErr.Message
		case "USER_NOT_FOUND":
			return http.StatusNotFound, ErrCodeNotFound, serviceErr.Message
		case "NO_CHAIN_ADAPTERS":
			return http.StatusServiceUnavailable, ErrCodeServiceUnavailable, serviceErr.Message
		case "TIER_LIMIT_EXCEEDED":
			return http.StatusForbidden, ErrCodeTierLimitExceeded, serviceErr.Message
		case "UNAUTHORIZED":
			return http.StatusUnauthorized, ErrCodeUnauthorized, serviceErr.Message
		case "FORBIDDEN":
			return http.StatusForbidden, ErrCodeForbidden, serviceErr.Message
		default:
			return http.StatusInternalServerError, ErrCodeInternalError, "An internal error occurred"
		}
	}

	// Default to internal server error
	return http.StatusInternalServerError, ErrCodeInternalError, "An internal error occurred"
}
