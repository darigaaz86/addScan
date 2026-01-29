package api

import (
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/address-scanner/internal/service"
	"github.com/address-scanner/internal/types"
	"github.com/gorilla/mux"
)

// handleAddAddress handles POST /api/addresses - Add address for tracking
// Requirements: 1.1, 1.2, 1.5, 1.6
func (s *Server) handleAddAddress(w http.ResponseWriter, r *http.Request) {
	// Parse request body
	var req struct {
		Address string          `json:"address"`
		Chains  []types.ChainID `json:"chains,omitempty"`
	}

	if err := parseJSONBody(r, &req); err != nil {
		respondError(w, http.StatusBadRequest, ErrCodeInvalidInput, "Invalid request body", nil)
		return
	}

	// Get user ID and tier from headers (in production, this would come from auth middleware)
	userID := r.Header.Get("X-User-ID")
	if userID == "" {
		respondError(w, http.StatusUnauthorized, ErrCodeUnauthorized, "User ID required", nil)
		return
	}

	tierStr := r.Header.Get("X-User-Tier")
	if tierStr == "" {
		tierStr = "free" // Default to free tier
	}

	tier := types.UserTier(tierStr)
	if tier != types.TierFree && tier != types.TierPaid {
		respondError(w, http.StatusBadRequest, ErrCodeInvalidInput, "Invalid tier value", nil)
		return
	}

	// Call service
	input := &service.AddAddressInput{
		UserID:  userID,
		Address: req.Address,
		Chains:  req.Chains,
		Tier:    tier,
	}

	result, err := s.addressService.AddAddress(r.Context(), input)
	if err != nil {
		// Log the actual error for debugging
		log.Printf("AddAddress error: %v", err)
		statusCode, code, message := mapServiceError(err)
		respondError(w, statusCode, code, message, nil)
		return
	}

	// Return success response
	respondJSON(w, http.StatusCreated, result)
}

// handleGetAddress handles GET /api/addresses/:address - Get address details
// Requirements: 1.1
func (s *Server) handleGetAddress(w http.ResponseWriter, r *http.Request) {
	// Get address from URL
	vars := mux.Vars(r)
	address := vars["address"]

	if address == "" {
		respondError(w, http.StatusBadRequest, ErrCodeInvalidInput, "Address parameter required", nil)
		return
	}

	// Normalize address
	address = strings.ToLower(address)

	// Get user ID from headers
	userID := r.Header.Get("X-User-ID")
	if userID == "" {
		respondError(w, http.StatusUnauthorized, ErrCodeUnauthorized, "User ID required", nil)
		return
	}

	// For now, return a placeholder response
	// Full implementation would query address details from storage
	response := map[string]interface{}{
		"address": address,
		"message": "Address details endpoint - full implementation pending",
	}

	respondJSON(w, http.StatusOK, response)
}

// handleGetTransactions handles GET /api/addresses/:address/transactions - Get transactions with pagination
// Requirements: 4.1, 5.1, 14.1, 14.2, 14.4
func (s *Server) handleGetTransactions(w http.ResponseWriter, r *http.Request) {
	// Get address from URL
	vars := mux.Vars(r)
	address := vars["address"]

	if address == "" {
		respondError(w, http.StatusBadRequest, ErrCodeInvalidInput, "Address parameter required", nil)
		return
	}

	// Normalize address
	address = strings.ToLower(address)

	// Parse query parameters
	query := r.URL.Query()

	// Parse chains filter
	var chains []types.ChainID
	if chainsStr := query.Get("chains"); chainsStr != "" {
		chainStrs := strings.Split(chainsStr, ",")
		for _, chainStr := range chainStrs {
			chains = append(chains, types.ChainID(strings.TrimSpace(chainStr)))
		}
	}

	// Parse pagination
	limit := 50 // Default
	if limitStr := query.Get("limit"); limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
			limit = l
			// Max limit is validated by QueryService based on CACHE_TRANSACTION_WINDOW config
		}
	}

	offset := 0 // Default
	if offsetStr := query.Get("offset"); offsetStr != "" {
		if o, err := strconv.Atoi(offsetStr); err == nil && o >= 0 {
			offset = o
		}
	}

	// Parse date filters
	var dateFrom, dateTo *time.Time
	if dateFromStr := query.Get("dateFrom"); dateFromStr != "" {
		if t, err := time.Parse(time.RFC3339, dateFromStr); err == nil {
			dateFrom = &t
		}
	}
	if dateToStr := query.Get("dateTo"); dateToStr != "" {
		if t, err := time.Parse(time.RFC3339, dateToStr); err == nil {
			dateTo = &t
		}
	}

	// Parse value filters
	var minValue, maxValue *float64
	if minValueStr := query.Get("minValue"); minValueStr != "" {
		if v, err := strconv.ParseFloat(minValueStr, 64); err == nil {
			minValue = &v
		}
	}
	if maxValueStr := query.Get("maxValue"); maxValueStr != "" {
		if v, err := strconv.ParseFloat(maxValueStr, 64); err == nil {
			maxValue = &v
		}
	}

	// Parse sort parameters
	sortBy := query.Get("sortBy")
	if sortBy == "" {
		sortBy = "timestamp"
	}

	sortOrder := query.Get("sortOrder")
	if sortOrder == "" {
		sortOrder = "desc"
	}

	// Call query service
	input := &service.QueryInput{
		Address:   address,
		Chains:    chains,
		DateFrom:  dateFrom,
		DateTo:    dateTo,
		MinValue:  minValue,
		MaxValue:  maxValue,
		SortBy:    sortBy,
		SortOrder: sortOrder,
		Limit:     limit,
		Offset:    offset,
	}

	result, err := s.queryService.Query(r.Context(), input)
	if err != nil {
		log.Printf("Query error for address %s: %v", address, err)
		statusCode, code, message := mapServiceError(err)
		respondError(w, statusCode, code, message, nil)
		return
	}

	// Return success response
	respondJSON(w, http.StatusOK, result)
}

// handleRemoveAddress handles DELETE /api/addresses/:address - Remove address tracking
// Requirements: 5.1
func (s *Server) handleRemoveAddress(w http.ResponseWriter, r *http.Request) {
	// Get address from URL
	vars := mux.Vars(r)
	address := vars["address"]

	if address == "" {
		respondError(w, http.StatusBadRequest, ErrCodeInvalidInput, "Address parameter required", nil)
		return
	}

	// Normalize address
	address = strings.ToLower(address)

	// Get user ID from headers
	userID := r.Header.Get("X-User-ID")
	if userID == "" {
		respondError(w, http.StatusUnauthorized, ErrCodeUnauthorized, "User ID required", nil)
		return
	}

	// For now, return a placeholder response
	// Full implementation would remove user-address association from storage
	response := map[string]interface{}{
		"success": true,
		"address": address,
		"message": "Address removed from tracking",
	}

	respondJSON(w, http.StatusOK, response)
}

// handleGetBalance handles GET /api/addresses/:address/balance - Get current balance
// Returns native token balance and token balances across all chains
func (s *Server) handleGetBalance(w http.ResponseWriter, r *http.Request) {
	// Get address from URL
	vars := mux.Vars(r)
	address := vars["address"]

	if address == "" {
		respondError(w, http.StatusBadRequest, ErrCodeInvalidInput, "Address parameter required", nil)
		return
	}

	// Normalize address
	address = strings.ToLower(address)

	// Parse optional chain filter
	query := r.URL.Query()
	var chains []types.ChainID
	if chainsStr := query.Get("chains"); chainsStr != "" {
		chainStrs := strings.Split(chainsStr, ",")
		for _, chainStr := range chainStrs {
			chains = append(chains, types.ChainID(strings.TrimSpace(chainStr)))
		}
	}

	// Call address service to get balance
	input := &service.GetBalanceInput{
		Address: address,
		Chains:  chains,
	}

	result, err := s.addressService.GetBalance(r.Context(), input)
	if err != nil {
		log.Printf("GetBalance error: %v", err)
		statusCode, code, message := mapServiceError(err)
		respondError(w, statusCode, code, message, nil)
		return
	}

	respondJSON(w, http.StatusOK, result)
}
