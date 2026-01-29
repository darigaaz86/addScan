package api

import (
	"net/http"
	"time"

	"github.com/address-scanner/internal/service"
	"github.com/gorilla/mux"
)

// handleCreatePortfolio handles POST /api/portfolios - Create portfolio
// Requirements: 13.1
func (s *Server) handleCreatePortfolio(w http.ResponseWriter, r *http.Request) {
	// Parse request body
	var req struct {
		Name        string   `json:"name"`
		Addresses   []string `json:"addresses"`
		Description *string  `json:"description,omitempty"`
	}

	if err := parseJSONBody(r, &req); err != nil {
		respondError(w, http.StatusBadRequest, ErrCodeInvalidInput, "Invalid request body", nil)
		return
	}

	// Get user ID from headers
	userID := r.Header.Get("X-User-ID")
	if userID == "" {
		respondError(w, http.StatusUnauthorized, ErrCodeUnauthorized, "User ID required", nil)
		return
	}

	// Call service
	input := &service.CreatePortfolioInput{
		UserID:      userID,
		Name:        req.Name,
		Addresses:   req.Addresses,
		Description: req.Description,
	}

	portfolio, err := s.portfolioService.CreatePortfolio(r.Context(), input)
	if err != nil {
		statusCode, code, message := mapServiceError(err)
		respondError(w, statusCode, code, message, nil)
		return
	}

	// Return success response
	respondJSON(w, http.StatusCreated, portfolio)
}

// handleGetPortfolio handles GET /api/portfolios/:id - Get portfolio details
// Requirements: 13.2, 13.3, 13.4
func (s *Server) handleGetPortfolio(w http.ResponseWriter, r *http.Request) {
	// Get portfolio ID from URL
	vars := mux.Vars(r)
	portfolioID := vars["id"]

	if portfolioID == "" {
		respondError(w, http.StatusBadRequest, ErrCodeInvalidInput, "Portfolio ID required", nil)
		return
	}

	// Get user ID from headers
	userID := r.Header.Get("X-User-ID")
	if userID == "" {
		respondError(w, http.StatusUnauthorized, ErrCodeUnauthorized, "User ID required", nil)
		return
	}

	// Call service
	portfolioView, err := s.portfolioService.GetPortfolio(r.Context(), portfolioID, userID)
	if err != nil {
		statusCode, code, message := mapServiceError(err)
		respondError(w, statusCode, code, message, nil)
		return
	}

	// Return success response
	respondJSON(w, http.StatusOK, portfolioView)
}

// handleUpdatePortfolio handles PUT /api/portfolios/:id - Update portfolio
// Requirements: 13.5, 15.7
func (s *Server) handleUpdatePortfolio(w http.ResponseWriter, r *http.Request) {
	// Get portfolio ID from URL
	vars := mux.Vars(r)
	portfolioID := vars["id"]

	if portfolioID == "" {
		respondError(w, http.StatusBadRequest, ErrCodeInvalidInput, "Portfolio ID required", nil)
		return
	}

	// Parse request body
	var req struct {
		Name        *string  `json:"name,omitempty"`
		Addresses   []string `json:"addresses,omitempty"`
		Description *string  `json:"description,omitempty"`
	}

	if err := parseJSONBody(r, &req); err != nil {
		respondError(w, http.StatusBadRequest, ErrCodeInvalidInput, "Invalid request body", nil)
		return
	}

	// Get user ID from headers
	userID := r.Header.Get("X-User-ID")
	if userID == "" {
		respondError(w, http.StatusUnauthorized, ErrCodeUnauthorized, "User ID required", nil)
		return
	}

	// Call service
	input := &service.UpdatePortfolioInput{
		PortfolioID: portfolioID,
		UserID:      userID,
		Name:        req.Name,
		Addresses:   req.Addresses,
		Description: req.Description,
	}

	portfolio, err := s.portfolioService.UpdatePortfolio(r.Context(), input)
	if err != nil {
		statusCode, code, message := mapServiceError(err)
		respondError(w, statusCode, code, message, nil)
		return
	}

	// Return success response
	respondJSON(w, http.StatusOK, portfolio)
}

// handleDeletePortfolio handles DELETE /api/portfolios/:id - Delete portfolio
// Requirements: 13.7
func (s *Server) handleDeletePortfolio(w http.ResponseWriter, r *http.Request) {
	// Get portfolio ID from URL
	vars := mux.Vars(r)
	portfolioID := vars["id"]

	if portfolioID == "" {
		respondError(w, http.StatusBadRequest, ErrCodeInvalidInput, "Portfolio ID required", nil)
		return
	}

	// Get user ID from headers
	userID := r.Header.Get("X-User-ID")
	if userID == "" {
		respondError(w, http.StatusUnauthorized, ErrCodeUnauthorized, "User ID required", nil)
		return
	}

	// Call service
	result, err := s.portfolioService.DeletePortfolio(r.Context(), portfolioID, userID)
	if err != nil {
		statusCode, code, message := mapServiceError(err)
		respondError(w, statusCode, code, message, nil)
		return
	}

	// Return success response
	respondJSON(w, http.StatusOK, result)
}

// handleGetStatistics handles GET /api/portfolios/:id/statistics - Get portfolio statistics
// Requirements: 15.1, 15.2, 15.3, 15.4, 15.5
func (s *Server) handleGetStatistics(w http.ResponseWriter, r *http.Request) {
	// Get portfolio ID from URL
	vars := mux.Vars(r)
	portfolioID := vars["id"]

	if portfolioID == "" {
		respondError(w, http.StatusBadRequest, ErrCodeInvalidInput, "Portfolio ID required", nil)
		return
	}

	// Get user ID from headers
	userID := r.Header.Get("X-User-ID")
	if userID == "" {
		respondError(w, http.StatusUnauthorized, ErrCodeUnauthorized, "User ID required", nil)
		return
	}

	// Call service
	statistics, err := s.portfolioService.GetStatistics(r.Context(), portfolioID, userID)
	if err != nil {
		statusCode, code, message := mapServiceError(err)
		respondError(w, statusCode, code, message, nil)
		return
	}

	// Return success response
	respondJSON(w, http.StatusOK, statistics)
}

// handleGetSnapshots handles GET /api/portfolios/:id/snapshots - Get daily snapshots
// Requirements: 16.1, 16.2, 16.3, 16.4
func (s *Server) handleGetSnapshots(w http.ResponseWriter, r *http.Request) {
	// Get portfolio ID from URL
	vars := mux.Vars(r)
	portfolioID := vars["id"]

	if portfolioID == "" {
		respondError(w, http.StatusBadRequest, ErrCodeInvalidInput, "Portfolio ID required", nil)
		return
	}

	// Get user ID from headers
	userID := r.Header.Get("X-User-ID")
	if userID == "" {
		respondError(w, http.StatusUnauthorized, ErrCodeUnauthorized, "User ID required", nil)
		return
	}

	// Parse query parameters for date range
	query := r.URL.Query()
	
	// Parse dateFrom (required)
	dateFromStr := query.Get("dateFrom")
	if dateFromStr == "" {
		respondError(w, http.StatusBadRequest, ErrCodeInvalidInput, "dateFrom parameter required", nil)
		return
	}
	dateFrom, err := time.Parse(time.RFC3339, dateFromStr)
	if err != nil {
		respondError(w, http.StatusBadRequest, ErrCodeInvalidInput, "Invalid dateFrom format (use RFC3339)", nil)
		return
	}

	// Parse dateTo (required)
	dateToStr := query.Get("dateTo")
	if dateToStr == "" {
		respondError(w, http.StatusBadRequest, ErrCodeInvalidInput, "dateTo parameter required", nil)
		return
	}
	dateTo, err := time.Parse(time.RFC3339, dateToStr)
	if err != nil {
		respondError(w, http.StatusBadRequest, ErrCodeInvalidInput, "Invalid dateTo format (use RFC3339)", nil)
		return
	}

	// Call snapshot service (not portfolio service)
	// The SnapshotService handles the actual snapshot retrieval
	snapshots, err := s.snapshotService.GetSnapshots(r.Context(), portfolioID, userID, dateFrom, dateTo)
	if err != nil {
		statusCode, code, message := mapServiceError(err)
		respondError(w, statusCode, code, message, nil)
		return
	}

	// Return success response
	respondJSON(w, http.StatusOK, snapshots)
}
