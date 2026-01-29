package api

import (
	"net/http"
	"strings"

	"github.com/gorilla/mux"
)

// handleSearchTransaction handles GET /api/search/transaction/:hash - Search by transaction hash
// Requirements: 9.1, 9.2
func (s *Server) handleSearchTransaction(w http.ResponseWriter, r *http.Request) {
	// Get transaction hash from URL
	vars := mux.Vars(r)
	hash := vars["hash"]

	if hash == "" {
		respondError(w, http.StatusBadRequest, ErrCodeInvalidInput, "Transaction hash required", nil)
		return
	}

	// Normalize hash (lowercase, ensure 0x prefix)
	hash = strings.ToLower(hash)
	if !strings.HasPrefix(hash, "0x") {
		hash = "0x" + hash
	}

	// Call query service to search by hash
	transaction, err := s.queryService.SearchByHash(r.Context(), hash)
	if err != nil {
		statusCode, code, message := mapServiceError(err)
		respondError(w, statusCode, code, message, nil)
		return
	}

	// Return success response
	respondJSON(w, http.StatusOK, transaction)
}
