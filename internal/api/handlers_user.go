package api

import (
	"net/http"

	"github.com/address-scanner/internal/models"
	"github.com/address-scanner/internal/types"
	"github.com/gorilla/mux"
)

// handleCreateUser handles POST /api/users - Create a new user
func (s *Server) handleCreateUser(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Email string         `json:"email"`
		Tier  types.UserTier `json:"tier"`
	}

	if err := parseJSONBody(r, &req); err != nil {
		respondError(w, http.StatusBadRequest, ErrCodeInvalidInput, "Invalid request body", nil)
		return
	}

	if req.Email == "" {
		respondError(w, http.StatusBadRequest, ErrCodeInvalidInput, "Email is required", nil)
		return
	}

	// Default to free tier if not specified
	if req.Tier == "" {
		req.Tier = types.TierFree
	}

	// Validate tier
	if req.Tier != types.TierFree && req.Tier != types.TierPaid {
		respondError(w, http.StatusBadRequest, ErrCodeInvalidInput, "Invalid tier (must be 'free' or 'paid')", nil)
		return
	}

	user := &models.User{
		Email: req.Email,
		Tier:  req.Tier,
	}

	if err := s.userRepo.Create(r.Context(), user); err != nil {
		respondError(w, http.StatusInternalServerError, ErrCodeInternalError, "Failed to create user", nil)
		return
	}

	respondJSON(w, http.StatusCreated, user)
}

// handleGetUser handles GET /api/users/:id - Get user by ID
func (s *Server) handleGetUser(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	userID := vars["id"]

	if userID == "" {
		respondError(w, http.StatusBadRequest, ErrCodeInvalidInput, "User ID required", nil)
		return
	}

	user, err := s.userRepo.GetByID(r.Context(), userID)
	if err != nil {
		respondError(w, http.StatusNotFound, ErrCodeNotFound, "User not found", nil)
		return
	}

	respondJSON(w, http.StatusOK, user)
}
