package api

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/address-scanner/internal/service"
	"github.com/address-scanner/internal/types"
	"github.com/gorilla/mux"
)

// handleGetAddressBalances handles GET /api/addresses/:address/balances
func (s *Server) handleGetAddressBalances(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	address := vars["address"]
	if address == "" {
		http.Error(w, "address is required", http.StatusBadRequest)
		return
	}

	chain := r.URL.Query().Get("chain")
	if chain == "" {
		chain = "ethereum"
	}

	balances, err := s.positionService.GetAddressBalances(r.Context(), address, types.ChainID(chain))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(balances)
}

// handleGetAddressAllChainBalances handles GET /api/addresses/:address/balances/all
func (s *Server) handleGetAddressAllChainBalances(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	address := vars["address"]
	if address == "" {
		http.Error(w, "address is required", http.StatusBadRequest)
		return
	}

	chains := []types.ChainID{types.ChainEthereum, types.ChainBase, types.ChainBNB}

	balances, err := s.positionService.GetAddressAllChains(r.Context(), address, chains)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(balances)
}

// handleGetAddressProtocols handles GET /api/addresses/:address/protocols
func (s *Server) handleGetAddressProtocols(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	address := vars["address"]
	if address == "" {
		http.Error(w, "address is required", http.StatusBadRequest)
		return
	}

	chain := r.URL.Query().Get("chain")
	if chain == "" {
		chain = "ethereum"
	}

	positions, err := s.positionService.GetProtocolPositions(r.Context(), address, types.ChainID(chain))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"address":   strings.ToLower(address),
		"chain":     chain,
		"protocols": positions,
	})
}

// handleGetPortfolioBalances handles GET /api/portfolios/:id/balances
func (s *Server) handleGetPortfolioBalances(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	portfolioID := vars["id"]
	if portfolioID == "" {
		http.Error(w, "portfolio id is required", http.StatusBadRequest)
		return
	}

	userID := r.Header.Get("X-User-ID")
	if userID == "" {
		http.Error(w, "X-User-ID header is required", http.StatusUnauthorized)
		return
	}

	// Get portfolio
	portfolio, err := s.portfolioService.GetPortfolio(r.Context(), portfolioID, userID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if portfolio == nil {
		http.Error(w, "portfolio not found", http.StatusNotFound)
		return
	}

	// Filter by chain if specified
	chainFilter := r.URL.Query().Get("chain")
	chains := []types.ChainID{types.ChainEthereum, types.ChainBase, types.ChainBNB}
	if chainFilter != "" {
		chains = []types.ChainID{types.ChainID(chainFilter)}
	}

	// Get balances for all addresses in portfolio
	balances, err := s.positionService.GetPortfolioBalances(r.Context(), portfolio.Addresses, chains)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(balances)
}

// handleGetPortfolioProtocols handles GET /api/portfolios/:id/protocols
func (s *Server) handleGetPortfolioProtocols(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	portfolioID := vars["id"]
	if portfolioID == "" {
		http.Error(w, "portfolio id is required", http.StatusBadRequest)
		return
	}

	userID := r.Header.Get("X-User-ID")
	if userID == "" {
		http.Error(w, "X-User-ID header is required", http.StatusUnauthorized)
		return
	}

	// Get portfolio
	portfolio, err := s.portfolioService.GetPortfolio(r.Context(), portfolioID, userID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if portfolio == nil {
		http.Error(w, "portfolio not found", http.StatusNotFound)
		return
	}

	// Get protocol positions for all addresses
	chains := []types.ChainID{types.ChainEthereum, types.ChainBase, types.ChainBNB}
	allPositions := make([]service.ProtocolPosition, 0)

	for _, address := range portfolio.Addresses {
		for _, chain := range chains {
			positions, err := s.positionService.GetProtocolPositions(r.Context(), address, chain)
			if err != nil {
				continue
			}
			allPositions = append(allPositions, positions...)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"portfolioId": portfolioID,
		"protocols":   allPositions,
	})
}

// handleGetPortfolioHoldings handles GET /api/portfolios/:id/holdings
func (s *Server) handleGetPortfolioHoldings(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	portfolioID := vars["id"]
	if portfolioID == "" {
		http.Error(w, "portfolio id is required", http.StatusBadRequest)
		return
	}

	userID := r.Header.Get("X-User-ID")
	if userID == "" {
		http.Error(w, "X-User-ID header is required", http.StatusUnauthorized)
		return
	}

	// Get portfolio
	portfolio, err := s.portfolioService.GetPortfolio(r.Context(), portfolioID, userID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if portfolio == nil {
		http.Error(w, "portfolio not found", http.StatusNotFound)
		return
	}

	chains := []types.ChainID{types.ChainEthereum, types.ChainBase, types.ChainBNB}

	// Get all balances
	balances, err := s.positionService.GetPortfolioBalances(r.Context(), portfolio.Addresses, chains)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Return grouped by token (top holdings)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"portfolioId": portfolioID,
		"byToken":     balances.ByToken,
		"byChain":     balances.ByChain,
	})
}

// handleGetAddressBalanceHistory handles GET /api/addresses/:address/history
func (s *Server) handleGetAddressBalanceHistory(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	address := vars["address"]
	if address == "" {
		http.Error(w, "address is required", http.StatusBadRequest)
		return
	}

	chain := r.URL.Query().Get("chain")
	if chain == "" {
		chain = "ethereum"
	}

	// Parse date range
	fromStr := r.URL.Query().Get("from")
	toStr := r.URL.Query().Get("to")

	var from, to time.Time
	var err error

	if fromStr != "" {
		from, err = time.Parse("2006-01-02", fromStr)
		if err != nil {
			http.Error(w, "invalid from date format (use YYYY-MM-DD)", http.StatusBadRequest)
			return
		}
	} else {
		from = time.Now().AddDate(0, -1, 0) // Default: 1 month ago
	}

	if toStr != "" {
		to, err = time.Parse("2006-01-02", toStr)
		if err != nil {
			http.Error(w, "invalid to date format (use YYYY-MM-DD)", http.StatusBadRequest)
			return
		}
	} else {
		to = time.Now()
	}

	history, err := s.balanceSnapshotService.GetBalanceHistory(r.Context(), address, chain, from, to)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"address": address,
		"chain":   chain,
		"from":    from.Format("2006-01-02"),
		"to":      to.Format("2006-01-02"),
		"history": history,
	})
}

// handleGetPortfolioHistory handles GET /api/portfolios/:id/history
func (s *Server) handleGetPortfolioHistory(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	portfolioID := vars["id"]
	if portfolioID == "" {
		http.Error(w, "portfolio id is required", http.StatusBadRequest)
		return
	}

	userID := r.Header.Get("X-User-ID")
	if userID == "" {
		http.Error(w, "X-User-ID header is required", http.StatusUnauthorized)
		return
	}

	// Verify portfolio ownership
	portfolio, err := s.portfolioService.GetPortfolio(r.Context(), portfolioID, userID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if portfolio == nil {
		http.Error(w, "portfolio not found", http.StatusNotFound)
		return
	}

	// Parse date range
	fromStr := r.URL.Query().Get("from")
	toStr := r.URL.Query().Get("to")

	var from, to time.Time

	if fromStr != "" {
		from, err = time.Parse("2006-01-02", fromStr)
		if err != nil {
			http.Error(w, "invalid from date format (use YYYY-MM-DD)", http.StatusBadRequest)
			return
		}
	} else {
		from = time.Now().AddDate(0, -1, 0) // Default: 1 month ago
	}

	if toStr != "" {
		to, err = time.Parse("2006-01-02", toStr)
		if err != nil {
			http.Error(w, "invalid to date format (use YYYY-MM-DD)", http.StatusBadRequest)
			return
		}
	} else {
		to = time.Now()
	}

	history, err := s.balanceSnapshotService.GetPortfolioHistory(r.Context(), portfolioID, from, to)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"portfolioId": portfolioID,
		"from":        from.Format("2006-01-02"),
		"to":          to.Format("2006-01-02"),
		"history":     history,
	})
}

// handleGetAddressDeFiActivity handles GET /api/addresses/:address/defi
func (s *Server) handleGetAddressDeFiActivity(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	address := vars["address"]
	if address == "" {
		http.Error(w, "address is required", http.StatusBadRequest)
		return
	}

	chain := r.URL.Query().Get("chain")
	if chain == "" {
		chain = "ethereum"
	}

	activity, err := s.defiDetector.GetProtocolActivity(r.Context(), address, types.ChainID(chain))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Convert map to slice for JSON
	protocols := make([]service.ProtocolActivity, 0, len(activity))
	for _, a := range activity {
		protocols = append(protocols, *a)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"address":   strings.ToLower(address),
		"chain":     chain,
		"protocols": protocols,
	})
}

// handleGetAddressDeFiInteractions handles GET /api/addresses/:address/defi/interactions
func (s *Server) handleGetAddressDeFiInteractions(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	address := vars["address"]
	if address == "" {
		http.Error(w, "address is required", http.StatusBadRequest)
		return
	}

	chain := r.URL.Query().Get("chain")
	if chain == "" {
		chain = "ethereum"
	}

	limit := 100 // Default limit

	interactions, err := s.defiDetector.DetectInteractions(r.Context(), address, types.ChainID(chain), limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"address":      strings.ToLower(address),
		"chain":        chain,
		"interactions": interactions,
	})
}
