// Package api provides the HTTP API server implementation.
package api

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/address-scanner/internal/service"
	"github.com/address-scanner/internal/storage"
	"github.com/gorilla/mux"
)

// Server represents the HTTP API server.
type Server struct {
	router           *mux.Router
	httpServer       *http.Server
	addressService   *service.AddressService
	portfolioService *service.PortfolioService
	queryService     *service.QueryService
	snapshotService  *service.SnapshotService
	userRepo         *storage.UserRepository
	goldskyRepo      *storage.GoldskyRepository
}

// ServerConfig holds server configuration.
type ServerConfig struct {
	Host            string
	Port            string
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	IdleTimeout     time.Duration
	ShutdownTimeout time.Duration
	FreeTierRPS     int // Requests per second for free tier
	BasicTierRPS    int // Requests per second for basic tier
	PremiumTierRPS  int // Requests per second for premium tier
}

// NewServer creates a new API server instance.
func NewServer(
	config *ServerConfig,
	addressService *service.AddressService,
	portfolioService *service.PortfolioService,
	queryService *service.QueryService,
	snapshotService *service.SnapshotService,
	userRepo *storage.UserRepository,
	goldskyRepo *storage.GoldskyRepository,
) *Server {
	s := &Server{
		router:           mux.NewRouter(),
		addressService:   addressService,
		portfolioService: portfolioService,
		queryService:     queryService,
		snapshotService:  snapshotService,
		userRepo:         userRepo,
		goldskyRepo:      goldskyRepo,
	}

	// Create rate limiter
	rateLimiter := NewRateLimiter(config.FreeTierRPS, config.BasicTierRPS, config.PremiumTierRPS)

	// Set up middleware (order matters!)
	s.router.Use(LoggingMiddleware)
	s.router.Use(RecoveryMiddleware)
	s.router.Use(CORSMiddleware)
	s.router.Use(RateLimitMiddleware(rateLimiter)) // Rate limiting after CORS
	s.router.Use(CompressionMiddleware)

	// Set up routes
	s.setupRoutes()

	// Create HTTP server
	s.httpServer = &http.Server{
		Addr:         fmt.Sprintf("%s:%s", config.Host, config.Port),
		Handler:      s.router,
		ReadTimeout:  config.ReadTimeout,
		WriteTimeout: config.WriteTimeout,
		IdleTimeout:  config.IdleTimeout,
	}

	return s
}

// setupRoutes configures all API routes.
func (s *Server) setupRoutes() {
	// Health check endpoint
	s.router.HandleFunc("/health", s.handleHealth).Methods("GET")

	// API v1 routes
	api := s.router.PathPrefix("/api").Subrouter()

	// Address endpoints
	api.HandleFunc("/addresses", s.handleAddAddress).Methods("POST")
	api.HandleFunc("/addresses/{address}", s.handleGetAddress).Methods("GET")
	api.HandleFunc("/addresses/{address}/transactions", s.handleGetTransactions).Methods("GET")
	api.HandleFunc("/addresses/{address}/balance", s.handleGetBalance).Methods("GET")
	api.HandleFunc("/addresses/{address}", s.handleRemoveAddress).Methods("DELETE")

	// Portfolio endpoints
	api.HandleFunc("/portfolios", s.handleCreatePortfolio).Methods("POST")
	api.HandleFunc("/portfolios/{id}", s.handleGetPortfolio).Methods("GET")
	api.HandleFunc("/portfolios/{id}", s.handleUpdatePortfolio).Methods("PUT")
	api.HandleFunc("/portfolios/{id}", s.handleDeletePortfolio).Methods("DELETE")
	api.HandleFunc("/portfolios/{id}/statistics", s.handleGetStatistics).Methods("GET")
	api.HandleFunc("/portfolios/{id}/snapshots", s.handleGetSnapshots).Methods("GET")

	// Search endpoints
	api.HandleFunc("/search/transaction/{hash}", s.handleSearchTransaction).Methods("GET")

	// User endpoints
	api.HandleFunc("/users", s.handleCreateUser).Methods("POST")
	api.HandleFunc("/users/{id}", s.handleGetUser).Methods("GET")

	// Goldsky webhook endpoints (no rate limiting needed)
	s.router.HandleFunc("/goldsky/traces", s.handleGoldskyTraces).Methods("POST")
	s.router.HandleFunc("/goldsky/logs", s.handleGoldskyLogs).Methods("POST")
}

// handleHealth handles health check requests.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	respondJSON(w, http.StatusOK, map[string]string{
		"status":  "healthy",
		"service": "address-scanner",
	})
}

// Start starts the HTTP server.
func (s *Server) Start() error {
	log.Printf("Starting API server on %s", s.httpServer.Addr)
	return s.httpServer.ListenAndServe()
}

// Shutdown gracefully shuts down the server.
func (s *Server) Shutdown(ctx context.Context) error {
	log.Println("Shutting down API server...")
	return s.httpServer.Shutdown(ctx)
}
