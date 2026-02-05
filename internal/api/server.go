// Package api provides the HTTP API server implementation.
package api

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/address-scanner/internal/models"
	"github.com/address-scanner/internal/service"
	"github.com/address-scanner/internal/storage"
	"github.com/address-scanner/internal/types"
	"github.com/gorilla/mux"
)

// Service interfaces for dependency injection and testing

// AddressServiceInterface defines the interface for address service operations
type AddressServiceInterface interface {
	AddAddress(ctx context.Context, input *service.AddAddressInput) (*service.AddressTrackingResult, error)
	GetBalance(ctx context.Context, input *service.GetBalanceInput) (*service.GetBalanceResult, error)
}

// PortfolioServiceInterface defines the interface for portfolio service operations
type PortfolioServiceInterface interface {
	CreatePortfolio(ctx context.Context, input *service.CreatePortfolioInput) (*models.Portfolio, error)
	GetPortfolio(ctx context.Context, portfolioID, userID string) (*service.PortfolioView, error)
	UpdatePortfolio(ctx context.Context, input *service.UpdatePortfolioInput) (*models.Portfolio, error)
	DeletePortfolio(ctx context.Context, portfolioID, userID string) (*service.DeletePortfolioResult, error)
	GetStatistics(ctx context.Context, portfolioID, userID string) (*service.PortfolioStatistics, error)
}

// QueryServiceInterface defines the interface for query service operations
type QueryServiceInterface interface {
	Query(ctx context.Context, input *service.QueryInput) (*service.QueryResult, error)
	SearchByHash(ctx context.Context, hash string) ([]*types.NormalizedTransaction, error)
}

// SnapshotServiceInterface defines the interface for snapshot service operations
type SnapshotServiceInterface interface {
	GetSnapshots(ctx context.Context, portfolioID, userID string, from, to time.Time) ([]*models.PortfolioSnapshot, error)
}

// BalanceSnapshotServiceInterface defines the interface for balance snapshot operations
type BalanceSnapshotServiceInterface interface {
	GetBalanceHistory(ctx context.Context, address, chain string, from, to time.Time) ([]storage.NativeBalanceSnapshot, error)
	GetPortfolioHistory(ctx context.Context, portfolioID string, from, to time.Time) ([]storage.PortfolioDailySummary, error)
}

// DeFiDetectorInterface defines the interface for DeFi detection operations
type DeFiDetectorInterface interface {
	DetectInteractions(ctx context.Context, address string, chain types.ChainID, limit int) ([]service.DeFiInteraction, error)
	GetProtocolActivity(ctx context.Context, address string, chain types.ChainID) (map[string]*service.ProtocolActivity, error)
}

// PositionServiceInterface defines the interface for position service operations
type PositionServiceInterface interface {
	GetAddressBalances(ctx context.Context, address string, chain types.ChainID) (*service.AddressBalances, error)
	GetAddressAllChains(ctx context.Context, address string, chains []types.ChainID) (map[string]*service.AddressBalances, error)
	GetProtocolPositions(ctx context.Context, address string, chain types.ChainID) ([]service.ProtocolPosition, error)
	GetPortfolioBalances(ctx context.Context, addresses []string, chains []types.ChainID) (*service.PortfolioBalances, error)
}

// Server represents the HTTP API server.
type Server struct {
	router                 *mux.Router
	httpServer             *http.Server
	addressService         AddressServiceInterface
	portfolioService       PortfolioServiceInterface
	queryService           QueryServiceInterface
	snapshotService        SnapshotServiceInterface
	balanceSnapshotService BalanceSnapshotServiceInterface
	positionService        PositionServiceInterface
	defiDetector           DeFiDetectorInterface
	userRepo               *storage.UserRepository
	goldskyRepo            *storage.GoldskyRepository
	config                 *ServerConfig
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
	balanceSnapshotService *service.BalanceSnapshotService,
	positionService *service.PositionService,
	defiDetector *service.DeFiDetector,
	userRepo *storage.UserRepository,
	goldskyRepo *storage.GoldskyRepository,
) *Server {
	s := &Server{
		router:                 mux.NewRouter(),
		addressService:         addressService,
		portfolioService:       portfolioService,
		queryService:           queryService,
		snapshotService:        snapshotService,
		balanceSnapshotService: balanceSnapshotService,
		positionService:        positionService,
		defiDetector:           defiDetector,
		userRepo:               userRepo,
		goldskyRepo:            goldskyRepo,
		config:                 config,
	}

	s.setupRouter()

	return s
}

// setupRouter configures the router with middleware and routes
func (s *Server) setupRouter() {
	// Create rate limiter
	rateLimiter := NewRateLimiter(s.config.FreeTierRPS, s.config.BasicTierRPS, s.config.PremiumTierRPS)

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
		Addr:         fmt.Sprintf("%s:%s", s.config.Host, s.config.Port),
		Handler:      s.router,
		ReadTimeout:  s.config.ReadTimeout,
		WriteTimeout: s.config.WriteTimeout,
		IdleTimeout:  s.config.IdleTimeout,
	}
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

	// Balance endpoints (DeBank-aligned)
	api.HandleFunc("/addresses/{address}/balances", s.handleGetAddressBalances).Methods("GET")
	api.HandleFunc("/addresses/{address}/balances/all", s.handleGetAddressAllChainBalances).Methods("GET")
	api.HandleFunc("/addresses/{address}/protocols", s.handleGetAddressProtocols).Methods("GET")
	api.HandleFunc("/portfolios/{id}/balances", s.handleGetPortfolioBalances).Methods("GET")
	api.HandleFunc("/portfolios/{id}/protocols", s.handleGetPortfolioProtocols).Methods("GET")
	api.HandleFunc("/portfolios/{id}/holdings", s.handleGetPortfolioHoldings).Methods("GET")
	api.HandleFunc("/portfolios/{id}/history", s.handleGetPortfolioHistory).Methods("GET")
	api.HandleFunc("/addresses/{address}/history", s.handleGetAddressBalanceHistory).Methods("GET")
	api.HandleFunc("/addresses/{address}/defi", s.handleGetAddressDeFiActivity).Methods("GET")
	api.HandleFunc("/addresses/{address}/defi/interactions", s.handleGetAddressDeFiInteractions).Methods("GET")

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
