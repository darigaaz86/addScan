package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/address-scanner/internal/models"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// PortfolioRepository handles portfolio data persistence
type PortfolioRepository struct {
	db *PostgresDB
}

// NewPortfolioRepository creates a new portfolio repository
func NewPortfolioRepository(db *PostgresDB) *PortfolioRepository {
	return &PortfolioRepository{db: db}
}

// Create creates a new portfolio
func (r *PortfolioRepository) Create(ctx context.Context, portfolio *models.Portfolio) error {
	if portfolio.ID == "" {
		portfolio.ID = uuid.New().String()
	}

	now := time.Now()
	portfolio.CreatedAt = now
	portfolio.UpdatedAt = now

	// Validate and normalize addresses
	normalizedAddresses := make([]string, len(portfolio.Addresses))
	for i, addr := range portfolio.Addresses {
		if err := ValidateAddress(addr); err != nil {
			return fmt.Errorf("invalid address at index %d: %w", i, err)
		}
		normalizedAddresses[i] = strings.ToLower(addr)
	}
	portfolio.Addresses = normalizedAddresses

	query := `
		INSERT INTO portfolios (id, user_id, name, description, addresses, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`

	_, err := r.db.Pool().Exec(ctx, query,
		portfolio.ID,
		portfolio.UserID,
		portfolio.Name,
		portfolio.Description,
		portfolio.Addresses,
		portfolio.CreatedAt,
		portfolio.UpdatedAt,
	)

	if err != nil {
		return fmt.Errorf("failed to create portfolio: %w", err)
	}

	return nil
}

// GetByID retrieves a portfolio by ID
func (r *PortfolioRepository) GetByID(ctx context.Context, id string) (*models.Portfolio, error) {
	query := `
		SELECT id, user_id, name, description, addresses, created_at, updated_at
		FROM portfolios
		WHERE id = $1
	`

	var portfolio models.Portfolio
	var addresses []string

	err := r.db.Pool().QueryRow(ctx, query, id).Scan(
		&portfolio.ID,
		&portfolio.UserID,
		&portfolio.Name,
		&portfolio.Description,
		&addresses,
		&portfolio.CreatedAt,
		&portfolio.UpdatedAt,
	)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("portfolio not found: %s", id)
		}
		return nil, fmt.Errorf("failed to get portfolio: %w", err)
	}

	portfolio.Addresses = addresses

	return &portfolio, nil
}

// GetByIDAndUser retrieves a portfolio by ID and verifies ownership
func (r *PortfolioRepository) GetByIDAndUser(ctx context.Context, id, userID string) (*models.Portfolio, error) {
	query := `
		SELECT id, user_id, name, description, addresses, created_at, updated_at
		FROM portfolios
		WHERE id = $1 AND user_id = $2
	`

	var portfolio models.Portfolio
	var addresses []string

	err := r.db.Pool().QueryRow(ctx, query, id, userID).Scan(
		&portfolio.ID,
		&portfolio.UserID,
		&portfolio.Name,
		&portfolio.Description,
		&addresses,
		&portfolio.CreatedAt,
		&portfolio.UpdatedAt,
	)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("portfolio not found or access denied: %s", id)
		}
		return nil, fmt.Errorf("failed to get portfolio: %w", err)
	}

	portfolio.Addresses = addresses

	return &portfolio, nil
}

// Update updates an existing portfolio
func (r *PortfolioRepository) Update(ctx context.Context, portfolio *models.Portfolio) error {
	portfolio.UpdatedAt = time.Now()

	// Validate and normalize addresses
	normalizedAddresses := make([]string, len(portfolio.Addresses))
	for i, addr := range portfolio.Addresses {
		if err := ValidateAddress(addr); err != nil {
			return fmt.Errorf("invalid address at index %d: %w", i, err)
		}
		normalizedAddresses[i] = strings.ToLower(addr)
	}
	portfolio.Addresses = normalizedAddresses

	query := `
		UPDATE portfolios
		SET name = $2, description = $3, addresses = $4, updated_at = $5
		WHERE id = $1
	`

	result, err := r.db.Pool().Exec(ctx, query,
		portfolio.ID,
		portfolio.Name,
		portfolio.Description,
		portfolio.Addresses,
		portfolio.UpdatedAt,
	)

	if err != nil {
		return fmt.Errorf("failed to update portfolio: %w", err)
	}

	if result.RowsAffected() == 0 {
		return fmt.Errorf("portfolio not found: %s", portfolio.ID)
	}

	return nil
}

// UpdateAddresses updates only the addresses in a portfolio
func (r *PortfolioRepository) UpdateAddresses(ctx context.Context, portfolioID string, addresses []string) error {
	// Validate and normalize addresses
	normalizedAddresses := make([]string, len(addresses))
	for i, addr := range addresses {
		if err := ValidateAddress(addr); err != nil {
			return fmt.Errorf("invalid address at index %d: %w", i, err)
		}
		normalizedAddresses[i] = strings.ToLower(addr)
	}

	query := `
		UPDATE portfolios
		SET addresses = $2, updated_at = $3
		WHERE id = $1
	`

	result, err := r.db.Pool().Exec(ctx, query,
		portfolioID,
		normalizedAddresses,
		time.Now(),
	)

	if err != nil {
		return fmt.Errorf("failed to update portfolio addresses: %w", err)
	}

	if result.RowsAffected() == 0 {
		return fmt.Errorf("portfolio not found: %s", portfolioID)
	}

	return nil
}

// AddAddress adds a single address to a portfolio
func (r *PortfolioRepository) AddAddress(ctx context.Context, portfolioID, address string) error {
	// Validate and normalize address
	if err := ValidateAddress(address); err != nil {
		return err
	}
	address = strings.ToLower(address)

	// Get current portfolio
	portfolio, err := r.GetByID(ctx, portfolioID)
	if err != nil {
		return err
	}

	// Check if address already exists
	for _, addr := range portfolio.Addresses {
		if addr == address {
			return fmt.Errorf("address already exists in portfolio: %s", address)
		}
	}

	// Add address
	portfolio.Addresses = append(portfolio.Addresses, address)
	portfolio.UpdatedAt = time.Now()

	query := `
		UPDATE portfolios
		SET addresses = $2, updated_at = $3
		WHERE id = $1
	`

	result, err := r.db.Pool().Exec(ctx, query,
		portfolioID,
		portfolio.Addresses,
		portfolio.UpdatedAt,
	)

	if err != nil {
		return fmt.Errorf("failed to add address to portfolio: %w", err)
	}

	if result.RowsAffected() == 0 {
		return fmt.Errorf("portfolio not found: %s", portfolioID)
	}

	return nil
}

// RemoveAddress removes a single address from a portfolio
func (r *PortfolioRepository) RemoveAddress(ctx context.Context, portfolioID, address string) error {
	// Validate and normalize address
	if err := ValidateAddress(address); err != nil {
		return err
	}
	address = strings.ToLower(address)

	// Get current portfolio
	portfolio, err := r.GetByID(ctx, portfolioID)
	if err != nil {
		return err
	}

	// Find and remove address
	found := false
	newAddresses := make([]string, 0, len(portfolio.Addresses))
	for _, addr := range portfolio.Addresses {
		if addr != address {
			newAddresses = append(newAddresses, addr)
		} else {
			found = true
		}
	}

	if !found {
		return fmt.Errorf("address not found in portfolio: %s", address)
	}

	portfolio.Addresses = newAddresses
	portfolio.UpdatedAt = time.Now()

	query := `
		UPDATE portfolios
		SET addresses = $2, updated_at = $3
		WHERE id = $1
	`

	result, err := r.db.Pool().Exec(ctx, query,
		portfolioID,
		portfolio.Addresses,
		portfolio.UpdatedAt,
	)

	if err != nil {
		return fmt.Errorf("failed to remove address from portfolio: %w", err)
	}

	if result.RowsAffected() == 0 {
		return fmt.Errorf("portfolio not found: %s", portfolioID)
	}

	return nil
}

// Delete deletes a portfolio
func (r *PortfolioRepository) Delete(ctx context.Context, id string) error {
	query := `DELETE FROM portfolios WHERE id = $1`

	result, err := r.db.Pool().Exec(ctx, query, id)
	if err != nil {
		return fmt.Errorf("failed to delete portfolio: %w", err)
	}

	if result.RowsAffected() == 0 {
		return fmt.Errorf("portfolio not found: %s", id)
	}

	return nil
}

// DeleteByIDAndUser deletes a portfolio and verifies ownership
func (r *PortfolioRepository) DeleteByIDAndUser(ctx context.Context, id, userID string) error {
	query := `DELETE FROM portfolios WHERE id = $1 AND user_id = $2`

	result, err := r.db.Pool().Exec(ctx, query, id, userID)
	if err != nil {
		return fmt.Errorf("failed to delete portfolio: %w", err)
	}

	if result.RowsAffected() == 0 {
		return fmt.Errorf("portfolio not found or access denied: %s", id)
	}

	return nil
}

// ListByUser retrieves all portfolios for a user
func (r *PortfolioRepository) ListByUser(ctx context.Context, userID string) ([]*models.Portfolio, error) {
	query := `
		SELECT id, user_id, name, description, addresses, created_at, updated_at
		FROM portfolios
		WHERE user_id = $1
		ORDER BY created_at DESC
	`

	rows, err := r.db.Pool().Query(ctx, query, userID)
	if err != nil {
		return nil, fmt.Errorf("failed to list portfolios: %w", err)
	}
	defer rows.Close()

	var portfolios []*models.Portfolio
	for rows.Next() {
		var portfolio models.Portfolio
		var addresses []string

		err := rows.Scan(
			&portfolio.ID,
			&portfolio.UserID,
			&portfolio.Name,
			&portfolio.Description,
			&addresses,
			&portfolio.CreatedAt,
			&portfolio.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan portfolio: %w", err)
		}

		portfolio.Addresses = addresses
		portfolios = append(portfolios, &portfolio)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating portfolios: %w", err)
	}

	return portfolios, nil
}

// Exists checks if a portfolio exists
func (r *PortfolioRepository) Exists(ctx context.Context, id string) (bool, error) {
	var exists bool
	query := `SELECT EXISTS(SELECT 1 FROM portfolios WHERE id = $1)`

	err := r.db.Pool().QueryRow(ctx, query, id).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check portfolio existence: %w", err)
	}

	return exists, nil
}

// ExistsByIDAndUser checks if a portfolio exists and belongs to a user
func (r *PortfolioRepository) ExistsByIDAndUser(ctx context.Context, id, userID string) (bool, error) {
	var exists bool
	query := `SELECT EXISTS(SELECT 1 FROM portfolios WHERE id = $1 AND user_id = $2)`

	err := r.db.Pool().QueryRow(ctx, query, id, userID).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check portfolio existence: %w", err)
	}

	return exists, nil
}

// CountByUser counts the number of portfolios for a user
func (r *PortfolioRepository) CountByUser(ctx context.Context, userID string) (int64, error) {
	var count int64
	query := `SELECT COUNT(*) FROM portfolios WHERE user_id = $1`

	err := r.db.Pool().QueryRow(ctx, query, userID).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count portfolios: %w", err)
	}

	return count, nil
}

// GetAddressCount returns the number of addresses in a portfolio
func (r *PortfolioRepository) GetAddressCount(ctx context.Context, portfolioID string) (int, error) {
	var count int
	query := `SELECT array_length(addresses, 1) FROM portfolios WHERE id = $1`

	err := r.db.Pool().QueryRow(ctx, query, portfolioID).Scan(&count)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, fmt.Errorf("portfolio not found: %s", portfolioID)
		}
		return 0, fmt.Errorf("failed to get address count: %w", err)
	}

	return count, nil
}

// ContainsAddress checks if a portfolio contains a specific address
func (r *PortfolioRepository) ContainsAddress(ctx context.Context, portfolioID, address string) (bool, error) {
	// Validate and normalize address
	if err := ValidateAddress(address); err != nil {
		return false, err
	}
	address = strings.ToLower(address)

	var contains bool
	query := `SELECT $2 = ANY(addresses) FROM portfolios WHERE id = $1`

	err := r.db.Pool().QueryRow(ctx, query, portfolioID, address).Scan(&contains)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, fmt.Errorf("portfolio not found: %s", portfolioID)
		}
		return false, fmt.Errorf("failed to check address in portfolio: %w", err)
	}

	return contains, nil
}
