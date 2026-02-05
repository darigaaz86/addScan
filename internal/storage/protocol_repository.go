package storage

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Protocol represents a DeFi protocol
type Protocol struct {
	ID                    uuid.UUID `json:"id"`
	Slug                  string    `json:"slug"`
	Name                  string    `json:"name"`
	Category              *string   `json:"category"`
	SiteURL               *string   `json:"siteUrl"`
	LogoURL               *string   `json:"logoUrl"`
	HasSupportedPortfolio bool      `json:"hasSupportedPortfolio"`
	CreatedAt             time.Time `json:"createdAt"`
	UpdatedAt             time.Time `json:"updatedAt"`
}

// ProtocolContract represents a contract belonging to a protocol
type ProtocolContract struct {
	ID              uuid.UUID `json:"id"`
	ProtocolID      uuid.UUID `json:"protocolId"`
	ContractAddress string    `json:"contractAddress"`
	Chain           string    `json:"chain"`
	ContractType    *string   `json:"contractType"`
	PositionType    *string   `json:"positionType"`
	Description     *string   `json:"description"`
	CreatedAt       time.Time `json:"createdAt"`
}

// TokenPattern represents a pattern rule for detecting DeFi tokens
type TokenPattern struct {
	ID           uuid.UUID `json:"id"`
	ProtocolID   uuid.UUID `json:"protocolId"`
	PatternType  string    `json:"patternType"` // prefix, suffix, contains, exact
	Pattern      string    `json:"pattern"`
	PositionType string    `json:"positionType"`
	Priority     int       `json:"priority"`
	CreatedAt    time.Time `json:"createdAt"`
}

// ProtocolRepository handles protocol data access
type ProtocolRepository struct {
	db *PostgresDB
}

// NewProtocolRepository creates a new protocol repository
func NewProtocolRepository(db *PostgresDB) *ProtocolRepository {
	return &ProtocolRepository{db: db}
}

// GetProtocol retrieves a protocol by slug
func (r *ProtocolRepository) GetProtocol(ctx context.Context, slug string) (*Protocol, error) {
	query := `
		SELECT id, slug, name, category, site_url, logo_url, has_supported_portfolio, created_at, updated_at
		FROM protocols
		WHERE slug = $1
	`

	var p Protocol
	err := r.db.Pool().QueryRow(ctx, query, slug).Scan(
		&p.ID, &p.Slug, &p.Name, &p.Category, &p.SiteURL, &p.LogoURL,
		&p.HasSupportedPortfolio, &p.CreatedAt, &p.UpdatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get protocol: %w", err)
	}

	return &p, nil
}

// GetProtocolByID retrieves a protocol by ID
func (r *ProtocolRepository) GetProtocolByID(ctx context.Context, id uuid.UUID) (*Protocol, error) {
	query := `
		SELECT id, slug, name, category, site_url, logo_url, has_supported_portfolio, created_at, updated_at
		FROM protocols
		WHERE id = $1
	`

	var p Protocol
	err := r.db.Pool().QueryRow(ctx, query, id).Scan(
		&p.ID, &p.Slug, &p.Name, &p.Category, &p.SiteURL, &p.LogoURL,
		&p.HasSupportedPortfolio, &p.CreatedAt, &p.UpdatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get protocol: %w", err)
	}

	return &p, nil
}

// GetProtocolByContract retrieves a protocol by contract address
func (r *ProtocolRepository) GetProtocolByContract(ctx context.Context, contractAddress, chain string) (*Protocol, *ProtocolContract, error) {
	query := `
		SELECT p.id, p.slug, p.name, p.category, p.site_url, p.logo_url, p.has_supported_portfolio, p.created_at, p.updated_at,
		       pc.id, pc.protocol_id, pc.contract_address, pc.chain, pc.contract_type, pc.position_type, pc.description, pc.created_at
		FROM protocol_contracts pc
		JOIN protocols p ON p.id = pc.protocol_id
		WHERE lower(pc.contract_address) = lower($1) AND pc.chain = $2
	`

	var p Protocol
	var pc ProtocolContract
	err := r.db.Pool().QueryRow(ctx, query, contractAddress, chain).Scan(
		&p.ID, &p.Slug, &p.Name, &p.Category, &p.SiteURL, &p.LogoURL,
		&p.HasSupportedPortfolio, &p.CreatedAt, &p.UpdatedAt,
		&pc.ID, &pc.ProtocolID, &pc.ContractAddress, &pc.Chain,
		&pc.ContractType, &pc.PositionType, &pc.Description, &pc.CreatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil, nil
		}
		return nil, nil, fmt.Errorf("failed to get protocol by contract: %w", err)
	}

	return &p, &pc, nil
}

// ListProtocols retrieves all protocols, optionally filtered by category
func (r *ProtocolRepository) ListProtocols(ctx context.Context, category *string) ([]Protocol, error) {
	query := `
		SELECT id, slug, name, category, site_url, logo_url, has_supported_portfolio, created_at, updated_at
		FROM protocols
	`
	args := []interface{}{}

	if category != nil {
		query += " WHERE category = $1"
		args = append(args, *category)
	}

	query += " ORDER BY name"

	rows, err := r.db.Pool().Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to list protocols: %w", err)
	}
	defer rows.Close()

	var protocols []Protocol
	for rows.Next() {
		var p Protocol
		if err := rows.Scan(
			&p.ID, &p.Slug, &p.Name, &p.Category, &p.SiteURL, &p.LogoURL,
			&p.HasSupportedPortfolio, &p.CreatedAt, &p.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan protocol: %w", err)
		}
		protocols = append(protocols, p)
	}

	return protocols, nil
}

// GetTokenPatterns retrieves all token patterns for a protocol
func (r *ProtocolRepository) GetTokenPatterns(ctx context.Context, protocolID uuid.UUID) ([]TokenPattern, error) {
	query := `
		SELECT id, protocol_id, pattern_type, pattern, position_type, priority, created_at
		FROM token_patterns
		WHERE protocol_id = $1
		ORDER BY priority DESC
	`

	rows, err := r.db.Pool().Query(ctx, query, protocolID)
	if err != nil {
		return nil, fmt.Errorf("failed to get token patterns: %w", err)
	}
	defer rows.Close()

	var patterns []TokenPattern
	for rows.Next() {
		var p TokenPattern
		if err := rows.Scan(
			&p.ID, &p.ProtocolID, &p.PatternType, &p.Pattern,
			&p.PositionType, &p.Priority, &p.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan token pattern: %w", err)
		}
		patterns = append(patterns, p)
	}

	return patterns, nil
}

// GetAllTokenPatterns retrieves all token patterns ordered by priority
func (r *ProtocolRepository) GetAllTokenPatterns(ctx context.Context) ([]TokenPattern, error) {
	query := `
		SELECT id, protocol_id, pattern_type, pattern, position_type, priority, created_at
		FROM token_patterns
		ORDER BY priority DESC, pattern_type
	`

	rows, err := r.db.Pool().Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get all token patterns: %w", err)
	}
	defer rows.Close()

	var patterns []TokenPattern
	for rows.Next() {
		var p TokenPattern
		if err := rows.Scan(
			&p.ID, &p.ProtocolID, &p.PatternType, &p.Pattern,
			&p.PositionType, &p.Priority, &p.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan token pattern: %w", err)
		}
		patterns = append(patterns, p)
	}

	return patterns, nil
}

// DetectProtocolBySymbol detects a protocol from token symbol using patterns
func (r *ProtocolRepository) DetectProtocolBySymbol(ctx context.Context, symbol string) (*Protocol, string, error) {
	patterns, err := r.GetAllTokenPatterns(ctx)
	if err != nil {
		return nil, "", err
	}

	// Find matching pattern (patterns are ordered by priority)
	for _, p := range patterns {
		matched := false
		switch p.PatternType {
		case "exact":
			matched = strings.EqualFold(symbol, p.Pattern)
		case "prefix":
			matched = strings.HasPrefix(symbol, p.Pattern)
		case "suffix":
			matched = strings.HasSuffix(symbol, p.Pattern)
		case "contains":
			matched = strings.Contains(symbol, p.Pattern)
		}

		if matched {
			protocol, err := r.GetProtocolByID(ctx, p.ProtocolID)
			if err != nil {
				return nil, "", err
			}
			return protocol, p.PositionType, nil
		}
	}

	return nil, "", nil
}

// CreateProtocol creates a new protocol
func (r *ProtocolRepository) CreateProtocol(ctx context.Context, p *Protocol) error {
	query := `
		INSERT INTO protocols (slug, name, category, site_url, logo_url, has_supported_portfolio)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING id, created_at, updated_at
	`

	err := r.db.Pool().QueryRow(ctx, query,
		p.Slug, p.Name, p.Category, p.SiteURL, p.LogoURL, p.HasSupportedPortfolio,
	).Scan(&p.ID, &p.CreatedAt, &p.UpdatedAt)
	if err != nil {
		return fmt.Errorf("failed to create protocol: %w", err)
	}

	return nil
}

// CreateProtocolContract creates a new protocol contract mapping
func (r *ProtocolRepository) CreateProtocolContract(ctx context.Context, pc *ProtocolContract) error {
	query := `
		INSERT INTO protocol_contracts (protocol_id, contract_address, chain, contract_type, position_type, description)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING id, created_at
	`

	err := r.db.Pool().QueryRow(ctx, query,
		pc.ProtocolID, strings.ToLower(pc.ContractAddress), pc.Chain,
		pc.ContractType, pc.PositionType, pc.Description,
	).Scan(&pc.ID, &pc.CreatedAt)
	if err != nil {
		return fmt.Errorf("failed to create protocol contract: %w", err)
	}

	return nil
}
