package storage

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// RegisteredToken represents a token in the registry
type RegisteredToken struct {
	ID              uuid.UUID  `json:"id"`
	ContractAddress string     `json:"contractAddress"`
	Chain           string     `json:"chain"`
	Symbol          *string    `json:"symbol"`
	Name            *string    `json:"name"`
	Decimals        int        `json:"decimals"`
	LogoURL         *string    `json:"logoUrl"`
	IsNative        bool       `json:"isNative"`
	IsStablecoin    bool       `json:"isStablecoin"`
	ProtocolID      *uuid.UUID `json:"protocolId"`
	UnderlyingToken *string    `json:"underlyingToken"`
	CreatedAt       time.Time  `json:"createdAt"`
	UpdatedAt       time.Time  `json:"updatedAt"`
}

// TokenRegistryRepository handles token registry data access
type TokenRegistryRepository struct {
	db *PostgresDB
}

// NewTokenRegistryRepository creates a new token registry repository
func NewTokenRegistryRepository(db *PostgresDB) *TokenRegistryRepository {
	return &TokenRegistryRepository{db: db}
}

// GetToken retrieves a token by contract address and chain
func (r *TokenRegistryRepository) GetToken(ctx context.Context, contractAddress, chain string) (*RegisteredToken, error) {
	query := `
		SELECT id, contract_address, chain, symbol, name, decimals, logo_url, 
		       is_native, is_stablecoin, protocol_id, underlying_token, created_at, updated_at
		FROM tokens
		WHERE lower(contract_address) = lower($1) AND chain = $2
	`

	var t RegisteredToken
	err := r.db.Pool().QueryRow(ctx, query, contractAddress, chain).Scan(
		&t.ID, &t.ContractAddress, &t.Chain, &t.Symbol, &t.Name, &t.Decimals,
		&t.LogoURL, &t.IsNative, &t.IsStablecoin, &t.ProtocolID, &t.UnderlyingToken,
		&t.CreatedAt, &t.UpdatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get token: %w", err)
	}

	return &t, nil
}

// GetTokenBySymbol retrieves a token by symbol and chain
func (r *TokenRegistryRepository) GetTokenBySymbol(ctx context.Context, symbol, chain string) (*RegisteredToken, error) {
	query := `
		SELECT id, contract_address, chain, symbol, name, decimals, logo_url, 
		       is_native, is_stablecoin, protocol_id, underlying_token, created_at, updated_at
		FROM tokens
		WHERE upper(symbol) = upper($1) AND chain = $2
		LIMIT 1
	`

	var t RegisteredToken
	err := r.db.Pool().QueryRow(ctx, query, symbol, chain).Scan(
		&t.ID, &t.ContractAddress, &t.Chain, &t.Symbol, &t.Name, &t.Decimals,
		&t.LogoURL, &t.IsNative, &t.IsStablecoin, &t.ProtocolID, &t.UnderlyingToken,
		&t.CreatedAt, &t.UpdatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get token by symbol: %w", err)
	}

	return &t, nil
}

// GetNativeToken retrieves the native token for a chain
func (r *TokenRegistryRepository) GetNativeToken(ctx context.Context, chain string) (*RegisteredToken, error) {
	query := `
		SELECT id, contract_address, chain, symbol, name, decimals, logo_url, 
		       is_native, is_stablecoin, protocol_id, underlying_token, created_at, updated_at
		FROM tokens
		WHERE chain = $1 AND is_native = true
		LIMIT 1
	`

	var t RegisteredToken
	err := r.db.Pool().QueryRow(ctx, query, chain).Scan(
		&t.ID, &t.ContractAddress, &t.Chain, &t.Symbol, &t.Name, &t.Decimals,
		&t.LogoURL, &t.IsNative, &t.IsStablecoin, &t.ProtocolID, &t.UnderlyingToken,
		&t.CreatedAt, &t.UpdatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get native token: %w", err)
	}

	return &t, nil
}

// ListTokensByChain retrieves all tokens for a chain
func (r *TokenRegistryRepository) ListTokensByChain(ctx context.Context, chain string) ([]RegisteredToken, error) {
	query := `
		SELECT id, contract_address, chain, symbol, name, decimals, logo_url, 
		       is_native, is_stablecoin, protocol_id, underlying_token, created_at, updated_at
		FROM tokens
		WHERE chain = $1
		ORDER BY is_native DESC, symbol
	`

	rows, err := r.db.Pool().Query(ctx, query, chain)
	if err != nil {
		return nil, fmt.Errorf("failed to list tokens: %w", err)
	}
	defer rows.Close()

	var tokens []RegisteredToken
	for rows.Next() {
		var t RegisteredToken
		if err := rows.Scan(
			&t.ID, &t.ContractAddress, &t.Chain, &t.Symbol, &t.Name, &t.Decimals,
			&t.LogoURL, &t.IsNative, &t.IsStablecoin, &t.ProtocolID, &t.UnderlyingToken,
			&t.CreatedAt, &t.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan token: %w", err)
		}
		tokens = append(tokens, t)
	}

	return tokens, nil
}

// ListStablecoins retrieves all stablecoins, optionally filtered by chain
func (r *TokenRegistryRepository) ListStablecoins(ctx context.Context, chain *string) ([]RegisteredToken, error) {
	query := `
		SELECT id, contract_address, chain, symbol, name, decimals, logo_url, 
		       is_native, is_stablecoin, protocol_id, underlying_token, created_at, updated_at
		FROM tokens
		WHERE is_stablecoin = true
	`
	args := []interface{}{}

	if chain != nil {
		query += " AND chain = $1"
		args = append(args, *chain)
	}

	query += " ORDER BY chain, symbol"

	rows, err := r.db.Pool().Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to list stablecoins: %w", err)
	}
	defer rows.Close()

	var tokens []RegisteredToken
	for rows.Next() {
		var t RegisteredToken
		if err := rows.Scan(
			&t.ID, &t.ContractAddress, &t.Chain, &t.Symbol, &t.Name, &t.Decimals,
			&t.LogoURL, &t.IsNative, &t.IsStablecoin, &t.ProtocolID, &t.UnderlyingToken,
			&t.CreatedAt, &t.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan token: %w", err)
		}
		tokens = append(tokens, t)
	}

	return tokens, nil
}

// ListTokensByProtocol retrieves all tokens associated with a protocol
func (r *TokenRegistryRepository) ListTokensByProtocol(ctx context.Context, protocolID uuid.UUID) ([]RegisteredToken, error) {
	query := `
		SELECT id, contract_address, chain, symbol, name, decimals, logo_url, 
		       is_native, is_stablecoin, protocol_id, underlying_token, created_at, updated_at
		FROM tokens
		WHERE protocol_id = $1
		ORDER BY chain, symbol
	`

	rows, err := r.db.Pool().Query(ctx, query, protocolID)
	if err != nil {
		return nil, fmt.Errorf("failed to list tokens by protocol: %w", err)
	}
	defer rows.Close()

	var tokens []RegisteredToken
	for rows.Next() {
		var t RegisteredToken
		if err := rows.Scan(
			&t.ID, &t.ContractAddress, &t.Chain, &t.Symbol, &t.Name, &t.Decimals,
			&t.LogoURL, &t.IsNative, &t.IsStablecoin, &t.ProtocolID, &t.UnderlyingToken,
			&t.CreatedAt, &t.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan token: %w", err)
		}
		tokens = append(tokens, t)
	}

	return tokens, nil
}

// CreateToken creates a new token in the registry
func (r *TokenRegistryRepository) CreateToken(ctx context.Context, t *RegisteredToken) error {
	query := `
		INSERT INTO tokens (contract_address, chain, symbol, name, decimals, logo_url, 
		                    is_native, is_stablecoin, protocol_id, underlying_token)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		RETURNING id, created_at, updated_at
	`

	err := r.db.Pool().QueryRow(ctx, query,
		strings.ToLower(t.ContractAddress), t.Chain, t.Symbol, t.Name, t.Decimals,
		t.LogoURL, t.IsNative, t.IsStablecoin, t.ProtocolID, t.UnderlyingToken,
	).Scan(&t.ID, &t.CreatedAt, &t.UpdatedAt)
	if err != nil {
		return fmt.Errorf("failed to create token: %w", err)
	}

	return nil
}

// UpsertToken creates or updates a token in the registry
func (r *TokenRegistryRepository) UpsertToken(ctx context.Context, t *RegisteredToken) error {
	query := `
		INSERT INTO tokens (contract_address, chain, symbol, name, decimals, logo_url, 
		                    is_native, is_stablecoin, protocol_id, underlying_token)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (contract_address, chain) DO UPDATE SET
			symbol = EXCLUDED.symbol,
			name = EXCLUDED.name,
			decimals = EXCLUDED.decimals,
			logo_url = COALESCE(EXCLUDED.logo_url, tokens.logo_url),
			is_stablecoin = EXCLUDED.is_stablecoin,
			protocol_id = EXCLUDED.protocol_id,
			underlying_token = EXCLUDED.underlying_token,
			updated_at = NOW()
		RETURNING id, created_at, updated_at
	`

	err := r.db.Pool().QueryRow(ctx, query,
		strings.ToLower(t.ContractAddress), t.Chain, t.Symbol, t.Name, t.Decimals,
		t.LogoURL, t.IsNative, t.IsStablecoin, t.ProtocolID, t.UnderlyingToken,
	).Scan(&t.ID, &t.CreatedAt, &t.UpdatedAt)
	if err != nil {
		return fmt.Errorf("failed to upsert token: %w", err)
	}

	return nil
}

// GetOrCreateToken retrieves a token or creates it if it doesn't exist
func (r *TokenRegistryRepository) GetOrCreateToken(ctx context.Context, contractAddress, chain, symbol, name string, decimals int) (*RegisteredToken, error) {
	// Try to get existing token
	token, err := r.GetToken(ctx, contractAddress, chain)
	if err != nil {
		return nil, err
	}
	if token != nil {
		return token, nil
	}

	// Create new token
	token = &RegisteredToken{
		ContractAddress: contractAddress,
		Chain:           chain,
		Symbol:          &symbol,
		Name:            &name,
		Decimals:        decimals,
	}

	if err := r.CreateToken(ctx, token); err != nil {
		// Handle race condition - try to get again
		token, err = r.GetToken(ctx, contractAddress, chain)
		if err != nil {
			return nil, err
		}
		if token != nil {
			return token, nil
		}
		return nil, fmt.Errorf("failed to create or get token: %w", err)
	}

	return token, nil
}

// SearchTokens searches tokens by symbol or name
func (r *TokenRegistryRepository) SearchTokens(ctx context.Context, query string, limit int) ([]RegisteredToken, error) {
	sqlQuery := `
		SELECT id, contract_address, chain, symbol, name, decimals, logo_url, 
		       is_native, is_stablecoin, protocol_id, underlying_token, created_at, updated_at
		FROM tokens
		WHERE upper(symbol) LIKE upper($1) OR upper(name) LIKE upper($1)
		ORDER BY 
			CASE WHEN upper(symbol) = upper($2) THEN 0 ELSE 1 END,
			is_native DESC,
			symbol
		LIMIT $3
	`

	searchPattern := "%" + query + "%"
	rows, err := r.db.Pool().Query(ctx, sqlQuery, searchPattern, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to search tokens: %w", err)
	}
	defer rows.Close()

	var tokens []RegisteredToken
	for rows.Next() {
		var t RegisteredToken
		if err := rows.Scan(
			&t.ID, &t.ContractAddress, &t.Chain, &t.Symbol, &t.Name, &t.Decimals,
			&t.LogoURL, &t.IsNative, &t.IsStablecoin, &t.ProtocolID, &t.UnderlyingToken,
			&t.CreatedAt, &t.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan token: %w", err)
		}
		tokens = append(tokens, t)
	}

	return tokens, nil
}
