package storage

import (
	"context"
	"strings"
	"sync"
)

// TokenWhitelistRepository manages the token whitelist
type TokenWhitelistRepository struct {
	db    *PostgresDB
	cache map[string]map[string]bool // chain -> tokenAddress -> exists
	mu    sync.RWMutex
}

// NewTokenWhitelistRepository creates a new token whitelist repository
func NewTokenWhitelistRepository(db *PostgresDB) *TokenWhitelistRepository {
	return &TokenWhitelistRepository{
		db:    db,
		cache: make(map[string]map[string]bool),
	}
}

// LoadCache loads all whitelisted tokens into memory
func (r *TokenWhitelistRepository) LoadCache(ctx context.Context) error {
	query := `SELECT chain, token_address FROM token_whitelist WHERE is_active = true`

	rows, err := r.db.Pool().Query(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()

	r.mu.Lock()
	defer r.mu.Unlock()

	// Reset cache
	r.cache = make(map[string]map[string]bool)

	for rows.Next() {
		var chain, tokenAddress string
		if err := rows.Scan(&chain, &tokenAddress); err != nil {
			return err
		}

		chain = strings.ToLower(chain)
		tokenAddress = strings.ToLower(tokenAddress)

		if r.cache[chain] == nil {
			r.cache[chain] = make(map[string]bool)
		}
		r.cache[chain][tokenAddress] = true
	}

	return rows.Err()
}

// IsWhitelisted checks if a token is in the whitelist
func (r *TokenWhitelistRepository) IsWhitelisted(chain, tokenAddress string) bool {
	chain = strings.ToLower(chain)
	tokenAddress = strings.ToLower(tokenAddress)

	r.mu.RLock()
	defer r.mu.RUnlock()

	if chainTokens, ok := r.cache[chain]; ok {
		return chainTokens[tokenAddress]
	}
	return false
}

// AddToken adds a token to the whitelist
func (r *TokenWhitelistRepository) AddToken(ctx context.Context, chain, tokenAddress, symbol, name string, decimals int) error {
	query := `
		INSERT INTO token_whitelist (chain, token_address, symbol, name, decimals)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (chain, token_address) DO UPDATE SET
			symbol = EXCLUDED.symbol,
			name = EXCLUDED.name,
			decimals = EXCLUDED.decimals,
			updated_at = NOW()
	`

	_, err := r.db.Pool().Exec(ctx, query,
		strings.ToLower(chain),
		strings.ToLower(tokenAddress),
		symbol,
		name,
		decimals,
	)
	if err != nil {
		return err
	}

	// Update cache
	r.mu.Lock()
	defer r.mu.Unlock()

	chain = strings.ToLower(chain)
	tokenAddress = strings.ToLower(tokenAddress)

	if r.cache[chain] == nil {
		r.cache[chain] = make(map[string]bool)
	}
	r.cache[chain][tokenAddress] = true

	return nil
}

// RemoveToken removes a token from the whitelist (soft delete)
func (r *TokenWhitelistRepository) RemoveToken(ctx context.Context, chain, tokenAddress string) error {
	query := `UPDATE token_whitelist SET is_active = false, updated_at = NOW() WHERE chain = $1 AND token_address = $2`

	_, err := r.db.Pool().Exec(ctx, query, strings.ToLower(chain), strings.ToLower(tokenAddress))
	if err != nil {
		return err
	}

	// Update cache
	r.mu.Lock()
	defer r.mu.Unlock()

	chain = strings.ToLower(chain)
	tokenAddress = strings.ToLower(tokenAddress)

	if r.cache[chain] != nil {
		delete(r.cache[chain], tokenAddress)
	}

	return nil
}
