package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/address-scanner/internal/models"
	"github.com/address-scanner/internal/types"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// UserRepository handles user data persistence
type UserRepository struct {
	db *PostgresDB
}

// NewUserRepository creates a new user repository
func NewUserRepository(db *PostgresDB) *UserRepository {
	return &UserRepository{db: db}
}

// Create creates a new user
func (r *UserRepository) Create(ctx context.Context, user *models.User) error {
	if user.ID == "" {
		user.ID = uuid.New().String()
	}

	now := time.Now()
	user.CreatedAt = now
	user.UpdatedAt = now

	// Validate tier
	if err := validateTier(user.Tier); err != nil {
		return err
	}

	// Serialize settings to JSON
	var settingsJSON []byte
	var err error
	if user.Settings != nil {
		settingsJSON, err = json.Marshal(user.Settings)
		if err != nil {
			return fmt.Errorf("failed to marshal settings: %w", err)
		}
	}

	query := `
		INSERT INTO users (id, email, tier, settings, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6)
	`

	_, err = r.db.Pool().Exec(ctx, query,
		user.ID,
		user.Email,
		user.Tier,
		settingsJSON,
		user.CreatedAt,
		user.UpdatedAt,
	)

	if err != nil {
		return fmt.Errorf("failed to create user: %w", err)
	}

	return nil
}

// GetByID retrieves a user by ID
func (r *UserRepository) GetByID(ctx context.Context, id string) (*models.User, error) {
	query := `
		SELECT id, email, tier, settings, created_at, updated_at
		FROM users
		WHERE id = $1
	`

	var user models.User
	var settingsJSON []byte

	err := r.db.Pool().QueryRow(ctx, query, id).Scan(
		&user.ID,
		&user.Email,
		&user.Tier,
		&settingsJSON,
		&user.CreatedAt,
		&user.UpdatedAt,
	)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("user not found: %s", id)
		}
		return nil, fmt.Errorf("failed to get user: %w", err)
	}

	// Deserialize settings
	if len(settingsJSON) > 0 {
		var settings models.UserSettings
		if err := json.Unmarshal(settingsJSON, &settings); err != nil {
			return nil, fmt.Errorf("failed to unmarshal settings: %w", err)
		}
		user.Settings = &settings
	}

	return &user, nil
}

// GetByEmail retrieves a user by email
func (r *UserRepository) GetByEmail(ctx context.Context, email string) (*models.User, error) {
	query := `
		SELECT id, email, tier, settings, created_at, updated_at
		FROM users
		WHERE email = $1
	`

	var user models.User
	var settingsJSON []byte

	err := r.db.Pool().QueryRow(ctx, query, email).Scan(
		&user.ID,
		&user.Email,
		&user.Tier,
		&settingsJSON,
		&user.CreatedAt,
		&user.UpdatedAt,
	)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("user not found: %s", email)
		}
		return nil, fmt.Errorf("failed to get user: %w", err)
	}

	// Deserialize settings
	if len(settingsJSON) > 0 {
		var settings models.UserSettings
		if err := json.Unmarshal(settingsJSON, &settings); err != nil {
			return nil, fmt.Errorf("failed to unmarshal settings: %w", err)
		}
		user.Settings = &settings
	}

	return &user, nil
}

// Update updates an existing user
func (r *UserRepository) Update(ctx context.Context, user *models.User) error {
	// Validate tier
	if err := validateTier(user.Tier); err != nil {
		return err
	}

	user.UpdatedAt = time.Now()

	// Serialize settings to JSON
	var settingsJSON []byte
	var err error
	if user.Settings != nil {
		settingsJSON, err = json.Marshal(user.Settings)
		if err != nil {
			return fmt.Errorf("failed to marshal settings: %w", err)
		}
	}

	query := `
		UPDATE users
		SET email = $2, tier = $3, settings = $4, updated_at = $5
		WHERE id = $1
	`

	result, err := r.db.Pool().Exec(ctx, query,
		user.ID,
		user.Email,
		user.Tier,
		settingsJSON,
		user.UpdatedAt,
	)

	if err != nil {
		return fmt.Errorf("failed to update user: %w", err)
	}

	if result.RowsAffected() == 0 {
		return fmt.Errorf("user not found: %s", user.ID)
	}

	return nil
}

// Delete deletes a user by ID
func (r *UserRepository) Delete(ctx context.Context, id string) error {
	query := `DELETE FROM users WHERE id = $1`

	result, err := r.db.Pool().Exec(ctx, query, id)
	if err != nil {
		return fmt.Errorf("failed to delete user: %w", err)
	}

	if result.RowsAffected() == 0 {
		return fmt.Errorf("user not found: %s", id)
	}

	return nil
}

// List retrieves all users with optional pagination
func (r *UserRepository) List(ctx context.Context, limit, offset int) ([]*models.User, error) {
	query := `
		SELECT id, email, tier, settings, created_at, updated_at
		FROM users
		ORDER BY created_at DESC
		LIMIT $1 OFFSET $2
	`

	rows, err := r.db.Pool().Query(ctx, query, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to list users: %w", err)
	}
	defer rows.Close()

	var users []*models.User
	for rows.Next() {
		var user models.User
		var settingsJSON []byte

		err := rows.Scan(
			&user.ID,
			&user.Email,
			&user.Tier,
			&settingsJSON,
			&user.CreatedAt,
			&user.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan user: %w", err)
		}

		// Deserialize settings
		if len(settingsJSON) > 0 {
			var settings models.UserSettings
			if err := json.Unmarshal(settingsJSON, &settings); err != nil {
				return nil, fmt.Errorf("failed to unmarshal settings: %w", err)
			}
			user.Settings = &settings
		}

		users = append(users, &user)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating users: %w", err)
	}

	return users, nil
}

// Count returns the total number of users
func (r *UserRepository) Count(ctx context.Context) (int64, error) {
	var count int64
	query := `SELECT COUNT(*) FROM users`

	err := r.db.Pool().QueryRow(ctx, query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count users: %w", err)
	}

	return count, nil
}

// UpdateTier updates a user's tier
func (r *UserRepository) UpdateTier(ctx context.Context, userID string, tier types.UserTier) error {
	// Validate tier
	if err := validateTier(tier); err != nil {
		return err
	}

	query := `
		UPDATE users
		SET tier = $2, updated_at = $3
		WHERE id = $1
	`

	result, err := r.db.Pool().Exec(ctx, query, userID, tier, time.Now())
	if err != nil {
		return fmt.Errorf("failed to update user tier: %w", err)
	}

	if result.RowsAffected() == 0 {
		return fmt.Errorf("user not found: %s", userID)
	}

	return nil
}

// Exists checks if a user exists by ID
func (r *UserRepository) Exists(ctx context.Context, id string) (bool, error) {
	var exists bool
	query := `SELECT EXISTS(SELECT 1 FROM users WHERE id = $1)`

	err := r.db.Pool().QueryRow(ctx, query, id).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check user existence: %w", err)
	}

	return exists, nil
}

// ExistsByEmail checks if a user exists by email
func (r *UserRepository) ExistsByEmail(ctx context.Context, email string) (bool, error) {
	var exists bool
	query := `SELECT EXISTS(SELECT 1 FROM users WHERE email = $1)`

	err := r.db.Pool().QueryRow(ctx, query, email).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check user existence by email: %w", err)
	}

	return exists, nil
}

// validateTier validates that the tier is one of the allowed values
func validateTier(tier types.UserTier) error {
	switch tier {
	case types.TierFree, types.TierPaid:
		return nil
	default:
		return &types.ServiceError{
			Code:    "INVALID_TIER",
			Message: fmt.Sprintf("invalid tier: %s (must be 'free' or 'paid')", tier),
			Details: map[string]interface{}{
				"tier":          tier,
				"allowed_tiers": []string{string(types.TierFree), string(types.TierPaid)},
			},
		}
	}
}

// GetUserTier is a helper function to get just the tier for a user
func (r *UserRepository) GetUserTier(ctx context.Context, userID string) (types.UserTier, error) {
	var tier types.UserTier
	query := `SELECT tier FROM users WHERE id = $1`

	err := r.db.Pool().QueryRow(ctx, query, userID).Scan(&tier)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", fmt.Errorf("user not found: %s", userID)
		}
		return "", fmt.Errorf("failed to get user tier: %w", err)
	}

	return tier, nil
}

// BeginTx starts a new transaction
func (r *UserRepository) BeginTx(ctx context.Context) (pgx.Tx, error) {
	return r.db.Pool().Begin(ctx)
}

// CreateWithTx creates a user within a transaction
func (r *UserRepository) CreateWithTx(ctx context.Context, tx pgx.Tx, user *models.User) error {
	if user.ID == "" {
		user.ID = uuid.New().String()
	}

	now := time.Now()
	user.CreatedAt = now
	user.UpdatedAt = now

	// Validate tier
	if err := validateTier(user.Tier); err != nil {
		return err
	}

	// Serialize settings to JSON
	var settingsJSON sql.NullString
	if user.Settings != nil {
		data, err := json.Marshal(user.Settings)
		if err != nil {
			return fmt.Errorf("failed to marshal settings: %w", err)
		}
		settingsJSON = sql.NullString{String: string(data), Valid: true}
	}

	query := `
		INSERT INTO users (id, email, tier, settings, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6)
	`

	_, err := tx.Exec(ctx, query,
		user.ID,
		user.Email,
		user.Tier,
		settingsJSON,
		user.CreatedAt,
		user.UpdatedAt,
	)

	if err != nil {
		return fmt.Errorf("failed to create user: %w", err)
	}

	return nil
}
