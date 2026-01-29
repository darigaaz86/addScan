// Package models provides data models for the address scanner system.
package models

import (
	"time"

	"github.com/address-scanner/internal/types"
)

// User represents a user in the system
type User struct {
	ID        string              `json:"id" db:"id"`
	Email     string              `json:"email" db:"email"`
	Tier      types.UserTier      `json:"tier" db:"tier"`
	Settings  *UserSettings       `json:"settings,omitempty" db:"settings"`
	CreatedAt time.Time           `json:"createdAt" db:"created_at"`
	UpdatedAt time.Time           `json:"updatedAt" db:"updated_at"`
}

// UserSettings represents user-specific settings
type UserSettings struct {
	DefaultChains        []types.ChainID `json:"defaultChains,omitempty"`
	NotificationsEnabled *bool           `json:"notificationsEnabled,omitempty"`
	EmailNotifications   *bool           `json:"emailNotifications,omitempty"`
}
