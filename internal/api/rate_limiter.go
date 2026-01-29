package api

import (
	"net/http"
	"sync"

	"github.com/address-scanner/internal/types"
	"golang.org/x/time/rate"
)

// RateLimiter manages rate limiting for API requests
type RateLimiter struct {
	limiters map[string]*rate.Limiter
	mu       sync.RWMutex
	
	// Rate limits per tier (requests per second)
	freeTierLimit    rate.Limit
	basicTierLimit   rate.Limit
	premiumTierLimit rate.Limit
	
	// Burst size (number of requests that can be made in a burst)
	burstSize int
}

// NewRateLimiter creates a new rate limiter
func NewRateLimiter(freeTierRPS, basicTierRPS, premiumTierRPS int) *RateLimiter {
	return &RateLimiter{
		limiters:         make(map[string]*rate.Limiter),
		freeTierLimit:    rate.Limit(freeTierRPS),
		basicTierLimit:   rate.Limit(basicTierRPS),
		premiumTierLimit: rate.Limit(premiumTierRPS),
		burstSize:        10, // Allow bursts of 10 requests
	}
}

// getLimiter returns the rate limiter for a specific user and tier
func (rl *RateLimiter) getLimiter(userID string, tier types.UserTier) *rate.Limiter {
	// Create a unique key for this user
	key := userID

	rl.mu.RLock()
	limiter, exists := rl.limiters[key]
	rl.mu.RUnlock()

	if exists {
		return limiter
	}

	// Determine rate limit based on tier
	var limit rate.Limit
	switch tier {
	case types.TierPaid:
		limit = rl.premiumTierLimit
	case types.TierFree:
		limit = rl.freeTierLimit
	default:
		limit = rl.freeTierLimit
	}

	// Create new limiter
	rl.mu.Lock()
	defer rl.mu.Unlock()

	// Double-check in case another goroutine created it
	if limiter, exists := rl.limiters[key]; exists {
		return limiter
	}

	limiter = rate.NewLimiter(limit, rl.burstSize)
	rl.limiters[key] = limiter

	return limiter
}

// RateLimitMiddleware creates a middleware that enforces rate limiting
func RateLimitMiddleware(rl *RateLimiter) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Get user ID and tier from headers
			userID := r.Header.Get("X-User-ID")
			if userID == "" {
				// No user ID - apply strictest rate limit
				userID = r.RemoteAddr // Use IP address as fallback
			}

			tierStr := r.Header.Get("X-User-Tier")
			tier := types.UserTier(tierStr)
			if tier == "" {
				tier = types.TierFree // Default to free tier
			}

			// Get limiter for this user
			limiter := rl.getLimiter(userID, tier)

			// Check if request is allowed
			if !limiter.Allow() {
				respondError(w, http.StatusTooManyRequests, "RATE_LIMIT_EXCEEDED", "Rate limit exceeded. Please try again later.", map[string]interface{}{
					"tier":  tier,
					"limit": limiter.Limit(),
				})
				return
			}

			// Request allowed - proceed
			next.ServeHTTP(w, r)
		})
	}
}
