package storage

import (
	"testing"
	"time"

	"github.com/address-scanner/internal/config"
)

func TestNewRedisCache(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cfg := &config.RedisConfig{
		Host:           "localhost",
		Port:           "6379",
		Password:       "",
		DB:             0,
		MaxConnections: 10,
	}

	cache, err := NewRedisCache(cfg)
	if err != nil {
		t.Skipf("Skipping test - Redis not available: %v", err)
		return
	}
	defer func() {
		if err := cache.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	}()

	// Test ping
	ctx := testContext(t)
	if err := cache.Ping(ctx); err != nil {
		t.Errorf("Ping() error = %v", err)
	}
}

func TestRedisCache_SetGet(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cfg := &config.RedisConfig{
		Host:           "localhost",
		Port:           "6379",
		Password:       "",
		DB:             0,
		MaxConnections: 10,
	}

	cache, err := NewRedisCache(cfg)
	if err != nil {
		t.Skipf("Skipping test - Redis not available: %v", err)
		return
	}
	defer func() {
		_ = cache.Close()
	}()

	ctx := testContext(t)

	// Test Set and Get
	key := "test:key"
	value := "test-value"
	ttl := 10 * time.Second

	if err := cache.Set(ctx, key, value, ttl); err != nil {
		t.Fatalf("Set() error = %v", err)
	}

	got, err := cache.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	if got != value {
		t.Errorf("Get() = %v, want %v", got, value)
	}

	// Cleanup
	if err := cache.Del(ctx, key); err != nil {
		t.Errorf("Del() error = %v", err)
	}
}

func TestRedisCache_Exists(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cfg := &config.RedisConfig{
		Host:           "localhost",
		Port:           "6379",
		Password:       "",
		DB:             0,
		MaxConnections: 10,
	}

	cache, err := NewRedisCache(cfg)
	if err != nil {
		t.Skipf("Skipping test - Redis not available: %v", err)
		return
	}
	defer func() {
		_ = cache.Close()
	}()

	ctx := testContext(t)

	key := "test:exists"
	value := "test-value"

	// Key should not exist initially
	exists, err := cache.Exists(ctx, key)
	if err != nil {
		t.Fatalf("Exists() error = %v", err)
	}
	if exists {
		t.Error("Exists() = true, want false for non-existent key")
	}

	// Set key
	if err := cache.Set(ctx, key, value, 10*time.Second); err != nil {
		t.Fatalf("Set() error = %v", err)
	}

	// Key should exist now
	exists, err = cache.Exists(ctx, key)
	if err != nil {
		t.Fatalf("Exists() error = %v", err)
	}
	if !exists {
		t.Error("Exists() = false, want true for existing key")
	}

	// Cleanup
	_ = cache.Del(ctx, key)
}
