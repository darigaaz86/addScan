package storage

import (
	"context"
	"testing"
	"time"
)

// testContext creates a context with timeout for tests
func testContext(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}
