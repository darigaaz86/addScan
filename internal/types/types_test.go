package types

import (
	"testing"
)

// Basic test to ensure testing framework is set up
func TestBasicSetup(t *testing.T) {
	t.Run("testing framework is working", func(t *testing.T) {
		if testing.Short() {
			t.Skip("skipping test in short mode")
		}
		// This test always passes - it's just to verify setup
	})
}
