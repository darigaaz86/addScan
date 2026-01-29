package types

import (
	"testing"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
)

// Sample property-based test to verify gopter is working
func TestPropertyBasedTestingSetup(t *testing.T) {
	properties := gopter.NewProperties(nil)

	// Property: For any two integers, addition is commutative
	properties.Property("addition is commutative", prop.ForAll(
		func(a, b int) bool {
			return a+b == b+a
		},
		gen.Int(),
		gen.Int(),
	))

	properties.TestingRun(t)
}
