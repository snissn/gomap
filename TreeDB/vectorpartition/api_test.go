package vectorpartition

import (
	"strings"
	"testing"
)

func TestValidateReferenceInputShapeMatchesReferencePreflight(t *testing.T) {
	c := DefaultConfig()
	c.Partitions = 238
	c.Repetitions = 1
	c.Pivots = 2
	c.MaxLeafBucket = 2
	c.Degree = 1
	if err := ValidateInputShape(c, 1_000_000, 1); err != nil {
		t.Fatalf("generic API preflight unexpectedly rejected reference-only shape: %v", err)
	}
	if err := ValidateReferenceInputShape(c, 1_000_000, 1); err == nil || !strings.Contains(err.Error(), "partition work") {
		t.Fatalf("reference API shape error=%v; expected partition-work rejection", err)
	}
}
