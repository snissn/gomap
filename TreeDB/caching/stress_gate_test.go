package caching

import (
	"os"
	"testing"
)

func requireTreeDBStress(t *testing.T) {
	t.Helper()
	if os.Getenv("TREEDB_STRESS") == "" {
		t.Skip("set TREEDB_STRESS=1 to run TreeDB stress/regression tests (may be slow and may currently fail)")
	}
}
