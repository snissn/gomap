package caching

import (
	"os"
	"testing"
)

func requireTreeDBStress(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping TreeDB stress/regression test in -short mode")
	}
	if os.Getenv("TREEDB_STRESS") != "1" {
		t.Skip("set TREEDB_STRESS=1 to run TreeDB stress/regression tests")
	}
}
