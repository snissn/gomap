package caching

import (
	"testing"
)

func requireTreeDBStress(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping TreeDB stress/regression test in -short mode")
	}
}
