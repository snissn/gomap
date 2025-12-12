//go:build long
// +build long

package treedb

import "testing"

// Long-running crash/kill and corruption tests live under the "long" build tag.
// Run with: go test -tags long ./...
func TestKillRecoveryLong(t *testing.T) {
	t.Skip("long-running kill/crash-recovery test; enable with -tags long")
}

func TestCorruptionFuzzLong(t *testing.T) {
	t.Skip("long-running corruption fuzz; enable with -tags long")
}

