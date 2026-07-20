package treedb

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/powerlossoracle"
)

// This package-internal witness cannot be anchored from the external
// treedb_test registry because it exercises private publication rendezvous.
func TestPowerLossInternalCounterexampleWitnessRegistryAnchor(t *testing.T) {
	const id = "stale-build-base-root-publication"
	const testName = "TestPowerLossCertificationStaleBuildBasePublicReopen"
	anchor := TestPowerLossCertificationStaleBuildBasePublicReopen
	if anchor == nil {
		t.Fatal("stale-build counterexample witness anchor is nil")
	}
	for _, witness := range powerlossoracle.CounterexampleWitnesses {
		if witness.ID == id {
			if witness.Package != "./TreeDB" || witness.TestName != testName {
				t.Fatalf("stale-build registry witness=(%s,%s) want=(./TreeDB,%s)", witness.Package, witness.TestName, testName)
			}
			return
		}
	}
	t.Fatalf("stale-build counterexample witness %q is absent from the code-owned registry", id)
}
