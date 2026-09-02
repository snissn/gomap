package treedb_test

import (
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/powerlossoracle"
)

const (
	powerLossWitnessNewMetaTest    = "TestPowerLossOracleCounterexampleNewMetaMissingClosure"
	powerLossWitnessNamespaceTest  = "TestPowerLossOracleAdversarialNewFileNamespaceMismatch"
	powerLossWitnessRelaxedRIDTest = "TestPowerLossOracleCounterexampleRelaxedCommandFrameMissingRID"
	powerLossWitnessChunkedTest    = "TestPowerLossOracleCounterexampleChunkedSyncIntermediateRoot"
)

// Keep these anchors in a file separate from the witnesses. Renaming or
// deleting a registered test function must fail at compile time rather than
// letting `go test -run '^missing$'` exit successfully with no matching test.
type powerLossWitnessTestKey struct {
	pkg      string
	testName string
}

var powerLossCounterexampleWitnessAnchors = map[powerLossWitnessTestKey]func(*testing.T){
	{pkg: "./TreeDB", testName: powerLossWitnessNewMetaTest}:    TestPowerLossOracleCounterexampleNewMetaMissingClosure,
	{pkg: "./TreeDB", testName: powerLossWitnessNamespaceTest}:  TestPowerLossOracleAdversarialNewFileNamespaceMismatch,
	{pkg: "./TreeDB", testName: powerLossWitnessRelaxedRIDTest}: TestPowerLossOracleCounterexampleRelaxedCommandFrameMissingRID,
	{pkg: "./TreeDB", testName: powerLossWitnessChunkedTest}:    TestPowerLossOracleCounterexampleChunkedSyncIntermediateRoot,
}

func TestPowerLossCounterexampleWitnessRegistryAnchors(t *testing.T) {
	registered := make(map[powerLossWitnessTestKey]bool)
	seenIDs := make(map[string]bool)
	for _, witness := range powerlossoracle.CounterexampleWitnesses {
		if seenIDs[witness.ID] {
			t.Fatalf("duplicate code-owned witness ID %q", witness.ID)
		}
		seenIDs[witness.ID] = true
		switch witness.Package {
		case "./TreeDB":
			registered[powerLossWitnessTestKey{pkg: witness.Package, testName: witness.TestName}] = true
		case "./TreeDB/db":
			// Anchored by the owning package's separate registry test.
		default:
			t.Fatalf("witness %q has unknown package owner %q", witness.ID, witness.Package)
		}
	}
	if len(registered) == 0 {
		t.Fatal("./TreeDB owns no registered counterexample witnesses")
	}
	if len(registered) != len(powerLossCounterexampleWitnessAnchors) {
		t.Fatalf("./TreeDB registered witness set size=%d, compile-time anchor set size=%d", len(registered), len(powerLossCounterexampleWitnessAnchors))
	}
	for key := range registered {
		if powerLossCounterexampleWitnessAnchors[key] == nil {
			t.Errorf("registered witness (%s, %s) has no compile-time function anchor", key.pkg, key.testName)
		}
	}
	for key, anchor := range powerLossCounterexampleWitnessAnchors {
		if anchor == nil {
			t.Errorf("compile-time witness anchor (%s, %s) is nil", key.pkg, key.testName)
			continue
		}
		if !registered[key] {
			t.Errorf("compile-time witness anchor (%s, %s) is absent from the code-owned registry", key.pkg, key.testName)
		}
		qualifiedName := runtime.FuncForPC(reflect.ValueOf(anchor).Pointer()).Name()
		actualName := qualifiedName[strings.LastIndex(qualifiedName, ".")+1:]
		if actualName != key.testName {
			t.Errorf("compile-time witness anchor (%s, %s) points to function %q", key.pkg, key.testName, actualName)
		}
	}
}
