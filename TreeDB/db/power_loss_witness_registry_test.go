package db

import (
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/powerlossoracle"
)

const (
	powerLossWitnessPageReuseTest  = "TestPowerLossOracleCounterexampleRecoverablePageReuse"
	powerLossWitnessStaleBuildTest = "TestPowerLossCertificationStaleBuildBasePublicReopen"
)

// This anchor deliberately lives outside the witness file so a test rename or
// deletion is a compile failure.
type powerLossWitnessTestKey struct {
	pkg      string
	testName string
}

var powerLossCounterexampleWitnessAnchors = map[powerLossWitnessTestKey]func(*testing.T){
	{pkg: "./TreeDB/db", testName: powerLossWitnessPageReuseTest}: TestPowerLossOracleCounterexampleRecoverablePageReuse,
}

func TestPowerLossCounterexampleWitnessRegistryAnchors(t *testing.T) {
	registered := make(map[powerLossWitnessTestKey]bool)
	for _, witness := range powerlossoracle.CounterexampleWitnesses {
		if witness.Package == "./TreeDB/db" {
			if witness.TestName == powerLossWitnessStaleBuildTest {
				// Anchored by the external db_test registry, which can name the
				// package-external certification function directly.
				continue
			}
			registered[powerLossWitnessTestKey{pkg: witness.Package, testName: witness.TestName}] = true
		}
	}
	if len(registered) == 0 {
		t.Fatal("./TreeDB/db owns no registered counterexample witnesses")
	}
	if len(registered) != len(powerLossCounterexampleWitnessAnchors) {
		t.Fatalf("./TreeDB/db registered witness set size=%d, compile-time anchor set size=%d", len(registered), len(powerLossCounterexampleWitnessAnchors))
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
