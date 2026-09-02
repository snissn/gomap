package db_test

import (
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/powerlossoracle"
)

func TestPowerLossExternalCounterexampleWitnessRegistryAnchor(t *testing.T) {
	const id = "stale-build-base-root-publication"
	const testName = "TestPowerLossCertificationStaleBuildBasePublicReopen"
	anchor := TestPowerLossCertificationStaleBuildBasePublicReopen
	qualified := runtime.FuncForPC(reflect.ValueOf(anchor).Pointer()).Name()
	actual := qualified[strings.LastIndex(qualified, ".")+1:]
	if actual != testName {
		t.Fatalf("stale-build anchor points to %q want %q", actual, testName)
	}
	for _, witness := range powerlossoracle.CounterexampleWitnesses {
		if witness.ID == id {
			if witness.Package != "./TreeDB/db" || witness.TestName != testName {
				t.Fatalf("stale-build registry witness=(%s,%s) want=(./TreeDB/db,%s)", witness.Package, witness.TestName, testName)
			}
			return
		}
	}
	t.Fatalf("stale-build counterexample witness %q is absent from the code-owned registry", id)
}
