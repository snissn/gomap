package db_test

import (
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/powerlossoracle"
)

func TestPowerLossExternalCounterexampleWitnessRegistryAnchor(t *testing.T) {
	const id = "stale-build-base-saved-primary-v5"
	const testName = "TestPowerLossCertificationStaleBuildBaseSavedPrimaryV5PublicReopen"
	anchor := TestPowerLossCertificationStaleBuildBaseSavedPrimaryV5PublicReopen
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

func TestPowerLossExternalCapsuleCounterexampleWitnessRegistryAnchorV6(t *testing.T) {
	const id = "stale-build-base-primary-capsule-v6"
	const testName = "TestPowerLossCertificationStaleBuildBasePrimaryCapsulePublicReopenV6"
	anchor := TestPowerLossCertificationStaleBuildBasePrimaryCapsulePublicReopenV6
	qualified := runtime.FuncForPC(reflect.ValueOf(anchor).Pointer()).Name()
	if actual := qualified[strings.LastIndex(qualified, ".")+1:]; actual != testName {
		t.Fatalf("capsule anchor points to %q", actual)
	}
	for _, witness := range powerlossoracle.CounterexampleWitnesses {
		if witness.ID == id {
			if witness.Package != "./TreeDB/db" || witness.TestName != testName {
				t.Fatalf("capsule registry witness=%+v", witness)
			}
			return
		}
	}
	t.Fatalf("capsule counterexample witness %q is absent", id)
}
