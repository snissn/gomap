package caching

import (
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

func TestValueLogDictCandidateK_DefaultForcePointers(t *testing.T) {
	db := &DB{
		forceValueLogPointers: true,
		valueLogDictMaxK:      valuelog.MaxFrameK,
	}
	got := db.valueLogDictCandidateK()
	want := []int{8, 16, 32, 64, 96, 128}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("valueLogDictCandidateK force pointers: got=%v want=%v", got, want)
	}
}

func TestValueLogDictCandidateK_ExplicitOverridesDefault(t *testing.T) {
	db := &DB{
		forceValueLogPointers:         true,
		valueLogAutotuneCandidateKSet: true,
		valueLogAutotuneOptions: valuelog.AutotuneOptions{
			CandidateK: []int{2, 4, 8},
		},
	}
	got := db.valueLogDictCandidateK()
	want := []int{2, 4, 8}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("valueLogDictCandidateK explicit: got=%v want=%v", got, want)
	}
	// Ensure caller receives a copy and cannot mutate DB-owned options by alias.
	got[0] = 99
	if db.valueLogAutotuneOptions.CandidateK[0] != 2 {
		t.Fatalf("valueLogDictCandidateK returned aliased slice")
	}
}

func TestValueLogDictCandidateK_ForcePointersRemapsImplicitDefaultCandidateSet(t *testing.T) {
	db := &DB{
		forceValueLogPointers: true,
		valueLogAutotuneOptions: valuelog.AutotuneOptions{
			CandidateK: []int{1, 2, 4, 8, 16, 32},
		},
	}
	got := db.valueLogDictCandidateK()
	want := []int{8, 16, 32, 64, 96, 128}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("valueLogDictCandidateK remap default: got=%v want=%v", got, want)
	}
}

func TestValueLogDictCandidateK_ForcePointersKeepsExplicitDefaultCandidateSet(t *testing.T) {
	db := &DB{
		forceValueLogPointers:         true,
		valueLogAutotuneCandidateKSet: true,
		valueLogAutotuneOptions: valuelog.AutotuneOptions{
			CandidateK: []int{1, 2, 4, 8, 16, 32},
		},
	}
	got := db.valueLogDictCandidateK()
	want := []int{1, 2, 4, 8, 16, 32}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("valueLogDictCandidateK explicit default: got=%v want=%v", got, want)
	}
}

func TestValueLogDictTrainerIOCost_SizePolicyDisablesIOCostModel(t *testing.T) {
	db := &DB{
		valueLogAutoPolicy:      uint8(vlogAutoSize),
		valueLogAutotuneOptions: valuelog.AutotuneOptions{Mode: valuelog.AutotuneMedium},
	}
	db.valueLogAutotuneMetrics.seed(0.25, 7.5)
	if got := db.valueLogDictTrainerIOCost(); got != 0 {
		t.Fatalf("valueLogDictTrainerIOCost size policy: got=%f want=0", got)
	}
}

func TestValueLogDictTrainerIOCost_ThroughputPolicyUsesIOCostModel(t *testing.T) {
	db := &DB{
		valueLogAutoPolicy:      uint8(vlogAutoThroughput),
		valueLogAutotuneOptions: valuelog.AutotuneOptions{Mode: valuelog.AutotuneMedium},
	}
	db.valueLogAutotuneMetrics.seed(0.25, 7.5)
	if got := db.valueLogDictTrainerIOCost(); got <= 0 {
		t.Fatalf("valueLogDictTrainerIOCost throughput policy: got=%f want>0", got)
	}
}
