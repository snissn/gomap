package db

import (
	"errors"
	"testing"
)

func TestSelectLeafGenerationPackCandidates(t *testing.T) {
	plan := LeafGenerationPlan{
		Admission:              leafGenerationPlanAdmissionEligible,
		CandidateGenerationIDs: []uint64{3, 5},
		Candidates: []LeafGenerationPlanGeneration{
			{GenerationID: 3, BytesTotal: 120, BytesLive: 100, BytesDead: 20, BytesToCopy: 100, LivePages: 4},
			{GenerationID: 5, BytesTotal: 240, BytesLive: 180, BytesDead: 60, BytesToCopy: 180, LivePages: 7},
		},
	}
	selected, err := SelectLeafGenerationPackCandidates(plan, LeafGenerationPackSelectOptions{})
	if err != nil {
		t.Fatalf("SelectLeafGenerationPackCandidates: %v", err)
	}
	if got, want := len(selected.GenerationIDs), 2; got != want {
		t.Fatalf("len(GenerationIDs)=%d, want %d", got, want)
	}
	if got, want := selected.BytesToCopy, int64(280); got != want {
		t.Fatalf("BytesToCopy=%d, want %d", got, want)
	}
	if got, want := selected.ExpectedReclaimBytes, int64(80); got != want {
		t.Fatalf("ExpectedReclaimBytes=%d, want %d", got, want)
	}
	if got, want := selected.ExpectedReclaimPerByteCopiedPPM, ratioPPM(80, 280); got != want {
		t.Fatalf("ExpectedReclaimPerByteCopiedPPM=%d, want %d", got, want)
	}

	selected, err = SelectLeafGenerationPackCandidates(plan, LeafGenerationPackSelectOptions{MaxGenerations: 1})
	if err != nil {
		t.Fatalf("MaxGenerations: %v", err)
	}
	if got, want := len(selected.GenerationIDs), 1; got != want || selected.GenerationIDs[0] != 5 {
		t.Fatalf("GenerationIDs=%v, want [5]", selected.GenerationIDs)
	}

	selected, err = SelectLeafGenerationPackCandidates(plan, LeafGenerationPackSelectOptions{MaxBytesToCopy: 150})
	if err != nil {
		t.Fatalf("MaxBytesToCopy: %v", err)
	}
	if got, want := len(selected.GenerationIDs), 1; got != want || selected.GenerationIDs[0] != 3 {
		t.Fatalf("GenerationIDs=%v, want [3]", selected.GenerationIDs)
	}
}

func TestSelectLeafGenerationPackCandidates_SkipsOversizeCandidateToFitLaterRankedGeneration(t *testing.T) {
	plan := LeafGenerationPlan{
		Admission: leafGenerationPlanAdmissionEligible,
		Candidates: []LeafGenerationPlanGeneration{
			{GenerationID: 7, BytesDead: 100, BytesLive: 100, BytesToCopy: 100},
			{GenerationID: 8, BytesDead: 30, BytesLive: 30, BytesToCopy: 30},
		},
	}
	selected, err := SelectLeafGenerationPackCandidates(plan, LeafGenerationPackSelectOptions{MaxBytesToCopy: 50})
	if err != nil {
		t.Fatalf("SelectLeafGenerationPackCandidates: %v", err)
	}
	if got, want := selected.GenerationIDs, []uint64{8}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("GenerationIDs=%v, want %v", got, want)
	}
}

func TestSelectLeafGenerationPackCandidates_StopsBeforeLowYieldCandidate(t *testing.T) {
	plan := LeafGenerationPlan{
		Admission: leafGenerationPlanAdmissionEligible,
		Candidates: []LeafGenerationPlanGeneration{
			{GenerationID: 11, BytesTotal: 100, BytesLive: 20, BytesDead: 80, BytesToCopy: 20},
			{GenerationID: 12, BytesTotal: 100, BytesLive: 95, BytesDead: 5, BytesToCopy: 95},
		},
	}
	selected, err := SelectLeafGenerationPackCandidates(plan, LeafGenerationPackSelectOptions{MinReclaimPerByteCopiedPPM: 800000})
	if err != nil {
		t.Fatalf("SelectLeafGenerationPackCandidates: %v", err)
	}
	if got, want := selected.GenerationIDs, []uint64{11}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("GenerationIDs=%v, want %v", got, want)
	}
	if got, want := selected.ExpectedReclaimPerByteCopiedPPM, ratioPPM(80, 20); got != want {
		t.Fatalf("ExpectedReclaimPerByteCopiedPPM=%d, want %d", got, want)
	}
}

func TestSelectLeafGenerationPackCandidates_BoundedWindowPrefersHigherReclaimSubset(t *testing.T) {
	plan := LeafGenerationPlan{
		Admission: leafGenerationPlanAdmissionEligible,
		Candidates: []LeafGenerationPlanGeneration{
			{GenerationID: 31, BytesDead: 50, BytesLive: 5, BytesToCopy: 5},
			{GenerationID: 32, BytesDead: 180, BytesLive: 30, BytesToCopy: 30},
			{GenerationID: 33, BytesDead: 180, BytesLive: 30, BytesToCopy: 30},
		},
	}
	selected, err := SelectLeafGenerationPackCandidates(plan, LeafGenerationPackSelectOptions{
		MaxGenerations: 2,
		MaxBytesToCopy: 60,
	})
	if err != nil {
		t.Fatalf("SelectLeafGenerationPackCandidates: %v", err)
	}
	if got, want := selected.GenerationIDs, []uint64{32, 33}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("GenerationIDs=%v, want %v", got, want)
	}
	if got, want := selected.ExpectedReclaimBytes, int64(360); got != want {
		t.Fatalf("ExpectedReclaimBytes=%d, want %d", got, want)
	}
	if got, want := selected.BytesToCopy, int64(60); got != want {
		t.Fatalf("BytesToCopy=%d, want %d", got, want)
	}
}

func TestSelectLeafGenerationPackCandidates_RejectsLowYieldPlanForSelection(t *testing.T) {
	plan := LeafGenerationPlan{
		Admission: leafGenerationPlanAdmissionEligible,
		Candidates: []LeafGenerationPlanGeneration{
			{GenerationID: 7, BytesDead: 5, BytesLive: 95, BytesToCopy: 95},
		},
	}
	if _, err := SelectLeafGenerationPackCandidates(plan, LeafGenerationPackSelectOptions{MinReclaimPerByteCopiedPPM: 100000}); err == nil {
		t.Fatalf("expected low-yield first candidate to fail selection")
	}
}

func TestSelectLeafGenerationPackCandidates_RejectsNonEligiblePlan(t *testing.T) {
	plan := LeafGenerationPlan{Admission: leafGenerationPlanAdmissionReclaimPerCopyTooLow}
	if _, err := SelectLeafGenerationPackCandidates(plan, LeafGenerationPackSelectOptions{}); err == nil {
		t.Fatalf("expected non-eligible plan to fail")
	}
}

func TestSelectLeafGenerationPackCandidates_PreservesOversizeDebtWhenRemainingCandidateLowYield(t *testing.T) {
	plan := LeafGenerationPlan{
		Admission: leafGenerationPlanAdmissionEligible,
		Candidates: []LeafGenerationPlanGeneration{
			{GenerationID: 21, BytesDead: 100, BytesLive: 100, BytesToCopy: 100},
			{GenerationID: 22, BytesDead: 5, BytesLive: 35, BytesToCopy: 35},
		},
	}
	_, err := SelectLeafGenerationPackCandidates(plan, LeafGenerationPackSelectOptions{
		MaxBytesToCopy:             50,
		MinReclaimPerByteCopiedPPM: 200000,
	})
	if err == nil {
		t.Fatal("expected oversize selection error")
	}
	if errors.Is(err, errLeafGenerationPackSelectionThreshold) {
		t.Fatalf("errors.Is(threshold)=true for mixed oversize/threshold rejection: %v", err)
	}
	if got := err.Error(); got != "leaf generation pack selection: no candidate generations fit max-bytes-to-copy=50" {
		t.Fatalf("error=%q, want oversize no-fit error", got)
	}
}

func TestSelectLeafGenerationPackCandidates_ThresholdErrorIsSentinelAndUnwrapsDetail(t *testing.T) {
	plan := LeafGenerationPlan{
		Admission: leafGenerationPlanAdmissionEligible,
		Candidates: []LeafGenerationPlanGeneration{
			{GenerationID: 7, BytesDead: 5, BytesLive: 95, BytesToCopy: 95},
		},
	}
	_, err := SelectLeafGenerationPackCandidates(plan, LeafGenerationPackSelectOptions{MinReclaimPerByteCopiedPPM: 100000})
	if err == nil {
		t.Fatal("expected low-yield selection error")
	}
	if !errors.Is(err, errLeafGenerationPackSelectionThreshold) {
		t.Fatalf("errors.Is(threshold)=false for %T %v", err, err)
	}
	if unwrapped := errors.Unwrap(err); unwrapped == nil || unwrapped == errLeafGenerationPackSelectionThreshold {
		t.Fatalf("Unwrap()=%v, want underlying detail error", unwrapped)
	}
	if got := err.Error(); got != "leaf generation pack selection: no candidate generations satisfy min-reclaim-per-byte-copied-ppm=100000" {
		t.Fatalf("error=%q, want detailed threshold error", got)
	}
}

func TestSelectLeafGenerationPackCandidates_OversizeLowYieldCandidateIsThresholdDebt(t *testing.T) {
	plan := LeafGenerationPlan{
		Admission: leafGenerationPlanAdmissionEligible,
		Candidates: []LeafGenerationPlanGeneration{
			{GenerationID: 40, BytesDead: 5, BytesLive: 95, BytesToCopy: 95},
		},
	}
	_, err := SelectLeafGenerationPackCandidates(plan, LeafGenerationPackSelectOptions{
		MaxBytesToCopy:             50,
		MinReclaimPerByteCopiedPPM: 100000,
	})
	if err == nil {
		t.Fatal("expected low-yield threshold error")
	}
	if !errors.Is(err, errLeafGenerationPackSelectionThreshold) {
		t.Fatalf("errors.Is(threshold)=false for oversize low-yield candidate: %v", err)
	}
}

func TestSelectLeafGenerationPackCandidates_BoundedLowYieldCombinationsRemainThresholdDebt(t *testing.T) {
	plan := LeafGenerationPlan{
		Admission: leafGenerationPlanAdmissionEligible,
		Candidates: []LeafGenerationPlanGeneration{
			{GenerationID: 41, BytesDead: 5, BytesLive: 45, BytesToCopy: 45},
			{GenerationID: 42, BytesDead: 5, BytesLive: 45, BytesToCopy: 45},
		},
	}
	_, err := SelectLeafGenerationPackCandidates(plan, LeafGenerationPackSelectOptions{
		MaxGenerations:             2,
		MaxBytesToCopy:             50,
		MinReclaimPerByteCopiedPPM: 500000,
	})
	if err == nil {
		t.Fatal("expected low-yield threshold error")
	}
	if !errors.Is(err, errLeafGenerationPackSelectionThreshold) {
		t.Fatalf("errors.Is(threshold)=false for bounded low-yield copy-bound combinations: %v", err)
	}
}

func TestSelectLeafGenerationPackCandidates_PrioritizesOversizeErrorWhenNothingFits(t *testing.T) {
	plan := LeafGenerationPlan{
		Admission: leafGenerationPlanAdmissionEligible,
		Candidates: []LeafGenerationPlanGeneration{
			{GenerationID: 13, BytesDead: 100, BytesLive: 100, BytesToCopy: 100},
			{GenerationID: 14, BytesDead: 90, BytesLive: 90, BytesToCopy: 90},
		},
	}
	_, err := SelectLeafGenerationPackCandidates(plan, LeafGenerationPackSelectOptions{
		MaxBytesToCopy:             50,
		MinReclaimPerByteCopiedPPM: 900000,
	})
	if err == nil {
		t.Fatal("expected oversize selection error")
	}
	if got := err.Error(); got != "leaf generation pack selection: no candidate generations fit max-bytes-to-copy=50" {
		t.Fatalf("error=%q, want oversize no-fit error", got)
	}
}

func TestSelectLeafGenerationPackCandidates_ForceBypassesReclaimThresholds(t *testing.T) {
	plan := LeafGenerationPlan{
		Admission: leafGenerationPlanAdmissionEligible,
		Candidates: []LeafGenerationPlanGeneration{
			{GenerationID: 17, BytesDead: 5, BytesLive: 95, BytesToCopy: 95},
		},
	}
	selected, err := SelectLeafGenerationPackCandidates(plan, LeafGenerationPackSelectOptions{
		Force:                      true,
		MinExpectedReclaimBytes:    100,
		MinExpectedReclaimRatioPPM: 900000,
		MinReclaimPerByteCopiedPPM: 900000,
	})
	if err != nil {
		t.Fatalf("SelectLeafGenerationPackCandidates: %v", err)
	}
	if got, want := selected.GenerationIDs, []uint64{17}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("GenerationIDs=%v, want %v", got, want)
	}
}

func TestSelectLeafGenerationPackCandidates_MaxBytesOnlyUsesGreedySelection(t *testing.T) {
	plan := LeafGenerationPlan{
		Admission: leafGenerationPlanAdmissionEligible,
		Candidates: []LeafGenerationPlanGeneration{
			{GenerationID: 21, BytesDead: 100, BytesLive: 10, BytesToCopy: 10},
			{GenerationID: 22, BytesDead: 300, BytesLive: 30, BytesToCopy: 30},
			{GenerationID: 23, BytesDead: 250, BytesLive: 20, BytesToCopy: 20},
		},
	}
	selected, err := SelectLeafGenerationPackCandidates(plan, LeafGenerationPackSelectOptions{
		MaxBytesToCopy: 30,
	})
	if err != nil {
		t.Fatalf("SelectLeafGenerationPackCandidates: %v", err)
	}
	if got, want := selected.GenerationIDs, []uint64{21, 23}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("GenerationIDs=%v, want %v", got, want)
	}
}
