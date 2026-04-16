package db

import "testing"

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
	if got, want := len(selected.GenerationIDs), 1; got != want || selected.GenerationIDs[0] != 3 {
		t.Fatalf("GenerationIDs=%v, want [3]", selected.GenerationIDs)
	}

	selected, err = SelectLeafGenerationPackCandidates(plan, LeafGenerationPackSelectOptions{MaxBytesToCopy: 150})
	if err != nil {
		t.Fatalf("MaxBytesToCopy: %v", err)
	}
	if got, want := len(selected.GenerationIDs), 1; got != want || selected.GenerationIDs[0] != 3 {
		t.Fatalf("GenerationIDs=%v, want [3]", selected.GenerationIDs)
	}
}

func TestSelectLeafGenerationPackCandidates_RejectsOversizeFirstCandidate(t *testing.T) {
	plan := LeafGenerationPlan{
		Admission: leafGenerationPlanAdmissionEligible,
		Candidates: []LeafGenerationPlanGeneration{
			{GenerationID: 7, BytesToCopy: 100},
		},
	}
	if _, err := SelectLeafGenerationPackCandidates(plan, LeafGenerationPackSelectOptions{MaxBytesToCopy: 50}); err == nil {
		t.Fatalf("expected oversize first candidate to fail")
	}
}

func TestSelectLeafGenerationPackCandidates_RejectsNonEligiblePlan(t *testing.T) {
	plan := LeafGenerationPlan{Admission: leafGenerationPlanAdmissionReclaimPerCopyTooLow}
	if _, err := SelectLeafGenerationPackCandidates(plan, LeafGenerationPackSelectOptions{}); err == nil {
		t.Fatalf("expected non-eligible plan to fail")
	}
}
