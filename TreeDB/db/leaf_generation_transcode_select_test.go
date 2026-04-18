package db

import "testing"

func TestSelectLeafGenerationTranscodeCandidates(t *testing.T) {
	plan := LeafGenerationTranscodePlan{
		Admission: leafGenerationTranscodePlanAdmissionEligible,
		Candidates: []LeafGenerationTranscodePlanGeneration{
			{
				GenerationID:                  3,
				BytesTotal:                    120,
				BytesLive:                     100,
				BytesDead:                     20,
				BytesToCopy:                   100,
				LivePages:                     4,
				SamplePages:                   4,
				EstimatedBytesAfter:           72,
				ExpectedBytesSaved:            28,
				ExpectedSavedPerByteCopiedPPM: ratioPPM(28, 100),
			},
			{
				GenerationID:                  5,
				BytesTotal:                    240,
				BytesLive:                     180,
				BytesDead:                     60,
				BytesToCopy:                   180,
				LivePages:                     7,
				SamplePages:                   7,
				EstimatedBytesAfter:           120,
				ExpectedBytesSaved:            60,
				ExpectedSavedPerByteCopiedPPM: ratioPPM(60, 180),
			},
		},
	}
	selected, err := SelectLeafGenerationTranscodeCandidates(plan, LeafGenerationTranscodeSelectOptions{})
	if err != nil {
		t.Fatalf("SelectLeafGenerationTranscodeCandidates: %v", err)
	}
	if got, want := len(selected.GenerationIDs), 2; got != want {
		t.Fatalf("len(GenerationIDs)=%d, want %d", got, want)
	}
	if got, want := selected.BytesToCopy, int64(280); got != want {
		t.Fatalf("BytesToCopy=%d, want %d", got, want)
	}
	if got, want := selected.ExpectedBytesSaved, int64(88); got != want {
		t.Fatalf("ExpectedBytesSaved=%d, want %d", got, want)
	}
	if got, want := selected.ExpectedBytesSavedPerByteCopiedPPM, ratioPPM(88, 280); got != want {
		t.Fatalf("ExpectedBytesSavedPerByteCopiedPPM=%d, want %d", got, want)
	}

	selected, err = SelectLeafGenerationTranscodeCandidates(plan, LeafGenerationTranscodeSelectOptions{MaxGenerations: 1})
	if err != nil {
		t.Fatalf("MaxGenerations: %v", err)
	}
	if got, want := selected.GenerationIDs, []uint64{5}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("GenerationIDs=%v, want %v", got, want)
	}
}

func TestSelectLeafGenerationTranscodeCandidates_BoundedWindowPrefersHigherSavedSubset(t *testing.T) {
	plan := LeafGenerationTranscodePlan{
		Admission: leafGenerationTranscodePlanAdmissionEligible,
		Candidates: []LeafGenerationTranscodePlanGeneration{
			{GenerationID: 31, BytesLive: 5, BytesToCopy: 5, EstimatedBytesAfter: 1, ExpectedBytesSaved: 4},
			{GenerationID: 32, BytesLive: 30, BytesToCopy: 30, EstimatedBytesAfter: 6, ExpectedBytesSaved: 24},
			{GenerationID: 33, BytesLive: 30, BytesToCopy: 30, EstimatedBytesAfter: 6, ExpectedBytesSaved: 24},
		},
	}
	selected, err := SelectLeafGenerationTranscodeCandidates(plan, LeafGenerationTranscodeSelectOptions{
		MaxGenerations: 2,
		MaxBytesToCopy: 60,
	})
	if err != nil {
		t.Fatalf("SelectLeafGenerationTranscodeCandidates: %v", err)
	}
	if got, want := selected.GenerationIDs, []uint64{32, 33}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("GenerationIDs=%v, want %v", got, want)
	}
	if got, want := selected.ExpectedBytesSaved, int64(48); got != want {
		t.Fatalf("ExpectedBytesSaved=%d, want %d", got, want)
	}
	if got, want := selected.BytesToCopy, int64(60); got != want {
		t.Fatalf("BytesToCopy=%d, want %d", got, want)
	}
}
