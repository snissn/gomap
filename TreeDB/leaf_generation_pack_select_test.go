package treedb

import "testing"

func TestSelectLeafGenerationPackCandidates_PublicHelper(t *testing.T) {
	plan := LeafGenerationPlan{
		Admission:              "eligible",
		CandidateGenerationIDs: []uint64{3, 5},
		Candidates: []LeafGenerationPlanGeneration{
			{GenerationID: 3, BytesTotal: 120, BytesLive: 100, BytesDead: 20, BytesToCopy: 100},
			{GenerationID: 5, BytesTotal: 240, BytesLive: 180, BytesDead: 60, BytesToCopy: 180},
		},
	}
	selected, err := SelectLeafGenerationPackCandidates(plan, LeafGenerationPackSelectOptions{MaxGenerations: 1})
	if err != nil {
		t.Fatalf("SelectLeafGenerationPackCandidates: %v", err)
	}
	if got, want := len(selected.GenerationIDs), 1; got != want || selected.GenerationIDs[0] != 3 {
		t.Fatalf("GenerationIDs=%v, want [3]", selected.GenerationIDs)
	}
	if got, want := selected.BytesToCopy, int64(100); got != want {
		t.Fatalf("BytesToCopy=%d, want %d", got, want)
	}
}
