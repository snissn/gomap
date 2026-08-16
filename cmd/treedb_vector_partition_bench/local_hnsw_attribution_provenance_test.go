package main

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

func TestLocalHNSWAttributionConstructionReducerAndHardMissSamplingV1(t *testing.T) {
	digest := strings.Repeat("a", 64)
	evidence := collections.VectorPartitionConstructionEvidenceV1{Schema: "treedb_vector_partition_construction_evidence_v1", Variant: "native", ManifestChecksum: digest, IndexDefinitionDigest: digest, Partitions: []collections.VectorPartitionConstructionPartitionEvidenceV1{{Selections: []collections.VectorPartitionConstructionSelectionV1{{Selected: 2, DiversitySelected: 1, BackfillSelected: 1}}, Events: []collections.VectorPartitionConstructionEdgeEventV1{{InsertionOrdinal: 1, Action: "initial_add"}, {InsertionOrdinal: 17, Action: "reciprocal_add"}, {InsertionOrdinal: 300, Action: "reciprocal_prune_keep"}, {InsertionOrdinal: 5000, Action: "reciprocal_prune_drop"}, {InsertionOrdinal: 1, Action: "final_survivor"}}}}}
	totals, err := localHNSWAttributionConstructionReduceV1(evidence)
	if err != nil || totals.DiversitySelected != 1 || totals.BackfillSelected != 1 || totals.InitialAdded != 1 || totals.ReciprocalAdded != 1 || totals.PruneKept != 1 || totals.PruneDropped != 1 || totals.FinalSurvivors != 1 || totals.InsertionAge != [4]uint64{2, 1, 1, 1} {
		t.Fatalf("totals=%+v err=%v", totals, err)
	}
	evidence.Partitions[0].Events[0].Action = "wrong"
	if _, err := localHNSWAttributionConstructionReduceV1(evidence); err == nil {
		t.Fatal("malformed construction action accepted")
	}
	var misses []localHNSWAttributionHardMissV1
	for i := 0; i < 64; i++ {
		miss, ok := localHNSWAttributionHardMissV1Build(i, fmt.Sprintf("%064x", i), "native", .9)
		if !ok {
			t.Fatal("valid miss rejected")
		}
		misses = append(misses, miss)
	}
	first := localHNSWAttributionHardMissesV1(append([]localHNSWAttributionHardMissV1(nil), misses...))
	second := localHNSWAttributionHardMissesV1(append([]localHNSWAttributionHardMissV1(nil), misses...))
	if len(first) != 32 || !reflect.DeepEqual(first, second) {
		t.Fatalf("non-deterministic bounded misses: %d", len(first))
	}
}

func TestLocalHNSWAttributionQueryUtilityReducerV1(t *testing.T) {
	trace := collections.VectorPartitionSearchAttributionV1{Schema: localHNSWAttributionSearchSchemaV1, FrontierAdmissions: 2, SeedCandidates: 1, SeedAdmissions: 1, VisitedRows: 1, VisitedOrdinalsSHA256: strings.Repeat("a", 64), TerminationReason: "candidate_limit", VisitedOrdinals: []uint32{0}, EdgeEvents: []collections.VectorPartitionSearchEdgeEventV1{
		{Layer: 1, SourceOrdinal: 0, DestinationOrdinal: 1, Scored: true}, // upper-layer score, not a new candidate
		{Layer: 0, SourceOrdinal: 0, DestinationOrdinal: 2, NewlyVisited: true, Scored: true, TopAdmission: true, FrontierAdmission: true},
		{Layer: 0, SourceOrdinal: 2, DestinationOrdinal: 1}, // already visited truth must not recover it
	}}
	origins := map[localHNSWAttributionFinalEdgeKeyV1]string{{0, 1, 1}: "reciprocal_add", {0, 2, 0}: "diversity_selected", {2, 1, 0}: "nearest_backfill"}
	utility, err := localHNSWAttributionQueryUtilityReduceV1(collections.VectorPartitionSearchMetricsV1{Candidates: 2, Edges: 3}, trace, origins, []string{"seed", "truth-unscored", "truth-scored"}, map[string]struct{}{"truth-unscored": {}, "truth-scored": {}})
	if err != nil || utility.Scored != 2 || utility.NewlyVisited != 1 || utility.TruthRecovered != 1 || utility.Reciprocal.Scored != 1 || utility.Reciprocal.TruthRecovered != 0 || utility.Diversity.TruthRecovered != 1 || utility.Unattributed.Examined != 0 {
		t.Fatalf("utility=%+v err=%v", utility, err)
	}
	delete(origins, localHNSWAttributionFinalEdgeKeyV1{0, 1, 1})
	if _, err := localHNSWAttributionQueryUtilityReduceV1(collections.VectorPartitionSearchMetricsV1{Candidates: 2, Edges: 3}, trace, origins, []string{"seed", "truth-unscored", "truth-scored"}, map[string]struct{}{"truth-unscored": {}, "truth-scored": {}}); err == nil {
		t.Fatal("unmatched final native edge accepted")
	}
}
