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
