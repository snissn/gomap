package main

import (
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

func TestLocalHNSWAttributionConstructionReducerAndHardMissSamplingV1(t *testing.T) {
	digest := strings.Repeat("a", 64)
	ordinals := make([]int, 5001)
	for i := range ordinals {
		ordinals[i] = i
	}
	ordinals[2], ordinals[17] = ordinals[17], ordinals[2]
	ordinals[3], ordinals[300] = ordinals[300], ordinals[3]
	ordinals[4], ordinals[5000] = ordinals[5000], ordinals[4]
	evidence := collections.VectorPartitionConstructionEvidenceV1{Schema: "treedb_vector_partition_construction_evidence_v1", Variant: "native", ManifestChecksum: digest, IndexDefinitionDigest: digest, Partitions: []collections.VectorPartitionConstructionPartitionEvidenceV1{{NativeInsertionOrdinals: ordinals, Selections: []collections.VectorPartitionConstructionSelectionV1{{Selected: 2, DiversitySelected: 1, BackfillSelected: 1}}, Events: []collections.VectorPartitionConstructionEdgeEventV1{{From: 1, To: 0, InsertionOrdinal: 1, Origin: "diversity_selected", Action: "initial_add"}, {From: 2, To: 1, InsertionOrdinal: 17, Origin: "reciprocal_add", Action: "reciprocal_add"}, {From: 3, To: 2, InsertionOrdinal: 300, Origin: "reciprocal_add", Action: "reciprocal_prune_keep"}, {From: 4, To: 3, InsertionOrdinal: 5000, Origin: "reciprocal_add", Action: "reciprocal_prune_drop"}, {From: 1, To: 0, InsertionOrdinal: 1, Origin: "diversity_selected", Action: "final_survivor"}}}}}
	totals, err := localHNSWAttributionConstructionReduceV1(evidence)
	if err != nil || totals.OriginOrder != localHNSWAttributionConstructionOriginOrderV1 || totals.DiversitySelected != 1 || totals.BackfillSelected != 1 || totals.InitialAdded != 1 || totals.ReciprocalAdded != 1 || totals.PruneKept != 1 || totals.PruneDropped != 1 || totals.FinalSurvivors != 1 || totals.InitialAddByOrigin != [5]uint64{1, 0, 0, 0, 0} || totals.ReciprocalAddByOrigin != [5]uint64{0, 0, 1, 0, 0} || totals.InitialAddAgeByOrigin != [5][4]uint64{{1, 0, 0, 0}, {}, {}, {}, {}} || totals.InitialAddDeltaByOrigin != [5][4]uint64{{1, 0, 0, 0}, {}, {}, {}, {}} || totals.ReciprocalAddAgeByOrigin != [5][4]uint64{{}, {}, {0, 1, 0, 0}, {}, {}} || totals.ReciprocalAddDeltaByOrigin != [5][4]uint64{{}, {}, {0, 1, 0, 0}, {}, {}} || totals.PruneDropAgeByOrigin != [5][4]uint64{{}, {}, {0, 0, 0, 1}, {}, {}} || totals.PruneDropDeltaByOrigin != [5][4]uint64{{}, {}, {0, 0, 0, 1}, {}, {}} || totals.FinalAgeByOrigin != [5][4]uint64{{1, 0, 0, 0}, {}, {}, {}, {}} || totals.FinalDeltaByOrigin != [5][4]uint64{{1, 0, 0, 0}, {}, {}, {}, {}} {
		t.Fatalf("totals=%+v err=%v", totals, err)
	}
	evidence.Partitions[0].Events[0].Action = "wrong"
	if _, err := localHNSWAttributionConstructionReduceV1(evidence); err == nil {
		t.Fatal("malformed construction action accepted")
	}
	evidence.Partitions[0].Events[0].Action = "initial_add"
	evidence.Partitions[0].NativeInsertionOrdinals[0] = 1
	if _, err := localHNSWAttributionConstructionReduceV1(evidence); err == nil {
		t.Fatal("non-permutation insertion ordinals accepted")
	}
	evidence.Partitions[0].NativeInsertionOrdinals[0] = 0
	evidence.Variant = string(collections.VectorPartitionLocalGraphVariantNativeV1)
	evidence.Partitions[0].Events = append(evidence.Partitions[0].Events, collections.VectorPartitionConstructionEdgeEventV1{From: 5, To: 0, InsertionOrdinal: 5, Origin: "reciprocity_repair", Action: "reciprocity_repair_add"})
	if _, err := localHNSWAttributionConstructionReduceV1(evidence); err == nil {
		t.Fatal("native repair action accepted")
	}
	evidence.Variant = string(collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationV1)
	if _, err := localHNSWAttributionConstructionReduceV1(evidence); err != nil {
		t.Fatalf("auxiliary repair action rejected: %v", err)
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
	tied := []localHNSWAttributionHardMissV1{
		{Rank: "same", Variant: "overlay_current", QueryOrdinal: 9, QuerySHA256: strings.Repeat("c", 64), OverlapBits: 2},
		{Rank: "same", Variant: "native", QueryOrdinal: 7, QuerySHA256: strings.Repeat("b", 64), OverlapBits: 1},
		{Rank: "same", Variant: "native", QueryOrdinal: 3, QuerySHA256: strings.Repeat("d", 64), OverlapBits: 3},
	}
	tiedFirst := localHNSWAttributionHardMissesV1(append([]localHNSWAttributionHardMissV1(nil), tied...))
	tied[0], tied[2] = tied[2], tied[0]
	tiedSecond := localHNSWAttributionHardMissesV1(append([]localHNSWAttributionHardMissV1(nil), tied...))
	if !reflect.DeepEqual(tiedFirst, tiedSecond) || tiedFirst[0].Variant != "native" || tiedFirst[0].QueryOrdinal != 3 || tiedFirst[1].QueryOrdinal != 7 || tiedFirst[2].Variant != "overlay_current" {
		t.Fatalf("rank ties do not have a total deterministic order: %+v", tiedFirst)
	}
}

func TestLocalHNSWAttributionQueryUtilityReducerV1(t *testing.T) {
	trace := collections.VectorPartitionSearchAttributionV1{Schema: localHNSWAttributionSearchSchemaV1, FrontierAdmissions: 2, SeedCandidates: 1, SeedAdmissions: 1, SeedEvents: []collections.VectorPartitionSearchSeedEventV1{{Ordinal: 0, Score: 1, TopAdmission: true, FrontierAdmission: true}}, VisitedRows: 1, VisitedOrdinalsSHA256: strings.Repeat("a", 64), TerminationReason: "candidate_limit", VisitedOrdinals: []uint32{0}, EdgeEvents: []collections.VectorPartitionSearchEdgeEventV1{
		{Layer: 1, SourceOrdinal: 0, DestinationOrdinal: 1, Scored: true, StateImprovement: true}, // upper-layer score improves the greedy state
		{Layer: 0, SourceOrdinal: 0, DestinationOrdinal: 2, NewlyVisited: true, Scored: true, TopAdmission: true, FrontierAdmission: true, StateImprovement: true},
		{Layer: 0, SourceOrdinal: 2, DestinationOrdinal: 1}, // already visited truth must not recover it
	}}
	origins := map[localHNSWAttributionFinalEdgeKeyV1]string{{0, 1, 1}: "reciprocal_add", {0, 2, 0}: "diversity_selected", {2, 1, 0}: "nearest_backfill"}
	utility, err := localHNSWAttributionQueryUtilityReduceV1(collections.VectorPartitionSearchMetricsV1{Candidates: 2, Edges: 3}, trace, origins, []string{"seed", "truth-unscored", "truth-scored"}, map[string]struct{}{"seed": {}, "truth-unscored": {}, "truth-scored": {}})
	if err != nil || utility.Scored != 2 || utility.NewlyVisited != 1 || utility.StateImprovements != 2 || utility.TruthRecovered != 3 || utility.Reciprocal.Scored != 1 || utility.Reciprocal.TruthRecovered != 1 || utility.Reciprocal.StateImprovements != 1 || utility.Diversity.TruthRecovered != 1 || utility.Unattributed.TruthRecovered != 1 {
		t.Fatalf("utility=%+v err=%v", utility, err)
	}
	delete(origins, localHNSWAttributionFinalEdgeKeyV1{0, 1, 1})
	if _, err := localHNSWAttributionQueryUtilityReduceV1(collections.VectorPartitionSearchMetricsV1{Candidates: 2, Edges: 3}, trace, origins, []string{"seed", "truth-unscored", "truth-scored"}, map[string]struct{}{"truth-unscored": {}, "truth-scored": {}}); err == nil {
		t.Fatal("unmatched final native edge accepted")
	}
}

func TestLocalHNSWAttributionQueryUtilityReducerCreditsUpperLayerBeforeSeedV1(t *testing.T) {
	trace := collections.VectorPartitionSearchAttributionV1{
		Schema:                localHNSWAttributionSearchSchemaV1,
		FrontierAdmissions:    1,
		SeedCandidates:        1,
		SeedAdmissions:        1,
		SeedEvents:            []collections.VectorPartitionSearchSeedEventV1{{Ordinal: 1, Score: 1, TopAdmission: true, FrontierAdmission: true}},
		VisitedRows:           1,
		VisitedOrdinalsSHA256: strings.Repeat("a", 64),
		TerminationReason:     "candidate_limit",
		VisitedOrdinals:       []uint32{1},
		EdgeEvents:            []collections.VectorPartitionSearchEdgeEventV1{{Layer: 1, SourceOrdinal: 0, DestinationOrdinal: 1, Scored: true}},
	}
	origins := map[localHNSWAttributionFinalEdgeKeyV1]string{{0, 1, 1}: "reciprocal_add"}
	truth := map[string]struct{}{"truth": {}}
	utility, err := localHNSWAttributionQueryUtilityReduceV1(collections.VectorPartitionSearchMetricsV1{Candidates: 1, Edges: 1}, trace, origins, []string{"entry", "truth"}, truth)
	if err != nil {
		t.Fatal(err)
	}
	if utility.TruthRecovered != 1 || utility.Reciprocal.TruthRecovered != 1 || utility.Unattributed.TruthRecovered != 0 {
		t.Fatalf("upper-layer recovery was stolen by later seed: %+v", utility)
	}
	recoveries := localHNSWAttributionTruthRecoveriesV1(trace, origins, []string{"entry", "truth"}, truth)
	if recoveries["truth"] != "reciprocal_add" {
		t.Fatalf("truth recovery origin=%q want reciprocal_add", recoveries["truth"])
	}
}

func TestLocalHNSWAttributionInitialEntryTruthRecoveryPrecedesEdgeV1(t *testing.T) {
	trace := collections.VectorPartitionSearchAttributionV1{
		Schema:                localHNSWAttributionSearchSchemaV1,
		FrontierAdmissions:    1,
		SeedCandidates:        1,
		SeedAdmissions:        1,
		VisitedRows:           1,
		VisitedOrdinalsSHA256: strings.Repeat("a", 64),
		TerminationReason:     "candidate_limit",
		VisitedOrdinals:       []uint32{1},
		SeedEvents: []collections.VectorPartitionSearchSeedEventV1{
			{Ordinal: 1, Score: 1, InitialEntry: true},
			{Ordinal: 0, Score: 0, TopAdmission: true, FrontierAdmission: true},
		},
		EdgeEvents: []collections.VectorPartitionSearchEdgeEventV1{{Layer: 1, SourceOrdinal: 0, DestinationOrdinal: 1, Scored: true}},
	}
	origins := map[localHNSWAttributionFinalEdgeKeyV1]string{{0, 1, 1}: "reciprocal_add"}
	truth := map[string]struct{}{"truth": {}}
	utility, err := localHNSWAttributionQueryUtilityReduceV1(collections.VectorPartitionSearchMetricsV1{Candidates: 1, Edges: 1}, trace, origins, []string{"entry", "truth"}, truth)
	if err != nil || utility.TruthRecovered != 1 || utility.Unattributed.TruthRecovered != 1 || utility.Reciprocal.TruthRecovered != 0 {
		t.Fatalf("initial entry recovery order utility=%+v err=%v", utility, err)
	}
	if got := localHNSWAttributionTruthRecoveriesV1(trace, origins, []string{"entry", "truth"}, truth)["truth"]; got != "unattributed" {
		t.Fatalf("initial entry recovery origin=%q", got)
	}
}

func TestLocalHNSWAttributionQueryMergeDeduplicatesTruthV1(t *testing.T) {
	records := []localHNSWAttributionQuerySearchV1{
		{Utility: localHNSWAttributionQueryUtilityV1{TruthRecovered: 1, Diversity: localHNSWAttributionQueryOriginUtilityV1{TruthRecovered: 1}}, TruthRecoveries: []localHNSWAttributionTruthRecoveryV1{{ID: "same", Origin: "diversity_selected"}}},
		{Utility: localHNSWAttributionQueryUtilityV1{TruthRecovered: 1, Overlay: localHNSWAttributionQueryOriginUtilityV1{TruthRecovered: 1}}, TruthRecoveries: []localHNSWAttributionTruthRecoveryV1{{ID: "same", Origin: "overlay_rewrite"}}},
	}
	for i := range records {
		records[i] = localHNSWAttributionTestQueryRecordV1(records[i])
	}
	_, work, err := localHNSWAttributionQueryMergeV1(records, [][]m8CanonicalResultV1{{}, {}}, []uint32{0, 1}, map[string]struct{}{"same": {}})
	if err != nil {
		t.Fatal(err)
	}
	if work.Utility.TruthRecovered != 1 || work.Utility.Diversity.TruthRecovered != 1 || work.Utility.Overlay.TruthRecovered != 0 {
		t.Fatalf("duplicate routed truth recovery=%+v", work.Utility)
	}
}

func TestLocalHNSWAttributionQueryUtilityOverflowFailsClosedV1(t *testing.T) {
	utility := localHNSWAttributionQueryUtilityV1{ExaminedNative: math.MaxUint64}
	if err := localHNSWAttributionQueryUtilityAddV1(&utility, localHNSWAttributionQueryUtilityV1{ExaminedNative: 1}); err == nil {
		t.Fatal("utility overflow accepted")
	}
}
