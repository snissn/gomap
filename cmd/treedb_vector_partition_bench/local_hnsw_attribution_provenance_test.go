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
	evidence := collections.VectorPartitionConstructionEvidenceV1{Schema: collections.VectorPartitionConstructionEvidenceSchemaV1, Variant: "native", ManifestChecksum: digest, IndexDefinitionDigest: digest, Partitions: []collections.VectorPartitionConstructionPartitionEvidenceV1{{TraceMode: "detailed", NativeInsertionOrdinals: ordinals, Selections: []collections.VectorPartitionConstructionSelectionV1{{Selected: 2, DiversitySelected: 1, BackfillSelected: 1}}, PruneKeeps: 1, CompactLifecycle: collections.VectorPartitionConstructionCompactLifecycleV1{PruneKeep: [6]uint64{0, 0, 1, 0, 0, 0}}, Events: []collections.VectorPartitionConstructionEdgeEventV1{{From: 1, To: 0, InsertionOrdinal: 1, Origin: "diversity_selected", Action: "initial_add"}, {From: 2, To: 1, InsertionOrdinal: 17, Origin: "reciprocal_add", Action: "reciprocal_add"}, {From: 4, To: 3, InsertionOrdinal: 5000, Origin: "reciprocal_add", Action: "reciprocal_prune_drop"}, {From: 1, To: 0, InsertionOrdinal: 1, Origin: "diversity_selected", Action: "final_survivor"}}}}}
	totals, err := localHNSWAttributionConstructionReduceV1(evidence)
	if err != nil || totals.OriginOrder != localHNSWAttributionConstructionOriginOrderV1 || totals.DiversitySelected != 1 || totals.BackfillSelected != 1 || totals.InitialAdded != 1 || totals.ReciprocalAdded != 1 || totals.PruneKept != 1 || totals.PruneDropped != 1 || totals.FinalSurvivors != 1 || totals.InitialAddByOrigin != [6]uint64{1, 0, 0, 0, 0, 0} || totals.ReciprocalAddByOrigin != [6]uint64{0, 0, 1, 0, 0, 0} || totals.InitialAddAgeByOrigin != [6][4]uint64{{1, 0, 0, 0}, {}, {}, {}, {}, {}} || totals.InitialAddDeltaByOrigin != [6][4]uint64{{1, 0, 0, 0}, {}, {}, {}, {}, {}} || totals.ReciprocalAddAgeByOrigin != [6][4]uint64{{}, {}, {0, 1, 0, 0}, {}, {}, {}} || totals.ReciprocalAddDeltaByOrigin != [6][4]uint64{{}, {}, {0, 1, 0, 0}, {}, {}, {}} || totals.PruneDropAgeByOrigin != [6][4]uint64{{}, {}, {0, 0, 0, 1}, {}, {}, {}} || totals.PruneDropDeltaByOrigin != [6][4]uint64{{}, {}, {0, 0, 0, 1}, {}, {}, {}} || totals.FinalAgeByOrigin != [6][4]uint64{{1, 0, 0, 0}, {}, {}, {}, {}, {}} || totals.FinalDeltaByOrigin != [6][4]uint64{{1, 0, 0, 0}, {}, {}, {}, {}, {}} {
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

func TestLocalHNSWAttributionConstructionReducerQualityPostfillOriginV1(t *testing.T) {
	digest := strings.Repeat("a", 64)
	evidence := collections.VectorPartitionConstructionEvidenceV1{
		Schema:                collections.VectorPartitionConstructionEvidenceSchemaV1,
		Variant:               string(collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256Layer0Initial2MQualityPostfillV1),
		ManifestChecksum:      digest,
		IndexDefinitionDigest: digest,
		Partitions: []collections.VectorPartitionConstructionPartitionEvidenceV1{{
			TraceMode:               "detailed",
			NativeInsertionOrdinals: []int{0, 1},
			CompactLifecycle:        collections.VectorPartitionConstructionCompactLifecycleV1{QualityPostfillAdd: [6]uint64{0, 0, 0, 0, 0, 1}},
			PostfillEdges:           1,
			Events: []collections.VectorPartitionConstructionEdgeEventV1{
				{From: 1, To: 0, InsertionOrdinal: 1, Origin: "quality_postfill", Action: "quality_postfill_add"},
				{From: 1, To: 0, InsertionOrdinal: 1, Origin: "quality_postfill", Action: "final_survivor"},
			},
		}},
	}
	totals, err := localHNSWAttributionConstructionReduceV1(evidence)
	if err != nil || totals.QualityPostfillAdded != 1 || totals.VariantRewriteAdded != 0 || totals.FinalQualityPostfill != 1 || totals.QualityPostfillAddByOrigin != [6]uint64{0, 0, 0, 0, 0, 1} {
		t.Fatalf("quality postfill origin reduction totals=%+v err=%v", totals, err)
	}
	bad := evidence
	bad.Partitions = append([]collections.VectorPartitionConstructionPartitionEvidenceV1(nil), evidence.Partitions...)
	bad.Partitions[0].Events = append([]collections.VectorPartitionConstructionEdgeEventV1(nil), evidence.Partitions[0].Events...)
	bad.Partitions[0].Events[0].Origin = "nearest_backfill"
	if _, err := localHNSWAttributionConstructionReduceV1(bad); err == nil {
		t.Fatal("postfill action with nearest-backfill origin accepted")
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

func TestLocalHNSWAttributionQueryUtilityReducerCreditsQualityPostfillV1(t *testing.T) {
	trace := collections.VectorPartitionSearchAttributionV1{
		Schema:                localHNSWAttributionSearchSchemaV1,
		FrontierAdmissions:    1,
		VisitedRows:           1,
		VisitedOrdinalsSHA256: strings.Repeat("a", 64),
		VisitedOrdinals:       []uint32{1},
		TerminationReason:     "distance_bound",
		EdgeEvents: []collections.VectorPartitionSearchEdgeEventV1{{
			Layer:              0,
			SourceOrdinal:      0,
			DestinationOrdinal: 1,
			NewlyVisited:       true,
			Scored:             true,
			TopAdmission:       true,
			FrontierAdmission:  true,
		}},
	}
	origins := map[localHNSWAttributionFinalEdgeKeyV1]string{{From: 0, To: 1, Layer: 0}: "quality_postfill"}
	utility, err := localHNSWAttributionQueryUtilityReduceV1(collections.VectorPartitionSearchMetricsV1{Candidates: 1, Edges: 1}, trace, origins, []string{"source", "quality-truth"}, map[string]struct{}{"quality-truth": {}})
	if err != nil || utility.QualityPostfill.Examined != 1 || utility.QualityPostfill.TruthRecovered != 1 || !localHNSWAttributionQueryUtilityConservedV1(utility, 1) {
		t.Fatalf("quality-postfill utility=%+v err=%v", utility, err)
	}
	var work localHNSWAttributionQueryWorkV1
	if err := localHNSWM18EdgeDiagnosisWorkAddV1(&work, 1, 1, 1, utility); err != nil || work.Utility.QualityPostfill != utility.QualityPostfill || !localHNSWAttributionQueryUtilityConservedV1(work.Utility, work.Edges) {
		t.Fatalf("quality-postfill merged work=%+v err=%v", work, err)
	}
	tampered := utility
	tampered.QualityPostfill.Examined--
	if localHNSWAttributionQueryUtilityConservedV1(tampered, 1) {
		t.Fatal("tampered quality-postfill utility accepted")
	}
}

func TestLocalHNSWAttributionQueryUtilityReducerSeparatesAuxiliaryMetricsV1(t *testing.T) {
	metrics := collections.VectorPartitionSearchMetricsV1{Candidates: 2, Edges: 1, AuxiliaryEdges: 1}
	trace := collections.VectorPartitionSearchAttributionV1{Schema: localHNSWAttributionSearchSchemaV1, FrontierAdmissions: 2, VisitedRows: 2, VisitedOrdinalsSHA256: strings.Repeat("a", 64), VisitedOrdinals: []uint32{0, 1}, TerminationReason: "distance_bound", EdgeEvents: []collections.VectorPartitionSearchEdgeEventV1{
		{Layer: 0, SourceOrdinal: 0, DestinationOrdinal: 1, NewlyVisited: true, Scored: true, TopAdmission: true, FrontierAdmission: true},
		{Layer: 0, SourceOrdinal: 1, DestinationOrdinal: 0, Auxiliary: true, NewlyVisited: true, Scored: true, TopAdmission: true, FrontierAdmission: true},
	}}
	origins := map[localHNSWAttributionFinalEdgeKeyV1]string{{From: 0, To: 1, Layer: 0}: "diversity_selected"}
	utility, err := localHNSWAttributionQueryUtilityReduceV1(metrics, trace, origins, []string{"a", "b"}, map[string]struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	total, err := localHNSWAttributionMetricEdgesV1(metrics)
	if err != nil || total != 2 || utility.ExaminedNative != 1 || utility.ExaminedAuxiliary != 1 || utility.Diversity.Examined != 1 || utility.Auxiliary.Examined != 1 || !localHNSWAttributionQueryUtilityConservedV1(utility, total) {
		t.Fatalf("total=%d utility=%+v err=%v", total, utility, err)
	}
	overflow := metrics
	overflow.Edges = math.MaxUint64
	if _, err := localHNSWAttributionMetricEdgesV1(overflow); err == nil {
		t.Fatal("accepted overflowing native plus auxiliary edge metrics")
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
		{Edges: 1, Utility: localHNSWAttributionQueryUtilityV1{ExaminedNative: 1, Scored: 1, TruthRecovered: 1, Diversity: localHNSWAttributionQueryOriginUtilityV1{Examined: 1, Scored: 1, TruthRecovered: 1}}, TruthRecoveries: []localHNSWAttributionTruthRecoveryV1{{ID: "same", Origin: "diversity_selected"}}},
		{Edges: 1, Utility: localHNSWAttributionQueryUtilityV1{ExaminedNative: 1, Scored: 1, TruthRecovered: 1, Overlay: localHNSWAttributionQueryOriginUtilityV1{Examined: 1, Scored: 1, TruthRecovered: 1}}, TruthRecoveries: []localHNSWAttributionTruthRecoveryV1{{ID: "same", Origin: "overlay_rewrite"}}},
	}
	for i := range records {
		records[i] = localHNSWAttributionTestQueryRecordV1(records[i])
	}
	_, work, err := localHNSWAttributionQueryMergeV1(records, [][]m8CanonicalResultV1{{}, {}}, localHNSWAttributionTestQueryPartitionDocumentIDsV1(records), []uint32{0, 1}, map[string]struct{}{"same": {}})
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
