package main

import (
	"os"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

func TestLocalHNSWAttributionQueryEvidenceV1(t *testing.T) {
	requireM8PersistentAssetSupportV1(t)
	vectors := make([][]float64, 16)
	for i := range vectors {
		vectors[i] = []float64{float64(i + 1), float64(i%3 + 1), 1}
	}
	source, err := newM8ProductionMultiGroupAssetsV1(vectors, []string{"a", "b"}, 4)
	if err != nil {
		t.Fatal(err)
	}
	sourceDir := source.dir
	source.owned = false
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(sourceDir)
	source, err = openM8ProductionExistingAssetSetV1(sourceDir)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	native, err := materializeRetainedLocalHNSWVariantV1(source, t.TempDir(), collections.VectorPartitionLocalGraphVariantNativeV1, 9985)
	if err != nil {
		t.Fatal(err)
	}
	defer native.Close()
	overlay, err := materializeRetainedLocalHNSWVariantV1(source, t.TempDir(), collections.VectorPartitionLocalGraphVariantOverlayCurrentV1, 9986)
	if err != nil {
		t.Fatal(err)
	}
	defer overlay.Close()
	query := []float32{1, 1, 1}
	var global []m8CanonicalResultV1
	for _, searcher := range native.searchers {
		results, _, err := searcher.SearchExactWithOptionsV1(t.Context(), query, collections.VectorPartitionSearchOptionsV1{TopK: 10, EfSearch: 128})
		if err != nil {
			t.Fatal(err)
		}
		for _, result := range results {
			global = append(global, m8CanonicalResultV1{ID: result.ID, Score: result.Score})
		}
	}
	global = m8CanonicalResultsV1(global, 10)
	evidence, err := localHNSWAttributionQueryEvidenceV1Build(t.Context(), source, native, overlay, 0, query, global)
	if err != nil {
		t.Fatal(err)
	}
	if evidence.Schema != localHNSWAttributionQuerySchemaV1 || len(evidence.QueryFP32SHA256) != 64 || len(evidence.GlobalTruth) != 10 || len(evidence.LowRoute) != 2 || len(evidence.HighRoute) != 4 || !localHNSWAttributionRoutePrefixV1(evidence.LowRoute, evidence.HighRoute) || !localHNSWAttributionRoutePermutationV1(evidence.HighRoute, 4) || len(evidence.Partitions) != 4 || evidence.RoutingRecall != evidence.Native.RoutingRecall || evidence.RoutingRecall != evidence.Overlay.RoutingRecall || evidence.Native.LowSelectedWork.Candidates == 0 || evidence.Native.LowSelectedWork.Edges == 0 || evidence.Overlay.LowSelectedWork.Candidates == 0 || evidence.Overlay.LowSelectedWork.Edges == 0 || !localHNSWAttributionQueryUtilityConservedV1(evidence.Native.LowSelectedWork.Utility, evidence.Native.LowSelectedWork.Edges) || !localHNSWAttributionQueryUtilityConservedV1(evidence.Overlay.HighSelectedWork.Utility, evidence.Overlay.HighSelectedWork.Edges) {
		t.Fatalf("evidence=%+v", evidence)
	}
	for partition, row := range evidence.Partitions {
		if row.Partition != uint32(partition) || len(row.Native.Results) == 0 || len(row.Overlay.Results) == 0 || row.Native.FrontierAdmissions == 0 || row.Overlay.FrontierAdmissions == 0 || len(row.Native.VisitedOrdinals) == 0 || len(row.Overlay.VisitedOrdinals) == 0 || !localHNSWAttributionSHA256V1(row.Native.VisitedOrdinalsSHA256) || !localHNSWAttributionSHA256V1(row.Overlay.VisitedOrdinalsSHA256) || row.Native.Utility.ExaminedNative+row.Native.Utility.ExaminedAuxiliary != row.Native.Edges || row.Overlay.Utility.ExaminedNative+row.Overlay.Utility.ExaminedAuxiliary != row.Overlay.Edges || row.Native.Utility.NewlyVisited > row.Native.Candidates || row.Overlay.Utility.NewlyVisited > row.Overlay.Candidates || row.Native.Utility.Scored < row.Native.Utility.NewlyVisited || row.Overlay.Utility.Scored < row.Overlay.Utility.NewlyVisited {
			t.Fatalf("partition=%+v", row)
		}
	}
	if _, err := localHNSWAttributionQueryEvidenceV1Build(t.Context(), source, native, overlay, 0, []float32{1, 1}, global); err == nil {
		t.Fatal("malformed query accepted")
	}
}

func TestLocalHNSWAttributionQueryMergePreservesOriginUtilityV1(t *testing.T) {
	records := []localHNSWAttributionQuerySearchV1{
		{Edges: 2, Utility: localHNSWAttributionQueryUtilityV1{ExaminedNative: 2, NewlyVisited: 1, Scored: 1, TopAdmissions: 1, FrontierAdmissions: 1, TruthRecovered: 1, Diversity: localHNSWAttributionQueryOriginUtilityV1{Examined: 2, NewlyVisited: 1, Scored: 1, TopAdmissions: 1, FrontierAdmissions: 1, TruthRecovered: 1}}},
		{Edges: 3, Utility: localHNSWAttributionQueryUtilityV1{ExaminedNative: 1, ExaminedAuxiliary: 2, NewlyVisited: 2, Scored: 3, TopAdmissions: 1, FrontierAdmissions: 1, TruthRecovered: 1, Reciprocal: localHNSWAttributionQueryOriginUtilityV1{Examined: 1, NewlyVisited: 1, Scored: 1}, Auxiliary: localHNSWAttributionQueryOriginUtilityV1{Examined: 2, NewlyVisited: 1, Scored: 2, TopAdmissions: 1, FrontierAdmissions: 1, TruthRecovered: 1}}},
	}
	_, work, err := localHNSWAttributionQueryMergeV1(records, make([][]m8CanonicalResultV1, len(records)), []uint32{0, 1})
	if err != nil {
		t.Fatal(err)
	}
	if work.Edges != 5 || work.Utility.TruthRecovered != 2 || work.Utility.Diversity.Examined != 2 || work.Utility.Reciprocal.Examined != 1 || work.Utility.Auxiliary.Examined != 2 || !localHNSWAttributionQueryUtilityConservedV1(work.Utility, work.Edges) {
		t.Fatalf("merged work=%+v", work)
	}
}
