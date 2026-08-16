package main

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"reflect"
	"strings"
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
	partitionDocumentIDs, err := localHNSWAttributionQueryPartitionDocumentIDsV1(source, native, overlay)
	if err != nil {
		t.Fatal(err)
	}
	if err := localHNSWAttributionQueryEvidenceValidateV1(evidence, partitionDocumentIDs); err != nil {
		t.Fatalf("valid query evidence rejected: %v", err)
	}
	if err := localHNSWAttributionQueryEvidenceScoresValidateV1(source, query, evidence, partitionDocumentIDs); err != nil {
		t.Fatalf("valid query scores rejected: %v", err)
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
	raw, err := json.Marshal(evidence)
	if err != nil {
		t.Fatal(err)
	}
	decodeEvidence := func() localHNSWAttributionQueryEvidenceV1 {
		var decoded localHNSWAttributionQueryEvidenceV1
		if err := json.Unmarshal(raw, &decoded); err != nil {
			t.Fatal(err)
		}
		return decoded
	}
	rebuildVariants := func(value *localHNSWAttributionQueryEvidenceV1, truth, exactLocal []m8CanonicalResultV1) error {
		nativeRecords := make([]localHNSWAttributionQuerySearchV1, len(value.Partitions))
		overlayRecords := make([]localHNSWAttributionQuerySearchV1, len(value.Partitions))
		nativeResults := make([][]m8CanonicalResultV1, len(value.Partitions))
		overlayResults := make([][]m8CanonicalResultV1, len(value.Partitions))
		for i, partition := range value.Partitions {
			var err error
			nativeRecords[i], overlayRecords[i] = partition.Native, partition.Overlay
			if nativeResults[i], err = localHNSWAttributionCanonicalQueryResultBitsV1(partition.Native.Results, false); err != nil {
				return err
			}
			if overlayResults[i], err = localHNSWAttributionCanonicalQueryResultBitsV1(partition.Overlay.Results, false); err != nil {
				return err
			}
		}
		var err error
		if value.Native, err = localHNSWAttributionQueryVariantEvidenceV1(truth, exactLocal, nativeRecords, nativeResults, partitionDocumentIDs, value.LowRoute, value.HighRoute); err != nil {
			return err
		}
		if value.Overlay, err = localHNSWAttributionQueryVariantEvidenceV1(truth, exactLocal, overlayRecords, overlayResults, partitionDocumentIDs, value.LowRoute, value.HighRoute); err != nil {
			return err
		}
		value.RoutingRecall = value.Native.RoutingRecall
		return nil
	}
	bad := decodeEvidence()
	bad.Partitions[0].Native.Results[0].ScoreBits ^= 1
	if err := localHNSWAttributionQueryEvidenceValidateV1(bad, partitionDocumentIDs); err == nil {
		t.Fatal("tampered partition result accepted")
	}
	bad = decodeEvidence()
	bad.HighRoute[0], bad.HighRoute[1] = bad.HighRoute[1], bad.HighRoute[0]
	if err := localHNSWAttributionQueryEvidenceValidateV1(bad, partitionDocumentIDs); err == nil {
		t.Fatal("noncanonical route accepted")
	}
	bad = decodeEvidence()
	bad.HighRoute[2], bad.HighRoute[3] = bad.HighRoute[3], bad.HighRoute[2]
	truth, err := localHNSWAttributionCanonicalQueryResultBitsV1(bad.GlobalTruth, true)
	if err != nil {
		t.Fatal(err)
	}
	exactLocal, err := localHNSWAttributionCanonicalQueryResultBitsV1(bad.NativeExactLocalTruth, false)
	if err != nil {
		t.Fatal(err)
	}
	nativeRecords := make([]localHNSWAttributionQuerySearchV1, len(bad.Partitions))
	overlayRecords := make([]localHNSWAttributionQuerySearchV1, len(bad.Partitions))
	nativeResults := make([][]m8CanonicalResultV1, len(bad.Partitions))
	overlayResults := make([][]m8CanonicalResultV1, len(bad.Partitions))
	for i, partition := range bad.Partitions {
		nativeRecords[i], overlayRecords[i] = partition.Native, partition.Overlay
		nativeResults[i], err = localHNSWAttributionCanonicalQueryResultBitsV1(partition.Native.Results, false)
		if err != nil {
			t.Fatal(err)
		}
		overlayResults[i], err = localHNSWAttributionCanonicalQueryResultBitsV1(partition.Overlay.Results, false)
		if err != nil {
			t.Fatal(err)
		}
	}
	bad.Native, err = localHNSWAttributionQueryVariantEvidenceV1(truth, exactLocal, nativeRecords, nativeResults, partitionDocumentIDs, bad.LowRoute, bad.HighRoute)
	if err != nil {
		t.Fatal(err)
	}
	bad.Overlay, err = localHNSWAttributionQueryVariantEvidenceV1(truth, exactLocal, overlayRecords, overlayResults, partitionDocumentIDs, bad.LowRoute, bad.HighRoute)
	if err != nil {
		t.Fatal(err)
	}
	bad.RoutingRecall = bad.Native.RoutingRecall
	if err := localHNSWAttributionQueryEvidenceValidateV1(bad, partitionDocumentIDs); err != nil {
		t.Fatalf("self-consistent alternate route rejected: %v", err)
	}
	if err := localHNSWAttributionCalibrationSummaryAddV1(t.Context(), &localHNSWAttributionCalibrationSummaryV1{}, bad, partitionDocumentIDs, source, native, overlay, query, global); err == nil {
		t.Fatal("decoded alternate router route accepted by calibration summary")
	}
	bad = decodeEvidence()
	bad.QueryFP32SHA256 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	if err := localHNSWAttributionQueryEvidenceValidateV1(bad, partitionDocumentIDs); err != nil {
		t.Fatalf("well-formed alternate query digest rejected before retained validation: %v", err)
	}
	if err := localHNSWAttributionCalibrationSummaryAddV1(t.Context(), &localHNSWAttributionCalibrationSummaryV1{}, bad, partitionDocumentIDs, source, native, overlay, query, global); err == nil {
		t.Fatal("decoded alternate query digest accepted by calibration summary")
	}
	bad = decodeEvidence()
	truth, err = localHNSWAttributionCanonicalQueryResultBitsV1(bad.GlobalTruth, true)
	if err != nil {
		t.Fatal(err)
	}
	alternativeLocal, err := localHNSWAttributionCanonicalQueryResultBitsV1(bad.Partitions[0].Native.Results, false)
	if err != nil || reflect.DeepEqual(alternativeLocal, truth) {
		t.Fatal("fixture lacks alternate exact-local truth")
	}
	bad.NativeExactLocalTruth = localHNSWAttributionQueryResultBitsV1(alternativeLocal)
	if err := rebuildVariants(&bad, truth, alternativeLocal); err != nil {
		t.Fatal(err)
	}
	if err := localHNSWAttributionQueryEvidenceValidateV1(bad, partitionDocumentIDs); err != nil {
		t.Fatalf("self-consistent alternate exact-local truth rejected: %v", err)
	}
	if err := localHNSWAttributionCalibrationSummaryAddV1(t.Context(), &localHNSWAttributionCalibrationSummaryV1{}, bad, partitionDocumentIDs, source, native, overlay, query, global); err == nil {
		t.Fatal("decoded alternate exact-local truth accepted by calibration summary")
	}
	bad = decodeEvidence()
	truth, err = localHNSWAttributionCanonicalQueryResultBitsV1(bad.GlobalTruth, true)
	if err != nil {
		t.Fatal(err)
	}
	truthIDs := localHNSWAttributionResultIDSetV1(truth)
	var replacement localHNSWAttributionQueryResultV1
	for _, partition := range bad.Partitions {
		for _, result := range partition.Native.Results {
			if _, exists := truthIDs[result.ID]; !exists {
				replacement = result
				break
			}
		}
		if replacement.ID != "" {
			break
		}
	}
	if replacement.ID == "" {
		t.Fatal("fixture lacks alternate global truth result")
	}
	alternativeTruth := append([]m8CanonicalResultV1(nil), truth[:len(truth)-1]...)
	alternativeTruth = append(alternativeTruth, m8CanonicalResultV1{ID: replacement.ID, Score: math.Float32frombits(replacement.ScoreBits)})
	alternativeTruth, err = localHNSWAttributionCanonicalResultsV1(alternativeTruth, true)
	if err != nil {
		t.Fatal(err)
	}
	bad.GlobalTruth = localHNSWAttributionQueryResultBitsV1(alternativeTruth)
	nativeRecords, _, err = localHNSWAttributionQueryVariantV1Build(t.Context(), native, query, localHNSWAttributionResultIDSetV1(alternativeTruth))
	if err != nil {
		t.Fatal(err)
	}
	overlayRecords, _, err = localHNSWAttributionQueryVariantV1Build(t.Context(), overlay, query, localHNSWAttributionResultIDSetV1(alternativeTruth))
	if err != nil {
		t.Fatal(err)
	}
	for i := range bad.Partitions {
		bad.Partitions[i].Native, bad.Partitions[i].Overlay = nativeRecords[i], overlayRecords[i]
	}
	exactLocal, err = localHNSWAttributionCanonicalQueryResultBitsV1(bad.NativeExactLocalTruth, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := rebuildVariants(&bad, alternativeTruth, exactLocal); err != nil {
		t.Fatal(err)
	}
	if err := localHNSWAttributionQueryEvidenceValidateV1(bad, partitionDocumentIDs); err != nil {
		t.Fatalf("self-consistent alternate global truth rejected: %v", err)
	}
	if err := localHNSWAttributionCalibrationSummaryAddV1(t.Context(), &localHNSWAttributionCalibrationSummaryV1{}, bad, partitionDocumentIDs, source, native, overlay, query, global); err == nil {
		t.Fatal("decoded alternate global truth accepted by calibration summary")
	}
	bad = decodeEvidence()
	bad.Partitions[0].Native.Results[0].ScoreBits ^= 1
	var decodedSummary localHNSWAttributionCalibrationSummaryV1
	if err := localHNSWAttributionCalibrationSummaryAddV1(t.Context(), &decodedSummary, bad, partitionDocumentIDs, source, native, overlay, query, global); err == nil {
		t.Fatal("decoded canonical result score accepted by calibration summary")
	}
	bad = decodeEvidence()
	bad.Partitions[0].Native.TerminationReason = "candidate_limit"
	if err := localHNSWAttributionQueryEvidenceValidateV1(bad, partitionDocumentIDs); err == nil {
		t.Fatal("decoded uncapped candidate-limit termination accepted")
	}
	bad = decodeEvidence()
	bad.PartitionRows[0]++
	if err := localHNSWAttributionQueryEvidenceValidateV1(bad, partitionDocumentIDs); err == nil {
		t.Fatal("self-declared partition row count accepted")
	}
	bad = decodeEvidence()
	bad.Partitions[0].Native.VisitedOrdinals[len(bad.Partitions[0].Native.VisitedOrdinals)-1] = math.MaxUint32
	bad.Partitions[0].Native.VisitedOrdinalsSHA256 = localHNSWAttributionVisitedOrdinalsSHA256V1(bad.Partitions[0].Native.VisitedOrdinals)
	if err := localHNSWAttributionQueryEvidenceValidateV1(bad, partitionDocumentIDs); err == nil {
		t.Fatal("out-of-range visited ordinal accepted")
	}
	bad = decodeEvidence()
	bad.Partitions[0].Native.Results[0].ID = "substituted-result-id"
	if err := localHNSWAttributionQueryEvidenceValidateV1(bad, partitionDocumentIDs); err == nil {
		t.Fatal("result ID outside authoritative pack accepted")
	}
	bad = decodeEvidence()
	bad.Partitions[0].Native.Results[0].ScoreBits ^= 1
	if err := localHNSWAttributionQueryEvidenceScoresValidateV1(source, query, bad, partitionDocumentIDs); err == nil {
		t.Fatal("tampered canonical result score accepted")
	}
	bad = decodeEvidence()
	if len(bad.Partitions[0].Native.VisitedOrdinals) <= len(bad.Partitions[0].Native.Results) {
		t.Fatal("fixture lacks a scored visited row outside returned top K")
	}
	bad.Partitions[0].Native.Results = bad.Partitions[0].Native.Results[:len(bad.Partitions[0].Native.Results)-1]
	if err := localHNSWAttributionCalibrationSummaryAddV1(t.Context(), &localHNSWAttributionCalibrationSummaryV1{}, bad, partitionDocumentIDs, source, native, overlay, query, global); err == nil {
		t.Fatal("decoded non-top-K visited result subset accepted")
	}
	bad = decodeEvidence()
	tamperedRecord := &bad.Partitions[0].Native
	if reflect.DeepEqual(tamperedRecord.Utility.Diversity, tamperedRecord.Utility.Backfill) {
		t.Fatal("fixture lacks distinct native origin buckets")
	}
	tamperedRecord.Utility.Diversity, tamperedRecord.Utility.Backfill = tamperedRecord.Utility.Backfill, tamperedRecord.Utility.Diversity
	for i := range tamperedRecord.TruthRecoveries {
		switch tamperedRecord.TruthRecoveries[i].Origin {
		case "diversity_selected":
			tamperedRecord.TruthRecoveries[i].Origin = "nearest_backfill"
		case "nearest_backfill":
			tamperedRecord.TruthRecoveries[i].Origin = "diversity_selected"
		}
	}
	truth, err = localHNSWAttributionCanonicalQueryResultBitsV1(bad.GlobalTruth, true)
	if err != nil {
		t.Fatal(err)
	}
	exactLocal, err = localHNSWAttributionCanonicalQueryResultBitsV1(bad.NativeExactLocalTruth, false)
	if err != nil {
		t.Fatal(err)
	}
	nativeRecords = make([]localHNSWAttributionQuerySearchV1, len(bad.Partitions))
	nativeResults = make([][]m8CanonicalResultV1, len(bad.Partitions))
	for i, partition := range bad.Partitions {
		nativeRecords[i] = partition.Native
		nativeResults[i], err = localHNSWAttributionCanonicalQueryResultBitsV1(partition.Native.Results, false)
		if err != nil {
			t.Fatal(err)
		}
	}
	bad.Native, err = localHNSWAttributionQueryVariantEvidenceV1(truth, exactLocal, nativeRecords, nativeResults, partitionDocumentIDs, bad.LowRoute, bad.HighRoute)
	if err != nil {
		t.Fatal(err)
	}
	if err := localHNSWAttributionQueryEvidenceValidateV1(bad, partitionDocumentIDs); err != nil {
		t.Fatalf("aggregate-only validation rejected cross-origin swap: %v", err)
	}
	if err := localHNSWAttributionCalibrationSummaryAddV1(t.Context(), &localHNSWAttributionCalibrationSummaryV1{}, bad, partitionDocumentIDs, source, native, overlay, query, global); err == nil {
		t.Fatal("decoded cross-origin utility swap accepted by attributed replay")
	}
}

func TestLocalHNSWAttributionQueryMergePreservesOriginUtilityV1(t *testing.T) {
	records := []localHNSWAttributionQuerySearchV1{
		{Edges: 2, Utility: localHNSWAttributionQueryUtilityV1{ExaminedNative: 2, NewlyVisited: 1, Scored: 1, TopAdmissions: 1, FrontierAdmissions: 1, TruthRecovered: 1, Diversity: localHNSWAttributionQueryOriginUtilityV1{Examined: 2, NewlyVisited: 1, Scored: 1, TopAdmissions: 1, FrontierAdmissions: 1, TruthRecovered: 1}}, TruthRecoveries: []localHNSWAttributionTruthRecoveryV1{{ID: "first", Origin: "diversity_selected"}}},
		{Edges: 3, Utility: localHNSWAttributionQueryUtilityV1{ExaminedNative: 1, ExaminedAuxiliary: 2, NewlyVisited: 2, Scored: 3, TopAdmissions: 1, FrontierAdmissions: 1, TruthRecovered: 1, Reciprocal: localHNSWAttributionQueryOriginUtilityV1{Examined: 1, NewlyVisited: 1, Scored: 1}, Auxiliary: localHNSWAttributionQueryOriginUtilityV1{Examined: 2, NewlyVisited: 1, Scored: 2, TopAdmissions: 1, FrontierAdmissions: 1, TruthRecovered: 1}}, TruthRecoveries: []localHNSWAttributionTruthRecoveryV1{{ID: "second", Origin: "auxiliary"}}},
	}
	for i := range records {
		records[i] = localHNSWAttributionTestQueryRecordV1(records[i])
	}
	_, work, err := localHNSWAttributionQueryMergeV1(records, make([][]m8CanonicalResultV1, len(records)), localHNSWAttributionTestQueryPartitionDocumentIDsV1(records), []uint32{0, 1}, map[string]struct{}{"first": {}, "second": {}})
	if err != nil {
		t.Fatal(err)
	}
	if work.Edges != 5 || work.Utility.TruthRecovered != 2 || work.Utility.Diversity.Examined != 2 || work.Utility.Reciprocal.Examined != 1 || work.Utility.Auxiliary.Examined != 2 || !localHNSWAttributionQueryUtilityConservedV1(work.Utility, work.Edges) {
		t.Fatalf("merged work=%+v", work)
	}
}

func TestLocalHNSWAttributionQueryUtilityAggregateDeduplicatesOverlapTruthV1(t *testing.T) {
	records := []localHNSWAttributionQuerySearchV1{
		{Edges: 1, Utility: localHNSWAttributionQueryUtilityV1{ExaminedNative: 1, Scored: 1, TruthRecovered: 1, Diversity: localHNSWAttributionQueryOriginUtilityV1{Examined: 1, Scored: 1, TruthRecovered: 1}}, TruthRecoveries: []localHNSWAttributionTruthRecoveryV1{{ID: "overlap", Origin: "diversity_selected"}}},
		{Edges: 1, Utility: localHNSWAttributionQueryUtilityV1{ExaminedNative: 1, Scored: 1, TruthRecovered: 1, Reciprocal: localHNSWAttributionQueryOriginUtilityV1{Examined: 1, Scored: 1, TruthRecovered: 1}}, TruthRecoveries: []localHNSWAttributionTruthRecoveryV1{{ID: "overlap", Origin: "reciprocal_add"}}},
	}
	for i := range records {
		records[i] = localHNSWAttributionTestQueryRecordV1(records[i])
	}
	utility, err := localHNSWAttributionQueryUtilityAggregateV1(records, localHNSWAttributionTestQueryPartitionDocumentIDsV1(records), map[string]struct{}{"overlap": {}})
	if err != nil {
		t.Fatal(err)
	}
	if utility.TruthRecovered != 1 || utility.Diversity.TruthRecovered != 1 || utility.Reciprocal.TruthRecovered != 0 {
		t.Fatalf("overlap truth was counted more than once: %+v", utility)
	}
}

func TestLocalHNSWAttributionDecodedTruthRecoveriesDeduplicateV1(t *testing.T) {
	records := []localHNSWAttributionQuerySearchV1{
		{Edges: 1, Utility: localHNSWAttributionQueryUtilityV1{ExaminedNative: 1, Scored: 1, TruthRecovered: 1, Diversity: localHNSWAttributionQueryOriginUtilityV1{Examined: 1, Scored: 1, TruthRecovered: 1}}, TruthRecoveries: []localHNSWAttributionTruthRecoveryV1{{ID: "overlap", Origin: "diversity_selected"}}},
		{Edges: 1, Utility: localHNSWAttributionQueryUtilityV1{ExaminedNative: 1, Scored: 1, TruthRecovered: 1, Reciprocal: localHNSWAttributionQueryOriginUtilityV1{Examined: 1, Scored: 1, TruthRecovered: 1}}, TruthRecoveries: []localHNSWAttributionTruthRecoveryV1{{ID: "overlap", Origin: "reciprocal_add"}}},
	}
	for i := range records {
		records[i] = localHNSWAttributionTestQueryRecordV1(records[i])
	}
	raw, err := json.Marshal(records)
	if err != nil {
		t.Fatal(err)
	}
	var decoded []localHNSWAttributionQuerySearchV1
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	utility, err := localHNSWAttributionQueryUtilityAggregateV1(decoded, localHNSWAttributionTestQueryPartitionDocumentIDsV1(decoded), map[string]struct{}{"overlap": {}})
	if err != nil || utility.TruthRecovered != 1 || utility.Diversity.TruthRecovered != 1 || utility.Reciprocal.TruthRecovered != 0 {
		t.Fatalf("decoded overlap truth was not deduplicated: utility=%+v err=%v", utility, err)
	}
	decoded[1].TruthRecoveries[0].Origin = "not_an_origin"
	if _, err := localHNSWAttributionQueryUtilityAggregateV1(decoded, localHNSWAttributionTestQueryPartitionDocumentIDsV1(decoded), map[string]struct{}{"overlap": {}}); err == nil {
		t.Fatal("malformed serialized truth recovery accepted")
	}
	decoded[1].TruthRecoveries[0].Origin = "reciprocal_add"
	decoded[0].TruthRecoveries[0].ID = "not_query_truth"
	if _, err := localHNSWAttributionQueryUtilityAggregateV1(decoded, localHNSWAttributionTestQueryPartitionDocumentIDsV1(decoded), map[string]struct{}{"overlap": {}}); err == nil {
		t.Fatal("non-truth serialized recovery accepted")
	}
	decoded[0].TruthRecoveries[0].ID = "overlap"
	decoded[0].VisitedOrdinals[0]++
	if _, err := localHNSWAttributionQueryUtilityAggregateV1(decoded, localHNSWAttributionTestQueryPartitionDocumentIDsV1(decoded), map[string]struct{}{"overlap": {}}); err == nil {
		t.Fatal("tampered visited ordinal digest accepted")
	}
	decoded[0].VisitedOrdinals[0]--
	decoded[0].Candidates++
	if _, err := localHNSWAttributionQueryUtilityAggregateV1(decoded, localHNSWAttributionTestQueryPartitionDocumentIDsV1(decoded), map[string]struct{}{"overlap": {}}); err == nil {
		t.Fatal("mismatched persisted candidate count accepted")
	}
	decoded[0].Candidates--
	decoded[0].Utility.Scored++
	if _, err := localHNSWAttributionQueryUtilityAggregateV1(decoded, localHNSWAttributionTestQueryPartitionDocumentIDsV1(decoded), map[string]struct{}{"overlap": {}}); err == nil {
		t.Fatal("non-conserved persisted utility accepted")
	}
	valid := localHNSWAttributionTestQueryRecordV1(localHNSWAttributionQuerySearchV1{Edges: 1, Utility: localHNSWAttributionQueryUtilityV1{ExaminedNative: 1, Diversity: localHNSWAttributionQueryOriginUtilityV1{Examined: 1}}})
	if _, err := localHNSWAttributionQueryUtilityAggregateV1([]localHNSWAttributionQuerySearchV1{valid}, localHNSWAttributionTestQueryPartitionDocumentIDsV1([]localHNSWAttributionQuerySearchV1{valid}), map[string]struct{}{"overlap": {}}); err != nil {
		t.Fatalf("valid native utility rejected: %v", err)
	}
	valid.Utility.ExaminedNative, valid.Utility.ExaminedAuxiliary = 0, 1
	if _, err := localHNSWAttributionQueryUtilityAggregateV1([]localHNSWAttributionQuerySearchV1{valid}, localHNSWAttributionTestQueryPartitionDocumentIDsV1([]localHNSWAttributionQuerySearchV1{valid}), map[string]struct{}{"overlap": {}}); err == nil {
		t.Fatal("swapped native/auxiliary examined aggregate accepted")
	}
	valid.Edges = 2
	valid.Utility.ExaminedNative, valid.Utility.ExaminedAuxiliary = 2, 0
	valid.Utility.Backfill.Examined = 1
	valid.Utility.Diversity.Scored = 2
	valid.Utility.Scored = 2
	if _, err := localHNSWAttributionQueryUtilityAggregateV1([]localHNSWAttributionQuerySearchV1{valid}, localHNSWAttributionTestQueryPartitionDocumentIDsV1([]localHNSWAttributionQuerySearchV1{valid}), map[string]struct{}{"overlap": {}}); err == nil {
		t.Fatal("scored work beyond examined edge accepted")
	}
	truthRecord := localHNSWAttributionTestQueryRecordV1(localHNSWAttributionQuerySearchV1{Edges: 1, Utility: localHNSWAttributionQueryUtilityV1{ExaminedNative: 1, Scored: 1, TruthRecovered: 1, Diversity: localHNSWAttributionQueryOriginUtilityV1{Examined: 1, Scored: 1, TruthRecovered: 1}}, TruthRecoveries: []localHNSWAttributionTruthRecoveryV1{{ID: "overlap", Origin: "diversity_selected"}}})
	if _, err := localHNSWAttributionQueryUtilityAggregateV1([]localHNSWAttributionQuerySearchV1{truthRecord}, localHNSWAttributionTestQueryPartitionDocumentIDsV1([]localHNSWAttributionQuerySearchV1{truthRecord}), map[string]struct{}{"overlap": {}}); err != nil {
		t.Fatalf("valid edge truth recovery rejected: %v", err)
	}
	truthRecord.Utility.Scored = 0
	truthRecord.Utility.Diversity.Scored = 0
	if _, err := localHNSWAttributionQueryUtilityAggregateV1([]localHNSWAttributionQuerySearchV1{truthRecord}, localHNSWAttributionTestQueryPartitionDocumentIDsV1([]localHNSWAttributionQuerySearchV1{truthRecord}), map[string]struct{}{"overlap": {}}); err == nil {
		t.Fatal("edge-origin truth recovery without scored edge accepted")
	}
}

func TestLocalHNSWAttributionQueryRecordBindsSeedsAndTerminationV1(t *testing.T) {
	record := localHNSWAttributionTestQueryRecordV1(localHNSWAttributionQuerySearchV1{
		Candidates:         1,
		Edges:              1,
		FrontierAdmissions: 1,
		SeedCandidates:     0,
		SeedAdmissions:     0,
		TerminationReason:  "distance_bound",
		Utility: localHNSWAttributionQueryUtilityV1{
			ExaminedNative:     1,
			NewlyVisited:       1,
			Scored:             1,
			TopAdmissions:      1,
			FrontierAdmissions: 1,
			Diversity: localHNSWAttributionQueryOriginUtilityV1{
				Examined: 1, NewlyVisited: 1, Scored: 1, TopAdmissions: 1, FrontierAdmissions: 1,
			},
		},
	})
	if _, err := localHNSWAttributionQueryRecordValidateV1(record, localHNSWAttributionTestQueryRecordDocumentIDsV1(record), map[string]struct{}{"truth": {}}); err != nil {
		t.Fatalf("valid seeded record rejected: %v", err)
	}
	raw, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &record); err != nil {
		t.Fatal(err)
	}
	unvisited := record
	unvisited.Results = append([]localHNSWAttributionQueryResultV1(nil), record.Results...)
	unvisited.Results[0].ID = "unvisited"
	if _, err := localHNSWAttributionQueryRecordValidateV1(unvisited, []string{"result", "visited", "unvisited"}, map[string]struct{}{"truth": {}}); err == nil {
		t.Fatal("decoded result mapped to an unvisited partition ordinal accepted")
	}
	zeroWork := record
	zeroWork.Candidates, zeroWork.Edges, zeroWork.FrontierAdmissions = 0, 0, 0
	zeroWork.SeedCandidates, zeroWork.SeedAdmissions = 0, 0
	zeroWork.VisitedOrdinals = nil
	zeroWork.VisitedOrdinalsSHA256 = localHNSWAttributionVisitedOrdinalsSHA256V1(nil)
	zeroWork.Utility = localHNSWAttributionQueryUtilityV1{}
	if _, err := localHNSWAttributionQueryRecordValidateV1(zeroWork, localHNSWAttributionTestQueryRecordDocumentIDsV1(record), map[string]struct{}{"truth": {}}); err == nil {
		t.Fatal("decoded zero-work record accepted")
	}
	record.SeedAdmissions++
	if _, err := localHNSWAttributionQueryRecordValidateV1(record, localHNSWAttributionTestQueryRecordDocumentIDsV1(record), map[string]struct{}{"truth": {}}); err == nil {
		t.Fatal("tampered seed admissions accepted")
	}
	record.SeedAdmissions--
	record.SeedAdmissions = record.SeedCandidates + 1
	record.FrontierAdmissions = record.Utility.FrontierAdmissions + record.SeedAdmissions
	if _, err := localHNSWAttributionQueryRecordValidateV1(record, localHNSWAttributionTestQueryRecordDocumentIDsV1(record), map[string]struct{}{"truth": {}}); err == nil {
		t.Fatal("seed admissions beyond seed candidates accepted")
	}
	record.SeedAdmissions = 0
	record.FrontierAdmissions = record.Utility.FrontierAdmissions
	record.SeedCandidates++
	if _, err := localHNSWAttributionQueryRecordValidateV1(record, localHNSWAttributionTestQueryRecordDocumentIDsV1(record), map[string]struct{}{"truth": {}}); err == nil {
		t.Fatal("tampered seed candidates accepted")
	}
	record.SeedCandidates--
	record.FrontierAdmissions++
	if _, err := localHNSWAttributionQueryRecordValidateV1(record, localHNSWAttributionTestQueryRecordDocumentIDsV1(record), map[string]struct{}{"truth": {}}); err == nil {
		t.Fatal("tampered frontier admissions accepted")
	}
	record.FrontierAdmissions--
	record.TerminationReason = "candidate_limit"
	if _, err := localHNSWAttributionQueryRecordValidateV1(record, localHNSWAttributionTestQueryRecordDocumentIDsV1(record), map[string]struct{}{"truth": {}}); err == nil {
		t.Fatal("uncapped candidate-limit termination accepted")
	}
	record.TerminationReason = "invalid"
	if _, err := localHNSWAttributionQueryRecordValidateV1(record, localHNSWAttributionTestQueryRecordDocumentIDsV1(record), map[string]struct{}{"truth": {}}); err == nil {
		t.Fatal("invalid termination accepted")
	}
}

func TestLocalHNSWAttributionQueryUtilityConservationRejectsOriginOverflowV1(t *testing.T) {
	record := localHNSWAttributionTestQueryRecordV1(localHNSWAttributionQuerySearchV1{
		Utility: localHNSWAttributionQueryUtilityV1{
			Diversity: localHNSWAttributionQueryOriginUtilityV1{Examined: math.MaxUint64},
			Backfill:  localHNSWAttributionQueryOriginUtilityV1{Examined: 1},
		},
	})
	if _, err := localHNSWAttributionQueryRecordValidateV1(record, localHNSWAttributionTestQueryRecordDocumentIDsV1(record), map[string]struct{}{"truth": {}}); err == nil {
		t.Fatal("overflowing origin conservation accepted")
	}
}

func localHNSWAttributionTestQueryRecordV1(record localHNSWAttributionQuerySearchV1) localHNSWAttributionQuerySearchV1 {
	record.SeedCandidates = 1
	record.Candidates = record.Utility.NewlyVisited + record.SeedCandidates
	record.SeedAdmissions = 0
	record.FrontierAdmissions = record.Utility.FrontierAdmissions
	if record.FrontierAdmissions == 0 {
		record.SeedAdmissions = 1
		record.FrontierAdmissions = 1
	}
	if record.TerminationReason == "" {
		record.TerminationReason = "distance_bound"
	}
	record.VisitedOrdinals = make([]uint32, record.Candidates)
	for i := range record.VisitedOrdinals {
		record.VisitedOrdinals[i] = uint32(i)
	}
	record.VisitedOrdinalsSHA256 = localHNSWAttributionVisitedOrdinalsSHA256V1(record.VisitedOrdinals)
	if len(record.Results) == 0 {
		record.Results = []localHNSWAttributionQueryResultV1{{ID: "result", ScoreBits: 0}}
	}
	return record
}

func localHNSWAttributionTestQueryRecordDocumentIDsV1(record localHNSWAttributionQuerySearchV1) []string {
	ids := make([]string, len(record.VisitedOrdinals))
	for i := range ids {
		ids[i] = fmt.Sprintf("row-%d", i)
	}
	for i, result := range record.Results {
		if i >= len(ids) {
			return nil
		}
		ids[i] = result.ID
	}
	return ids
}

func localHNSWAttributionTestQueryPartitionDocumentIDsV1(records []localHNSWAttributionQuerySearchV1) [][]string {
	ids := make([][]string, len(records))
	for i, record := range records {
		ids[i] = localHNSWAttributionTestQueryRecordDocumentIDsV1(record)
	}
	return ids
}

func TestLocalHNSWAttributionTruthRecoverySerializationAndUnderflowV1(t *testing.T) {
	records := localHNSWAttributionTruthRecoveryRecordsV1(map[string]string{"z": "auxiliary", "a": "diversity_selected"})
	if want := []localHNSWAttributionTruthRecoveryV1{{ID: "a", Origin: "diversity_selected"}, {ID: "z", Origin: "auxiliary"}}; !reflect.DeepEqual(records, want) {
		t.Fatalf("truth recovery records=%+v want=%+v", records, want)
	}
	encoded, err := json.Marshal(localHNSWAttributionQuerySearchV1{TruthRecoveries: records})
	again, againErr := json.Marshal(localHNSWAttributionQuerySearchV1{TruthRecoveries: records})
	if err != nil || againErr != nil || string(encoded) != string(again) || !strings.Contains(string(encoded), `"truth_recoveries":[{"id":"a","origin":"diversity_selected"},{"id":"z","origin":"auxiliary"}]`) {
		t.Fatalf("truth recovery JSON err=%v json=%s", err, encoded)
	}
	utility := localHNSWAttributionQueryUtilityV1{TruthRecovered: 1}
	if err := localHNSWAttributionQueryUtilityRemoveTruthRecoveryV1(&utility, "diversity_selected"); err == nil || utility.TruthRecovered != 1 {
		t.Fatalf("truth recovery underflow accepted or mutated utility=%+v err=%v", utility, err)
	}
}
