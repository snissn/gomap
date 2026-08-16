package main

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

func TestLocalHNSWM18EdgeDiagnosisContractV1(t *testing.T) {
	if !localHNSWM18EdgeDiagnosisPointsV1([]int{80, 81, 88, 96}) {
		t.Fatal("accepted grid rejected")
	}
	if localHNSWM18EdgeDiagnosisPointsV1([]int{80, 88, 96}) || localHNSWM18EdgeDiagnosisPointsV1([]int{64, 80, 96, 128}) {
		t.Fatal("non-contract grid accepted")
	}
	misses := []localHNSWM18EdgeDiagnosisHardMissV1{{QueryOrdinal: 2, QuerySHA256: "b", Rank: "a"}, {QueryOrdinal: 1, QuerySHA256: "a", Rank: "a"}}
	got := localHNSWM18EdgeDiagnosisMissesV1(misses)
	if !slices.EqualFunc(got, []localHNSWM18EdgeDiagnosisHardMissV1{{QueryOrdinal: 1, QuerySHA256: "a", Rank: "a"}, {QueryOrdinal: 2, QuerySHA256: "b", Rank: "a"}}, func(a, b localHNSWM18EdgeDiagnosisHardMissV1) bool { return a == b }) {
		t.Fatalf("non-deterministic hard-miss order: %#v", got)
	}
}

func TestLocalHNSWM18EdgeDiagnosisHardMissSelectionIsOnlineAndBoundedV1(t *testing.T) {
	var cell localHNSWM18EdgeDiagnosisCellV1
	all := make([]localHNSWM18EdgeDiagnosisHardMissV1, 0, 40)
	for i := 39; i >= 0; i-- {
		digest := fmt.Sprintf("%064x", i+1)
		miss := localHNSWM18EdgeDiagnosisHardMissV1{QueryOrdinal: i, QuerySHA256: digest, Rank: fmt.Sprintf("%064x", i)}
		all = append(all, miss)
		localHNSWM18EdgeDiagnosisHardMissAddV1(&cell, miss, []localHNSWM18EdgeTraceV1{{QuerySHA: digest, Partition: 0}, {QuerySHA: digest, Partition: 1}})
		if len(cell.HardMisses) > 32 || len(cell.Traces) > 64 {
			t.Fatalf("unbounded online selection: misses=%d traces=%d", len(cell.HardMisses), len(cell.Traces))
		}
	}
	want := localHNSWM18EdgeDiagnosisMissesV1(all)
	if !slices.Equal(cell.HardMisses, want) {
		t.Fatalf("online misses differ from batch selection: got=%+v want=%+v", cell.HardMisses, want)
	}
	allowed := make(map[string]bool, len(want))
	for _, miss := range want {
		allowed[miss.QuerySHA256] = true
	}
	for _, trace := range cell.Traces {
		if !allowed[trace.QuerySHA] {
			t.Fatalf("retained trace outside selected misses: %+v", trace)
		}
	}
}

func TestLocalHNSWM18EdgeDiagnosisWorkOverflowV1(t *testing.T) {
	work := localHNSWAttributionQueryWorkV1{Candidates: math.MaxUint64}
	if err := localHNSWM18EdgeDiagnosisWorkAddV1(&work, 1, 0, 0, localHNSWAttributionQueryUtilityV1{}); err == nil {
		t.Fatal("candidate overflow accepted")
	}
}

func TestLocalHNSWM18EdgeTraceReplayRejectsTamperingV1(t *testing.T) {
	ids := []string{"entry", "truth"}
	truth := map[string]struct{}{"truth": {}}
	origins := map[localHNSWAttributionFinalEdgeKeyV1]string{{From: 0, To: 1, Layer: 0}: "diversity_selected"}
	edge := collections.VectorPartitionSearchEdgeEventV1{Layer: 0, SourceOrdinal: 0, DestinationOrdinal: 1, NewlyVisited: true, Scored: true, TopAdmission: true, FrontierAdmission: true, StateImprovement: true}
	seed := collections.VectorPartitionSearchSeedEventV1{Ordinal: 0, TopAdmission: true, FrontierAdmission: true, InitialEntry: true}
	visited := []uint32{0, 1}
	trace := collections.VectorPartitionSearchAttributionV1{Schema: localHNSWAttributionSearchSchemaV1, FrontierAdmissions: 2, SeedCandidates: 1, SeedAdmissions: 1, VisitedRows: 2, VisitedOrdinalsSHA256: localHNSWAttributionVisitedOrdinalsSHA256V1(visited), TerminationReason: "distance_bound", VisitedOrdinals: visited, EdgeEvents: []collections.VectorPartitionSearchEdgeEventV1{edge}, SeedEvents: []collections.VectorPartitionSearchSeedEventV1{seed}}
	metrics := collections.VectorPartitionSearchMetricsV1{Candidates: 2, Edges: 1, Route: collections.VectorPartitionSearchRouteHNSWSearchPackV1}
	utility, err := localHNSWAttributionQueryUtilityReduceV1(metrics, trace, origins, ids, truth)
	if err != nil {
		t.Fatal(err)
	}
	recoveries := localHNSWAttributionTruthRecoveryRecordsV1(localHNSWAttributionTruthRecoveriesV1(trace, origins, ids, truth))
	record := localHNSWAttributionQuerySearchV1{Results: []localHNSWAttributionQueryResultV1{{ID: "entry"}}, Candidates: 2, Edges: 1, FrontierAdmissions: 2, SeedCandidates: 1, SeedAdmissions: 1, TerminationReason: "distance_bound", VisitedOrdinalsSHA256: trace.VisitedOrdinalsSHA256, VisitedOrdinals: visited, Utility: utility, TruthRecoveries: recoveries}
	value := localHNSWM18EdgeTraceV1{Schema: localHNSWM18EdgeTraceSchemaV1, QuerySHA: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Partition: 0, Record: record, Edges: []collections.VectorPartitionSearchEdgeEventV1{edge}, Seeds: []collections.VectorPartitionSearchSeedEventV1{seed}}
	if err := localHNSWM18EdgeTraceValidateV1(value, ids, origins, truth); err != nil {
		t.Fatalf("valid replay rejected: %v", err)
	}
	tampered := value
	tampered.Edges = append([]collections.VectorPartitionSearchEdgeEventV1(nil), value.Edges...)
	tampered.Edges[0].StateImprovement = false
	if err := localHNSWM18EdgeTraceValidateV1(tampered, ids, origins, truth); err == nil {
		t.Fatal("edge utility tamper accepted")
	}
	tampered = value
	tampered.QuerySHA = "not-a-sha"
	if err := localHNSWM18EdgeTraceValidateV1(tampered, ids, origins, truth); err == nil {
		t.Fatal("query identity tamper accepted")
	}
	tampered = value
	tampered.Record.TruthRecoveries = nil
	if err := localHNSWM18EdgeTraceValidateV1(tampered, ids, origins, truth); err == nil {
		t.Fatal("truth recovery tamper accepted")
	}
}

func TestLocalHNSWM18EdgeCheckpointOriginsRoundTripV1(t *testing.T) {
	origins := make([]map[localHNSWAttributionFinalEdgeKeyV1]string, 40)
	for partition := range origins {
		origins[partition] = map[localHNSWAttributionFinalEdgeKeyV1]string{
			{Layer: 0, From: partition + 2, To: partition + 1}: "nearest_backfill",
			{Layer: 1, From: partition, To: partition + 3}:     "reciprocal_add",
		}
	}
	path := filepath.Join(t.TempDir(), "origins.bin")
	if err := localHNSWM18EdgeOriginsWriteV1(path, origins); err != nil {
		t.Fatal(err)
	}
	digest, err := localHNSWAttributionRegularFileSHA256V1(path, localHNSWM18EdgeOriginsMaxBytesV1)
	if err != nil {
		t.Fatal(err)
	}
	got, err := localHNSWM18EdgeOriginsReadV1(localHNSWAttributionFileInputV1{Path: path, SHA256: digest})
	if err != nil || !reflect.DeepEqual(got, origins) {
		t.Fatalf("round trip err=%v got=%v", err, got)
	}
	bad := make([]map[localHNSWAttributionFinalEdgeKeyV1]string, len(origins))
	copy(bad, origins)
	bad[0] = map[localHNSWAttributionFinalEdgeKeyV1]string{{Layer: 0, From: 0, To: 1}: "unknown"}
	if err := localHNSWM18EdgeOriginsWriteV1(filepath.Join(t.TempDir(), "bad.bin"), bad); err == nil {
		t.Fatal("accepted invalid final origin")
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{1}); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := localHNSWM18EdgeOriginsReadV1(localHNSWAttributionFileInputV1{Path: path, SHA256: digest}); err == nil {
		t.Fatal("accepted changed origin sidecar")
	}
}
