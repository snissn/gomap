package documentservice

import (
	"context"
	"encoding/json"
	"errors"
	"runtime"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/workstats"
)

func TestWorkStatsServiceScanAttribution(t *testing.T) {
	svc, db := newTestService(t)
	defer db.Close()
	defer svc.Close()
	ctx := context.Background()
	if _, err := svc.CreateIndex(ctx, CreateIndexRequest{Name: "scans", Dimension: 2}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.UpsertDocuments(ctx, "scans", UpsertDocumentsRequest{Documents: []Document{{ID: "a", Content: "alpha", Embedding: []float32{1, 0}}, {ID: "b", Content: "beta", Embedding: []float32{0, 1}}}}); err != nil {
		t.Fatal(err)
	}
	filter := &Filter{Field: "content", Operator: "==", Value: "alpha"}
	before := workstats.Read()
	if out, err := svc.SearchDenseVector(ctx, "scans", DenseVectorSearchRequest{QueryEmbedding: []float32{1, 0}, TopK: 1, Route: RouteExact}); err != nil || len(out.Documents) != 1 {
		t.Fatalf("exact=%+v %v", out, err)
	}
	if out, err := svc.CountDocuments(ctx, "scans", CountDocumentsRequest{Filter: filter}); err != nil || out.Count != 1 {
		t.Fatalf("count=%+v %v", out, err)
	}
	if out, err := svc.CountDocuments(ctx, "scans", CountDocumentsRequest{}); err != nil || out.Count != 2 {
		t.Fatalf("IDs=%+v %v", out, err)
	}
	if out, err := svc.FilterDocuments(ctx, "scans", FilterDocumentsRequest{Filter: filter}); err != nil || len(out.Documents) != 1 {
		t.Fatalf("filter=%+v %v", out, err)
	}
	if out, err := svc.FilterDocuments(ctx, "scans", FilterDocumentsRequest{CursorPage: true, Limit: 1}); err != nil || len(out.Documents) != 1 {
		t.Fatalf("cursor=%+v %v", out, err)
	}
	if out, err := svc.DeleteDocuments(ctx, "scans", DeleteDocumentsRequest{Filter: filter}); err != nil || out.Deleted != 1 {
		t.Fatalf("delete=%+v %v", out, err)
	}
	after := workstats.Read()
	for _, pair := range [][2]workstats.ScanStats{{before.Scans.DenseExact, after.Scans.DenseExact}, {before.Scans.FilteredCount, after.Scans.FilteredCount}, {before.Scans.CountIDs, after.Scans.CountIDs}, {before.Scans.FilteredRetrieval, after.Scans.FilteredRetrieval}, {before.Scans.Cursor, after.Scans.Cursor}, {before.Scans.MutationMatch, after.Scans.MutationMatch}} {
		if pair[1].Starts-pair[0].Starts != 1 || pair[1].Rows-pair[0].Rows != 2 {
			t.Fatalf("scan before=%+v after=%+v", pair[0], pair[1])
		}
	}
	// Failed work remains visible, and invalid requests do not claim dispatch.
	col, _, err := svc.openIndex(ctx, "scans", 0)
	if err != nil {
		t.Fatal(err)
	}
	injected := errors.New("scan callback failed")
	if err := svc.scanDocuments(ctx, col, &workstats.Scans.FilteredRetrieval, func(Document) error { return injected }); !errors.Is(err, injected) {
		t.Fatal(err)
	}
	failed := workstats.Read()
	if failed.Scans.FilteredRetrieval.Rows-after.Scans.FilteredRetrieval.Rows != 1 {
		t.Fatal("failed prefix was not observed")
	}
}

func TestWorkStatsDiagnosticsLifetimeAndAvailability(t *testing.T) {
	svc, db := newTestService(t)
	defer db.Close()
	defer svc.Close()
	before := svc.DiagnosticsSnapshot(nil).Work
	other := New(collections.NewCollectionManager(db))
	defer other.Close()
	after := other.DiagnosticsSnapshot(nil).Work
	if before.PID != after.PID || before.OriginUnixNano != after.OriginUnixNano || before.Scope != "process" || before.OriginKind != "go_package_init" || before.OriginUnixNano <= 0 {
		t.Fatalf("before=%+v after=%+v", before, after)
	}
	if before.IndexedJSON != after.IndexedJSON || before.Typed != after.Typed || before.Replay != after.Replay || before.Scans != after.Scans || before.Runtime != after.Runtime || before.RowIndexCache != after.RowIndexCache {
		t.Fatal("diagnostics or manager creation recorded work")
	}
	// A real retained allocation advances runtime lifetime totals before JSON serialization.
	memoryBefore := svc.DiagnosticsSnapshot(nil).Work.Memory
	allocations := make([][]byte, 64)
	for i := range allocations {
		allocations[i] = make([]byte, 4096+i)
		allocations[i][0] = byte(i)
	}
	observed := svc.DiagnosticsSnapshot(nil)
	runtime.KeepAlive(allocations)
	if observed.Work.Memory.TotalAlloc < memoryBefore.TotalAlloc+64*4096 || observed.Work.Memory.Mallocs < memoryBefore.Mallocs+64 || observed.Work.Memory.HeapSys == 0 {
		t.Fatalf("memory before=%+v after=%+v", memoryBefore, observed.Work.Memory)
	}
	raw, err := json.Marshal(observed)
	if err != nil {
		t.Fatal(err)
	}
	var obj map[string]json.RawMessage
	if err = json.Unmarshal(raw, &obj); err != nil {
		t.Fatal(err)
	}
	var work map[string]json.RawMessage
	if err = json.Unmarshal(obj["work"], &work); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"schema_version", "scope", "pid", "origin_kind", "origin_unix_nano", "snapshot_unix_nano", "available", "indexed_json", "typed", "runtime", "replay", "scans", "memory", "row_index_cache"} {
		if _, ok := work[key]; !ok {
			t.Fatalf("missing %s", key)
		}
	}
	var decodedMemory workstats.MemoryStats
	if err = json.Unmarshal(work["memory"], &decodedMemory); err != nil {
		t.Fatal(err)
	}
	if decodedMemory != observed.Work.Memory {
		t.Fatal("serialization did not preserve sampled memory totals")
	}
	var decodedCache workstats.RowIndexCacheStats
	if err = json.Unmarshal(work["row_index_cache"], &decodedCache); err != nil || decodedCache != observed.Work.RowIndexCache || decodedCache.ByteLimit != 64<<20 {
		t.Fatalf("cache serialization=%+v err=%v", decodedCache, err)
	}
	var available map[string]bool
	if err = json.Unmarshal(work["available"], &available); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"graph", "output", "fold"} {
		value, ok := available[key]
		if !ok || value {
			t.Fatalf("unimplemented group %s=%t present=%t", key, value, ok)
		}
	}
	if !after.Available.IndexedJSON || !after.Available.Typed || !after.Available.Replay || !after.Available.RuntimeQuery || !after.Available.AttributedScans || !after.Available.RowIndexCache {
		t.Fatalf("missing implemented producer: %+v", after.Available)
	}
}
