package documentservice

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/snissn/gomap/TreeDB/collections"
)

func TestDiagnosticsHandlerSnapshotIsReadOnlyAndBounded(t *testing.T) {
	svc, db := newTestService(t)
	defer db.Close()
	ctx := context.Background()
	h := svc.DiagnosticsHandler(func() map[string]string { return map[string]string{"treedb.commit_seq": "7"} })
	if _, err := svc.CreateIndex(ctx, CreateIndexRequest{Name: "docs", Dimension: 2}); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	if _, err := svc.UpsertDocuments(ctx, "docs", UpsertDocumentsRequest{Documents: []Document{{ID: "a", Embedding: []float32{1, 0}}}}); err != nil {
		t.Fatalf("UpsertDocuments: %v", err)
	}
	req := httptest.NewRequest(http.MethodGet, "/debug/treedb/stats", nil)
	res := httptest.NewRecorder()
	h.ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("snapshot status=%d body=%s", res.Code, res.Body.String())
	}
	var snapshot DiagnosticsSnapshot
	if err := json.NewDecoder(res.Body).Decode(&snapshot); err != nil {
		t.Fatalf("decode snapshot: %v", err)
	}
	if snapshot.Database["treedb.commit_seq"] != "7" || snapshot.LastOpened == nil || snapshot.LastOpened.Name != "docs" || snapshot.LastOpened.Insert.Documents != 1 {
		t.Fatalf("snapshot=%+v", snapshot)
	}
	if got, err := svc.CountDocuments(ctx, "docs", CountDocumentsRequest{}); err != nil || got.Count != 1 {
		t.Fatalf("snapshot mutated service: count=%+v err=%v", got, err)
	}
	res = httptest.NewRecorder()
	h.ServeHTTP(res, httptest.NewRequest(http.MethodGet, "/v1/health", nil))
	if res.Code != http.StatusNotFound {
		t.Fatalf("unexpected document API exposure: status=%d", res.Code)
	}
}

func TestDiagnosticsSnapshotConcurrentWithWrites(t *testing.T) {
	svc, db := newTestService(t)
	defer db.Close()
	ctx := context.Background()
	h := svc.DiagnosticsHandler(func() map[string]string { return map[string]string{"treedb.commit_seq": "1"} })
	if _, err := svc.CreateIndex(ctx, CreateIndexRequest{Name: "docs", Dimension: 2}); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 100; i++ {
			res := httptest.NewRecorder()
			h.ServeHTTP(res, httptest.NewRequest(http.MethodGet, "/debug/treedb/stats", nil))
			if res.Code != http.StatusOK {
				t.Errorf("snapshot status=%d", res.Code)
				return
			}
		}
	}()
	for i := 0; i < 20; i++ {
		if _, err := svc.UpsertDocuments(ctx, "docs", UpsertDocumentsRequest{Documents: []Document{{ID: string(rune('a' + i)), Embedding: []float32{1, 0}}}}); err != nil {
			t.Fatalf("UpsertDocuments %d: %v", i, err)
		}
	}
	<-done
}

func TestDiagnosticsSnapshotKeepsCompletedInsertAcrossReopenAndFailedUpsert(t *testing.T) {
	svc, db := newTestService(t)
	defer db.Close()
	ctx := context.Background()
	h := svc.DiagnosticsHandler(nil)
	if _, err := svc.CreateIndex(ctx, CreateIndexRequest{Name: "docs", Dimension: 2}); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	if _, err := svc.UpsertDocuments(ctx, "docs", UpsertDocumentsRequest{Documents: []Document{{ID: "a", Embedding: []float32{1, 0}}}}); err != nil {
		t.Fatalf("UpsertDocuments: %v", err)
	}
	snapshot := func() DiagnosticsSnapshot {
		t.Helper()
		res := httptest.NewRecorder()
		h.ServeHTTP(res, httptest.NewRequest(http.MethodGet, "/debug/treedb/stats", nil))
		var out DiagnosticsSnapshot
		if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
			t.Fatalf("decode snapshot: %v", err)
		}
		return out
	}
	if got := snapshot().LastOpened; got == nil || got.Insert.Documents != 1 {
		t.Fatalf("initial insert snapshot=%+v", got)
	}
	if _, err := svc.OpenIndex(ctx, "docs"); err != nil {
		t.Fatalf("OpenIndex: %v", err)
	}
	if _, err := svc.UpsertDocuments(ctx, "docs", UpsertDocumentsRequest{Documents: []Document{{ID: "a", Embedding: []float32{0, 1}}}}); err != nil {
		t.Fatalf("update-only UpsertDocuments: %v", err)
	}
	if _, err := svc.UpsertDocuments(ctx, "docs", UpsertDocumentsRequest{}); ErrorCodeOf(err) != CodeInvalidRequest {
		t.Fatalf("validation upsert err=%v", err)
	}
	if got := snapshot().LastOpened; got == nil || got.Name != "docs" || got.Insert.Documents != 1 {
		t.Fatalf("completed insert clobbered by reopen/validation: %+v", got)
	}
}

func TestDiagnosticsSnapshotRestoresCompletedInsertForReopenedIndex(t *testing.T) {
	svc, db := newTestService(t)
	defer db.Close()
	ctx := context.Background()
	h := svc.DiagnosticsHandler(nil)
	for _, name := range []string{"a", "b"} {
		if _, err := svc.CreateIndex(ctx, CreateIndexRequest{Name: name, Dimension: 2}); err != nil {
			t.Fatalf("CreateIndex %q: %v", name, err)
		}
	}
	if _, err := svc.UpsertDocuments(ctx, "a", UpsertDocumentsRequest{Documents: []Document{{ID: "a", Embedding: []float32{1, 0}}}}); err != nil {
		t.Fatalf("UpsertDocuments a: %v", err)
	}
	if _, err := svc.OpenIndex(ctx, "b"); err != nil {
		t.Fatalf("OpenIndex b: %v", err)
	}
	if _, err := svc.OpenIndex(ctx, "a"); err != nil {
		t.Fatalf("OpenIndex a: %v", err)
	}
	res := httptest.NewRecorder()
	h.ServeHTTP(res, httptest.NewRequest(http.MethodGet, "/debug/treedb/stats", nil))
	var snapshot DiagnosticsSnapshot
	if err := json.NewDecoder(res.Body).Decode(&snapshot); err != nil {
		t.Fatalf("decode snapshot: %v", err)
	}
	if snapshot.LastOpened == nil || snapshot.LastOpened.Name != "a" || snapshot.LastOpened.Insert.Documents != 1 {
		t.Fatalf("a insert snapshot was not restored: %+v", snapshot.LastOpened)
	}
	// A stale record is never exposed under a different generation.
	svc.noteDiagnosticsIndex("a", IndexInfo{Name: "a", Generation: snapshot.LastOpened.Generation + 1})
	if got := svc.DiagnosticsSnapshot(nil).LastOpened; got == nil || got.Generation != snapshot.LastOpened.Generation+1 || got.Insert.Documents != 0 {
		t.Fatalf("stale generation resurrected completed insert: %+v", got)
	}
}

func TestDiagnosticsSnapshotOpenDoesNotPublishStaleCompletedInsert(t *testing.T) {
	svc, db := newTestService(t)
	defer db.Close()
	ctx := context.Background()
	svc.DiagnosticsHandler(nil)
	for _, name := range []string{"a", "b"} {
		if _, err := svc.CreateIndex(ctx, CreateIndexRequest{Name: name, Dimension: 2}); err != nil {
			t.Fatalf("CreateIndex %q: %v", name, err)
		}
	}
	a, err := svc.OpenIndex(ctx, "a")
	if err != nil {
		t.Fatalf("OpenIndex a: %v", err)
	}
	b, err := svc.OpenIndex(ctx, "b")
	if err != nil {
		t.Fatalf("OpenIndex b: %v", err)
	}
	svc.publishDiagnosticsInsert(a.Name, a, collections.CollectionInsertStats{Documents: 1})
	svc.noteDiagnosticsIndex(b.Name, b)
	loaded := make(chan struct{})
	release := make(chan struct{})
	svc.diagnosticsBeforeActivePublish = func() {
		close(loaded)
		<-release
	}
	opened := make(chan struct{})
	go func() {
		svc.noteDiagnosticsIndex(a.Name, a)
		close(opened)
	}()
	<-loaded
	published := make(chan struct{})
	go func() {
		svc.publishDiagnosticsInsert(a.Name, a, collections.CollectionInsertStats{Documents: 2})
		close(published)
	}()
	close(release)
	<-opened
	<-published
	svc.diagnosticsBeforeActivePublish = nil
	if got := svc.DiagnosticsSnapshot(nil).LastOpened; got == nil || got.Name != "a" || got.Insert.Documents != 2 {
		t.Fatalf("stale completed insert published after overlapping open: %+v", got)
	}
}

func TestDiagnosticsSnapshotPublishesFallbackSingletonInsert(t *testing.T) {
	svc, db := newTestService(t)
	defer db.Close()
	ctx := context.Background()
	svc.DiagnosticsHandler(nil)
	info, err := svc.CreateIndex(ctx, CreateIndexRequest{Name: "docs", Dimension: 2})
	if err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	col, _, err := svc.openIndex(ctx, "docs", 0)
	if err != nil {
		t.Fatalf("openIndex: %v", err)
	}
	doc, err := prepareDocumentsForWrite([]Document{{ID: "fallback", Embedding: []float32{1, 0}}}, info)
	if err != nil {
		t.Fatalf("prepareDocumentsForWrite: %v", err)
	}
	inserted, updated, err := upsertPreparedDocumentWithInsertCallback(ctx, col, doc[0], true, func() {
		svc.publishDiagnosticsInsert(info.Name, info, col.LastInsertStats())
	})
	if err != nil || !inserted || updated {
		t.Fatalf("fallback singleton insert inserted=%v updated=%v err=%v", inserted, updated, err)
	}
	if got := svc.DiagnosticsSnapshot(nil).LastOpened; got == nil || got.Name != "docs" || got.Insert.Documents != 1 {
		t.Fatalf("fallback singleton insert was not published: %+v", got)
	}
}

func TestDiagnosticsSnapshotCachedNativeCreateIsLastOpened(t *testing.T) {
	svc, db := newTestService(t)
	defer db.Close()
	ctx := context.Background()
	svc.DiagnosticsHandler(nil)
	request := func(name string) CreateIndexRequest {
		return CreateIndexRequest{Name: name, Dimension: 2, Metric: MetricCosine, VectorIndexOptions: &BenchmarkVectorIndexOptions{Strategy: collections.VectorIndexStrategyNativeRuntime}}
	}
	if _, err := svc.CreateIndex(ctx, request("a")); err != nil {
		t.Fatalf("CreateIndex a: %v", err)
	}
	if _, err := svc.CreateIndex(ctx, request("b")); err != nil {
		t.Fatalf("CreateIndex b: %v", err)
	}
	if _, err := svc.CreateIndex(ctx, request("a")); err != nil {
		t.Fatalf("compatible cached CreateIndex a: %v", err)
	}
	if got := svc.DiagnosticsSnapshot(nil).LastOpened; got == nil || got.Name != "a" {
		t.Fatalf("cached CreateIndex did not become last opened: %+v", got)
	}
}

func TestDiagnosticsSnapshotCachedBenchmarkSearchIsLastOpened(t *testing.T) {
	svc, db := newTestService(t)
	defer db.Close()
	ctx := context.Background()
	svc.DiagnosticsHandler(nil)
	request := func(name string) CreateIndexRequest {
		return CreateIndexRequest{Name: name, Dimension: 2, Metric: MetricCosine, VectorIndexOptions: &BenchmarkVectorIndexOptions{Strategy: collections.VectorIndexStrategyNativeRuntime}}
	}
	for _, name := range []string{"a", "b"} {
		if _, err := svc.CreateIndex(ctx, request(name)); err != nil {
			t.Fatalf("CreateIndex %q: %v", name, err)
		}
		if _, err := svc.UpsertDocuments(ctx, name, UpsertDocumentsRequest{Documents: []Document{{ID: name, Embedding: []float32{1, 0}}}, DeferVectorIndexRebuild: true}); err != nil {
			t.Fatalf("UpsertDocuments %q: %v", name, err)
		}
	}
	if got := svc.benchmarkSearchCacheSizeForTest(); got != 2 {
		t.Fatalf("native cache size=%d want 2", got)
	}
	if _, err := svc.SearchBenchmarkVector(ctx, "a", BenchmarkVectorSearchRequest{QueryEmbedding: []float32{1, 0}, TopK: 1, EfSearch: 8, StatsMode: collections.VectorIndexSearchStatsModeProduction}); err != nil {
		t.Fatalf("SearchBenchmarkVector a: %v", err)
	}
	if got := svc.DiagnosticsSnapshot(nil).LastOpened; got == nil || got.Name != "a" {
		t.Fatalf("cached benchmark search did not become last opened: %+v", got)
	}
}

func BenchmarkDiagnosticsOpenIndex(b *testing.B) {
	for _, diagnostics := range []bool{false, true} {
		b.Run(map[bool]string{false: "off", true: "on"}[diagnostics], func(b *testing.B) {
			svc, db := newTestService(b)
			defer db.Close()
			ctx := context.Background()
			if diagnostics {
				svc.DiagnosticsHandler(func() map[string]string { return nil })
			}
			if _, err := svc.CreateIndex(ctx, CreateIndexRequest{Name: "docs", Dimension: 2}); err != nil {
				b.Fatalf("CreateIndex: %v", err)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := svc.OpenIndex(ctx, "docs"); err != nil {
					b.Fatalf("OpenIndex: %v", err)
				}
			}
		})
	}
}
