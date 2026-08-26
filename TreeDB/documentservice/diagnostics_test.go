package documentservice

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
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
