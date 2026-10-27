package documentservice

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// Same public owner, corpus and finite limits as the maintained write-load
// fixture: 32 once-only requests, four writers, no caller retries or worker
// maintenance handle substituted for New(manager).
func TestOrdinaryColumnGraphNativeConcurrentWrite4096(t *testing.T) {
	ctx := context.Background()
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir())
	opts.MemtableMode = "cow_btree"
	opts.BackgroundCheckpointInterval, opts.BackgroundCheckpointIdleDuration = -1, -1
	var owner *treedb.DB
	var manager *collections.CollectionManager
	var svc *Service
	var budget *memtable.COWBudget
	closeAll := func() {
		if svc != nil {
			if err := svc.Close(); err != nil {
				t.Error(err)
			}
			svc = nil
		}
		if manager != nil {
			if err := manager.Close(); err != nil {
				t.Error(err)
			}
			manager = nil
		}
		if owner != nil {
			if err := owner.Close(); err != nil {
				t.Error(err)
			}
			owner = nil
		}
		if budget != nil {
			stats := budget.Stats()
			if stats.TotalBytes != 0 || stats.ReservedBytes != 0 || stats.Views != 0 || stats.ExternalLeases != 0 || stats.Sources != 0 || stats.Generations != 0 {
				t.Errorf("final original budget owners=%+v", stats)
			}
			budget = nil
		}
	}
	t.Cleanup(closeAll)
	open := func() {
		var err error
		owner, err = treedb.Open(opts)
		if err != nil {
			t.Fatal(err)
		}
		backend, err := owner.NativeSemanticBackend()
		if err != nil {
			t.Fatal(err)
		}
		manager = collections.NewCollectionManager(backend)
		svc = New(manager)
		capture, err := backend.CaptureNativeSemanticState()
		if err != nil {
			t.Fatal(err)
		}
		// This is the actual original budget object, not a fabricated closed-owner
		// statistic or an optional Stats path silently replaced with zeros.
		var ok bool
		budget, ok = capture.NativeAllocator().(*memtable.COWBudget)
		if !ok {
			capture.Close()
			t.Fatal("native owner lacks original COW budget")
		}
		if err := capture.Close(); err != nil {
			t.Fatal(err)
		}
	}
	open()
	const name = "shared_cow_consumers"
	checkInfo := func(info IndexInfo) {
		t.Helper()
		if info.Name != name || info.Dimension != 2 || info.Metric != MetricCosine || info.VectorStrategy != collections.VectorIndexStrategyColumnGraph || info.TypedInput || info.TextIndexName != defaultTextIndexName || len(info.ScalarFields) != 1 || info.ScalarFields[0].Field != "meta.tenant" || info.ScalarFields[0].ValueType != ScalarFieldString {
			t.Fatalf("ordinary index changed: %+v", info)
		}
	}
	info, err := svc.CreateIndex(ctx, CreateIndexRequest{Name: name, Dimension: 2, ScalarFields: []ScalarFieldDeclaration{{Field: "meta.tenant", ValueType: ScalarFieldString}}})
	if err != nil {
		t.Fatal(err)
	}
	checkInfo(info)
	docs := make([]Document, 4096)
	for i := range docs {
		docs[i] = Document{ID: fmt.Sprintf("doc-%04d", i), Content: fmt.Sprintf("content-%04d", i), Embedding: []float32{float32(i + 1), 1}, Meta: map[string]any{"tenant": fmt.Sprintf("tenant-%02d", i%16)}}
	}
	type result struct {
		response  UpsertDocumentsResponse
		err       error
		attempted bool
	}
	var results [32]result
	start := make(chan struct{})
	var ready, done sync.WaitGroup
	ready.Add(4)
	done.Add(4)
	for worker := 0; worker < 4; worker++ {
		go func(worker int) {
			defer done.Done()
			ready.Done()
			<-start
			for batch := worker; batch < len(results); batch += 4 {
				first := batch * 128
				results[batch].attempted = true
				results[batch].response, results[batch].err = svc.UpsertDocuments(ctx, name, UpsertDocumentsRequest{Documents: docs[first : first+128], DeferVectorIndexRebuild: true})
			}
		}(worker)
	}
	ready.Wait()
	close(start)
	done.Wait()
	failed := false
	for batch, got := range results {
		if !got.attempted || got.err != nil {
			t.Errorf("batch%d attempted=%t err=%v cause=%v", batch, got.attempted, got.err, errors.Unwrap(got.err))
			failed = true
			continue
		}
		checkInfo(got.response.Index)
		if got.response.Upserted != 128 || got.response.Inserted != 128 || got.response.Updated != 0 || len(got.response.IDs) != 128 {
			t.Errorf("batch%d response=%+v", batch, got.response)
			failed = true
			continue
		}
		for j, id := range got.response.IDs {
			if id != docs[batch*128+j].ID {
				t.Errorf("batch%d ID%d=%q", batch, j, id)
				failed = true
			}
		}
	}
	if failed {
		t.Fatal("once-only concurrent public requests failed; no caller retry")
	}
	verify := func(phase string) {
		t.Helper()
		count, err := svc.CountDocuments(ctx, name, CountDocumentsRequest{})
		if err != nil || count.Count != 4096 {
			t.Fatalf("phase=%s count=%d err=%v cause=%v budget=%+v", phase, count.Count, err, errors.Unwrap(err), budget.Stats())
		}
		all, err := svc.FilterDocuments(ctx, name, FilterDocumentsRequest{ReturnEmbedding: true})
		if err != nil || all.Truncated || all.MatchedCount != 4096 || len(all.Documents) != 4096 {
			t.Fatalf("all documents: len=%d matched=%d truncated=%t err=%v", len(all.Documents), all.MatchedCount, all.Truncated, err)
		}
		for i, got := range all.Documents {
			if !reflect.DeepEqual(got, docs[i]) {
				t.Fatalf("document%d got=%+v want=%+v", i, got, docs[i])
			}
		}
		for tenant := 0; tenant < 16; tenant++ {
			count, err := svc.CountDocuments(ctx, name, CountDocumentsRequest{Filter: &Filter{Field: "meta.tenant", Operator: "==", Value: fmt.Sprintf("tenant-%02d", tenant)}})
			if err != nil || count.Count != 256 {
				t.Fatalf("tenant%d count=%d err=%v", tenant, count.Count, err)
			}
		}
	}
	verify("after-writes")
	if _, err := svc.OptimizeIndex(ctx, name, OptimizeIndexRequest{}); err != nil {
		t.Fatal(err)
	}
	closeAll()
	open()
	info, err = svc.OpenIndex(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	checkInfo(info)
	verify("after-reopen")
	closeAll()
}
