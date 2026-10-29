package collections_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/collections"
	"github.com/snissn/gomap/TreeDB/documentservice"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
)

// Keep this public-input regression runnable on the original source. The exact
// command driver remains a separate transport/ACK proof; this isolates its real
// Service Create/Upsert path without shrinking the corpus or batch.
func TestNativeEffectImagePublicSeed(t *testing.T) {
	opts := treedb.OptionsFor(treedb.ProfileCommandWALDurable, t.TempDir())
	opts.MemtableMode = "cow_btree"
	owner, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	backend, err := owner.NativeSemanticBackend()
	if err != nil {
		owner.Close()
		t.Fatal(err)
	}
	cut, err := backend.CaptureNativeSemanticState()
	if err != nil {
		owner.Close()
		t.Fatal(err)
	}
	budget := cut.NativeAllocator().(interface{ Stats() memtable.COWStats })
	if err := cut.Close(); err != nil {
		owner.Close()
		t.Fatal(err)
	}
	manager := collections.NewCollectionManager(backend)
	svc := documentservice.NewWithDeferredVectorBuildMaintenance(manager, owner.DeferredVectorBuildMaintenance())
	t.Cleanup(func() {
		if err := svc.Close(); err != nil {
			t.Error(err)
		}
		if err := manager.Close(); err != nil {
			t.Error(err)
		}
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
		if stats := budget.Stats(); stats.TotalBytes != 0 || stats.ExternalLeases != 0 {
			t.Errorf("full Close retained receipts: %+v", stats)
		}
	})
	ctx := context.Background()
	_, err = svc.CreateIndex(ctx, documentservice.CreateIndexRequest{
		Name: "docs", Dimension: 8,
		VectorIndexOptions: &documentservice.BenchmarkVectorIndexOptions{Strategy: collections.VectorIndexStrategyColumnGraph, M: 16, EfConstruction: 128, EfSearch: 64},
		ScalarFields:       []documentservice.ScalarFieldDeclaration{{Field: "meta.tenant", ValueType: documentservice.ScalarFieldString}, {Field: "meta.rank", ValueType: documentservice.ScalarFieldInt64}},
	})
	if err != nil {
		t.Fatal(err)
	}
	content := "refund policy " + strings.Repeat("payload ", 32)
	if len(content) != 270 {
		t.Fatal("command content changed")
	}
	for first := 0; first < 4096; first += 128 {
		docs := make([]documentservice.Document, 128)
		for j := range docs {
			i := first + j
			vector := make([]float32, 8)
			vector[0] = 1
			for d := 1; d < 8; d++ {
				vector[d] = float32((i+d)%17) / 17
			}
			docs[j] = documentservice.Document{ID: fmt.Sprintf("d%06d", i), Content: content, Embedding: vector, Meta: map[string]any{"tenant": fmt.Sprintf("t%02d", i%16), "rank": i}}
		}
		response, err := svc.UpsertDocuments(ctx, "docs", documentservice.UpsertDocumentsRequest{Documents: docs, DeferVectorIndexRebuild: true})
		if err != nil {
			t.Fatalf("unchanged command seed prefix %d: %v", first, err)
		}
		if response.Upserted != 128 {
			t.Fatalf("prefix %d upserted %d", first, response.Upserted)
		}
	}
	col, err := manager.OpenCollection("docs")
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"d000000", "d001024", "d004095"} {
		doc, err := col.Get([]byte(id))
		if err != nil || !strings.Contains(string(doc), content) {
			t.Fatalf("published %s: %s %v", id, doc, err)
		}
	}
	before, err := backend.CaptureNativeSemanticState()
	if err != nil {
		t.Fatal(err)
	}
	defer before.Close()
	sequence, lsn := before.LogicalSequence(), backend.CommandWALNextLSN()
	// The first staged row must not escape when a later existing ID refuses.
	_, err = col.InsertBatch([][]byte{[]byte("not-published"), []byte("d000000")}, [][]byte{[]byte(`{"content":"fresh"}`), []byte(`{"content":"duplicate"}`)})
	if err == nil {
		t.Fatal("duplicate insertion accepted")
	}
	if _, found, err := col.GetInto([]byte("not-published"), nil); err != nil || found {
		t.Fatalf("failed batch published first row: found=%t err=%v", found, err)
	}
	after, err := backend.CaptureNativeSemanticState()
	if err != nil {
		t.Fatal(err)
	}
	defer after.Close()
	if after.LogicalSequence() != sequence || backend.CommandWALNextLSN() != lsn {
		t.Fatal("refused insertion changed logical publication/WAL")
	}
}
