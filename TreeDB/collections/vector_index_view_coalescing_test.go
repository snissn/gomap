package collections

import (
	"fmt"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestNativeVectorAcknowledgedMutationsCoalesceSearchViewPublication(t *testing.T) {
	d, col, def := newNativeScalarTestCollection(t, []IndexDefinition{{Name: "tenant_idx", Field: "tenant", ValueType: IndexValueString}})
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatal(err)
	}
	index := col.registeredVectorIndex(def.Name)
	if index == nil {
		t.Fatal("registered native runtime is unavailable")
	}
	baseline := index.acquireSearchView()
	if baseline == nil {
		t.Fatal("initial immutable search view is unavailable")
	}
	index.releaseSearchView(baseline)

	for i := range 8 {
		id := []byte(fmt.Sprintf("doc-%02d", i))
		document := []byte(fmt.Sprintf(`{"embedding":[1,%0.2f],"tenant":"alpha"}`, float64(i+1)/100))
		if _, err := col.InsertBatch([][]byte{id}, [][]byte{document}); err != nil {
			t.Fatalf("insert %q: %v", id, err)
		}
		if current := col.registeredVectorIndex(def.Name); current != index {
			t.Fatalf("acknowledged insert %d replaced runtime %p with %p", i, index, current)
		}
		if got := index.searchView.Load(); got != baseline {
			t.Fatalf("acknowledged insert %d published immutable view %p, want retained %p", i, got, baseline)
		}
	}
	if replaced, err := col.Replace([]byte("doc-03"), []byte(`{"embedding":[1,0.04],"tenant":"beta"}`)); err != nil || !replaced {
		t.Fatalf("replace matched=%v err=%v", replaced, err)
	}
	if deleted, err := col.DeleteBatch([][]byte{[]byte("doc-04")}); err != nil || deleted != 1 {
		t.Fatalf("delete count=%d err=%v", deleted, err)
	}
	if current := col.registeredVectorIndex(def.Name); current != index {
		t.Fatalf("acknowledged update/delete replaced runtime %p with %p", index, current)
	}
	if got := index.searchView.Load(); got != baseline {
		t.Fatalf("acknowledged update/delete published immutable view %p, want retained %p", got, baseline)
	}
	if index.searchViewCurrent.Load() {
		t.Fatal("acknowledged mutations left the retained immutable view marked current")
	}

	beta := searchNativeScalarTest(t, col, def, HybridScalarFilter{IndexName: "tenant_idx", Value: "beta"}, 8)
	if len(beta.Results) != 1 || string(beta.Results[0].ID) != "doc-03" {
		t.Fatalf("beta results=%+v", beta.Results)
	}
	published := index.searchView.Load()
	if published == nil || published == baseline {
		t.Fatalf("first post-ack acquisition published view %p, want one replacement for %p", published, baseline)
	}
	index.mu.RLock()
	currentMutation, currentGeneration := index.mutationSeq, index.sourceDocumentGeneration
	index.mu.RUnlock()
	if published.mutationSeq != currentMutation || published.sourceDocumentGeneration != currentGeneration {
		t.Fatalf("published identity mutation=%d generation=%d want mutation=%d generation=%d", published.mutationSeq, published.sourceDocumentGeneration, currentMutation, currentGeneration)
	}

	alpha := searchNativeScalarTest(t, col, def, HybridScalarFilter{IndexName: "tenant_idx", Value: "alpha"}, 8)
	if len(alpha.Results) != 6 {
		t.Fatalf("alpha results=%+v want six live scalar members", alpha.Results)
	}
	if got := index.searchView.Load(); got != published {
		t.Fatalf("subsequent acquisition republished immutable view %p, want %p", got, published)
	}
}

func TestVectorIndexStaleAcquisitionPublishesBesidePriorReader(t *testing.T) {
	index, err := newVectorIndex(nil, VectorIndexOptions{
		Name: "embedding", Field: "embedding", Metric: VectorMetricCosine,
		Dimensions: 2, M: 4, EfConstruction: 16, EfSearch: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	index.mu.Lock()
	if err := index.insertVectorLocked([]byte("old"), []float32{1, 0}); err != nil {
		index.mu.Unlock()
		t.Fatal(err)
	}
	index.mu.Unlock()
	index.recordSourceDocumentState(1, backenddb.StateToken{})
	old := index.acquireSearchView()
	if old == nil || len(old.nodes) != 1 {
		if old != nil {
			index.releaseSearchView(old)
		}
		t.Fatal("initial immutable reader is unavailable")
	}

	mutated := make(chan struct{})
	acknowledge := make(chan struct{})
	writerDone := make(chan error, 1)
	go func() {
		index.mu.Lock()
		err := index.insertVectorLocked([]byte("new"), []float32{0, 1})
		index.mu.Unlock()
		close(mutated)
		<-acknowledge
		if err == nil {
			index.recordSourceDocumentState(2, backenddb.StateToken{})
		}
		writerDone <- err
	}()
	<-mutated
	duringMutation := index.acquireSearchView()
	if duringMutation != old {
		if duringMutation != nil {
			index.releaseSearchView(duringMutation)
		}
		index.releaseSearchView(old)
		t.Fatalf("concurrent search acquired view %p, want prior acknowledged generation %p", duringMutation, old)
	}
	index.releaseSearchView(duringMutation)
	close(acknowledge)
	if err := <-writerDone; err != nil {
		index.releaseSearchView(old)
		t.Fatal(err)
	}
	if got := index.searchView.Load(); got != old {
		index.releaseSearchView(old)
		t.Fatalf("writer published view %p while prior reader held %p", got, old)
	}

	current := index.acquireSearchView()
	if current == nil {
		index.releaseSearchView(old)
		t.Fatal("current immutable view is unavailable")
	}
	if current == old || len(current.nodes) != 2 || current.sourceDocumentGeneration != 2 {
		index.releaseSearchView(current)
		index.releaseSearchView(old)
		t.Fatalf("current view=%p old=%p nodes=%d generation=%d", current, old, len(current.nodes), current.sourceDocumentGeneration)
	}
	if len(old.nodes) != 1 || string(old.nodes[0].documentID) != "old" {
		index.releaseSearchView(current)
		index.releaseSearchView(old)
		t.Fatal("prior immutable reader changed after current generation publication")
	}
	index.releaseSearchView(current)
	index.releaseSearchView(old)

	again := index.acquireSearchView()
	if again != current {
		if again != nil {
			index.releaseSearchView(again)
		}
		t.Fatalf("steady acquisition view=%p want %p", again, current)
	}
	index.releaseSearchView(again)
}
