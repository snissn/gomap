package collections

import (
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestVectorIndexNextMutationRetainsLatestAcknowledgedView(t *testing.T) {
	index, err := newColumnGraphPartitionLiveDomain(nil, columnGraphPartitionLiveOptions{
		Name: "embedding", Field: "embedding", Metric: VectorMetricCosine,
		Dimensions: 2, M: 4, EfConstruction: 16, EfSearch: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	index.mu.Lock()
	if err := index.insertVectorLocked([]byte("seed"), []float32{1, 0}); err != nil {
		index.mu.Unlock()
		t.Fatal(err)
	}
	index.mu.Unlock()
	index.recordSourceDocumentState(1, backenddb.StateToken{})
	initial := index.acquireSearchView()
	if initial == nil {
		t.Fatal("initial immutable view is unavailable")
	}
	index.releaseSearchView(initial)
	index.preserveAcknowledgedView.Store(true)

	index.mu.Lock()
	if err := index.insertVectorLocked([]byte("first"), []float32{0.8, 0.2}); err != nil {
		index.mu.Unlock()
		t.Fatal(err)
	}
	index.mu.Unlock()
	index.recordSourceDocumentState(2, backenddb.StateToken{})
	if got := index.searchView.Load(); got != initial {
		t.Fatalf("first acknowledged mutation published eagerly: view=%p initial=%p", got, initial)
	}

	index.mu.Lock()
	if err := index.insertVectorLocked([]byte("second"), []float32{0, 1}); err != nil {
		index.mu.Unlock()
		t.Fatal(err)
	}
	index.mu.Unlock()
	duringSecond := index.acquireSearchView()
	if duringSecond == nil {
		t.Fatal("latest acknowledged view is unavailable during next mutation")
	}
	if duringSecond == initial || len(duringSecond.nodes) != 2 || duringSecond.sourceDocumentGeneration != 2 {
		index.releaseSearchView(duringSecond)
		t.Fatalf("view=%p initial=%p nodes=%d generation=%d want first acknowledged mutation", duringSecond, initial, len(duringSecond.nodes), duringSecond.sourceDocumentGeneration)
	}
	index.releaseSearchView(duringSecond)

	index.recordSourceDocumentState(3, backenddb.StateToken{})
	current := index.acquireSearchView()
	if current == nil {
		t.Fatal("current immutable view is unavailable")
	}
	if len(current.nodes) != 3 || current.sourceDocumentGeneration != 3 {
		index.releaseSearchView(current)
		t.Fatalf("current nodes=%d generation=%d want 3/3", len(current.nodes), current.sourceDocumentGeneration)
	}
	index.releaseSearchView(current)
}

func TestVectorIndexStaleAcquisitionPublishesBesidePriorReader(t *testing.T) {
	index, err := newColumnGraphPartitionLiveDomain(nil, columnGraphPartitionLiveOptions{
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
