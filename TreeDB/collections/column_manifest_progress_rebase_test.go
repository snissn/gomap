package collections

import (
	"bytes"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestColumnGraphInsertPlanningSurvivesCheckpointManifestProgress(t *testing.T) {
	dir, d, _ := openValidatedFloat32ProjectionCollection(t, 3)
	defer func() {
		if d != nil {
			_ = d.Close()
		}
	}()
	planned, err := NewCommandWALReplayCollectionManager(d).OpenCollection("docs")
	if err != nil {
		t.Fatalf("open planned handle: %v", err)
	}
	publisher, err := NewCollectionManager(d).OpenCollection("docs")
	if err != nil {
		t.Fatalf("open publisher handle: %v", err)
	}
	planningPaused := make(chan struct{})
	releasePlanning := make(chan struct{})
	var pauseClaimed atomic.Bool
	var releaseClaimed atomic.Bool
	release := func() {
		if releaseClaimed.CompareAndSwap(false, true) {
			close(releasePlanning)
		}
	}
	defer release()
	installTestBeforeInsertBatchPlanningHookForTests(t, func() {
		if pauseClaimed.CompareAndSwap(false, true) {
			close(planningPaused)
			<-releasePlanning
		}
	})

	plannedMeta := planned.Meta()
	bIDs := [][]byte{[]byte("b1"), []byte("b2")}
	bVectors := [][]float32{{0, 1, 0}, {0, 0, 1}}
	bDocuments := validatedFloat32ProjectionDocuments(t, bIDs, bVectors)
	type insertResult struct {
		ids   [][]byte
		stats CollectionInsertStats
		err   error
	}
	bDone := make(chan insertResult, 1)
	go func() {
		ids, stats, err := planned.InsertBatchWithStatsValidatedFloat32Projection(
			bIDs, bDocuments, "embedding", VectorMetricCosine, bVectors,
		)
		bDone <- insertResult{ids: ids, stats: stats, err: err}
	}()
	select {
	case <-planningPaused:
	case result := <-bDone:
		t.Fatalf("planned insert completed before pause: err=%v", result.err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for planned insert pause")
	}

	aIDs := [][]byte{[]byte("a")}
	aVectors := [][]float32{{1, 0, 0}}
	aDocuments := validatedFloat32ProjectionDocuments(t, aIDs, aVectors)
	if _, _, err := publisher.InsertBatchWithStatsValidatedFloat32Projection(
		aIDs, aDocuments, "embedding", VectorMetricCosine, aVectors,
	); err != nil {
		release()
		t.Fatalf("publisher insert: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		release()
		t.Fatalf("checkpoint publisher insert: %v", err)
	}

	currentSnap := d.AcquireSnapshot()
	if currentSnap == nil {
		release()
		t.Fatal("acquire current snapshot: nil")
	}
	currentCatalog, err := loadCollectionCatalog(currentSnap, "docs")
	_ = currentSnap.Close()
	if err != nil {
		release()
		t.Fatalf("load current catalog: %v", err)
	}
	if currentCatalog == nil {
		release()
		t.Fatal("load current catalog: nil")
	}
	if sameCollectionMeta(currentCatalog.meta, plannedMeta) || !sameCollectionMetaIgnoringColumnManifestProgress(currentCatalog.meta, plannedMeta) {
		release()
		t.Fatalf("checkpoint delta was not manifest progress only: planned=%+v current=%+v", plannedMeta.Options.ColumnStore, currentCatalog.meta.Options.ColumnStore)
	}
	release()

	var bResult insertResult
	select {
	case bResult = <-bDone:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for planned insert")
	}
	if bResult.err != nil {
		t.Fatalf("planned insert after checkpoint: %v", bResult.err)
	}
	if len(bResult.ids) != len(bIDs) || bResult.stats.ColumnPublishValidatedFloat32ProjectionRows != len(bIDs) {
		t.Fatalf("planned insert result ids=%q stats=%+v", bResult.ids, bResult.stats)
	}
	if err := planned.Flush(); err != nil {
		t.Fatalf("flush planned insert: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint planned insert: %v", err)
	}

	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	d = nil
	reopened := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopened.Close() }()
	reopenedCollection, err := NewCollectionManager(reopened).OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection after reopen: %v", err)
	}
	for i, id := range append(aIDs, bIDs...) {
		want := append(aDocuments, bDocuments...)[i]
		got, err := reopenedCollection.Get(id)
		if err != nil {
			t.Fatalf("get %q after reopen: %v", id, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("get %q after reopen=%s want %s", id, got, want)
		}
	}
}

func TestColumnGraphInsertPlanningRejectsStructuralSchemaDrift(t *testing.T) {
	_, d, _ := openValidatedFloat32ProjectionCollection(t, 3)
	defer func() { _ = d.Close() }()
	planned, err := NewCommandWALReplayCollectionManager(d).OpenCollection("docs")
	if err != nil {
		t.Fatalf("open planned handle: %v", err)
	}
	baseLSN := d.State().AppliedCommandLSN
	var hookRuns atomic.Int32
	installTestBeforeInsertBatchPlanningHookForTests(t, func() {
		if hookRuns.Add(1) == 1 {
			changed := copyCollectionMeta(planned.meta)
			changed.VectorIndexes[0].Dimensions++
			planned.meta = changed
		}
	})

	ids := [][]byte{[]byte("structural")}
	vectors := [][]float32{{1, 0, 0}}
	_, _, err = planned.InsertBatchWithStatsValidatedFloat32Projection(
		ids,
		validatedFloat32ProjectionDocuments(t, ids, vectors),
		"embedding",
		VectorMetricCosine,
		vectors,
	)
	if err == nil || !strings.Contains(err.Error(), "concurrent schema modification") {
		t.Fatalf("structural schema drift error=%v, want concurrent schema modification", err)
	}
	if got := d.State().AppliedCommandLSN; got != baseLSN {
		t.Fatalf("AppliedCommandLSN=%d want unchanged %d", got, baseLSN)
	}
}
