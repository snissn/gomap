package collections

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"unsafe"
)

func TestVectorIndexConstructionDecisionObserverPreservesFrozenPrefix4461(t *testing.T) {
	const (
		rowCount   = 96
		dimensions = 16
	)
	rows := vectorIndexReciprocalParityRows4257(rowCount, dimensions, true)
	off := buildVectorIndexDecisionObserver4461(t, rows, nil)
	firstObserver := &vectorIndexConstructionDecisionObserverV1{}
	first := buildVectorIndexDecisionObserver4461(t, rows, firstObserver)
	secondObserver := &vectorIndexConstructionDecisionObserverV1{}
	second := buildVectorIndexDecisionObserver4461(t, rows, secondObserver)

	wantTopology := snapshotVectorIndexTopology4257(off)
	wantSearch := snapshotVectorIndexSearches4257(t, off, rows)
	wantMeta := vectorIndexDecisionMetadata4461(off)
	for name, index := range map[string]*VectorIndex{"observer-on": first, "observer-repeat": second} {
		if got := snapshotVectorIndexTopology4257(index); !reflect.DeepEqual(got, wantTopology) {
			t.Fatalf("%s changed topology", name)
		}
		if got := snapshotVectorIndexSearches4257(t, index, rows); !reflect.DeepEqual(got, wantSearch) {
			t.Fatalf("%s changed search results", name)
		}
		if got := vectorIndexDecisionMetadata4461(index); got != wantMeta {
			t.Fatalf("%s metadata=%+v want %+v", name, got, wantMeta)
		}
		if index.frozenPrefixBatches == 0 {
			t.Fatalf("%s did not use frozen-prefix construction", name)
		}
	}
	firstStats, secondStats := firstObserver.snapshot(), secondObserver.snapshot()
	firstPhases := []VectorIndexConstructionDecisionPhaseSnapshot{firstStats.Planning, firstStats.Reciprocal}
	secondPhases := []VectorIndexConstructionDecisionPhaseSnapshot{secondStats.Planning, secondStats.Reciprocal}
	for phase := range firstPhases {
		firstPhase, secondPhase := firstPhases[phase], secondPhases[phase]
		if firstPhase.DigestXOR == 0 || firstPhase.DigestSum == 0 || firstPhase.Decisions == 0 {
			t.Fatalf("phase %d missing decision evidence: %+v", phase, firstPhase)
		}
		if firstPhase.DigestXOR != secondPhase.DigestXOR || firstPhase.DigestSum != secondPhase.DigestSum {
			t.Fatalf("phase %d digest changed: first=%x/%x second=%x/%x", phase, firstPhase.DigestXOR, firstPhase.DigestSum, secondPhase.DigestXOR, secondPhase.DigestSum)
		}
		if firstPhase.DirectExactFP32Rows+firstPhase.IndexedExactFP32Rows == 0 ||
			firstPhase.DirectExactFP32Calls+firstPhase.IndexedExactFP32Calls == 0 ||
			firstPhase.ExactFP32Dimensions == 0 {
			t.Fatalf("phase %d invalid bounded row/call evidence: %+v", phase, firstPhase)
		}
		if firstPhase.ApproximateScoreRows != 0 || firstPhase.ApproximateScoreCalls != 0 {
			t.Fatalf("phase %d exact route reported approximate scores: %+v", phase, firstPhase)
		}
		if firstPhase.Accepted+firstPhase.Rejected != firstPhase.DiversityPredicates ||
			firstPhase.DiversityCandidates == 0 ||
			firstPhase.DiversityComparisonsExecuted == 0 ||
			firstPhase.DiversityComparisonsRequested < firstPhase.DiversityComparisonsExecuted {
			t.Fatalf("phase %d inconsistent diversity accounting: %+v", phase, firstPhase)
		}
	}
	if size := unsafe.Sizeof(*firstObserver); size > 70<<10 {
		t.Fatalf("observer size=%d exceeds 70 KiB", size)
	}
	replacementObserver := &vectorIndexConstructionDecisionObserverV1{}
	replacementContext := vectorIndexConstructionDecisionContextV1{observer: replacementObserver, phase: vectorIndexConstructionDecisionPlanning, dimensions: dimensions}
	for pair := 0; pair < vectorIndexConstructionDecisionPairSlots*2; pair++ {
		replacementContext.recordRowFrom(pair, pair+1, false)
	}
	replacementStats := replacementObserver.snapshot().Planning
	if replacementStats.RowPairReplacements == 0 || replacementStats.Saturated {
		t.Fatalf("rolling row-pair sketch replacements=%d saturated=%t", replacementStats.RowPairReplacements, replacementStats.Saturated)
	}

	dir := t.TempDir()
	db := openCollectionCommandWALDB(t, dir)
	mgr := NewCollectionManager(db)
	def := VectorIndexDefinition{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Dimensions: dimensions, M: 16, EfConstruction: 64, Strategy: VectorIndexStrategyNativeRuntime}
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs", Options: CollectionOptions{DocumentFormat: DocumentFormatJSON}, VectorIndexes: []VectorIndexDefinition{def}}); err != nil {
		t.Fatal(err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatal(err)
	}
	persisted := col.registeredVectorIndex(def.Name)
	persisted.decisionObserver = &vectorIndexConstructionDecisionObserverV1{}
	ids, documents := vectorIndexDecisionDocuments4461(t, rows)
	if _, err = col.InsertBatch(ids, documents); err != nil {
		t.Fatal(err)
	}
	if got := snapshotVectorIndexTopology4257(persisted); !reflect.DeepEqual(got, wantTopology) {
		t.Fatal("native observer build changed topology")
	}
	persistedMeta := persisted.persistMetaLocked()
	if status, saveErr := persisted.SaveNativeSnapshot(); saveErr != nil || !status.Loaded {
		t.Fatalf("SaveNativeSnapshot status=%+v err=%v", status, saveErr)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}

	reopened := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("docs")
	if err != nil {
		t.Fatal(err)
	}
	loaded, status, err := reopenedCol.LoadNativeVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def))
	if err != nil || loaded == nil || !status.Loaded {
		t.Fatalf("LoadNativeVectorIndexSnapshot loaded=%v status=%+v err=%v", loaded != nil, status, err)
	}
	if got := snapshotVectorIndexTopology4257(loaded); !reflect.DeepEqual(got, wantTopology) {
		t.Fatal("reopened topology changed")
	}
	if got := snapshotVectorIndexSearches4257(t, loaded, rows); !reflect.DeepEqual(got, wantSearch) {
		t.Fatal("reopened search results changed")
	}
	if got := loaded.persistMetaLocked(); !reflect.DeepEqual(got, persistedMeta) {
		t.Fatalf("reopened metadata=%+v want %+v", got, persistedMeta)
	}
}

type vectorIndexDecisionMetadataSnapshot4461 struct {
	entry, maxLevel, dimensions, m, efConstruction int
	mutationSeq, frozenPrefixBatches               uint64
}

func vectorIndexDecisionMetadata4461(index *VectorIndex) vectorIndexDecisionMetadataSnapshot4461 {
	return vectorIndexDecisionMetadataSnapshot4461{index.entry, index.maxLevel, index.dimensions, index.m, index.efConstruction, index.mutationSeq, index.frozenPrefixBatches}
}

// buildVectorIndexDecisionObserver4461 is also the post-merge analysis seam:
// a focused same-package test can load preserved source rows and snapshot the
// returned observer without changing the production builder.
func buildVectorIndexDecisionObserver4461(t testing.TB, rows [][]float32, observer *vectorIndexConstructionDecisionObserverV1) *VectorIndex {
	t.Helper()
	index, err := newVectorIndex(nil, VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Dimensions: len(rows[0]), M: 16, EfConstruction: 64})
	if err != nil {
		t.Fatal(err)
	}
	index.setNativePersistent(true)
	index.decisionObserver = observer
	ids := make([][]byte, len(rows))
	for row := range ids {
		ids[row] = []byte(fmt.Sprintf("doc-%04d", row))
	}
	if err := index.insertVectorBatchLocked(ids, rows); err != nil {
		t.Fatal(err)
	}
	return index
}

func vectorIndexDecisionDocuments4461(t testing.TB, rows [][]float32) ([][]byte, [][]byte) {
	t.Helper()
	ids := make([][]byte, len(rows))
	documents := make([][]byte, len(rows))
	for row := range rows {
		ids[row] = []byte(fmt.Sprintf("doc-%04d", row))
		var err error
		documents[row], err = json.Marshal(map[string]any{"embedding": rows[row]})
		if err != nil {
			t.Fatal(err)
		}
	}
	return ids, documents
}
