package collections

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestNativeRuntimeInsertCacheOwnsOnlyCurrentJSONBatch4243(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	def := VectorIndexDefinition{Name: "embedding_native", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2, M: 4, Strategy: VectorIndexStrategyNativeRuntime}
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs", Options: CollectionOptions{DocumentFormat: DocumentFormatJSON}, VectorIndexes: []VectorIndexDefinition{def}}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}

	firstDocument := []byte(`{"embedding":[1,0],"label":"first"}`)
	wantFirst := bytes.Clone(firstDocument)
	if _, err := col.InsertBatch([][]byte{[]byte("first")}, [][]byte{firstDocument}); err != nil {
		t.Fatalf("first InsertBatch: %v", err)
	}
	copy(firstDocument, bytes.Repeat([]byte{'x'}, len(firstDocument)))

	domain := col.writeDomain
	domain.mu.RLock()
	firstRef, firstFound := domain.primaryCache.lookupRef([]byte("first"))
	firstCacheLen := domain.primaryCache.len()
	domain.mu.RUnlock()
	if !firstFound || firstCacheLen != 1 || !bytes.Equal(firstRef.value, wantFirst) {
		t.Fatalf("owned first cache found=%v len=%d value=%q want=%q", firstFound, firstCacheLen, firstRef.value, wantFirst)
	}
	gotFirst, err := col.Get([]byte("first"))
	if err != nil || !bytes.Equal(gotFirst, wantFirst) {
		t.Fatalf("Get first=%q err=%v want=%q", gotFirst, err, wantFirst)
	}

	secondDocument := []byte(`{"embedding":[0,1],"label":"second"}`)
	if _, err := col.InsertBatch([][]byte{[]byte("second")}, [][]byte{secondDocument}); err != nil {
		t.Fatalf("second InsertBatch: %v", err)
	}
	domain.mu.RLock()
	_, oldFound := domain.primaryCache.lookupRef([]byte("first"))
	secondRef, secondFound := domain.primaryCache.lookupRef([]byte("second"))
	secondCacheLen := domain.primaryCache.len()
	domain.mu.RUnlock()
	if oldFound || !secondFound || secondCacheLen != 1 || !bytes.Equal(secondRef.value, secondDocument) {
		t.Fatalf("bounded cache old=%v second=%v len=%d value=%q", oldFound, secondFound, secondCacheLen, secondRef.value)
	}

	cached := col.snapshotVectorIndexPrimaryCache([][]byte{[]byte("second")})
	defer putUpdateBatchBufferedEntries(cached.primaryEntries, cached.primaryBuffer)
	if !cached.enabled || !cached.primaryEntries[0].found || !bytes.Equal(cached.primaryEntries[0].value, secondDocument) {
		t.Fatalf("current-root vector cache=%+v", cached)
	}

	// The insert cache is only a reconciliation hint. Ordinary reads remain
	// canonical even if the private cache entry is damaged.
	domain.mu.Lock()
	secondRef, _ = domain.primaryCache.lookupRef([]byte("second"))
	copy(secondRef.value, bytes.Repeat([]byte{'x'}, len(secondRef.value)))
	domain.mu.Unlock()
	gotSecond, err := col.Get([]byte("second"))
	if err != nil || !bytes.Equal(gotSecond, secondDocument) {
		t.Fatalf("canonical Get second=%q err=%v want=%q", gotSecond, err, secondDocument)
	}
}

func TestInsertCacheExcludesRetainedPayloadTransforms4243(t *testing.T) {
	domain := &collectionWriteDomain{}
	col := &Collection{writeDomain: domain}
	meta := CollectionMeta{
		Name:          "docs",
		VectorIndexes: []VectorIndexDefinition{{Name: "embedding", Strategy: VectorIndexStrategyNativeRuntime}},
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
			ColumnStore: &ColumnStoreConfig{
				Enabled:         true,
				RetainedPayload: ColumnRetainedPayloadNonColumn,
			},
		},
	}
	col.replacePrimaryDocumentCacheAfterInsert(7, meta, []noIndexBatchEntry{{id: []byte("a"), document: []byte(`{"embedding":[1,0]}`)}})
	if domain.primaryCache != nil || domain.primaryCacheSystemRoot != 0 {
		t.Fatalf("retained-payload transform populated primary cache: %+v", domain.primaryCache)
	}
}

func TestInsertCacheRequiresNativeVectorDeclaration4243(t *testing.T) {
	domain := &collectionWriteDomain{}
	col := &Collection{writeDomain: domain}
	meta := CollectionMeta{Name: "docs", Options: CollectionOptions{DocumentFormat: DocumentFormatJSON}}
	col.replacePrimaryDocumentCacheAfterInsert(7, meta, []noIndexBatchEntry{{id: []byte("a"), document: []byte(`{"embedding":[1,0]}`)}})
	if domain.primaryCache != nil || domain.primaryCacheSystemRoot != 0 {
		t.Fatalf("non-vector collection populated primary cache: %+v", domain.primaryCache)
	}
}

func TestNativeRuntimeDelayedInsertAcrossManagersReadsLatestDocument4243(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()
	firstManager := NewCollectionManager(d)
	def := VectorIndexDefinition{Name: "embedding_native", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2, M: 4, Strategy: VectorIndexStrategyNativeRuntime}
	if _, err := firstManager.CreateCollection(&CollectionMeta{Name: "docs", Options: CollectionOptions{DocumentFormat: DocumentFormatJSON}, VectorIndexes: []VectorIndexDefinition{def}}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	first, err := firstManager.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection first: %v", err)
	}
	if _, err := first.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex first: %v", err)
	}

	ids, err := first.insertBatch([][]byte{[]byte("a")}, [][]byte{[]byte(`{"embedding":[1,0]}`)}, false, nil)
	if err != nil {
		t.Fatalf("delayed insertBatch: %v", err)
	}
	firstRoot := first.writeDomain.primaryCacheSystemRoot

	secondManager := NewCollectionManager(d)
	second, err := secondManager.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection second manager: %v", err)
	}
	matched, err := second.Replace([]byte("a"), []byte(`{"embedding":[0,1]}`))
	if err != nil || !matched {
		t.Fatalf("Replace matched=%v err=%v", matched, err)
	}
	currentRoot := currentDBSystemRootForTest4243(d)
	if firstRoot == 0 || currentRoot == firstRoot {
		t.Fatalf("root did not advance: first=%d current=%d", firstRoot, currentRoot)
	}
	if cached := first.snapshotVectorIndexPrimaryCache(ids); cached.enabled {
		putUpdateBatchBufferedEntries(cached.primaryEntries, cached.primaryBuffer)
		t.Fatal("stale first-manager cache matched the current root")
	}
	if err := first.notifyVectorIndexesUpsert(ids); err != nil {
		t.Fatalf("delayed notifyVectorIndexesUpsert: %v", err)
	}

	var buffer VectorIndexSearchBuffer
	got, err := first.SearchVectorIndexWithBuffer(VectorIndexSearchOptions{IndexName: def.Name, Query: []float32{0, 1}, TopK: 1, EfSearch: 8, StatsMode: VectorIndexSearchStatsModeProduction}, &buffer)
	if err != nil || len(got.Results) != 1 || string(got.Results[0].ID) != "a" {
		t.Fatalf("search after delayed reconciliation results=%+v err=%v", got.Results, err)
	}
}

func TestNativeParallelReciprocalLinksPersistExactTopology4243(t *testing.T) {
	dir := t.TempDir()
	d := openCollectionCommandWALDB(t, dir)
	mgr := NewCollectionManager(d)
	def := VectorIndexDefinition{Name: "embedding_native", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 8, M: 16, EfConstruction: 32, Strategy: VectorIndexStrategyNativeRuntime}
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs", Options: CollectionOptions{DocumentFormat: DocumentFormatJSON}, VectorIndexes: []VectorIndexDefinition{def}}); err != nil {
		_ = d.Close()
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		_ = d.Close()
		t.Fatalf("OpenCollection: %v", err)
	}
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		_ = d.Close()
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	ids := make([][]byte, 96)
	documents := make([][]byte, 96)
	for row := range ids {
		ids[row] = []byte(fmt.Sprintf("doc-%03d", row))
		documents[row] = []byte(fmt.Sprintf(`{"embedding":[%d,%d,%d,%d,%d,%d,%d,%d]}`,
			row%17+1, row%13+2, row%11+3, row%7+4, row%5+5, row%3+6, row%19+7, row%23+8))
	}
	if _, err := col.InsertBatch(ids, documents); err != nil {
		_ = d.Close()
		t.Fatalf("InsertBatch: %v", err)
	}
	index := col.registeredVectorIndex(def.Name)
	if index == nil || !index.parallelReciprocalLinks {
		_ = d.Close()
		t.Fatalf("native parallel index=%v", index)
	}
	if index.frozenPrefixBatches == 0 {
		_ = d.Close()
		t.Fatal("native InsertBatch did not use frozen-prefix construction")
	}
	wantTopology := snapshotVectorIndexTopology4257(index)
	if status, err := index.SaveNativeSnapshot(); err != nil || !status.Loaded {
		_ = d.Close()
		t.Fatalf("SaveNativeSnapshot status=%+v err=%v", status, err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection after reopen: %v", err)
	}
	loaded, status, err := reopenedCol.LoadNativeVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def))
	if err != nil || loaded == nil || !status.Loaded {
		t.Fatalf("LoadNativeVectorIndexSnapshot loaded=%v status=%+v err=%v", loaded != nil, status, err)
	}
	if gotTopology := snapshotVectorIndexTopology4257(loaded); !reflect.DeepEqual(gotTopology, wantTopology) {
		t.Fatal("reopened native topology differs from parallel-built topology")
	}
}

func currentDBSystemRootForTest4243(d *backenddb.DB) uint64 {
	snap := d.AcquireSnapshot()
	if snap == nil {
		return 0
	}
	defer func() { _ = snap.Close() }()
	return snapshotSystemRoot(snap)
}
