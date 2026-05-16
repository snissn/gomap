package collections

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestCollectionVectorIndexSnapshotReopenSearch(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	col := openVectorIndexTestCollection(t, d)
	if _, err := col.InsertBatch(
		[][]byte{[]byte("a"), []byte("b"), []byte("c")},
		[][]byte{
			[]byte(`{"embedding":[1,0]}`),
			[]byte(`{"embedding":[0,1]}`),
			[]byte(`{"embedding":[0.8,0.2]}`),
		},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	index, err := col.BuildVectorIndex(VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, M: 4})
	if err != nil {
		t.Fatalf("build vector index: %v", err)
	}
	saveStatus, err := index.SaveSnapshot()
	if err != nil {
		t.Fatalf("save snapshot: %v", err)
	}
	if !saveStatus.Loaded || saveStatus.Epoch == 0 || saveStatus.ManifestPath == "" {
		t.Fatalf("unexpected save status: %+v", saveStatus)
	}
	if saveStatus.BytesDisk <= 0 {
		t.Fatalf("save status missing disk bytes: %+v", saveStatus)
	}
	saveStats := index.Stats()
	if saveStats.Epoch != saveStatus.Epoch || saveStats.BytesDisk != saveStatus.BytesDisk || saveStats.SnapshotDirty {
		t.Fatalf("unexpected saved stats: %+v status=%+v", saveStats, saveStatus)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("d")},
		[][]byte{[]byte(`{"embedding":[0.7,0.3]}`)},
	); err != nil {
		t.Fatalf("insert after snapshot: %v", err)
	}
	if dirtyStats := index.Stats(); dirtyStats.Epoch != saveStatus.Epoch || !dirtyStats.SnapshotDirty {
		t.Fatalf("snapshot mutation did not mark stats dirty: %+v", dirtyStats)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection after reopen: %v", err)
	}
	loaded, loadStatus, err := reopenedCol.LoadVectorIndexSnapshot(VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine})
	if err != nil {
		t.Fatalf("load vector index snapshot: %v", err)
	}
	if !loadStatus.Loaded || loadStatus.ExactFallbackReason != "" || loadStatus.Epoch != saveStatus.Epoch || loaded == nil {
		t.Fatalf("unexpected load status loaded=%v status=%+v", loaded != nil, loadStatus)
	}
	if loadStatus.BytesDisk != saveStatus.BytesDisk {
		t.Fatalf("loaded disk bytes=%d want saved %d", loadStatus.BytesDisk, saveStatus.BytesDisk)
	}
	loadedStats := loaded.Stats()
	if loadedStats.Epoch != saveStatus.Epoch || loadedStats.BytesDisk != saveStatus.BytesDisk || loadedStats.SnapshotDirty {
		t.Fatalf("unexpected loaded stats: %+v status=%+v", loadedStats, loadStatus)
	}
	results, trace, err := loaded.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 2, DisableExactFallback: true})
	if err != nil {
		t.Fatalf("search loaded index: %v", err)
	}
	requireVectorResultIDs(t, results, "a", "c")
	if trace.RerankCount == 0 {
		t.Fatalf("loaded index did not rerank canonical rows: %+v", trace)
	}
	if got, want := loaded.Stats().MaxLevel, index.Stats().MaxLevel; got != want {
		t.Fatalf("loaded max level=%d want %d", got, want)
	}
}

func TestCollectionVectorIndexSnapshotUnregisteredAdHocUsesLegacyPath(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	col := openVectorIndexTestCollection(t, d)
	if _, err := col.InsertBatch(
		[][]byte{[]byte("a"), []byte("b")},
		[][]byte{
			[]byte(`{"embedding":[1,0]}`),
			[]byte(`{"embedding":[0,1]}`),
		},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	index, err := col.BuildVectorIndex(VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, M: 4})
	if err != nil {
		t.Fatalf("build vector index: %v", err)
	}
	col.UnregisterVectorIndex("embedding")
	saveStatus, err := index.SaveSnapshot()
	if err != nil {
		t.Fatalf("save unregistered ad hoc snapshot: %v", err)
	}
	if !saveStatus.Loaded || saveStatus.ManifestPath == "" || saveStatus.RootID != 0 || saveStatus.ExactFallbackReason != "" {
		t.Fatalf("unexpected unregistered ad hoc save status: %+v", saveStatus)
	}
	if _, err := os.Stat(saveStatus.ManifestPath); err != nil {
		t.Fatalf("legacy manifest was not written: %v", err)
	}
}

func TestCollectionVectorIndexNativeRootSnapshotReopenSearch(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "docs",
		VectorIndexes: []VectorIndexDefinition{{
			Name:       "embedding",
			Field:      "embedding",
			Metric:     VectorMetricCosine,
			Dimensions: 2,
			M:          4,
		}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("a"), []byte("b"), []byte("c")},
		[][]byte{
			[]byte(`{"embedding":[1,0]}`),
			[]byte(`{"embedding":[0,1]}`),
			[]byte(`{"embedding":[0.8,0.2]}`),
		},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	index, err := col.BuildVectorIndex(VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2, M: 4})
	if err != nil {
		t.Fatalf("build vector index: %v", err)
	}
	saveStatus, err := index.SaveSnapshot()
	if err != nil {
		t.Fatalf("save native snapshot: %v", err)
	}
	if !saveStatus.Loaded || saveStatus.RootName != collectionVectorIndexRootName("docs", "embedding") || saveStatus.RootID == 0 || saveStatus.ManifestPath != "" {
		t.Fatalf("unexpected native save status: %+v", saveStatus)
	}
	if saveStatus.BytesDisk <= 0 {
		t.Fatalf("native save status missing bytes: %+v", saveStatus)
	}
	if _, err := os.Stat(filepath.Join(dir, vectorIndexDirName)); !os.IsNotExist(err) {
		t.Fatalf("native snapshot created sidecar dir err=%v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection after reopen: %v", err)
	}
	loaded, loadStatus, err := reopenedCol.LoadVectorIndexSnapshot(VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2})
	if err != nil {
		t.Fatalf("load native vector index snapshot: %v", err)
	}
	if loaded == nil || !loadStatus.Loaded || loadStatus.ExactFallbackReason != "" || loadStatus.RootID != saveStatus.RootID {
		t.Fatalf("unexpected native load status loaded=%v status=%+v save=%+v", loaded != nil, loadStatus, saveStatus)
	}
	results, trace, err := loaded.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 2, DisableExactFallback: true})
	if err != nil {
		t.Fatalf("search native-loaded index: %v", err)
	}
	requireVectorResultIDs(t, results, "a", "c")
	if trace.RerankCount == 0 {
		t.Fatalf("native-loaded index did not rerank canonical rows: %+v", trace)
	}
	if _, err := os.Stat(filepath.Join(dir, vectorIndexDirName)); !os.IsNotExist(err) {
		t.Fatalf("native load created sidecar dir err=%v", err)
	}
}

func TestCollectionVectorIndexNativeRootMissingFallsBack(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "docs",
		VectorIndexes: []VectorIndexDefinition{{
			Name:       "embedding",
			Field:      "embedding",
			Metric:     VectorMetricCosine,
			Dimensions: 2,
		}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	loaded, status, err := col.LoadVectorIndexSnapshot(VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2})
	if err != nil {
		t.Fatalf("load missing native snapshot: %v", err)
	}
	if loaded != nil || status.Loaded || status.ExactFallbackReason != vectorIndexFallbackMissingGraphRoot || status.RootName != collectionVectorIndexRootName("docs", "embedding") {
		t.Fatalf("missing native snapshot status loaded=%v status=%+v", loaded != nil, status)
	}
}

func TestCollectionVectorIndexNativeRootIncompleteFallsBack(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	def := VectorIndexDefinition{
		Name:       "embedding",
		Field:      "embedding",
		Metric:     VectorMetricCosine,
		Dimensions: 2,
		M:          4,
	}
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs", VectorIndexes: []VectorIndexDefinition{def}}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("a"), []byte("b")},
		[][]byte{
			[]byte(`{"embedding":[1,0]}`),
			[]byte(`{"embedding":[0,1]}`),
		},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	index, err := col.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("build vector index: %v", err)
	}
	saveStatus, err := index.SaveSnapshot()
	if err != nil {
		t.Fatalf("save native snapshot: %v", err)
	}
	rootName := saveStatus.RootName
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("nil snapshot")
	}
	catalog, err := loadCollectionCatalog(snap, "docs")
	if err != nil {
		_ = snap.Close()
		t.Fatalf("load catalog: %v", err)
	}
	baseRoot := catalog.rootID(rootName)
	baseRootIDs := map[string]uint64{rootName: baseRoot}
	baseSystemRoot := snapshotSystemRoot(snap)
	baseCommitSeq := snapshotCommitSeq(snap)
	table := newCollectionRunTable(1)
	table.DeleteSteal(vectorIndexNativeNodeKey(0))
	table.Freeze()
	iter := table.NewIterator(nil, nil)
	_, _, err = d.PublishOrderedRootDeltaGroupWithSystemDeltaBuilder([]backenddb.OrderedRootDeltaPublishInput{{
		BaseRoot:      baseRoot,
		Iter:          iter,
		StoragePolicy: backenddb.OrderedRootStorageDefault,
	}}, func(rootIDs []uint64) (iterator.UnsafeIterator, error) {
		return col.buildRootDescriptorSystemDeltaIteratorForMeta(catalog.meta, baseCommitSeq, baseSystemRoot, []string{rootName}, baseRootIDs, rootIDs)
	})
	_ = iter.Close()
	resetCollectionRunTable(table)
	_ = snap.Close()
	if err != nil {
		t.Fatalf("publish incomplete graph root: %v", err)
	}
	loaded, status, err := col.LoadVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("load incomplete native snapshot: %v", err)
	}
	if loaded != nil || status.Loaded || status.ExactFallbackReason != vectorIndexFallbackMissingGraphRootEntry {
		t.Fatalf("incomplete native snapshot status loaded=%v status=%+v", loaded != nil, status)
	}
}

func TestCollectionVectorIndexNativeRootMaintainsInsertedDocuments(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := NewCollectionManager(d)
	def := VectorIndexDefinition{
		Name:       "embedding",
		Field:      "embedding",
		Metric:     VectorMetricCosine,
		Dimensions: 2,
		M:          4,
	}
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.CreateVectorIndex(def); err != nil {
		t.Fatalf("create vector index: %v", err)
	}
	indexes := col.registeredVectorIndexes()
	if len(indexes) != 1 {
		t.Fatalf("registered vector indexes=%d want 1", len(indexes))
	}
	if got := collectionManagerHandleCount(mgr); got != 0 {
		t.Fatalf("clean registered vector index retained %d manager handles want 0", got)
	}
	index := indexes[0]
	if _, err := col.InsertBatch(
		[][]byte{[]byte("a"), []byte("b"), []byte("c")},
		[][]byte{
			[]byte(`{"embedding":[1,0]}`),
			[]byte(`{"embedding":[0,1]}`),
			[]byte(`{"embedding":[0.8,0.2]}`),
		},
	); err != nil {
		t.Fatalf("insert after vector index registration: %v", err)
	}
	if dirtyStats := index.Stats(); dirtyStats.LiveDocs != 3 || !dirtyStats.SnapshotDirty {
		t.Fatalf("inserted vector index not dirty before flush: %+v", dirtyStats)
	}
	if got := collectionManagerHandleCount(mgr); got != 1 {
		t.Fatalf("dirty vector index retained %d manager handles want 1", got)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush maintained vector index: %v", err)
	}
	if got := collectionManagerHandleCount(mgr); got != 0 {
		t.Fatalf("clean flushed vector index retained %d manager handles want 0", got)
	}
	if stats := index.Stats(); stats.LiveDocs != 3 || stats.SnapshotDirty {
		t.Fatalf("inserted vector index not maintained and persisted: %+v", stats)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	reopened, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("docs")
	if err != nil {
		t.Fatalf("open reopened collection: %v", err)
	}
	loaded, status, err := reopenedCol.LoadVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("load maintained vector index: %v", err)
	}
	if loaded == nil || !status.Loaded {
		t.Fatalf("maintained vector index did not load loaded=%v status=%+v", loaded != nil, status)
	}
	results, _, err := loaded.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 2, DisableExactFallback: true})
	if err != nil {
		t.Fatalf("search maintained vector index: %v", err)
	}
	requireVectorResultIDs(t, results, "a", "c")
}

func TestCollectionVectorIndexNativeRootCreateOnExistingDocumentsBuildsFullGraphOnWrite(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	def := VectorIndexDefinition{
		Name:       "embedding",
		Field:      "embedding",
		Metric:     VectorMetricCosine,
		Dimensions: 2,
		M:          4,
	}
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("a")},
		[][]byte{[]byte(`{"embedding":[1,0]}`)},
	); err != nil {
		t.Fatalf("insert before vector index registration: %v", err)
	}
	if _, err := col.CreateVectorIndex(def); err != nil {
		t.Fatalf("create vector index: %v", err)
	}
	if got := len(col.registeredVectorIndexes()); got != 0 {
		t.Fatalf("registered vector indexes=%d want 0 for non-empty collection", got)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("b")},
		[][]byte{[]byte(`{"embedding":[0,1]}`)},
	); err != nil {
		t.Fatalf("insert after vector index metadata: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush collection: %v", err)
	}
	loaded, status, err := col.LoadVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("load vector index snapshot: %v", err)
	}
	if loaded == nil || !status.Loaded {
		t.Fatalf("vector index did not load after first write loaded=%v status=%+v", loaded != nil, status)
	}
	results, _, err := loaded.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 2, DisableExactFallback: true})
	if err != nil {
		t.Fatalf("search vector index after first write: %v", err)
	}
	requireVectorResultIDs(t, results, "a", "b")
}

func TestCollectionVectorIndexNativeRootMissingGraphBuildsRuntimeOnWrite(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := NewCollectionManager(d)
	def := VectorIndexDefinition{
		Name:       "embedding",
		Field:      "embedding",
		Metric:     VectorMetricCosine,
		Dimensions: 2,
		M:          4,
	}
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.CreateVectorIndex(def); err != nil {
		t.Fatalf("create vector index: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close before first vector write: %v", err)
	}

	reopened, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("docs")
	if err != nil {
		t.Fatalf("open reopened collection: %v", err)
	}
	if _, status, err := reopenedCol.LoadVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def)); err != nil {
		t.Fatalf("load missing graph root: %v", err)
	} else if status.ExactFallbackReason != vectorIndexFallbackMissingGraphRoot {
		t.Fatalf("missing graph root status=%+v", status)
	}
	if _, err := reopenedCol.InsertBatch(
		[][]byte{[]byte("a")},
		[][]byte{[]byte(`{"embedding":[1,0]}`)},
	); err != nil {
		t.Fatalf("insert after missing graph root: %v", err)
	}
	if err := reopenedCol.Flush(); err != nil {
		t.Fatalf("flush rebuilt missing graph root: %v", err)
	}
	loaded, status, err := reopenedCol.LoadVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("load rebuilt graph root: %v", err)
	}
	if loaded == nil || !status.Loaded {
		t.Fatalf("rebuilt graph root did not load loaded=%v status=%+v", loaded != nil, status)
	}
	results, _, err := loaded.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 1, DisableExactFallback: true})
	if err != nil {
		t.Fatalf("search rebuilt missing graph root: %v", err)
	}
	requireVectorResultIDs(t, results, "a")
}

func TestCollectionVectorIndexNativeRootStaleRuntimeRefreshesAfterRecreate(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	staleCol, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open stale collection: %v", err)
	}
	oldDef := VectorIndexDefinition{
		Name:       "embedding",
		Field:      "embedding",
		Metric:     VectorMetricCosine,
		Dimensions: 2,
		M:          4,
	}
	if _, err := staleCol.CreateVectorIndex(oldDef); err != nil {
		t.Fatalf("create old vector index: %v", err)
	}
	if _, err := staleCol.InsertBatch(
		[][]byte{[]byte("a")},
		[][]byte{[]byte(`{"embedding":[1,0]}`)},
	); err != nil {
		t.Fatalf("insert old vector document: %v", err)
	}
	oldIndex, err := staleCol.BuildVectorIndex(vectorIndexOptionsFromDefinition(oldDef))
	if err != nil {
		t.Fatalf("build old vector index: %v", err)
	}
	if _, err := oldIndex.SaveSnapshot(); err != nil {
		t.Fatalf("save old vector index: %v", err)
	}

	freshCol, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open fresh collection: %v", err)
	}
	if _, err := freshCol.DropVectorIndex("embedding"); err != nil {
		t.Fatalf("drop old vector index: %v", err)
	}
	newDef := VectorIndexDefinition{
		Name:       "embedding",
		Field:      "other",
		Metric:     VectorMetricCosine,
		Dimensions: 3,
		M:          4,
	}
	if _, err := freshCol.CreateVectorIndex(newDef); err != nil {
		t.Fatalf("create replacement vector index: %v", err)
	}
	if _, err := staleCol.InsertBatch(
		[][]byte{[]byte("b")},
		[][]byte{[]byte(`{"other":[1,0,0]}`)},
	); err != nil {
		t.Fatalf("insert through stale runtime after recreate: %v", err)
	}
	if err := staleCol.Flush(); err != nil {
		t.Fatalf("flush stale runtime after recreate: %v", err)
	}
	loaded, status, err := staleCol.LoadVectorIndexSnapshot(vectorIndexOptionsFromDefinition(newDef))
	if err != nil {
		t.Fatalf("load replacement vector index: %v", err)
	}
	if loaded == nil || !status.Loaded {
		t.Fatalf("replacement vector index did not load loaded=%v status=%+v", loaded != nil, status)
	}
	results, _, err := loaded.Search([]float32{1, 0, 0}, VectorIndexSearchOptions{TopK: 1, DisableExactFallback: true})
	if err != nil {
		t.Fatalf("search replacement vector index: %v", err)
	}
	requireVectorResultIDs(t, results, "b")
}

func TestCollectionVectorIndexNativeRootBuildAfterCreatePersistsExistingDocumentsOnFlush(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	def := VectorIndexDefinition{
		Name:       "embedding",
		Field:      "embedding",
		Metric:     VectorMetricCosine,
		Dimensions: 2,
		M:          4,
	}
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("a"), []byte("b")},
		[][]byte{
			[]byte(`{"embedding":[1,0]}`),
			[]byte(`{"embedding":[0,1]}`),
		},
	); err != nil {
		t.Fatalf("insert before vector index registration: %v", err)
	}
	if _, err := col.CreateVectorIndex(def); err != nil {
		t.Fatalf("create vector index: %v", err)
	}
	index, err := col.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("build vector index after metadata create: %v", err)
	}
	if stats := index.Stats(); stats.LiveDocs != 2 || !stats.SnapshotDirty {
		t.Fatalf("built native vector index not marked dirty for first flush: %+v", stats)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush built vector index: %v", err)
	}
	loaded, status, err := col.LoadVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("load flushed built vector index: %v", err)
	}
	if loaded == nil || !status.Loaded {
		t.Fatalf("built vector index did not persist on flush loaded=%v status=%+v", loaded != nil, status)
	}
	results, _, err := loaded.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 2, DisableExactFallback: true})
	if err != nil {
		t.Fatalf("search flushed built vector index: %v", err)
	}
	requireVectorResultIDs(t, results, "a", "b")
}

func TestCollectionVectorIndexNativeRootBuildAfterCreatePersistsExistingDocumentsOnClose(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := NewCollectionManager(d)
	def := VectorIndexDefinition{
		Name:       "embedding",
		Field:      "embedding",
		Metric:     VectorMetricCosine,
		Dimensions: 2,
		M:          4,
	}
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("a"), []byte("b")},
		[][]byte{
			[]byte(`{"embedding":[1,0]}`),
			[]byte(`{"embedding":[0,1]}`),
		},
	); err != nil {
		t.Fatalf("insert before vector index registration: %v", err)
	}
	if _, err := col.CreateVectorIndex(def); err != nil {
		t.Fatalf("create vector index: %v", err)
	}
	index, err := col.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("build vector index after metadata create: %v", err)
	}
	if stats := index.Stats(); stats.LiveDocs != 2 || !stats.SnapshotDirty {
		t.Fatalf("built native vector index not marked dirty for close: %+v", stats)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	reopened, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("docs")
	if err != nil {
		t.Fatalf("open reopened collection: %v", err)
	}
	loaded, status, err := reopenedCol.LoadVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("load close-persisted built vector index: %v", err)
	}
	if loaded == nil || !status.Loaded {
		t.Fatalf("built vector index did not persist on close loaded=%v status=%+v", loaded != nil, status)
	}
	results, _, err := loaded.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 2, DisableExactFallback: true})
	if err != nil {
		t.Fatalf("search close-persisted built vector index: %v", err)
	}
	requireVectorResultIDs(t, results, "a", "b")
}

func TestCollectionVectorIndexNativeRootAdHocRuntimeBecomesDeclaredFullSnapshot(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	def := VectorIndexDefinition{
		Name:       "embedding",
		Field:      "embedding",
		Metric:     VectorMetricCosine,
		Dimensions: 2,
		M:          4,
	}
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("a"), []byte("b")},
		[][]byte{
			[]byte(`{"embedding":[1,0]}`),
			[]byte(`{"embedding":[0,1]}`),
		},
	); err != nil {
		t.Fatalf("insert before vector index metadata: %v", err)
	}
	if _, err := col.BuildVectorIndex(vectorIndexOptionsFromDefinition(def)); err != nil {
		t.Fatalf("build ad hoc vector index: %v", err)
	}
	if _, err := col.CreateVectorIndex(def); err != nil {
		t.Fatalf("create vector index metadata: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush declared ad hoc vector index: %v", err)
	}
	loaded, status, err := col.LoadVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("load declared ad hoc vector index: %v", err)
	}
	if loaded == nil || !status.Loaded {
		t.Fatalf("declared ad hoc vector index did not persist loaded=%v status=%+v", loaded != nil, status)
	}
	results, _, err := loaded.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 2, DisableExactFallback: true})
	if err != nil {
		t.Fatalf("search declared ad hoc vector index: %v", err)
	}
	requireVectorResultIDs(t, results, "a", "b")
}

func TestCollectionVectorIndexNativeRootIgnoresStaleDeclaredAdHocBeforeFirstNativeSave(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	def := VectorIndexDefinition{
		Name:       "embedding",
		Field:      "embedding",
		Metric:     VectorMetricCosine,
		Dimensions: 2,
		M:          4,
	}
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("a"), []byte("b")},
		[][]byte{
			[]byte(`{"embedding":[1,0]}`),
			[]byte(`{"embedding":[0,1]}`),
		},
	); err != nil {
		t.Fatalf("insert before vector index metadata: %v", err)
	}
	stale, err := col.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("build ad hoc vector index: %v", err)
	}
	if _, err := col.CreateVectorIndex(def); err != nil {
		t.Fatalf("create vector index metadata: %v", err)
	}
	rebuild, err := col.RebuildVectorIndex(def.Name)
	if err != nil {
		t.Fatalf("rebuild vector index: %v", err)
	}
	if rebuild.RootID == 0 || rebuild.ExactFallbackReason != "" {
		t.Fatalf("unexpected rebuild status: %+v", rebuild)
	}
	if staleStatus, err := stale.SaveNativeSnapshot(); err != nil || staleStatus.Loaded || staleStatus.ExactFallbackReason != vectorIndexFallbackStaleRuntimeIndex {
		t.Fatalf("stale declared ad hoc native save status=%+v err=%v, want ignored", staleStatus, err)
	}
	status, err := col.VectorIndexStatus(def.Name)
	if err != nil {
		t.Fatalf("status after stale declared ad hoc save: %v", err)
	}
	if status.RootID != rebuild.RootID || status.ExactFallbackReason != "" {
		t.Fatalf("stale declared ad hoc save changed status=%+v rebuild=%+v", status, rebuild)
	}
}

func TestCollectionVectorIndexNativeRootInsertMaintenanceErrorIsCommitAmbiguous(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	def := VectorIndexDefinition{
		Name:       "embedding",
		Field:      "embedding",
		Metric:     VectorMetricCosine,
		Dimensions: 2,
		M:          4,
	}
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.CreateVectorIndex(def); err != nil {
		t.Fatalf("create vector index: %v", err)
	}
	_, err = col.InsertBatch(
		[][]byte{[]byte("bad")},
		[][]byte{[]byte(`{"embedding":[1,0,0]}`)},
	)
	if !errors.Is(err, ErrCommitAmbiguous) {
		t.Fatalf("insert err=%v want ErrCommitAmbiguous", err)
	}
	doc, getErr := col.Get([]byte("bad"))
	if getErr != nil {
		t.Fatalf("get committed document: %v", getErr)
	}
	if len(doc) == 0 {
		t.Fatal("post-commit vector maintenance error lost committed document")
	}
}

func TestCollectionVectorIndexNativeRootInsertReturnsIDOnCommitAmbiguousMaintenanceError(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	def := VectorIndexDefinition{
		Name:       "embedding",
		Field:      "embedding",
		Metric:     VectorMetricCosine,
		Dimensions: 2,
		M:          4,
	}
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.CreateVectorIndex(def); err != nil {
		t.Fatalf("create vector index: %v", err)
	}
	id, err := col.Insert([]byte("bad"), []byte(`{"embedding":[1,0,0]}`))
	if !errors.Is(err, ErrCommitAmbiguous) {
		t.Fatalf("insert err=%v want ErrCommitAmbiguous", err)
	}
	if string(id) != "bad" {
		t.Fatalf("insert id=%q want bad for committed ambiguous insert", id)
	}
	doc, getErr := col.Get(id)
	if getErr != nil {
		t.Fatalf("get committed document: %v", getErr)
	}
	if len(doc) == 0 {
		t.Fatalf("committed document %x missing after ambiguous insert", id)
	}
}

func TestCommitAmbiguousErrorPreservesExistingAmbiguousError(t *testing.T) {
	inner := commitAmbiguousError("flush", errors.New("boom"))
	got := commitAmbiguousError("insert", inner)
	if got != inner {
		t.Fatalf("nested ambiguous error=%v want original %v", got, inner)
	}
	if !errors.Is(got, ErrCommitAmbiguous) {
		t.Fatalf("ambiguous error=%v want ErrCommitAmbiguous", got)
	}
}

func TestDeclaredNativeVectorIndexesLoadedNoDeclaredAdHocIndex(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	index, err := newVectorIndex(col, VectorIndexOptions{Name: "ad_hoc", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2})
	if err != nil {
		t.Fatalf("new vector index: %v", err)
	}
	col.RegisterVectorIndex(index)
	if !col.declaredNativeVectorIndexesLoadedForCurrentCatalog() {
		t.Fatal("ad-hoc registered vector index forced native catalog load with no declared vector indexes")
	}
	index.setNativePersistent(true)
	if col.declaredNativeVectorIndexesLoadedForCurrentCatalog() {
		t.Fatal("native-persistent runtime index without declaration should require reconciliation")
	}
}

func TestCollectionVectorIndexNativeRootMaintainsUpdatedDocument(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	def := VectorIndexDefinition{
		Name:       "embedding",
		Field:      "embedding",
		Metric:     VectorMetricCosine,
		Dimensions: 2,
		M:          4,
	}
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs", VectorIndexes: []VectorIndexDefinition{def}}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("a"), []byte("b")},
		[][]byte{
			[]byte(`{"embedding":[1,0],"name":"a"}`),
			[]byte(`{"embedding":[0,1],"name":"b"}`),
		},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	index, err := col.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("build vector index: %v", err)
	}
	if _, err := index.SaveSnapshot(); err != nil {
		t.Fatalf("save initial vector snapshot: %v", err)
	}
	matched, modified, err := col.Update([]byte("a"), func(current []byte) ([]byte, bool, error) {
		return []byte(`{"embedding":[0,1],"name":"a"}`), true, nil
	})
	if err != nil {
		t.Fatalf("update vector document: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("update matched=%v modified=%v", matched, modified)
	}
	if dirtyStats := index.Stats(); dirtyStats.LiveDocs != 2 || !dirtyStats.SnapshotDirty {
		t.Fatalf("updated vector index not dirty before flush: %+v", dirtyStats)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush updated vector index: %v", err)
	}
	if stats := index.Stats(); stats.LiveDocs != 2 || stats.SnapshotDirty {
		t.Fatalf("updated vector index not maintained and persisted: %+v", stats)
	}
	loaded, status, err := col.LoadVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("load updated vector index: %v", err)
	}
	if loaded == nil || !status.Loaded {
		t.Fatalf("updated vector index did not load loaded=%v status=%+v", loaded != nil, status)
	}
	results, _, err := loaded.Search([]float32{0, 1}, VectorIndexSearchOptions{TopK: 2, DisableExactFallback: true})
	if err != nil {
		t.Fatalf("search updated vector index: %v", err)
	}
	requireVectorResultIDs(t, results, "a", "b")
}

func TestCollectionVectorSearchExactReadsBSONNumericVectors(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "docs",
		Options: CollectionOptions{DocumentFormat: DocumentFormatBSON},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatchValidatedBSON(
		[][]byte{[]byte("a"), []byte("b")},
		[][]byte{
			mustBSONCollectionDocument(t, bson.D{{Key: "embedding", Value: bson.A{int32(1), int64(0)}}, {Key: "name", Value: "a"}}),
			mustBSONCollectionDocument(t, bson.D{{Key: "embedding", Value: bson.A{int32(0), int64(1)}}, {Key: "name", Value: "b"}}),
		},
	); err != nil {
		t.Fatalf("insert BSON docs: %v", err)
	}
	results, err := col.SearchVectorsExact([]float32{0, 1}, VectorSearchOptions{
		Field:  "embedding",
		Metric: VectorMetricCosine,
		TopK:   2,
	})
	if err != nil {
		t.Fatalf("exact BSON vector search: %v", err)
	}
	requireVectorResultIDs(t, results, "b", "a")
}

func TestCollectionVectorIndexNativeRootMaintainsBSONSetUpdate(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	def := VectorIndexDefinition{
		Name:       "embedding",
		Field:      "embedding",
		Metric:     VectorMetricCosine,
		Dimensions: 2,
		M:          4,
	}
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "docs",
		Options: CollectionOptions{DocumentFormat: DocumentFormatBSON},
		VectorIndexes: []VectorIndexDefinition{
			def,
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatchValidatedBSON(
		[][]byte{[]byte("a"), []byte("b")},
		[][]byte{
			mustBSONCollectionDocument(t, bson.D{{Key: "embedding", Value: bson.A{1.0, 0.0}}, {Key: "name", Value: "a"}}),
			mustBSONCollectionDocument(t, bson.D{{Key: "embedding", Value: bson.A{0.0, 1.0}}, {Key: "name", Value: "b"}}),
		},
	); err != nil {
		t.Fatalf("insert BSON docs: %v", err)
	}
	index, err := col.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("build vector index: %v", err)
	}
	if _, err := index.SaveSnapshot(); err != nil {
		t.Fatalf("save initial vector snapshot: %v", err)
	}
	matched, modified, err := col.UpdateBSONSet([]byte("a"), []BSONSetField{{
		Key:   "embedding",
		Value: mustBSONRawValue(t, bson.A{0.0, 1.0}),
	}})
	if err != nil {
		t.Fatalf("UpdateBSONSet: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("UpdateBSONSet matched=%v modified=%v", matched, modified)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush BSON set vector index: %v", err)
	}
	loaded, status, err := col.LoadVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("load BSON set vector index: %v", err)
	}
	if loaded == nil || !status.Loaded {
		t.Fatalf("BSON set vector index did not load loaded=%v status=%+v", loaded != nil, status)
	}
	results, _, err := loaded.Search([]float32{0, 1}, VectorIndexSearchOptions{TopK: 2, DisableExactFallback: true})
	if err != nil {
		t.Fatalf("search BSON set vector index: %v", err)
	}
	requireVectorResultIDs(t, results, "a", "b")
}

func TestCollectionVectorIndexNativeRootMaintainsBSONSetBatchUpdate(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	def := VectorIndexDefinition{
		Name:       "embedding",
		Field:      "embedding",
		Metric:     VectorMetricCosine,
		Dimensions: 2,
		M:          4,
	}
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "docs",
		Options: CollectionOptions{DocumentFormat: DocumentFormatBSON},
		VectorIndexes: []VectorIndexDefinition{
			def,
		},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatchValidatedBSON(
		[][]byte{[]byte("a"), []byte("b")},
		[][]byte{
			mustBSONCollectionDocument(t, bson.D{{Key: "embedding", Value: bson.A{1.0, 0.0}}, {Key: "name", Value: "a"}}),
			mustBSONCollectionDocument(t, bson.D{{Key: "embedding", Value: bson.A{0.0, 1.0}}, {Key: "name", Value: "b"}}),
		},
	); err != nil {
		t.Fatalf("insert BSON docs: %v", err)
	}
	index, err := col.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("build vector index: %v", err)
	}
	if _, err := index.SaveSnapshot(); err != nil {
		t.Fatalf("save initial vector snapshot: %v", err)
	}
	results, batched, err := col.UpdateBSONSetBatchIfNoSecondaryUniqueIndexChanges([]BSONSetUpdateBatchItem{{
		DocumentID: []byte("a"),
		Fields: []BSONSetField{{
			Key:   "embedding",
			Value: mustBSONRawValue(t, bson.A{0.0, 1.0}),
		}},
	}})
	if err != nil {
		t.Fatalf("UpdateBSONSetBatchIfNoSecondaryUniqueIndexChanges: %v", err)
	}
	if !batched || len(results) != 1 || !results[0].Matched || !results[0].Modified {
		t.Fatalf("unexpected BSON set batch result batched=%v results=%+v", batched, results)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush BSON set batch vector index: %v", err)
	}
	loaded, status, err := col.LoadVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("load BSON set batch vector index: %v", err)
	}
	if loaded == nil || !status.Loaded {
		t.Fatalf("BSON set batch vector index did not load loaded=%v status=%+v", loaded != nil, status)
	}
	searchResults, _, err := loaded.Search([]float32{0, 1}, VectorIndexSearchOptions{TopK: 2, DisableExactFallback: true})
	if err != nil {
		t.Fatalf("search BSON set batch vector index: %v", err)
	}
	requireVectorResultIDs(t, searchResults, "a", "b")
}

func TestCollectionVectorIndexNativeRootMaintainsSingleDelete(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	def := VectorIndexDefinition{
		Name:       "embedding",
		Field:      "embedding",
		Metric:     VectorMetricCosine,
		Dimensions: 2,
		M:          4,
	}
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs", VectorIndexes: []VectorIndexDefinition{def}}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("a"), []byte("b"), []byte("c")},
		[][]byte{
			[]byte(`{"embedding":[1,0]}`),
			[]byte(`{"embedding":[0,1]}`),
			[]byte(`{"embedding":[0.9,0.1]}`),
		},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	index, err := col.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("build vector index: %v", err)
	}
	if _, err := index.SaveSnapshot(); err != nil {
		t.Fatalf("save initial vector snapshot: %v", err)
	}
	deleted, err := col.DeleteDocument([]byte("c"))
	if err != nil {
		t.Fatalf("delete vector document: %v", err)
	}
	if !deleted {
		t.Fatal("delete reported missing document")
	}
	if dirtyStats := index.Stats(); dirtyStats.LiveDocs != 2 || !dirtyStats.SnapshotDirty {
		t.Fatalf("deleted vector index not dirty before flush: %+v", dirtyStats)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush deleted vector index: %v", err)
	}
	if stats := index.Stats(); stats.LiveDocs != 2 || stats.SnapshotDirty {
		t.Fatalf("deleted vector index not maintained and persisted: %+v", stats)
	}
	loaded, status, err := col.LoadVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("load deleted vector index: %v", err)
	}
	if loaded == nil || !status.Loaded {
		t.Fatalf("deleted vector index did not load loaded=%v status=%+v", loaded != nil, status)
	}
	results, _, err := loaded.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 3, DisableExactFallback: true})
	if err != nil {
		t.Fatalf("search deleted vector index: %v", err)
	}
	for _, result := range results {
		if string(result.DocumentID) == "c" {
			t.Fatalf("deleted document returned from maintained vector index: %+v", results)
		}
	}
}

func TestCollectionVectorIndexNativeRootDeleteAfterMetadataOnlyCreate(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("a"), []byte("b"), []byte("c")},
		[][]byte{
			[]byte(`{"embedding":[1,0]}`),
			[]byte(`{"embedding":[0,1]}`),
			[]byte(`{"embedding":[0.9,0.1]}`),
		},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	def := VectorIndexDefinition{
		Name:       "embedding",
		Field:      "embedding",
		Metric:     VectorMetricCosine,
		Dimensions: 2,
		M:          4,
	}
	if _, err := col.CreateVectorIndex(def); err != nil {
		t.Fatalf("create vector index: %v", err)
	}

	type deleteResult struct {
		deleted bool
		err     error
	}
	done := make(chan deleteResult, 1)
	go func() {
		deleted, err := col.DeleteDocument([]byte("c"))
		done <- deleteResult{deleted: deleted, err: err}
	}()
	select {
	case got := <-done:
		if got.err != nil {
			t.Fatalf("delete vector document: %v", got.err)
		}
		if !got.deleted {
			t.Fatal("delete reported missing document")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("DeleteDocument deadlocked while building metadata-only vector index")
	}

	if err := col.Flush(); err != nil {
		t.Fatalf("flush vector index: %v", err)
	}
	loaded, status, err := col.LoadVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("load vector index: %v", err)
	}
	if loaded == nil || !status.Loaded {
		t.Fatalf("vector index did not load loaded=%v status=%+v", loaded != nil, status)
	}
	results, _, err := loaded.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 3, DisableExactFallback: true})
	if err != nil {
		t.Fatalf("search vector index: %v", err)
	}
	for _, result := range results {
		if string(result.DocumentID) == "c" {
			t.Fatalf("deleted document returned from maintained vector index: %+v", results)
		}
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}
}

func TestCollectionVectorIndexNativeRootSaveReplacesPriorSnapshot(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	def := VectorIndexDefinition{
		Name:       "embedding",
		Field:      "embedding",
		Metric:     VectorMetricCosine,
		Dimensions: 2,
		M:          4,
	}
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs", VectorIndexes: []VectorIndexDefinition{def}}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("a"), []byte("b"), []byte("c")},
		[][]byte{
			[]byte(`{"embedding":[1,0]}`),
			[]byte(`{"embedding":[0,1]}`),
			[]byte(`{"embedding":[0.7,0.3]}`),
		},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	index, err := col.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("build vector index: %v", err)
	}
	if _, err := index.SaveSnapshot(); err != nil {
		t.Fatalf("save first native snapshot: %v", err)
	}
	if err := col.Delete([]byte("c")); err != nil {
		t.Fatalf("delete c: %v", err)
	}
	reduced, err := col.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("rebuild reduced vector index: %v", err)
	}
	secondStatus, err := reduced.SaveSnapshot()
	if err != nil {
		t.Fatalf("save replacement native snapshot: %v", err)
	}
	if !secondStatus.Loaded || secondStatus.RootID == 0 {
		t.Fatalf("unexpected replacement save status: %+v", secondStatus)
	}
	loaded, status, err := col.LoadVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("load replacement native snapshot: %v", err)
	}
	if loaded == nil || !status.Loaded {
		t.Fatalf("unexpected replacement load status loaded=%v status=%+v", loaded != nil, status)
	}
	if stats := loaded.Stats(); stats.LiveDocs != 2 || stats.Nodes != 2 {
		t.Fatalf("replacement load retained stale graph entries: %+v", stats)
	}
	results, _, err := loaded.Search([]float32{0.7, 0.3}, VectorIndexSearchOptions{TopK: 3, DisableExactFallback: true})
	if err != nil {
		t.Fatalf("search replacement native snapshot: %v", err)
	}
	for _, result := range results {
		if string(result.DocumentID) == "c" {
			t.Fatalf("replacement search returned deleted document: %+v", results)
		}
	}
}

func TestCollectionVectorIndexNativeRootAutoPersistRebuildReplacesPriorSnapshot(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	def := VectorIndexDefinition{
		Name:       "embedding",
		Field:      "embedding",
		Metric:     VectorMetricCosine,
		Dimensions: 2,
		M:          4,
	}
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs", VectorIndexes: []VectorIndexDefinition{def}}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("a"), []byte("b"), []byte("c")},
		[][]byte{
			[]byte(`{"embedding":[1,0]}`),
			[]byte(`{"embedding":[0,1]}`),
			[]byte(`{"embedding":[0.7,0.3]}`),
		},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	index, err := col.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("build vector index: %v", err)
	}
	if _, err := index.SaveSnapshot(); err != nil {
		t.Fatalf("save first native snapshot: %v", err)
	}
	if err := col.Delete([]byte("c")); err != nil {
		t.Fatalf("delete c: %v", err)
	}
	reduced, err := col.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("rebuild reduced vector index: %v", err)
	}
	if stats := reduced.Stats(); stats.LiveDocs != 2 || !stats.SnapshotDirty {
		t.Fatalf("rebuilt vector index should be dirty before flush: %+v", stats)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush rebuilt vector index: %v", err)
	}
	if stats := reduced.Stats(); stats.LiveDocs != 2 || stats.SnapshotDirty {
		t.Fatalf("rebuilt vector index should be clean after flush: %+v", stats)
	}
	loaded, status, err := col.LoadVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("load auto-persisted replacement native snapshot: %v", err)
	}
	if loaded == nil || !status.Loaded {
		t.Fatalf("unexpected replacement load status loaded=%v status=%+v", loaded != nil, status)
	}
	if stats := loaded.Stats(); stats.LiveDocs != 2 || stats.Nodes != 2 {
		t.Fatalf("auto-persisted replacement retained stale graph entries: %+v", stats)
	}
	results, _, err := loaded.Search([]float32{0.7, 0.3}, VectorIndexSearchOptions{TopK: 3, DisableExactFallback: true})
	if err != nil {
		t.Fatalf("search auto-persisted replacement native snapshot: %v", err)
	}
	for _, result := range results {
		if string(result.DocumentID) == "c" {
			t.Fatalf("auto-persisted replacement search returned deleted document: %+v", results)
		}
	}
}

func TestCollectionVectorIndexNativeRootUsesCurrentCatalogForStaleHandles(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	staleCol, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open stale collection handle: %v", err)
	}
	freshCol, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open fresh collection handle: %v", err)
	}
	def := VectorIndexDefinition{
		Name:       "embedding",
		Field:      "embedding",
		Metric:     VectorMetricCosine,
		Dimensions: 2,
		M:          4,
	}
	if _, err := freshCol.CreateVectorIndex(def); err != nil {
		t.Fatalf("create vector index metadata: %v", err)
	}
	if len(staleCol.Meta().VectorIndexes) != 0 {
		t.Fatalf("stale handle unexpectedly saw vector metadata: %+v", staleCol.Meta())
	}
	if _, err := staleCol.InsertBatch(
		[][]byte{[]byte("a"), []byte("b")},
		[][]byte{
			[]byte(`{"embedding":[1,0]}`),
			[]byte(`{"embedding":[0,1]}`),
		},
	); err != nil {
		t.Fatalf("insert through stale handle: %v", err)
	}
	index, err := staleCol.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("build vector index through stale handle: %v", err)
	}
	saveStatus, err := index.SaveSnapshot()
	if err != nil {
		t.Fatalf("save through stale handle: %v", err)
	}
	if !saveStatus.Loaded || saveStatus.ManifestPath != "" || saveStatus.RootID == 0 {
		t.Fatalf("stale handle did not use native root save: %+v", saveStatus)
	}
	loaded, loadStatus, err := staleCol.LoadVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("load through stale handle: %v", err)
	}
	if loaded == nil || !loadStatus.Loaded || loadStatus.RootID != saveStatus.RootID {
		t.Fatalf("stale handle did not use native root load loaded=%v status=%+v save=%+v", loaded != nil, loadStatus, saveStatus)
	}
	if _, err := staleCol.InsertBatch(
		[][]byte{[]byte("c")},
		[][]byte{[]byte(`{"embedding":[0.8,0.2]}`)},
	); err != nil {
		t.Fatalf("insert through stale handle after native save: %v", err)
	}
	if err := staleCol.Flush(); err != nil {
		t.Fatalf("flush stale handle after native save: %v", err)
	}
	reloaded, reloadStatus, err := staleCol.LoadVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("reload through stale handle: %v", err)
	}
	if reloaded == nil || !reloadStatus.Loaded {
		t.Fatalf("stale handle did not persist subsequent dirty graph loaded=%v status=%+v", reloaded != nil, reloadStatus)
	}
	results, _, err := reloaded.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 2, DisableExactFallback: true})
	if err != nil {
		t.Fatalf("search stale handle reloaded vector index: %v", err)
	}
	requireVectorResultIDs(t, results, "a", "c")
}

func TestCollectionVectorIndexNativeRootStaleHandleSingleInsertMaintainsGraph(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	staleCol, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open stale collection handle: %v", err)
	}
	freshCol, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open fresh collection handle: %v", err)
	}
	def := VectorIndexDefinition{
		Name:       "embedding",
		Field:      "embedding",
		Metric:     VectorMetricCosine,
		Dimensions: 2,
		M:          4,
	}
	if _, err := freshCol.CreateVectorIndex(def); err != nil {
		t.Fatalf("create vector index metadata: %v", err)
	}
	if len(staleCol.Meta().VectorIndexes) != 0 {
		t.Fatalf("stale handle unexpectedly saw vector metadata: %+v", staleCol.Meta())
	}
	if _, err := freshCol.InsertBatch(
		[][]byte{[]byte("a"), []byte("b")},
		[][]byte{
			[]byte(`{"embedding":[1,0]}`),
			[]byte(`{"embedding":[0,1]}`),
		},
	); err != nil {
		t.Fatalf("seed vector documents: %v", err)
	}
	index, err := freshCol.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("build vector index: %v", err)
	}
	if _, err := index.SaveSnapshot(); err != nil {
		t.Fatalf("save vector index: %v", err)
	}
	if _, err := staleCol.Insert([]byte("c"), []byte(`{"embedding":[0.9,0.1]}`)); err != nil {
		t.Fatalf("single insert through stale handle: %v", err)
	}
	if err := staleCol.Flush(); err != nil {
		t.Fatalf("flush stale handle: %v", err)
	}
	reloaded, status, err := freshCol.LoadVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("reload vector index: %v", err)
	}
	if reloaded == nil || !status.Loaded {
		t.Fatalf("vector index did not reload after stale insert loaded=%v status=%+v", reloaded != nil, status)
	}
	results, _, err := reloaded.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 2, DisableExactFallback: true})
	if err != nil {
		t.Fatalf("search reloaded vector index: %v", err)
	}
	requireVectorResultIDs(t, results, "a", "c")
}

func TestCollectionVectorIndexNativeRootFlushAllOnClosePersistsDirtyGraph(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := NewCollectionManager(d)
	def := VectorIndexDefinition{
		Name:       "embedding",
		Field:      "embedding",
		Metric:     VectorMetricCosine,
		Dimensions: 2,
		M:          4,
	}
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs"}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.CreateVectorIndex(def); err != nil {
		t.Fatalf("create vector index: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("a"), []byte("b")},
		[][]byte{
			[]byte(`{"embedding":[1,0]}`),
			[]byte(`{"embedding":[0,1]}`),
		},
	); err != nil {
		t.Fatalf("insert dirty vector documents: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	reopened, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("docs")
	if err != nil {
		t.Fatalf("open reopened collection: %v", err)
	}
	loaded, status, err := reopenedCol.LoadVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("load close-flushed vector index: %v", err)
	}
	if loaded == nil || !status.Loaded {
		t.Fatalf("close did not persist dirty vector index loaded=%v status=%+v", loaded != nil, status)
	}
	results, _, err := loaded.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 2, DisableExactFallback: true})
	if err != nil {
		t.Fatalf("search close-flushed vector index: %v", err)
	}
	requireVectorResultIDs(t, results, "a", "b")
}

func TestCollectionVectorIndexNativeRootRebuildPersistsFullSnapshotOnClose(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := NewCollectionManager(d)
	def := VectorIndexDefinition{
		Name:       "embedding",
		Field:      "embedding",
		Metric:     VectorMetricCosine,
		Dimensions: 2,
		M:          4,
	}
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs", VectorIndexes: []VectorIndexDefinition{def}}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("a"), []byte("b"), []byte("c")},
		[][]byte{
			[]byte(`{"embedding":[1,0]}`),
			[]byte(`{"embedding":[0.9,0.1]}`),
			[]byte(`{"embedding":[0,1]}`),
		},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	index, err := col.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("build vector index: %v", err)
	}
	if _, err := index.SaveSnapshot(); err != nil {
		t.Fatalf("save initial native snapshot: %v", err)
	}
	if deleted, err := col.DeleteBatch([][]byte{[]byte("a")}); err != nil || deleted != 1 {
		t.Fatalf("delete a deleted=%d err=%v", deleted, err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush tombstone delta: %v", err)
	}
	if stats := index.Stats(); stats.DeletedDocs == 0 || !stats.RebuildNeeded {
		t.Fatalf("pre-rebuild stats=%+v want tombstone and rebuild-needed", stats)
	}
	if err := index.Rebuild(); err != nil {
		t.Fatalf("rebuild vector index: %v", err)
	}
	if stats := index.Stats(); stats.DeletedDocs != 0 || !stats.SnapshotDirty || stats.Epoch != 0 {
		t.Fatalf("post-rebuild stats=%+v want full snapshot pending", stats)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	reopened, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("docs")
	if err != nil {
		t.Fatalf("open reopened collection: %v", err)
	}
	loaded, status, err := reopenedCol.LoadVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("load rebuilt vector index: %v", err)
	}
	if loaded == nil || !status.Loaded {
		t.Fatalf("rebuilt vector index did not reload loaded=%v status=%+v", loaded != nil, status)
	}
	if stats := loaded.Stats(); stats.DeletedDocs != 0 || stats.LiveDocs != 2 {
		t.Fatalf("reloaded rebuilt stats=%+v want live=2 deleted=0", stats)
	}
	results, _, err := loaded.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 2, DisableExactFallback: true})
	if err != nil {
		t.Fatalf("search rebuilt vector index: %v", err)
	}
	requireVectorResultIDs(t, results, "b", "c")
}

func TestCollectionVectorIndexNativeRootStatusReportsMissingRoot(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	def := VectorIndexDefinition{
		Name:       "embedding",
		Field:      "embedding",
		Metric:     VectorMetricCosine,
		Dimensions: 2,
		M:          4,
	}
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs", VectorIndexes: []VectorIndexDefinition{def}}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	status, err := col.VectorIndexStatus("embedding")
	if err != nil {
		t.Fatalf("vector index status: %v", err)
	}
	if status.NativeRootLoaded || status.ExactFallbackReason != vectorIndexFallbackMissingGraphRoot || !status.RebuildNeeded {
		t.Fatalf("missing root status=%+v want fallback and rebuild-needed", status)
	}
}

func TestCollectionVectorIndexNativeRootRebuildAPIAcceptsEmptyGraph(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	def := VectorIndexDefinition{
		Name:       "embedding",
		Field:      "embedding",
		Metric:     VectorMetricCosine,
		Dimensions: 2,
		M:          4,
	}
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs", VectorIndexes: []VectorIndexDefinition{def}}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	rebuild, err := col.RebuildVectorIndex("embedding")
	if err != nil {
		t.Fatalf("rebuild empty vector index: %v", err)
	}
	if !rebuild.NativeRootLoaded || rebuild.ExactFallbackReason != "" || rebuild.RebuildNeeded {
		t.Fatalf("empty rebuild status=%+v want clean loaded root", rebuild)
	}
	if rebuild.RootID == 0 || rebuild.Stats.LiveDocs != 0 || rebuild.Stats.Nodes != 0 {
		t.Fatalf("empty rebuild stats/root mismatch: %+v", rebuild)
	}
	status, err := col.VectorIndexStatus("embedding")
	if err != nil {
		t.Fatalf("empty vector index status: %v", err)
	}
	if !status.NativeRootLoaded || status.ExactFallbackReason != "" || status.RebuildNeeded {
		t.Fatalf("empty persisted status=%+v want clean loaded root", status)
	}
	loaded, loadStatus, err := col.LoadVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("load empty vector index: %v", err)
	}
	if loaded == nil || !loadStatus.Loaded || loadStatus.ExactFallbackReason != "" || loadStatus.RootID != rebuild.RootID {
		t.Fatalf("empty load loaded=%v status=%+v rebuild=%+v", loaded != nil, loadStatus, rebuild)
	}
	results, trace, err := loaded.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 2, DisableExactFallback: true})
	if err != nil {
		t.Fatalf("search empty vector index: %v", err)
	}
	if len(results) != 0 || trace.ReturnedCount != 0 {
		t.Fatalf("empty search results=%+v trace=%+v", results, trace)
	}
}

func TestCollectionVectorIndexNativeRootRebuildAPIPersistsCleanGraph(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	def := VectorIndexDefinition{
		Name:       "embedding",
		Field:      "embedding",
		Metric:     VectorMetricCosine,
		Dimensions: 2,
		M:          4,
	}
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs", VectorIndexes: []VectorIndexDefinition{def}}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("a"), []byte("b"), []byte("c")},
		[][]byte{
			[]byte(`{"embedding":[1,0]}`),
			[]byte(`{"embedding":[0.9,0.1]}`),
			[]byte(`{"embedding":[0,1]}`),
		},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	index, err := col.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("build vector index: %v", err)
	}
	if _, err := index.SaveSnapshot(); err != nil {
		t.Fatalf("save vector index: %v", err)
	}
	if deleted, err := col.DeleteBatch([][]byte{[]byte("a")}); err != nil || deleted != 1 {
		t.Fatalf("delete a deleted=%d err=%v", deleted, err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush dirty tombstone: %v", err)
	}
	dirty, err := col.VectorIndexStatus("embedding")
	if err != nil {
		t.Fatalf("dirty vector index status: %v", err)
	}
	if dirty.Stats.DeletedDocs == 0 || !dirty.RebuildNeeded {
		t.Fatalf("dirty status=%+v want tombstones and rebuild-needed", dirty)
	}

	rebuild, err := col.RebuildVectorIndex("embedding")
	if err != nil {
		t.Fatalf("rebuild vector index: %v", err)
	}
	if !rebuild.NativeRootLoaded || rebuild.RootID == 0 || rebuild.Stats.LiveDocs != 2 || rebuild.Stats.DeletedDocs != 0 || rebuild.RebuildNeeded {
		t.Fatalf("unexpected rebuild status: %+v", rebuild)
	}
	mgr.collectionsMu.RLock()
	_, registeredForAutoPersist := mgr.collections[col]
	mgr.collectionsMu.RUnlock()
	if !registeredForAutoPersist {
		t.Fatalf("rebuilt native index did not register collection handle for auto-persist")
	}
	clean, err := col.VectorIndexStatus("embedding")
	if err != nil {
		t.Fatalf("clean vector index status: %v", err)
	}
	if !clean.NativeRootLoaded || clean.RebuildNeeded || clean.Stats.DeletedDocs != 0 {
		t.Fatalf("clean status=%+v want loaded non-dirty graph", clean)
	}
	loaded, loadStatus, err := col.LoadVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("load rebuilt vector index: %v", err)
	}
	if loaded == nil || !loadStatus.Loaded || loadStatus.RootID != rebuild.RootID {
		t.Fatalf("unexpected rebuilt load loaded=%v status=%+v rebuild=%+v", loaded != nil, loadStatus, rebuild)
	}
	results, _, err := loaded.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 2, DisableExactFallback: true})
	if err != nil {
		t.Fatalf("search rebuilt vector index: %v", err)
	}
	requireVectorResultIDs(t, results, "b", "c")
}

func TestCollectionVectorIndexNativeRootRebuildIgnoresStaleRuntimeSave(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	def := VectorIndexDefinition{
		Name:       "embedding",
		Field:      "embedding",
		Metric:     VectorMetricCosine,
		Dimensions: 2,
		M:          4,
	}
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs", VectorIndexes: []VectorIndexDefinition{def}}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("a"), []byte("b"), []byte("c")},
		[][]byte{
			[]byte(`{"embedding":[1,0]}`),
			[]byte(`{"embedding":[0.9,0.1]}`),
			[]byte(`{"embedding":[0,1]}`),
		},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	stale, err := col.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("build vector index: %v", err)
	}
	if _, err := stale.SaveSnapshot(); err != nil {
		t.Fatalf("save seed vector index: %v", err)
	}
	if deleted, err := col.DeleteBatch([][]byte{[]byte("a")}); err != nil || deleted != 1 {
		t.Fatalf("delete a deleted=%d err=%v", deleted, err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush dirty tombstone: %v", err)
	}
	rebuild, err := col.RebuildVectorIndex("embedding")
	if err != nil {
		t.Fatalf("rebuild vector index: %v", err)
	}
	if rebuild.RootID == 0 || rebuild.Stats.DeletedDocs != 0 || rebuild.Stats.LiveDocs != 2 {
		t.Fatalf("unexpected rebuild status: %+v", rebuild)
	}

	if staleStatus, err := stale.SaveNativeSnapshot(); err != nil || staleStatus.Loaded || staleStatus.ExactFallbackReason != vectorIndexFallbackStaleRuntimeIndex {
		t.Fatalf("stale runtime native save status=%+v err=%v, want ignored", staleStatus, err)
	}
	stale.TombstoneDocumentID([]byte("b"))
	if staleStatus, err := stale.SaveNativeDeltaSnapshot(); err != nil || staleStatus.Loaded || staleStatus.ExactFallbackReason != vectorIndexFallbackStaleRuntimeIndex {
		t.Fatalf("stale runtime native delta status=%+v err=%v, want ignored", staleStatus, err)
	}
	status, err := col.VectorIndexStatus("embedding")
	if err != nil {
		t.Fatalf("status after stale save: %v", err)
	}
	if status.RootID != rebuild.RootID || status.Stats.DeletedDocs != 0 || status.Stats.LiveDocs != 2 {
		t.Fatalf("status after stale save=%+v rebuild=%+v, want rebuilt root unchanged", status, rebuild)
	}
}

func TestCollectionDropVectorIndexClearsNativeRootStatus(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	def := VectorIndexDefinition{
		Name:       "embedding",
		Field:      "embedding",
		Metric:     VectorMetricCosine,
		Dimensions: 2,
		M:          4,
	}
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs", VectorIndexes: []VectorIndexDefinition{def}}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("a")},
		[][]byte{[]byte(`{"embedding":[1,0]}`)},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	index, err := col.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("build vector index: %v", err)
	}
	if _, err := index.SaveSnapshot(); err != nil {
		t.Fatalf("save vector index: %v", err)
	}
	if _, err := col.DropVectorIndex("embedding"); err != nil {
		t.Fatalf("drop vector index: %v", err)
	}
	if _, err := col.VectorIndexStatus("embedding"); !errors.Is(err, ErrIndexNotFound) {
		t.Fatalf("status dropped vector index err=%v want ErrIndexNotFound", err)
	}
	loaded, status, err := col.LoadNativeVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("load dropped vector index: %v", err)
	}
	if loaded != nil || status.ExactFallbackReason != vectorIndexFallbackMissingVectorIndexMetadata {
		t.Fatalf("dropped vector index load loaded=%v status=%+v want metadata fallback", loaded != nil, status)
	}
}

func TestCollectionVectorIndexNativeRootReopenMaintainsLoadedGraphOnWrite(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := NewCollectionManager(d)
	def := VectorIndexDefinition{
		Name:       "embedding",
		Field:      "embedding",
		Metric:     VectorMetricCosine,
		Dimensions: 2,
		M:          4,
	}
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs", VectorIndexes: []VectorIndexDefinition{def}}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("a"), []byte("b")},
		[][]byte{
			[]byte(`{"embedding":[1,0]}`),
			[]byte(`{"embedding":[0,1]}`),
		},
	); err != nil {
		t.Fatalf("insert initial vector documents: %v", err)
	}
	index, err := col.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("build vector index: %v", err)
	}
	if _, err := index.SaveSnapshot(); err != nil {
		t.Fatalf("save initial vector snapshot: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	reopened, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("docs")
	if err != nil {
		t.Fatalf("open reopened collection: %v", err)
	}
	if got := len(reopenedCol.registeredVectorIndexes()); got != 0 {
		t.Fatalf("reopened collection eagerly registered %d vector indexes want 0", got)
	}
	if _, err := reopenedCol.InsertBatch(
		[][]byte{[]byte("c")},
		[][]byte{[]byte(`{"embedding":[0.9,0.1]}`)},
	); err != nil {
		t.Fatalf("insert after reopen: %v", err)
	}
	if got := len(reopenedCol.registeredVectorIndexes()); got != 1 {
		t.Fatalf("write did not lazily load persisted vector index, got %d registered indexes", got)
	}
	if err := reopenedCol.Flush(); err != nil {
		t.Fatalf("flush reopened vector write: %v", err)
	}
	loaded, status, err := reopenedCol.LoadVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("load maintained vector index: %v", err)
	}
	if loaded == nil || !status.Loaded {
		t.Fatalf("maintained vector index did not load loaded=%v status=%+v", loaded != nil, status)
	}
	results, _, err := loaded.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 3, DisableExactFallback: true})
	if err != nil {
		t.Fatalf("search maintained vector index: %v", err)
	}
	requireVectorResultIDs(t, results, "a", "c", "b")
}

func TestCollectionVectorIndexNativeDeltaRejectsStalePersistedRoot(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	def := VectorIndexDefinition{
		Name:       "embedding",
		Field:      "embedding",
		Metric:     VectorMetricCosine,
		Dimensions: 2,
		M:          4,
	}
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs", VectorIndexes: []VectorIndexDefinition{def}}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	seedCol, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open seed collection: %v", err)
	}
	if _, err := seedCol.InsertBatch(
		[][]byte{[]byte("a"), []byte("b")},
		[][]byte{
			[]byte(`{"embedding":[1,0]}`),
			[]byte(`{"embedding":[0,1]}`),
		},
	); err != nil {
		t.Fatalf("insert seed vectors: %v", err)
	}
	seedIndex, err := seedCol.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("build seed vector index: %v", err)
	}
	seedStatus, err := seedIndex.SaveSnapshot()
	if err != nil {
		t.Fatalf("save seed vector index: %v", err)
	}

	handleA, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open handle A: %v", err)
	}
	if _, statusA, err := handleA.LoadVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def)); err != nil {
		t.Fatalf("load handle A vector index: %v", err)
	} else if !statusA.Loaded || statusA.RootID != seedStatus.RootID {
		t.Fatalf("handle A load status=%+v seed=%+v", statusA, seedStatus)
	}
	handleB, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open handle B: %v", err)
	}
	if _, statusB, err := handleB.LoadVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def)); err != nil {
		t.Fatalf("load handle B vector index: %v", err)
	} else if !statusB.Loaded || statusB.RootID != seedStatus.RootID {
		t.Fatalf("handle B load status=%+v seed=%+v", statusB, seedStatus)
	}

	if _, err := handleA.InsertBatch(
		[][]byte{[]byte("c")},
		[][]byte{[]byte(`{"embedding":[0.9,0.1]}`)},
	); err != nil {
		t.Fatalf("insert through handle A: %v", err)
	}
	if err := handleA.Flush(); err != nil {
		t.Fatalf("flush handle A: %v", err)
	}
	if _, statusA2, err := handleA.LoadVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def)); err != nil {
		t.Fatalf("reload handle A vector index: %v", err)
	} else if !statusA2.Loaded || statusA2.RootID == seedStatus.RootID {
		t.Fatalf("handle A did not advance persisted root status=%+v seed=%+v", statusA2, seedStatus)
	}
	staleIndex := handleB.registeredVectorIndex(def.Name)
	if staleIndex == nil {
		t.Fatal("handle B did not keep loaded vector index registered")
	}
	if _, err := staleIndex.SaveNativeSnapshot(); !errors.Is(err, errVectorIndexStaleNativeRoot) {
		t.Fatalf("stale full snapshot save err=%v want stale native root", err)
	}

	if _, err := handleB.InsertBatch(
		[][]byte{[]byte("d")},
		[][]byte{[]byte(`{"embedding":[0.8,0.2]}`)},
	); err != nil {
		t.Fatalf("insert through handle B: %v", err)
	}
	if err := handleB.Flush(); !errors.Is(err, errVectorIndexStaleNativeRoot) {
		t.Fatalf("flush handle B err=%v want stale native root", err)
	}
	loaded, status, err := handleA.LoadVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("load graph after stale rejection: %v", err)
	}
	if loaded == nil || !status.Loaded {
		t.Fatalf("graph did not load after stale rejection loaded=%v status=%+v", loaded != nil, status)
	}
	results, _, err := loaded.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 3, DisableExactFallback: true})
	if err != nil {
		t.Fatalf("search graph after stale rejection: %v", err)
	}
	requireVectorResultIDs(t, results, "a", "c", "b")
}

func TestCollectionVectorIndexNativeFullSaveRejectsDroppedRecreatedRoot(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	def := VectorIndexDefinition{
		Name:       "embedding",
		Field:      "embedding",
		Metric:     VectorMetricCosine,
		Dimensions: 2,
		M:          4,
	}
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs", VectorIndexes: []VectorIndexDefinition{def}}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	seedCol, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open seed collection: %v", err)
	}
	if _, err := seedCol.InsertBatch(
		[][]byte{[]byte("a")},
		[][]byte{[]byte(`{"embedding":[1,0]}`)},
	); err != nil {
		t.Fatalf("insert seed vector: %v", err)
	}
	seedIndex, err := seedCol.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("build seed vector index: %v", err)
	}
	seedStatus, err := seedIndex.SaveSnapshot()
	if err != nil {
		t.Fatalf("save seed vector index: %v", err)
	}

	staleCol, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open stale collection: %v", err)
	}
	if _, status, err := staleCol.LoadVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def)); err != nil {
		t.Fatalf("load stale vector index: %v", err)
	} else if !status.Loaded || status.RootID != seedStatus.RootID {
		t.Fatalf("stale load status=%+v seed=%+v", status, seedStatus)
	}

	freshCol, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open fresh collection: %v", err)
	}
	if _, err := freshCol.DropVectorIndex("embedding"); err != nil {
		t.Fatalf("drop vector index: %v", err)
	}
	if _, err := freshCol.CreateVectorIndex(def); err != nil {
		t.Fatalf("recreate vector index: %v", err)
	}
	if _, status, err := freshCol.LoadVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def)); err != nil {
		t.Fatalf("load recreated empty root: %v", err)
	} else if status.ExactFallbackReason != vectorIndexFallbackMissingGraphRoot {
		t.Fatalf("recreated vector index status=%+v want missing graph root", status)
	}

	staleIndex := staleCol.registeredVectorIndex(def.Name)
	if staleIndex == nil {
		t.Fatal("stale collection did not keep loaded vector index registered")
	}
	if _, err := staleIndex.SaveNativeSnapshot(); !errors.Is(err, errVectorIndexStaleNativeRoot) {
		t.Fatalf("stale full snapshot save err=%v want stale native root", err)
	}
	if _, status, err := freshCol.LoadVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def)); err != nil {
		t.Fatalf("reload recreated empty root: %v", err)
	} else if status.ExactFallbackReason != vectorIndexFallbackMissingGraphRoot {
		t.Fatalf("stale save changed recreated vector index status=%+v", status)
	}
}

func TestCollectionVectorIndexSnapshotLoadsLegacyEdgesWithoutDistances(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	col := openVectorIndexTestCollection(t, d)
	if _, err := col.InsertBatch(
		[][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")},
		[][]byte{
			[]byte(`{"embedding":[1,0]}`),
			[]byte(`{"embedding":[0.8,0.2]}`),
			[]byte(`{"embedding":[0,1]}`),
			[]byte(`{"embedding":[0.2,0.8]}`),
		},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	index, err := col.BuildVectorIndex(VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, M: 4})
	if err != nil {
		t.Fatalf("build vector index: %v", err)
	}
	saveStatus, err := index.SaveSnapshot()
	if err != nil {
		t.Fatalf("save snapshot: %v", err)
	}
	rewriteVectorIndexSnapshotJSONFile(t, saveStatus.ManifestPath, vectorIndexEdgesFile, func(edges []vectorIndexPersistEdges) []vectorIndexPersistEdges {
		for i := range edges {
			edges[i].Distances = nil
		}
		return edges
	})
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection after reopen: %v", err)
	}
	loaded, loadStatus, err := reopenedCol.LoadVectorIndexSnapshot(VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine})
	if err != nil {
		t.Fatalf("load legacy vector index snapshot: %v", err)
	}
	if loaded == nil || !loadStatus.Loaded || loadStatus.ExactFallbackReason != "" {
		t.Fatalf("unexpected legacy load status loaded=%v status=%+v", loaded != nil, loadStatus)
	}
	results, _, err := loaded.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 2, DisableExactFallback: true})
	if err != nil {
		t.Fatalf("search legacy-loaded index: %v", err)
	}
	requireVectorResultIDs(t, results, "a", "b")
}

func TestCollectionVectorIndexSnapshotRecomputesShortEdgeDistances(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	col := openVectorIndexTestCollection(t, d)
	if _, err := col.InsertBatch(
		[][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")},
		[][]byte{
			[]byte(`{"embedding":[1,0]}`),
			[]byte(`{"embedding":[0.8,0.2]}`),
			[]byte(`{"embedding":[0,1]}`),
			[]byte(`{"embedding":[0.2,0.8]}`),
		},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	index, err := col.BuildVectorIndex(VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, M: 4})
	if err != nil {
		t.Fatalf("build vector index: %v", err)
	}
	saveStatus, err := index.SaveSnapshot()
	if err != nil {
		t.Fatalf("save snapshot: %v", err)
	}
	rewriteVectorIndexSnapshotJSONFile(t, saveStatus.ManifestPath, vectorIndexEdgesFile, func(edges []vectorIndexPersistEdges) []vectorIndexPersistEdges {
		for i := range edges {
			if len(edges[i].Distances) > 0 {
				edges[i].Distances = edges[i].Distances[:len(edges[i].Distances)-1]
				break
			}
		}
		return edges
	})
	loaded, loadStatus, err := col.LoadVectorIndexSnapshot(VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine})
	if err != nil {
		t.Fatalf("load short-distance snapshot: %v", err)
	}
	if loaded == nil || !loadStatus.Loaded || loadStatus.ExactFallbackReason != "" {
		t.Fatalf("unexpected short-distance load status loaded=%v status=%+v", loaded != nil, loadStatus)
	}
	results, _, err := loaded.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 2, DisableExactFallback: true})
	if err != nil {
		t.Fatalf("search short-distance loaded index: %v", err)
	}
	requireVectorResultIDs(t, results, "a", "b")
}

func TestVectorIndexLoadPersistSnapshotRecomputesNonFiniteEdgeDistances(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	col := openVectorIndexTestCollection(t, d)
	if _, err := col.InsertBatch(
		[][]byte{[]byte("a"), []byte("b"), []byte("c")},
		[][]byte{
			[]byte(`{"embedding":[1,0]}`),
			[]byte(`{"embedding":[0.8,0.2]}`),
			[]byte(`{"embedding":[0,1]}`),
		},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	index, err := col.BuildVectorIndex(VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, M: 4})
	if err != nil {
		t.Fatalf("build vector index: %v", err)
	}
	snapshot, _ := index.persistSnapshot()
	for i := range snapshot.Edges {
		if len(snapshot.Edges[i].Distances) > 0 {
			snapshot.Edges[i].Distances[0] = float32(math.Inf(1))
			break
		}
	}
	loaded, err := newVectorIndex(col, VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine})
	if err != nil {
		t.Fatalf("new vector index: %v", err)
	}
	if reason := loaded.loadPersistSnapshot(snapshot); reason != "" {
		t.Fatalf("load snapshot with non-finite distance reason=%q", reason)
	}
	results, _, err := loaded.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 2, DisableExactFallback: true})
	if err != nil {
		t.Fatalf("search non-finite-distance loaded index: %v", err)
	}
	requireVectorResultIDs(t, results, "a", "b")
}

func TestVectorIndexLoadPersistSnapshotRejectsExtraEdgeDistances(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	col := openVectorIndexTestCollection(t, d)
	if _, err := col.InsertBatch(
		[][]byte{[]byte("a"), []byte("b"), []byte("c")},
		[][]byte{
			[]byte(`{"embedding":[1,0]}`),
			[]byte(`{"embedding":[0.8,0.2]}`),
			[]byte(`{"embedding":[0,1]}`),
		},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	index, err := col.BuildVectorIndex(VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, M: 4})
	if err != nil {
		t.Fatalf("build vector index: %v", err)
	}
	snapshot, _ := index.persistSnapshot()
	for i := range snapshot.Edges {
		if len(snapshot.Edges[i].Neighbor) > 0 {
			snapshot.Edges[i].Distances = append(snapshot.Edges[i].Distances, 0)
			break
		}
	}
	loaded, err := newVectorIndex(col, VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine})
	if err != nil {
		t.Fatalf("new vector index: %v", err)
	}
	if reason := loaded.loadPersistSnapshot(snapshot); reason != "invalid_edge_distance_count" {
		t.Fatalf("load snapshot reason=%q want invalid_edge_distance_count", reason)
	}
}

func TestVectorIndexPersistSnapshotClampsNegativeInfiniteEdgeDistances(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	col := openVectorIndexTestCollection(t, d)
	if _, err := col.InsertBatch(
		[][]byte{[]byte("a"), []byte("b"), []byte("c")},
		[][]byte{
			[]byte(`{"embedding":[1,0]}`),
			[]byte(`{"embedding":[0.8,0.2]}`),
			[]byte(`{"embedding":[0,1]}`),
		},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	index, err := col.BuildVectorIndex(VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, M: 4})
	if err != nil {
		t.Fatalf("build vector index: %v", err)
	}
	index.mu.Lock()
	for i := range index.nodes {
		for layer := range index.nodes[i].neighbors {
			if len(index.nodes[i].neighbors[layer]) > 0 {
				index.nodes[i].neighbors[layer][0].distance = float32(math.Inf(-1))
				index.mu.Unlock()
				snapshot, _ := index.persistSnapshot()
				for _, edge := range snapshot.Edges {
					for _, distance := range edge.Distances {
						if math.IsInf(float64(distance), 0) || math.IsNaN(float64(distance)) {
							t.Fatalf("persisted non-finite edge distance: %v", distance)
						}
					}
				}
				if _, err := json.Marshal(snapshot.Edges); err != nil {
					t.Fatalf("marshal snapshot edges: %v", err)
				}
				return
			}
		}
	}
	index.mu.Unlock()
	t.Fatal("index has no edge distances to mutate")
}

func TestCollectionVectorIndexSnapshotInt8Encoding(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	col := openVectorIndexTestCollection(t, d)
	if _, err := col.InsertBatch(
		[][]byte{[]byte("a"), []byte("b"), []byte("c")},
		[][]byte{
			[]byte(`{"embedding":[1,0,0,0]}`),
			[]byte(`{"embedding":[0,1,0,0]}`),
			[]byte(`{"embedding":[0.9,0.1,0,0]}`),
		},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	index, err := col.BuildVectorIndex(VectorIndexOptions{
		Name:     "embedding_i8",
		Field:    "embedding",
		Metric:   VectorMetricCosine,
		M:        4,
		Encoding: VectorIndexEncodingInt8,
	})
	if err != nil {
		t.Fatalf("build int8 vector index: %v", err)
	}
	saveStatus, err := index.SaveSnapshot()
	if err != nil {
		t.Fatalf("save int8 snapshot: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection after reopen: %v", err)
	}
	loaded, status, err := reopenedCol.LoadVectorIndexSnapshot(VectorIndexOptions{
		Name:     "embedding_i8",
		Field:    "embedding",
		Metric:   VectorMetricCosine,
		Encoding: VectorIndexEncodingInt8,
	})
	if err != nil {
		t.Fatalf("load int8 vector index snapshot: %v", err)
	}
	if loaded == nil || !status.Loaded || status.Epoch != saveStatus.Epoch {
		t.Fatalf("unexpected int8 load status loaded=%v status=%+v", loaded != nil, status)
	}
	if stats := loaded.Stats(); stats.Encoding != VectorIndexEncodingInt8 || stats.BytesDisk <= 0 || stats.SnapshotDirty {
		t.Fatalf("unexpected loaded int8 stats: %+v", stats)
	}
	results, trace, err := loaded.Search([]float32{1, 0, 0, 0}, VectorIndexSearchOptions{TopK: 2, DisableExactFallback: true})
	if err != nil {
		t.Fatalf("search loaded int8 index: %v", err)
	}
	requireVectorResultIDs(t, results, "a", "c")
	if trace.RerankCount == 0 {
		t.Fatalf("loaded int8 index did not rerank canonical rows: %+v", trace)
	}

	mismatched, mismatchStatus, err := reopenedCol.LoadVectorIndexSnapshot(VectorIndexOptions{
		Name:   "embedding_i8",
		Field:  "embedding",
		Metric: VectorMetricCosine,
	})
	if err != nil {
		t.Fatalf("load int8 snapshot as float32: %v", err)
	}
	if mismatched != nil || mismatchStatus.Loaded || mismatchStatus.ExactFallbackReason != "manifest_encoding_mismatch" {
		t.Fatalf("expected encoding mismatch fallback loaded=%v status=%+v", mismatched != nil, mismatchStatus)
	}
}

func TestCollectionVectorIndexSnapshotMissingManifestFallsBack(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	col := openVectorIndexTestCollection(t, d)
	loaded, status, err := col.LoadVectorIndexSnapshot(VectorIndexOptions{Name: "missing", Field: "embedding", Metric: VectorMetricCosine})
	if err != nil {
		t.Fatalf("load missing snapshot: %v", err)
	}
	if loaded != nil || status.Loaded || status.ExactFallbackReason != "missing_manifest" {
		t.Fatalf("missing snapshot status loaded=%v status=%+v", loaded != nil, status)
	}
}

func TestCollectionVectorIndexSnapshotChecksumMismatchFallsBackToExact(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	col := openVectorIndexTestCollection(t, d)
	if _, err := col.InsertBatch(
		[][]byte{[]byte("a"), []byte("b")},
		[][]byte{
			[]byte(`{"embedding":[1,0]}`),
			[]byte(`{"embedding":[0,1]}`),
		},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	index, err := col.BuildVectorIndex(VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine})
	if err != nil {
		t.Fatalf("build vector index: %v", err)
	}
	saveStatus, err := index.SaveSnapshot()
	if err != nil {
		t.Fatalf("save snapshot: %v", err)
	}
	corruptVectorIndexSnapshotFile(t, saveStatus.ManifestPath, vectorIndexNodesFile)
	if err := d.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection after reopen: %v", err)
	}
	loaded, status, err := reopenedCol.LoadVectorIndexSnapshot(VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine})
	if err != nil {
		t.Fatalf("load corrupt snapshot: %v", err)
	}
	if loaded != nil || status.Loaded || status.ExactFallbackReason != "file_checksum_mismatch" {
		t.Fatalf("corrupt snapshot status loaded=%v status=%+v", loaded != nil, status)
	}
	exact, err := reopenedCol.SearchVectorsExact([]float32{1, 0}, VectorSearchOptions{
		Field:  "embedding",
		Metric: VectorMetricCosine,
		TopK:   1,
	})
	if err != nil {
		t.Fatalf("exact fallback search: %v", err)
	}
	requireVectorResultIDs(t, exact, "a")
}

func TestCollectionVectorIndexSnapshotIncompleteEpochFallsBack(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	col := openVectorIndexTestCollection(t, d)
	if _, err := col.InsertBatch(
		[][]byte{[]byte("a")},
		[][]byte{[]byte(`{"embedding":[1,0]}`)},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	index, err := col.BuildVectorIndex(VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine})
	if err != nil {
		t.Fatalf("build vector index: %v", err)
	}
	saveStatus, err := index.SaveSnapshot()
	if err != nil {
		t.Fatalf("save snapshot: %v", err)
	}
	deleteVectorIndexSnapshotFile(t, saveStatus.ManifestPath, vectorIndexEdgesFile)
	loaded, status, err := col.LoadVectorIndexSnapshot(VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine})
	if err != nil {
		t.Fatalf("load incomplete snapshot: %v", err)
	}
	if loaded != nil || status.Loaded || status.ExactFallbackReason != "missing_epoch_file" {
		t.Fatalf("incomplete snapshot status loaded=%v status=%+v", loaded != nil, status)
	}
}

func TestCollectionVectorIndexSnapshotManifestCountMismatchFallsBack(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	col := openVectorIndexTestCollection(t, d)
	if _, err := col.InsertBatch(
		[][]byte{[]byte("a"), []byte("b")},
		[][]byte{
			[]byte(`{"embedding":[1,0]}`),
			[]byte(`{"embedding":[0,1]}`),
		},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	index, err := col.BuildVectorIndex(VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine})
	if err != nil {
		t.Fatalf("build vector index: %v", err)
	}
	saveStatus, err := index.SaveSnapshot()
	if err != nil {
		t.Fatalf("save snapshot: %v", err)
	}
	rewriteVectorIndexManifest(t, saveStatus.ManifestPath, func(manifest *vectorIndexManifest) {
		manifest.NodeCount++
	})

	loaded, status, err := col.LoadVectorIndexSnapshot(VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine})
	if err != nil {
		t.Fatalf("load mismatched snapshot: %v", err)
	}
	if loaded != nil || status.Loaded || status.ExactFallbackReason != "manifest_node_count_mismatch" {
		t.Fatalf("mismatched snapshot status loaded=%v status=%+v", loaded != nil, status)
	}
}

func TestCollectionVectorIndexPruneOldSnapshots(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	col := openVectorIndexTestCollection(t, d)
	if _, err := col.InsertBatch(
		[][]byte{[]byte("a"), []byte("b")},
		[][]byte{
			[]byte(`{"embedding":[1,0]}`),
			[]byte(`{"embedding":[0,1]}`),
		},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	index, err := col.BuildVectorIndex(VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine})
	if err != nil {
		t.Fatalf("build vector index: %v", err)
	}
	first, err := index.SaveSnapshot()
	if err != nil {
		t.Fatalf("save first snapshot: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("c")},
		[][]byte{[]byte(`{"embedding":[0.9,0.1]}`)},
	); err != nil {
		t.Fatalf("insert after first snapshot: %v", err)
	}
	second, err := index.SaveSnapshot()
	if err != nil {
		t.Fatalf("save second snapshot: %v", err)
	}
	if first.Epoch == second.Epoch {
		t.Fatalf("snapshot epochs collided: first=%+v second=%+v", first, second)
	}
	epochs, err := vectorIndexEpochDirs(filepath.Dir(second.ManifestPath))
	if err != nil {
		t.Fatalf("list epochs: %v", err)
	}
	if len(epochs) != 2 {
		t.Fatalf("epochs before prune=%v want 2", epochs)
	}
	pruned, err := index.PruneOldSnapshots(1)
	if err != nil {
		t.Fatalf("prune old snapshots: %v", err)
	}
	if pruned.RemovedEpochs != 1 || pruned.RemovedBytes <= 0 || pruned.ActiveEpoch == "" {
		t.Fatalf("unexpected prune status: %+v", pruned)
	}
	epochs, err = vectorIndexEpochDirs(filepath.Dir(second.ManifestPath))
	if err != nil {
		t.Fatalf("list epochs after prune: %v", err)
	}
	if len(epochs) != 1 || epochs[0] != pruned.ActiveEpoch {
		t.Fatalf("epochs after prune=%v status=%+v", epochs, pruned)
	}
	loaded, status, err := col.LoadVectorIndexSnapshot(VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine})
	if err != nil {
		t.Fatalf("load after prune: %v", err)
	}
	if loaded == nil || !status.Loaded || status.Epoch != second.Epoch {
		t.Fatalf("unexpected load after prune loaded=%v status=%+v", loaded != nil, status)
	}
}

func corruptVectorIndexSnapshotFile(tb testing.TB, manifestPath, fileName string) {
	tb.Helper()
	path := vectorIndexSnapshotFilePath(tb, manifestPath, fileName)
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		tb.Fatalf("open snapshot file for corruption: %v", err)
	}
	if _, err := f.WriteAt([]byte{' '}, 0); err != nil {
		_ = f.Close()
		tb.Fatalf("corrupt snapshot file: %v", err)
	}
	if err := f.Close(); err != nil {
		tb.Fatalf("close corrupted snapshot file: %v", err)
	}
}

func deleteVectorIndexSnapshotFile(tb testing.TB, manifestPath, fileName string) {
	tb.Helper()
	if err := os.Remove(vectorIndexSnapshotFilePath(tb, manifestPath, fileName)); err != nil {
		tb.Fatalf("delete snapshot file: %v", err)
	}
}

func rewriteVectorIndexManifest(tb testing.TB, manifestPath string, rewrite func(*vectorIndexManifest)) {
	tb.Helper()
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		tb.Fatalf("read manifest: %v", err)
	}
	var manifest vectorIndexManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		tb.Fatalf("decode manifest: %v", err)
	}
	rewrite(&manifest)
	data, err = json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		tb.Fatalf("encode manifest: %v", err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(manifestPath, data, 0o644); err != nil {
		tb.Fatalf("write manifest: %v", err)
	}
}

func rewriteVectorIndexSnapshotJSONFile[T any](tb testing.TB, manifestPath, fileName string, rewrite func(T) T) {
	tb.Helper()
	path := vectorIndexSnapshotFilePath(tb, manifestPath, fileName)
	data, err := os.ReadFile(path)
	if err != nil {
		tb.Fatalf("read snapshot file: %v", err)
	}
	var payload T
	if err := json.Unmarshal(data, &payload); err != nil {
		tb.Fatalf("decode snapshot file: %v", err)
	}
	data, err = json.MarshalIndent(rewrite(payload), "", "  ")
	if err != nil {
		tb.Fatalf("encode snapshot file: %v", err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(path, data, 0o644); err != nil {
		tb.Fatalf("write snapshot file: %v", err)
	}
	sum := sha256.Sum256(data)
	rewriteVectorIndexManifest(tb, manifestPath, func(manifest *vectorIndexManifest) {
		for i := range manifest.Files {
			if manifest.Files[i].Name == fileName {
				manifest.Files[i].Size = int64(len(data))
				manifest.Files[i].SHA256 = hex.EncodeToString(sum[:])
				return
			}
		}
		tb.Fatalf("manifest missing file entry %q", fileName)
	})
}

func vectorIndexSnapshotFilePath(tb testing.TB, manifestPath, fileName string) string {
	tb.Helper()
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		tb.Fatalf("read manifest: %v", err)
	}
	var manifest vectorIndexManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		tb.Fatalf("decode manifest: %v", err)
	}
	return filepath.Join(filepath.Dir(manifestPath), manifest.EpochDir, fileName)
}
