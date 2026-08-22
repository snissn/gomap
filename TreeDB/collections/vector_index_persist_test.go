package collections

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
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

func TestCollectionVectorIndexNativeRootRejectsCommandWALReplayBeyondGraph(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{
		Dir:                    dir,
		Durability:             backenddb.DurabilityWALOffRelaxed,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatalf("open setup db: %v", err)
	}
	mgr := NewCollectionManager(d)
	def := VectorIndexDefinition{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2, M: 4}
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs", VectorIndexes: []VectorIndexDefinition{def}}); err != nil {
		_ = d.Close()
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		_ = d.Close()
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("a")}, [][]byte{[]byte(`{"embedding":[1,0]}`)}); err != nil {
		_ = d.Close()
		t.Fatalf("insert seed: %v", err)
	}
	index, err := col.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		_ = d.Close()
		t.Fatalf("build vector index: %v", err)
	}
	if status, err := index.SaveNativeSnapshot(); err != nil || !status.Loaded {
		_ = d.Close()
		t.Fatalf("save native snapshot: status=%+v err=%v", status, err)
	}
	if err := d.Checkpoint(); err != nil {
		_ = d.Close()
		t.Fatalf("checkpoint setup db: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close setup db: %v", err)
	}
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("enable command WAL: %v", err)
	}
	payload, err := commitlog.EncodeCollectionInsertBatchByIDPayload("docs", []commitlog.CollectionDocument{{
		ID:       []byte("b"),
		Document: []byte(`{"embedding":[0,1]}`),
	}})
	if err != nil {
		t.Fatalf("encode replay insert: %v", err)
	}
	writeCollectionCommandWALFrame(t, dir, 1, commitlog.CommandKindCollectionInsertBatchByID, commitlog.PayloadFormatCollectionInsertBatchByIDV1, payload)

	reopened := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("docs")
	if err != nil {
		t.Fatalf("open replayed collection: %v", err)
	}
	var searchBuffer VectorIndexSearchBuffer
	response, err := reopenedCol.SearchVectorIndexWithBuffer(VectorIndexSearchOptions{
		IndexName: def.Name,
		Query:     []float32{0, 1},
		TopK:      1,
		StatsMode: VectorIndexSearchStatsModeProduction,
	}, &searchBuffer)
	if !errors.Is(err, ErrVectorIndexSearchUnavailable) || response.Status.ExactFallbackReason != vectorIndexFallbackStaleDocumentRoot {
		t.Fatalf("stale native search response=%+v err=%v", response, err)
	}
	loaded, status, err := reopenedCol.LoadNativeVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("load stale native snapshot: %v", err)
	}
	if loaded != nil || status.Loaded || status.ExactFallbackReason != vectorIndexFallbackStaleDocumentRoot {
		t.Fatalf("stale native snapshot loaded=%v status=%+v", loaded != nil, status)
	}
	rebuilt, err := reopenedCol.ensureDeclaredNativeVectorIndexesLoaded()
	if err != nil {
		t.Fatalf("rebuild stale native snapshot: %v", err)
	}
	if _, ok := rebuilt[def.Name]; !ok {
		t.Fatalf("rebuilt indexes=%v want %q", rebuilt, def.Name)
	}
	current := reopenedCol.registeredVectorIndex(def.Name)
	if current == nil {
		t.Fatal("rebuilt native index is nil")
	}
	if stats := current.Stats(); stats.LiveANNFullRebuilds != 1 {
		t.Fatalf("rebuilt native stats=%+v", stats)
	}
	results, _, err := current.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 2, DisableExactFallback: true})
	if err != nil {
		t.Fatalf("search rebuilt native index: %v", err)
	}
	requireVectorResultIDs(t, results, "a", "b")
}

func TestCollectionVectorIndexNativeRootLiveDeltaReopensWithoutRebuild(t *testing.T) {
	dir := t.TempDir()
	d := openCollectionCommandWALDB(t, dir)
	mgr := NewCollectionManager(d)
	def := VectorIndexDefinition{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2, M: 4}
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs", VectorIndexes: []VectorIndexDefinition{def}}); err != nil {
		_ = d.Close()
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		_ = d.Close()
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("a")}, [][]byte{[]byte(`{"embedding":[1,0]}`)}); err != nil {
		_ = d.Close()
		t.Fatalf("insert seed: %v", err)
	}
	index := col.registeredVectorIndex(def.Name)
	if index == nil {
		_ = d.Close()
		t.Fatal("seed insert did not build native index")
	}
	if _, err := index.SaveNativeSnapshot(); err != nil {
		_ = d.Close()
		t.Fatalf("save seed graph: %v", err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("b")}, [][]byte{[]byte(`{"embedding":[0,1]}`)}); err != nil {
		_ = d.Close()
		t.Fatalf("insert live delta: %v", err)
	}
	if err := col.Flush(); err != nil {
		_ = d.Close()
		t.Fatalf("flush live delta: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close live delta db: %v", err)
	}

	reopened := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("docs")
	if err != nil {
		t.Fatalf("open live delta collection: %v", err)
	}
	loaded, status, err := reopenedCol.LoadNativeVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("load live delta graph: %v", err)
	}
	if loaded == nil || !status.Loaded || status.ExactFallbackReason != "" {
		t.Fatalf("live delta graph loaded=%v status=%+v", loaded != nil, status)
	}
	if stats := loaded.Stats(); stats.LiveANNFullRebuilds != 0 {
		t.Fatalf("live delta reopened through rebuild: %+v", stats)
	}
	results, _, err := loaded.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 2, DisableExactFallback: true})
	if err != nil {
		t.Fatalf("search live delta graph: %v", err)
	}
	requireVectorResultIDs(t, results, "a", "b")
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
	indexes := reopenedCol.registeredVectorIndexes()
	if got := len(indexes); got != 1 {
		t.Fatalf("registered vector indexes after rebuild=%d want 1", got)
	}
	if stats := indexes[0].Stats(); stats.LiveDocs != 1 || stats.Nodes != 1 || stats.DeletedDocs != 0 {
		t.Fatalf("missing-root rebuild replayed committed batch: %+v", stats)
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

func TestCollectionVectorIndexNativeRootDelayedLoadKeepsNewerPublishedGraph(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	def := VectorIndexDefinition{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2, M: 4}
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs", VectorIndexes: []VectorIndexDefinition{def}}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	seed, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open seed collection: %v", err)
	}
	if _, err := seed.InsertBatch([][]byte{[]byte("a")}, [][]byte{[]byte(`{"embedding":[1,0]}`)}); err != nil {
		t.Fatalf("insert seed: %v", err)
	}
	current := seed.registeredVectorIndex(def.Name)
	first, err := current.SaveNativeSnapshot()
	if err != nil || !first.Loaded {
		t.Fatalf("save first root: status=%+v err=%v", first, err)
	}

	loader, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open loader collection: %v", err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	var hookMu sync.Mutex
	hookUsed := false
	restore := setNativeVectorIndexBeforeInstallHookForTest(func(name string) {
		if name != def.Name {
			return
		}
		hookMu.Lock()
		firstCall := !hookUsed
		hookUsed = true
		hookMu.Unlock()
		if firstCall {
			close(entered)
			<-release
		}
	})
	defer restore()
	type loadResult struct {
		index  *VectorIndex
		status VectorIndexLoadStatus
		err    error
	}
	loaded := make(chan loadResult, 1)
	go func() {
		index, status, err := loader.LoadNativeVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def))
		loaded <- loadResult{index: index, status: status, err: err}
	}()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("native install hook was not reached")
	}

	if _, err := seed.InsertBatch([][]byte{[]byte("b")}, [][]byte{[]byte(`{"embedding":[0,1]}`)}); err != nil {
		t.Fatalf("insert before second root: %v", err)
	}
	second, err := current.SaveNativeDeltaSnapshot()
	if err != nil || !second.Loaded || second.RootID == first.RootID {
		t.Fatalf("save second root: first=%+v second=%+v err=%v", first, second, err)
	}
	close(release)
	result := <-loaded
	if result.err != nil || result.index != current || !result.status.Loaded || result.status.Epoch != second.RootID || result.status.RootID != second.RootID {
		t.Fatalf("delayed load replaced newer graph: current=%p loaded=%p status=%+v err=%v", current, result.index, result.status, result.err)
	}
	got, _, err := result.index.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 2, DisableExactFallback: true})
	if err != nil {
		t.Fatalf("search newer graph: %v", err)
	}
	requireVectorResultIDs(t, got, "a", "b")
}

func TestCollectionVectorIndexNativeRootDelayedEmptyBuildKeepsFirstMutation(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := NewCollectionManager(d)
	def := VectorIndexDefinition{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 2, M: 4}
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
		t.Fatalf("close before race: %v", err)
	}

	d, err = backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr = NewCollectionManager(d)
	builder, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open builder collection: %v", err)
	}
	writer, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open writer collection: %v", err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	var hookMu sync.Mutex
	hookUsed := false
	restore := setNativeVectorIndexBeforeInstallHookForTest(func(name string) {
		if name != def.Name {
			return
		}
		hookMu.Lock()
		firstCall := !hookUsed
		hookUsed = true
		hookMu.Unlock()
		if firstCall {
			close(entered)
			<-release
		}
	})
	defer restore()
	built := make(chan struct {
		index *VectorIndex
		err   error
	}, 1)
	go func() {
		index, err := builder.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
		built <- struct {
			index *VectorIndex
			err   error
		}{index: index, err: err}
	}()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("native install hook was not reached")
	}
	if _, err := writer.InsertBatch([][]byte{[]byte("a")}, [][]byte{[]byte(`{"embedding":[1,0]}`)}); err != nil {
		t.Fatalf("insert first mutation: %v", err)
	}
	current := writer.registeredVectorIndex(def.Name)
	if current == nil || !current.needsNativeAutoPersist() {
		t.Fatalf("first mutation did not install a dirty graph: current=%p", current)
	}
	close(release)
	result := <-built
	if result.err != nil || result.index != current || builder.registeredVectorIndex(def.Name) != current {
		t.Fatalf("delayed empty build replaced dirty graph: current=%p built=%p registered=%p err=%v", current, result.index, builder.registeredVectorIndex(def.Name), result.err)
	}
	got, _, err := result.index.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 1, DisableExactFallback: true})
	if err != nil {
		t.Fatalf("search first mutation: %v", err)
	}
	requireVectorResultIDs(t, got, "a")
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

func TestCollectionVectorIndexNativeRootStaleAdHocRuntimeBecomesDeclaredFullSnapshot(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
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
	staleCol, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open stale collection: %v", err)
	}
	if _, err := staleCol.InsertBatch(
		[][]byte{[]byte("a"), []byte("b")},
		[][]byte{
			[]byte(`{"embedding":[1,0]}`),
			[]byte(`{"embedding":[0,1]}`),
		},
	); err != nil {
		t.Fatalf("insert before vector index metadata: %v", err)
	}
	if _, err := staleCol.BuildVectorIndex(vectorIndexOptionsFromDefinition(def)); err != nil {
		t.Fatalf("build stale ad hoc vector index: %v", err)
	}
	freshCol, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open fresh collection: %v", err)
	}
	if _, err := freshCol.CreateVectorIndex(def); err != nil {
		t.Fatalf("create vector index metadata: %v", err)
	}
	if _, err := staleCol.InsertBatch(
		[][]byte{[]byte("c")},
		[][]byte{[]byte(`{"embedding":[1,1]}`)},
	); err != nil {
		t.Fatalf("insert through stale ad hoc runtime after declaration: %v", err)
	}
	if err := staleCol.Flush(); err != nil {
		t.Fatalf("flush promoted stale ad hoc vector index: %v", err)
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
		t.Fatalf("load promoted stale ad hoc vector index: %v", err)
	}
	if loaded == nil || !status.Loaded {
		t.Fatalf("promoted stale ad hoc vector index did not persist loaded=%v status=%+v", loaded != nil, status)
	}
	results, _, err := loaded.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 3, DisableExactFallback: true})
	if err != nil {
		t.Fatalf("search promoted stale ad hoc vector index: %v", err)
	}
	requireVectorResultIDs(t, results, "a", "c", "b")
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

func TestCollectionVectorIndexNativeRootStaleInsertReturnsIDOnCommitAmbiguousMaintenanceError(t *testing.T) {
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
	staleCol, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open stale collection: %v", err)
	}
	freshCol, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open fresh collection: %v", err)
	}
	if _, err := freshCol.CreateVectorIndex(def); err != nil {
		t.Fatalf("create vector index: %v", err)
	}

	id, err := staleCol.Insert([]byte("bad"), []byte(`{"embedding":[1,0,0]}`))
	if !errors.Is(err, ErrCommitAmbiguous) {
		t.Fatalf("stale insert err=%v want ErrCommitAmbiguous", err)
	}
	if string(id) != "bad" {
		t.Fatalf("stale insert id=%q want bad for committed ambiguous insert", id)
	}
	doc, getErr := staleCol.Get(id)
	if getErr != nil {
		t.Fatalf("get committed document: %v", getErr)
	}
	if len(doc) == 0 {
		t.Fatalf("committed document %x missing after stale ambiguous insert", id)
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

func TestCollectionVectorIndexNativeRootAutoPersistRuntimeRebuildUsesFullSnapshot(t *testing.T) {
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
	if err := index.Rebuild(); err != nil {
		t.Fatalf("rebuild runtime vector index: %v", err)
	}
	if stats := index.Stats(); stats.LiveDocs != 2 || stats.DeletedDocs != 0 || !stats.SnapshotDirty {
		t.Fatalf("rebuilt runtime vector index should be dirty before flush: %+v", stats)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush rebuilt runtime vector index: %v", err)
	}
	if stats := index.Stats(); stats.LiveDocs != 2 || stats.Nodes != 2 || stats.DeletedDocs != 0 || stats.SnapshotDirty {
		t.Fatalf("rebuilt runtime vector index should be clean after flush: %+v", stats)
	}
	loaded, status, err := col.LoadVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("load auto-persisted rebuilt runtime native snapshot: %v", err)
	}
	if loaded == nil || !status.Loaded {
		t.Fatalf("unexpected rebuilt runtime load status loaded=%v status=%+v", loaded != nil, status)
	}
	if stats := loaded.Stats(); stats.LiveDocs != 2 || stats.Nodes != 2 || stats.DeletedDocs != 0 {
		t.Fatalf("auto-persisted rebuilt runtime retained stale graph entries: %+v", stats)
	}
	results, _, err := loaded.Search([]float32{0.7, 0.3}, VectorIndexSearchOptions{TopK: 3, DisableExactFallback: true})
	if err != nil {
		t.Fatalf("search auto-persisted rebuilt runtime native snapshot: %v", err)
	}
	for _, result := range results {
		if string(result.DocumentID) == "c" {
			t.Fatalf("auto-persisted rebuilt runtime search returned deleted document: %+v", results)
		}
	}
}

func TestCollectionVectorIndexNativeRootAutoPersistRuntimeRebuildWithoutDirtyDeltaPublishesSnapshot(t *testing.T) {
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
	if _, err := index.SaveSnapshot(); err != nil {
		t.Fatalf("save first native snapshot: %v", err)
	}
	if err := index.Rebuild(); err != nil {
		t.Fatalf("rebuild runtime vector index: %v", err)
	}
	if stats := index.Stats(); !stats.SnapshotDirty {
		t.Fatalf("rebuilt runtime vector index should be dirty before flush: %+v", stats)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush rebuilt runtime vector index: %v", err)
	}
	if stats := index.Stats(); stats.SnapshotDirty {
		t.Fatalf("rebuilt runtime vector index should be clean after flush: %+v", stats)
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

func TestCollectionVectorIndexNativeDeltaPersistsPromotedEntryMeta(t *testing.T) {
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
	if _, err := col.Insert([]byte("base"), []byte(`{"embedding":[1,0]}`)); err != nil {
		t.Fatalf("insert base: %v", err)
	}
	index, err := col.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("build vector index: %v", err)
	}
	if _, err := index.SaveSnapshot(); err != nil {
		t.Fatalf("save snapshot: %v", err)
	}

	index.mu.RLock()
	oldMaxLevel := index.maxLevel
	index.mu.RUnlock()
	var promotedID []byte
	for i := 0; i < 100000; i++ {
		candidate := []byte(fmt.Sprintf("promoted-%d", i))
		if index.levelForDocumentID(candidate) > oldMaxLevel {
			promotedID = candidate
			break
		}
	}
	if len(promotedID) == 0 {
		t.Fatalf("could not find promoted entry id above level %d", oldMaxLevel)
	}
	if _, err := col.Insert(promotedID, []byte(`{"embedding":[0.9,0.1]}`)); err != nil {
		t.Fatalf("insert promoted: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush promoted delta: %v", err)
	}

	loaded, status, err := col.LoadVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("load vector index: %v", err)
	}
	if loaded == nil || !status.Loaded {
		t.Fatalf("vector index did not load loaded=%v status=%+v", loaded != nil, status)
	}
	loaded.mu.RLock()
	entry := loaded.entry
	var entryDocumentID []byte
	if entry >= 0 && entry < len(loaded.nodes) {
		entryDocumentID = append([]byte(nil), loaded.nodes[entry].documentID...)
	}
	loadedMaxLevel := loaded.maxLevel
	loaded.mu.RUnlock()
	if !bytes.Equal(entryDocumentID, promotedID) {
		t.Fatalf("loaded entry document=%q want promoted %q", entryDocumentID, promotedID)
	}
	if loadedMaxLevel <= oldMaxLevel {
		t.Fatalf("loaded max level=%d want above old level %d", loadedMaxLevel, oldMaxLevel)
	}
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

func TestCollectionVectorIndexNativeRootAdHocDirtyRuntimeDoesNotPinManagerHandle(t *testing.T) {
	col := &Collection{meta: CollectionMeta{Name: "docs"}}
	native, err := newVectorIndex(col, VectorIndexOptions{
		Name:       "embedding",
		Field:      "embedding",
		Metric:     VectorMetricCosine,
		Dimensions: 2,
		M:          4,
	})
	if err != nil {
		t.Fatalf("new native vector index: %v", err)
	}
	native.setNativePersistent(true)
	native.recordPersistedSnapshot(10, 128, 0)
	adHoc, err := newVectorIndex(col, VectorIndexOptions{
		Name:       "scratch_embedding",
		Field:      "embedding",
		Metric:     VectorMetricCosine,
		Dimensions: 2,
		M:          4,
	})
	if err != nil {
		t.Fatalf("new ad hoc vector index: %v", err)
	}
	adHoc.mutationSeq = 1
	col.vectorIndexes = map[string]*VectorIndex{
		native.name: native,
		adHoc.name:  adHoc,
	}

	if col.hasDirtyNativeVectorIndex() {
		t.Fatal("clean native index plus dirty ad hoc runtime pinned the native manager handle")
	}
}

func TestVectorIndexStatusRootIDUsesNewestOverlayRoot(t *testing.T) {
	rootName := collectionVectorIndexRootName("docs", "embedding")
	catalog := &collectionCatalog{
		rootOverlays: map[string][]uint64{
			rootName: {30, 20},
		},
	}
	rootID, overlayRootIDs := vectorIndexStatusRootID(catalog, rootName)
	if rootID != 30 {
		t.Fatalf("status root=%d want newest overlay root 30", rootID)
	}
	if len(overlayRootIDs) != 2 || overlayRootIDs[0] != 30 || overlayRootIDs[1] != 20 {
		t.Fatalf("overlay roots=%v want [30 20]", overlayRootIDs)
	}
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

func TestCollectionVectorIndexNativeRootMutationRepairsSnapshotFallback(t *testing.T) {
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
		t.Fatalf("insert seed vectors: %v", err)
	}
	index, err := col.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("build vector index: %v", err)
	}
	if _, err := index.SaveSnapshot(); err != nil {
		t.Fatalf("save vector index: %v", err)
	}
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("acquire snapshot")
	}
	catalog, err := loadCollectionCatalog(snap, "docs")
	_ = snap.Close()
	if err != nil {
		t.Fatalf("load catalog: %v", err)
	}
	newMeta, err := normalizeCollectionMeta(CollectionMeta{
		Name: "docs",
		VectorIndexes: []VectorIndexDefinition{{
			Name:           "embedding",
			Field:          "embedding",
			Metric:         VectorMetricCosine,
			Dimensions:     2,
			M:              8,
			EfConstruction: 16,
			EfSearch:       32,
		}},
	})
	if err != nil {
		t.Fatalf("normalize collection meta: %v", err)
	}
	encodedMeta, err := encodeCollectionMeta(newMeta)
	if err != nil {
		t.Fatalf("encode collection meta: %v", err)
	}
	_, _, err = d.PublishOrderedRootDeltaGroupWithSystemDeltaBuilder(nil, func([]uint64) (iterator.UnsafeIterator, error) {
		return col.buildSchemaOnlySystemDeltaIterator(catalog.meta, encodedMeta, nil)
	})
	if err != nil {
		t.Fatalf("publish stale vector metadata: %v", err)
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
		t.Fatalf("insert after snapshot fallback: %v", err)
	}
	if got := len(reopenedCol.registeredVectorIndexes()); got != 1 {
		t.Fatalf("snapshot fallback did not repair vector index, got %d registered indexes", got)
	}
	loaded := reopenedCol.registeredVectorIndex("embedding")
	if loaded == nil {
		t.Fatal("snapshot fallback repair did not leave a registered vector index")
	}
	results, _, err := loaded.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 3, DisableExactFallback: true})
	if err != nil {
		t.Fatalf("search repaired vector index: %v", err)
	}
	requireVectorResultIDs(t, results, "a", "c", "b")
}

func TestCollectionVectorIndexNativeRootStatusUsesPersistedStatsForStaleRuntime(t *testing.T) {
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
		[][]byte{[]byte("a"), []byte("b"), []byte("c")},
		[][]byte{
			[]byte(`{"embedding":[1,0]}`),
			[]byte(`{"embedding":[0.9,0.1]}`),
			[]byte(`{"embedding":[0,1]}`),
		},
	); err != nil {
		t.Fatalf("insert seed vectors: %v", err)
	}
	seedIndex, err := seedCol.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("build seed vector index: %v", err)
	}
	seedStatus, err := seedIndex.SaveNativeSnapshot()
	if err != nil {
		t.Fatalf("save seed native snapshot: %v", err)
	}
	if !seedStatus.Loaded || seedStatus.RootID == 0 {
		t.Fatalf("seed native status=%+v", seedStatus)
	}

	staleCol, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open stale collection: %v", err)
	}
	if loaded, status, err := staleCol.LoadVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def)); err != nil {
		t.Fatalf("load stale runtime: %v", err)
	} else if loaded == nil || !status.Loaded || status.RootID != seedStatus.RootID {
		t.Fatalf("stale runtime load loaded=%v status=%+v seed=%+v", loaded != nil, status, seedStatus)
	}

	freshCol, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open fresh collection: %v", err)
	}
	if loaded, status, err := freshCol.LoadVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def)); err != nil {
		t.Fatalf("load fresh runtime: %v", err)
	} else if loaded == nil || !status.Loaded || status.RootID != seedStatus.RootID {
		t.Fatalf("fresh runtime load loaded=%v status=%+v seed=%+v", loaded != nil, status, seedStatus)
	}
	if deleted, err := freshCol.DeleteBatch([][]byte{[]byte("a")}); err != nil || deleted != 1 {
		t.Fatalf("delete through fresh collection deleted=%d err=%v", deleted, err)
	}
	if err := freshCol.Flush(); err != nil {
		t.Fatalf("flush fresh tombstone: %v", err)
	}
	freshStatus, err := freshCol.VectorIndexStatus(def.Name)
	if err != nil {
		t.Fatalf("fresh vector index status: %v", err)
	}
	if freshStatus.RootID == seedStatus.RootID || freshStatus.Stats.DeletedDocs == 0 || !freshStatus.RebuildNeeded {
		t.Fatalf("fresh status=%+v seed=%+v, want dirty advanced native root", freshStatus, seedStatus)
	}

	staleStatus, err := staleCol.VectorIndexStatus(def.Name)
	if err != nil {
		t.Fatalf("stale vector index status: %v", err)
	}
	if !staleStatus.Registered || staleStatus.RootID != freshStatus.RootID {
		t.Fatalf("stale status did not inspect current root stale=%+v fresh=%+v", staleStatus, freshStatus)
	}
	if staleStatus.Stats.DeletedDocs != freshStatus.Stats.DeletedDocs || !staleStatus.RebuildNeeded {
		t.Fatalf("stale status=%+v fresh=%+v, want persisted dirty stats", staleStatus, freshStatus)
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

func TestCollectionVectorIndexNativeRootRebuildUsesCurrentRootForStaleHandle(t *testing.T) {
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
	if _, err := freshCol.InsertBatch(
		[][]byte{[]byte("a"), []byte("b")},
		[][]byte{
			[]byte(`{"embedding":[1,0]}`),
			[]byte(`{"embedding":[0,1]}`),
		},
	); err != nil {
		t.Fatalf("insert seed vectors: %v", err)
	}
	if _, err := freshCol.CreateVectorIndex(def); err != nil {
		t.Fatalf("create vector index metadata: %v", err)
	}
	freshIndex, err := freshCol.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("build fresh vector index: %v", err)
	}
	freshStatus, err := freshIndex.SaveSnapshot()
	if err != nil {
		t.Fatalf("save fresh vector root: %v", err)
	}
	if !freshStatus.Loaded || freshStatus.RootID == 0 {
		t.Fatalf("fresh save status=%+v", freshStatus)
	}
	if len(staleCol.Meta().VectorIndexes) != 0 {
		t.Fatalf("stale handle unexpectedly saw vector metadata: %+v", staleCol.Meta())
	}

	rebuild, err := staleCol.RebuildVectorIndex(def.Name)
	if err != nil {
		t.Fatalf("rebuild through stale handle: %v", err)
	}
	if !rebuild.NativeRootLoaded || rebuild.RootID == 0 || rebuild.Stats.LiveDocs != 2 || rebuild.RebuildNeeded {
		t.Fatalf("unexpected stale-handle rebuild status: %+v", rebuild)
	}
	loaded, loadStatus, err := freshCol.LoadVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("load rebuilt root: %v", err)
	}
	if loaded == nil || !loadStatus.Loaded || loadStatus.RootID != rebuild.RootID {
		t.Fatalf("load status=%+v loaded=%v rebuild=%+v", loadStatus, loaded != nil, rebuild)
	}
	if _, err := staleCol.InsertBatch(
		[][]byte{[]byte("c")},
		[][]byte{[]byte(`{"embedding":[0.95,0.05]}`)},
	); err != nil {
		t.Fatalf("insert through rebuilt stale handle: %v", err)
	}
	if err := mgr.FlushAll(); err != nil {
		t.Fatalf("flush rebuilt stale handle: %v", err)
	}
	reloaded, reloadStatus, err := freshCol.LoadVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("reload rebuilt stale-handle root: %v", err)
	}
	if reloaded == nil || !reloadStatus.Loaded || reloadStatus.RootID == rebuild.RootID {
		t.Fatalf("reload status=%+v loaded=%v rebuild=%+v", reloadStatus, reloaded != nil, rebuild)
	}
	results, _, err := reloaded.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 3, DisableExactFallback: true})
	if err != nil {
		t.Fatalf("search rebuilt stale-handle root: %v", err)
	}
	requireVectorResultIDs(t, results, "a", "c", "b")
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

func TestCollectionVectorIndexNativeRootRebuildRejectsStaleRuntime(t *testing.T) {
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
	rebuild, err := col.RebuildVectorIndex("embedding")
	if err != nil {
		t.Fatalf("rebuild vector index: %v", err)
	}
	if current := col.registeredVectorIndex(def.Name); current == nil || current == stale {
		t.Fatalf("rebuild did not replace stale runtime current=%p stale=%p", current, stale)
	}

	if err := stale.Rebuild(); !errors.Is(err, errVectorIndexStaleRuntime) {
		t.Fatalf("stale runtime rebuild err=%v want stale runtime", err)
	}
	if current := col.registeredVectorIndex(def.Name); current == stale {
		t.Fatal("stale runtime re-registered itself after failed rebuild")
	}
	status, err := col.VectorIndexStatus(def.Name)
	if err != nil {
		t.Fatalf("status after stale rebuild: %v", err)
	}
	if status.RootID != rebuild.RootID || status.Stats.LiveDocs != rebuild.Stats.LiveDocs || status.Stats.DeletedDocs != rebuild.Stats.DeletedDocs {
		t.Fatalf("stale rebuild changed status=%+v rebuild=%+v", status, rebuild)
	}
}

func TestCollectionVectorIndexNativeRootRebuildRejectsRegisteredStaleRoot(t *testing.T) {
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
	staleCol, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open stale collection: %v", err)
	}
	freshCol, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open fresh collection: %v", err)
	}
	if _, err := staleCol.InsertBatch(
		[][]byte{[]byte("a"), []byte("b")},
		[][]byte{
			[]byte(`{"embedding":[1,0]}`),
			[]byte(`{"embedding":[0,1]}`),
		},
	); err != nil {
		t.Fatalf("insert seed vectors: %v", err)
	}
	stale, err := staleCol.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("build stale collection index: %v", err)
	}
	seed, err := stale.SaveNativeSnapshot()
	if err != nil {
		t.Fatalf("save seed native root: %v", err)
	}
	if !seed.Loaded || seed.RootID == 0 {
		t.Fatalf("seed native status=%+v", seed)
	}
	if _, err := freshCol.InsertBatch(
		[][]byte{[]byte("c")},
		[][]byte{[]byte(`{"embedding":[0.9,0.1]}`)},
	); err != nil {
		t.Fatalf("insert fresh vector: %v", err)
	}
	rebuild, err := freshCol.RebuildVectorIndex(def.Name)
	if err != nil {
		t.Fatalf("fresh rebuild: %v", err)
	}
	if rebuild.RootID == 0 || rebuild.RootID == seed.RootID || rebuild.Stats.LiveDocs != 3 {
		t.Fatalf("unexpected fresh rebuild=%+v seed=%+v", rebuild, seed)
	}
	if current := staleCol.registeredVectorIndex(def.Name); current == nil || current == stale || current != freshCol.registeredVectorIndex(def.Name) {
		t.Fatalf("collection handles did not converge on replacement index current=%p stale=%p fresh=%p", current, stale, freshCol.registeredVectorIndex(def.Name))
	}

	if err := stale.Rebuild(); !errors.Is(err, errVectorIndexStaleRuntime) {
		t.Fatalf("registered stale root rebuild err=%v want stale runtime", err)
	}
	if staleStatus, err := stale.SaveNativeSnapshot(); err != nil || staleStatus.Loaded || staleStatus.ExactFallbackReason != vectorIndexFallbackStaleRuntimeIndex {
		t.Fatalf("registered stale root save status=%+v err=%v, want ignored", staleStatus, err)
	}
	status, err := freshCol.VectorIndexStatus(def.Name)
	if err != nil {
		t.Fatalf("status after stale rebuild: %v", err)
	}
	if status.RootID != rebuild.RootID || status.Stats.LiveDocs != 3 {
		t.Fatalf("stale registered rebuild changed status=%+v rebuild=%+v", status, rebuild)
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

func TestCollectionVectorIndexNativeEmptyRootMaintainsInsertedDocuments(t *testing.T) {
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
	index, err := col.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("build empty vector index: %v", err)
	}
	if _, err := index.SaveSnapshot(); err != nil {
		t.Fatalf("save empty vector snapshot: %v", err)
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
		[][]byte{[]byte("a")},
		[][]byte{[]byte(`{"embedding":[1,0]}`)},
	); err != nil {
		t.Fatalf("insert after empty vector snapshot reopen: %v", err)
	}
	if got := len(reopenedCol.registeredVectorIndexes()); got != 1 {
		t.Fatalf("write did not lazily load empty vector index, got %d registered indexes", got)
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
	results, _, err := loaded.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 1, DisableExactFallback: true})
	if err != nil {
		t.Fatalf("search maintained vector index: %v", err)
	}
	requireVectorResultIDs(t, results, "a")
}

func TestCollectionVectorIndexNativeDeltaSharesPersistedRootAcrossHandles(t *testing.T) {
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
	sharedIndex := handleA.registeredVectorIndex(def.Name)
	if sharedIndex == nil || sharedIndex != handleB.registeredVectorIndex(def.Name) {
		t.Fatalf("handles do not share loaded vector index A=%p B=%p", sharedIndex, handleB.registeredVectorIndex(def.Name))
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
	_, statusA2, err := handleA.LoadVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("reload handle A vector index: %v", err)
	} else if !statusA2.Loaded || statusA2.RootID == seedStatus.RootID {
		t.Fatalf("handle A did not advance persisted root status=%+v seed=%+v", statusA2, seedStatus)
	}
	sharedIndex = handleA.registeredVectorIndex(def.Name)
	if sharedIndex == nil || sharedIndex != handleB.registeredVectorIndex(def.Name) {
		t.Fatalf("handles did not converge after reload A=%p B=%p", sharedIndex, handleB.registeredVectorIndex(def.Name))
	}

	if _, err := handleB.InsertBatch(
		[][]byte{[]byte("d")},
		[][]byte{[]byte(`{"embedding":[0.8,0.2]}`)},
	); err != nil {
		t.Fatalf("insert through handle B: %v", err)
	}
	if err := handleB.Flush(); err != nil {
		t.Fatalf("flush handle B: %v", err)
	}
	loaded, status, err := handleA.LoadVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("load graph after stale rejection: %v", err)
	}
	if loaded == nil || !status.Loaded {
		t.Fatalf("graph did not load after stale rejection loaded=%v status=%+v", loaded != nil, status)
	}
	if status.RootID == statusA2.RootID {
		t.Fatalf("handle B flush did not advance shared root status=%+v previous=%+v", status, statusA2)
	}
	results, _, err := loaded.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 4, DisableExactFallback: true})
	if err != nil {
		t.Fatalf("search graph after stale rejection: %v", err)
	}
	requireVectorResultIDs(t, results, "a", "c", "d", "b")
}

func TestCollectionVectorIndexNativeFullSaveIgnoresSupersededPreRootHandle(t *testing.T) {
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
	if _, err := seedCol.CreateVectorIndex(def); err != nil {
		t.Fatalf("create vector index metadata: %v", err)
	}

	staleCol, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open stale collection: %v", err)
	}
	staleIndex, err := staleCol.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("build pre-root vector index: %v", err)
	}

	freshCol, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open fresh collection: %v", err)
	}
	if _, err := freshCol.InsertBatch(
		[][]byte{[]byte("b")},
		[][]byte{[]byte(`{"embedding":[0,1]}`)},
	); err != nil {
		t.Fatalf("insert fresh vector: %v", err)
	}
	freshIndex, err := freshCol.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("build fresh vector index: %v", err)
	}
	freshStatus, err := freshIndex.SaveSnapshot()
	if err != nil {
		t.Fatalf("save fresh vector index: %v", err)
	}
	if !freshStatus.Loaded || freshStatus.RootID == 0 {
		t.Fatalf("fresh save status=%+v", freshStatus)
	}

	if staleStatus, err := staleIndex.SaveNativeSnapshot(); err != nil || staleStatus.Loaded || staleStatus.ExactFallbackReason != vectorIndexFallbackStaleRuntimeIndex {
		t.Fatalf("superseded pre-root full snapshot save status=%+v err=%v, want ignored stale runtime", staleStatus, err)
	}
	loaded, status, err := freshCol.LoadVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("load graph after stale full-save rejection: %v", err)
	}
	if loaded == nil || !status.Loaded || status.RootID != freshStatus.RootID {
		t.Fatalf("graph changed after stale full-save rejection loaded=%v status=%+v fresh=%+v", loaded != nil, status, freshStatus)
	}
	results, _, err := loaded.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 2, DisableExactFallback: true})
	if err != nil {
		t.Fatalf("search graph after stale full-save rejection: %v", err)
	}
	requireVectorResultIDs(t, results, "a", "b")
}

func TestCollectionVectorIndexNativeFullSaveIgnoresDroppedRecreatedPreRoot(t *testing.T) {
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
	staleCol, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open stale collection: %v", err)
	}
	if _, err := staleCol.CreateVectorIndex(def); err != nil {
		t.Fatalf("create vector index metadata: %v", err)
	}
	if _, err := staleCol.InsertBatch(
		[][]byte{[]byte("a")},
		[][]byte{[]byte(`{"embedding":[1,0]}`)},
	); err != nil {
		t.Fatalf("insert stale vector: %v", err)
	}
	staleIndex, err := staleCol.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("build pre-root vector index: %v", err)
	}

	freshCol, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open fresh collection: %v", err)
	}
	if _, err := freshCol.DropVectorIndex("embedding"); err != nil {
		t.Fatalf("drop vector index: %v", err)
	}
	if deleted, err := freshCol.DeleteBatch([][]byte{[]byte("a")}); err != nil || deleted != 1 {
		t.Fatalf("delete vector while index absent deleted=%d err=%v", deleted, err)
	}
	if _, err := freshCol.InsertBatch(
		[][]byte{[]byte("b")},
		[][]byte{[]byte(`{"embedding":[0,1]}`)},
	); err != nil {
		t.Fatalf("insert replacement vector while index absent: %v", err)
	}
	if _, err := freshCol.CreateVectorIndex(def); err != nil {
		t.Fatalf("recreate vector index metadata: %v", err)
	}
	if _, status, err := freshCol.LoadVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def)); err != nil {
		t.Fatalf("load recreated empty root: %v", err)
	} else if status.ExactFallbackReason != vectorIndexFallbackMissingGraphRoot {
		t.Fatalf("recreated vector index status=%+v want missing graph root", status)
	}

	if staleStatus, err := staleIndex.SaveNativeSnapshot(); err != nil || staleStatus.Loaded || staleStatus.ExactFallbackReason != vectorIndexFallbackStaleRuntimeIndex {
		t.Fatalf("stale dropped/recreated pre-root save status=%+v err=%v, want ignored stale runtime", staleStatus, err)
	}
	freshIndex, err := freshCol.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("build replacement vector index: %v", err)
	}
	freshStatus, err := freshIndex.SaveSnapshot()
	if err != nil {
		t.Fatalf("save replacement vector index: %v", err)
	}
	loaded, status, err := freshCol.LoadVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("load replacement graph: %v", err)
	}
	if loaded == nil || !status.Loaded || status.RootID != freshStatus.RootID {
		t.Fatalf("replacement graph did not load loaded=%v status=%+v fresh=%+v", loaded != nil, status, freshStatus)
	}
	results, _, err := loaded.Search([]float32{0, 1}, VectorIndexSearchOptions{TopK: 2, DisableExactFallback: true})
	if err != nil {
		t.Fatalf("search replacement graph: %v", err)
	}
	requireVectorResultIDs(t, results, "b")
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
	staleIndex := staleCol.registeredVectorIndex(def.Name)
	if staleIndex == nil {
		t.Fatal("stale collection did not load vector index")
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

	if current := staleCol.registeredVectorIndex(def.Name); current != nil {
		t.Fatalf("drop/recreate did not clear shared registered index current=%p", current)
	}
	if staleStatus, err := staleIndex.SaveNativeSnapshot(); err != nil || staleStatus.Loaded || staleStatus.ExactFallbackReason != vectorIndexFallbackStaleRuntimeIndex {
		t.Fatalf("stale full snapshot save status=%+v err=%v, want ignored stale runtime", staleStatus, err)
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

func TestCollectionLegacyVectorSnapshotSaveSerializesPruneAfterEpochRename(t *testing.T) {
	d := openCollectionCommandWALDB(t, t.TempDir())
	defer func() { _ = d.Close() }()
	col := openVectorIndexTestCollection(t, d)
	if _, err := col.InsertBatch(
		[][]byte{[]byte("a"), []byte("b")},
		[][]byte{[]byte(`{"embedding":[1,0]}`), []byte(`{"embedding":[0,1]}`)},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	opts := VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine}
	index, err := col.BuildVectorIndex(opts)
	if err != nil {
		t.Fatalf("build vector index: %v", err)
	}
	first, err := index.SaveSnapshot()
	if err != nil {
		t.Fatalf("save first snapshot: %v", err)
	}
	if first.ManifestPath == "" {
		t.Fatalf("first save did not use legacy sidecar: %+v", first)
	}

	secondCol, err := NewCollectionManager(d).OpenCollection("docs")
	if err != nil {
		t.Fatalf("open second collection handle: %v", err)
	}
	pruneIndex, err := newVectorIndex(secondCol, opts)
	if err != nil {
		t.Fatalf("new second-handle vector index: %v", err)
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	var enteredOnce sync.Once
	restore := setLegacyVectorSnapshotPostEpochRenameHookForTest(func() {
		enteredOnce.Do(func() { close(entered) })
		<-release
	})
	defer restore()
	var releaseOnce sync.Once
	defer func() { releaseOnce.Do(func() { close(release) }) }()

	saveDone := make(chan struct {
		status VectorIndexLoadStatus
		err    error
	}, 1)
	go func() {
		status, err := index.SaveSnapshot()
		saveDone <- struct {
			status VectorIndexLoadStatus
			err    error
		}{status, err}
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("second save did not reach post-epoch-rename hook")
	}

	pruneLockAttempted := make(chan struct{})
	var pruneLockAttemptedOnce sync.Once
	restoreBeforeLock := setLegacyVectorSidecarBeforeLockHookForTest(func() {
		pruneLockAttemptedOnce.Do(func() { close(pruneLockAttempted) })
	})
	defer restoreBeforeLock()

	pruneDone := make(chan struct {
		status VectorIndexPruneStatus
		err    error
	}, 1)
	go func() {
		status, err := pruneIndex.PruneOldSnapshots(1)
		pruneDone <- struct {
			status VectorIndexPruneStatus
			err    error
		}{status, err}
	}()
	select {
	case <-pruneLockAttempted:
	case <-time.After(5 * time.Second):
		t.Fatal("prune did not attempt to acquire the legacy sidecar lock")
	}
	select {
	case got := <-pruneDone:
		t.Fatalf("prune completed while save was paused after epoch rename: %+v", got)
	case <-time.After(100 * time.Millisecond):
	}

	releaseOnce.Do(func() { close(release) })
	var second struct {
		status VectorIndexLoadStatus
		err    error
	}
	select {
	case second = <-saveDone:
	case <-time.After(5 * time.Second):
		t.Fatal("second save did not complete after release")
	}
	if second.err != nil {
		t.Fatalf("save second snapshot: %v", second.err)
	}
	var pruned struct {
		status VectorIndexPruneStatus
		err    error
	}
	select {
	case pruned = <-pruneDone:
	case <-time.After(5 * time.Second):
		t.Fatal("prune did not complete after save release")
	}
	if pruned.err != nil {
		t.Fatalf("prune after save: %v", pruned.err)
	}
	if pruned.status.RemovedEpochs != 1 {
		t.Fatalf("prune status=%+v, want one old epoch removed", pruned.status)
	}

	loaded, status, err := secondCol.LoadVectorIndexSnapshot(opts)
	if err != nil {
		t.Fatalf("load published legacy snapshot: %v", err)
	}
	if loaded == nil || !status.Loaded || status.Epoch != second.status.Epoch {
		t.Fatalf("loaded=%v status=%+v, want second epoch %d", loaded != nil, status, second.status.Epoch)
	}
	epochs, err := vectorIndexEpochDirs(filepath.Dir(second.status.ManifestPath))
	if err != nil {
		t.Fatal(err)
	}
	wantEpoch := fmt.Sprintf("epoch-%020d", second.status.Epoch)
	if len(epochs) != 1 || epochs[0] != wantEpoch {
		t.Fatalf("epochs=%v, want only %q", epochs, wantEpoch)
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
