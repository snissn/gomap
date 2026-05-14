package collections

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
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

func TestCollectionVectorIndexSnapshotStaleAfterPostSaveMutationFallsBack(t *testing.T) {
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
	index, err := col.BuildVectorIndex(VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, M: 4})
	if err != nil {
		t.Fatalf("build vector index: %v", err)
	}
	saveStatus, err := index.SaveSnapshot()
	if err != nil {
		t.Fatalf("save snapshot: %v", err)
	}
	if !saveStatus.Loaded {
		t.Fatalf("snapshot was not saved: %+v", saveStatus)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("c")},
		[][]byte{[]byte(`{"embedding":[0.95,0.05]}`)},
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
	if loaded != nil || loadStatus.Loaded || loadStatus.ExactFallbackReason != "stale_collection_snapshot" {
		t.Fatalf("stale snapshot loaded=%v status=%+v", loaded != nil, loadStatus)
	}
}

func TestCollectionVectorIndexSnapshotIgnoresUnrelatedCollectionCommit(t *testing.T) {
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
		t.Fatalf("insert docs: %v", err)
	}
	index, err := col.BuildVectorIndex(VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, M: 4})
	if err != nil {
		t.Fatalf("build vector index: %v", err)
	}
	saveStatus, err := index.SaveSnapshot()
	if err != nil {
		t.Fatalf("save snapshot: %v", err)
	}
	if !saveStatus.Loaded {
		t.Fatalf("snapshot was not saved: %+v", saveStatus)
	}

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "other"}); err != nil {
		t.Fatalf("create unrelated collection: %v", err)
	}
	other, err := mgr.OpenCollection("other")
	if err != nil {
		t.Fatalf("open unrelated collection: %v", err)
	}
	if _, err := other.InsertBatch(
		[][]byte{[]byte("x")},
		[][]byte{[]byte(`{"value":1}`)},
	); err != nil {
		t.Fatalf("insert unrelated row: %v", err)
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
	if loaded == nil || !loadStatus.Loaded || loadStatus.ExactFallbackReason != "" || loadStatus.Epoch != saveStatus.Epoch {
		t.Fatalf("unrelated commit invalidated snapshot loaded=%v status=%+v", loaded != nil, loadStatus)
	}
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

func TestCollectionVectorIndexSnapshotPreservesBinaryDocumentIDs(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	col := openVectorIndexTestCollection(t, d)
	binaryID := []byte{0xff, 0x00, 'a'}
	if _, err := col.InsertBatch(
		[][]byte{binaryID, []byte("b")},
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
	saveStatus, err := index.SaveSnapshot()
	if err != nil {
		t.Fatalf("save snapshot: %v", err)
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
	loaded, status, err := reopenedCol.LoadVectorIndexSnapshot(VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine})
	if err != nil {
		t.Fatalf("load vector index snapshot: %v", err)
	}
	if loaded == nil || !status.Loaded || status.Epoch != saveStatus.Epoch {
		t.Fatalf("unexpected load status loaded=%v status=%+v", loaded != nil, status)
	}
	results, _, err := loaded.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 1, DisableExactFallback: true})
	if err != nil {
		t.Fatalf("search loaded index: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("result count=%d want 1", len(results))
	}
	if !bytes.Equal(results[0].DocumentID, binaryID) {
		t.Fatalf("result id=%x want %x", results[0].DocumentID, binaryID)
	}
}

func TestCollectionVectorIndexSnapshotEmptyIndexLoadsAndTracksMutations(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	col := openVectorIndexTestCollection(t, d)
	index, err := col.BuildVectorIndex(VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, M: 4})
	if err != nil {
		t.Fatalf("build empty vector index: %v", err)
	}
	saveStatus, err := index.SaveSnapshot()
	if err != nil {
		t.Fatalf("save empty snapshot: %v", err)
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
	loaded, status, err := reopenedCol.LoadVectorIndexSnapshot(VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine})
	if err != nil {
		t.Fatalf("load empty vector index snapshot: %v", err)
	}
	if loaded == nil || !status.Loaded || status.ExactFallbackReason != "" || status.Epoch != saveStatus.Epoch {
		t.Fatalf("unexpected empty load status loaded=%v status=%+v", loaded != nil, status)
	}
	if stats := loaded.Stats(); stats.LiveDocs != 0 || stats.Dimensions != 0 || stats.SnapshotDirty {
		t.Fatalf("unexpected loaded empty stats: %+v", stats)
	}
	if _, err := reopenedCol.Insert([]byte("after"), []byte(`{"embedding":[1,0]}`)); err != nil {
		t.Fatalf("insert after empty snapshot load: %v", err)
	}
	results, _, err := loaded.Search([]float32{1, 0}, VectorIndexSearchOptions{TopK: 1, DisableExactFallback: true})
	if err != nil {
		t.Fatalf("search after empty snapshot mutation: %v", err)
	}
	requireVectorResultIDs(t, results, "after")
	if stats := loaded.Stats(); stats.LiveDocs != 1 || !stats.SnapshotDirty {
		t.Fatalf("mutation after empty load did not update stats: %+v", stats)
	}
}

func TestCollectionVectorIndexSnapshotEpochAdvancesPastPublishedManifest(t *testing.T) {
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
	first, err := index.SaveSnapshot()
	if err != nil {
		t.Fatalf("save first snapshot: %v", err)
	}
	publishedEpoch := first.Epoch + 1000
	rewriteVectorIndexManifest(t, first.ManifestPath, func(manifest *vectorIndexManifest) {
		manifest.Epoch = publishedEpoch
	})
	second, err := index.SaveSnapshot()
	if err != nil {
		t.Fatalf("save second snapshot: %v", err)
	}
	if second.Epoch <= publishedEpoch {
		t.Fatalf("second epoch=%d did not advance past published epoch=%d", second.Epoch, publishedEpoch)
	}
}

func TestCollectionVectorIndexSnapshotConcurrentSaveEpochsDoNotCollide(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
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
	first, err := index.SaveSnapshot()
	if err != nil {
		t.Fatalf("save first snapshot: %v", err)
	}
	rewriteVectorIndexManifest(t, first.ManifestPath, func(manifest *vectorIndexManifest) {
		manifest.Epoch = ^uint64(0) - 100
	})

	const saves = 4
	start := make(chan struct{})
	statuses := make([]VectorIndexLoadStatus, saves)
	errs := make([]error, saves)
	var wg sync.WaitGroup
	wg.Add(saves)
	for i := 0; i < saves; i++ {
		go func(i int) {
			defer wg.Done()
			<-start
			statuses[i], errs[i] = index.SaveSnapshot()
		}(i)
	}
	close(start)
	wg.Wait()

	seen := make(map[uint64]struct{}, saves)
	for i := 0; i < saves; i++ {
		if errs[i] != nil {
			t.Fatalf("save %d: %v", i, errs[i])
		}
		if !statuses[i].Loaded || statuses[i].Epoch == 0 {
			t.Fatalf("save %d returned status %+v", i, statuses[i])
		}
		if _, ok := seen[statuses[i].Epoch]; ok {
			t.Fatalf("duplicate snapshot epoch %d across statuses %+v", statuses[i].Epoch, statuses)
		}
		seen[statuses[i].Epoch] = struct{}{}
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

func TestCollectionVectorIndexPruneSerializesWithSnapshotSave(t *testing.T) {
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
	if _, err := index.SaveSnapshot(); err != nil {
		t.Fatalf("save first snapshot: %v", err)
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	var saveStatus VectorIndexLoadStatus
	var pruneStatus VectorIndexPruneStatus
	var saveErr, pruneErr error
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		saveStatus, saveErr = index.SaveSnapshot()
	}()
	go func() {
		defer wg.Done()
		<-start
		pruneStatus, pruneErr = index.PruneOldSnapshots(1)
	}()
	close(start)
	wg.Wait()
	if saveErr != nil {
		t.Fatalf("concurrent save: %v", saveErr)
	}
	if pruneErr != nil {
		t.Fatalf("concurrent prune: %v", pruneErr)
	}
	if !saveStatus.Loaded || saveStatus.Epoch == 0 {
		t.Fatalf("unexpected save status: %+v", saveStatus)
	}
	if pruneStatus.ActiveEpoch == "" {
		t.Fatalf("unexpected prune status: %+v", pruneStatus)
	}
	loaded, loadStatus, err := col.LoadVectorIndexSnapshot(VectorIndexOptions{Name: "embedding", Field: "embedding", Metric: VectorMetricCosine})
	if err != nil {
		t.Fatalf("load after concurrent prune/save: %v", err)
	}
	if loaded == nil || !loadStatus.Loaded || loadStatus.ExactFallbackReason != "" {
		t.Fatalf("load after concurrent prune/save loaded=%v status=%+v", loaded != nil, loadStatus)
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
