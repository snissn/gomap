package collections

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
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
			snapshot.Edges[i].Distances[0] = float32(math.Inf(-1))
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
