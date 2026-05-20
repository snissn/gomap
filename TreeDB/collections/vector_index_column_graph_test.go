package collections

import (
	"encoding/json"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestCollectionVectorIndexColumnGraphContractReportsPhysicalAssetsMissing(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	def := columnGraphVectorIndexTestDefinition()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "docs",
		Options: CollectionOptions{
			ColumnStore: columnGraphVectorIndexTestColumnStore(nil),
		},
		VectorIndexes: []VectorIndexDefinition{def},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	status, err := col.VectorIndexStatus(def.Name)
	if err != nil {
		t.Fatalf("VectorIndexStatus: %v", err)
	}
	assertColumnGraphUnavailableStatus(t, status, vectorIndexFallbackColumnGraphPhysicalMissing)
	if status.Registered || status.NativeRuntimeUsed || status.NativeRootLoaded {
		t.Fatalf("column_graph status used native runtime/root: %+v", status)
	}

	loaded, loadStatus, err := col.LoadVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("LoadVectorIndexSnapshot: %v", err)
	}
	if loaded != nil {
		t.Fatalf("LoadVectorIndexSnapshot returned native runtime for column_graph")
	}
	if loadStatus.Strategy != VectorIndexStrategyColumnGraph || loadStatus.ExactFallbackReason != vectorIndexFallbackColumnGraphPhysicalMissing || loadStatus.ColumnGraphUnavailableReason != vectorIndexFallbackColumnGraphPhysicalMissing {
		t.Fatalf("unexpected column_graph load status: %+v", loadStatus)
	}
	nativeLoaded, nativeStatus, err := col.LoadNativeVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("LoadNativeVectorIndexSnapshot: %v", err)
	}
	if nativeLoaded != nil || nativeStatus.Strategy != VectorIndexStrategyNativeRuntime || nativeStatus.ExactFallbackReason != vectorIndexFallbackStrategyMismatch {
		t.Fatalf("native loader should reject column_graph definition, loaded=%v status=%+v", nativeLoaded != nil, nativeStatus)
	}

	rebuild, err := col.RebuildVectorIndex(def.Name)
	if err != nil {
		t.Fatalf("RebuildVectorIndex returned error instead of status: %v", err)
	}
	assertColumnGraphUnavailableStatus(t, rebuild, vectorIndexFallbackColumnGraphPhysicalMissing)
	if col.hasRegisteredVectorIndex(def.Name) {
		t.Fatalf("column_graph rebuild registered a native runtime")
	}
}

func TestLoadVectorIndexSnapshotColumnGraphDoesNotReportNativeLoaded(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	def := columnGraphVectorIndexTestDefinition()
	active := ColumnManifestIdentity{Generation: 3, Version: columnManifestIdentityVersion, Checksum: 0x44}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "docs",
		Options: CollectionOptions{
			ColumnStore: columnGraphVectorIndexTestColumnStore(nil),
		},
		VectorIndexes: []VectorIndexDefinition{def},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	publishColumnStoreCatalogForTest(t, d, CollectionMeta{
		Name: "docs",
		Options: CollectionOptions{
			ColumnStore: columnGraphVectorIndexTestColumnStore(&active),
		},
		VectorIndexes: []VectorIndexDefinition{def},
	}, active)
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	graph := newColumnGraphVectorIndexTestGraph(t)
	loader := testColumnVectorGraphIndexLoader{
		result: ColumnVectorGraphIndexLoadResult{
			Graph: graph,
			Status: VectorIndexLoadStatus{
				Strategy:                      VectorIndexStrategyColumnGraph,
				PhysicalColumnAssetsSupported: true,
				BytesDisk:                     123,
			},
		},
	}

	loadedGraph, graphStatus, err := col.loadColumnGraphVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def), loader)
	if err != nil {
		t.Fatalf("LoadColumnGraphVectorIndexSnapshot: %v", err)
	}
	if loadedGraph == nil || !graphStatus.Loaded || !graphStatus.ColumnGraphLoaded {
		t.Fatalf("explicit column_graph loader did not report loaded graph: graph=%v status=%+v", loadedGraph != nil, graphStatus)
	}

	loaded, status, err := col.loadVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def), loader)
	if err != nil {
		t.Fatalf("LoadVectorIndexSnapshot: %v", err)
	}
	if loaded != nil || status.Loaded || !status.ColumnGraphLoaded {
		t.Fatalf("generic loader must not report a usable native index while preserving column graph status, loaded=%v status=%+v", loaded != nil, status)
	}
	if status.ExactFallbackReason != vectorIndexFallbackColumnGraphHandleMissing || status.ColumnGraphUnavailableReason != "" {
		t.Fatalf("unexpected generic column_graph load reason: %+v", status)
	}
	if !status.PhysicalColumnAssetsSupported || status.RebuildNeeded {
		t.Fatalf("generic column_graph status lost physical support or requested rebuild: %+v", status)
	}
}

func TestColumnGraphUnavailableStatusPreservesPhysicalSupportForLoaderFailures(t *testing.T) {
	status := VectorIndexLoadStatus{
		Strategy:                      VectorIndexStrategyColumnGraph,
		PhysicalColumnAssetsSupported: true,
	}
	mergeColumnGraphLoadStatus(&status, VectorIndexLoadStatus{
		PhysicalColumnAssetsSupported: true,
		ColumnGraphUnavailableReason:  vectorIndexFallbackColumnGraphManifestInvalid,
		BytesDisk:                     4096,
	})
	if status.PhysicalColumnAssetsSupported == false {
		t.Fatalf("loader failure discarded physical column support: %+v", status)
	}
	if status.ExactFallbackReason != vectorIndexFallbackColumnGraphManifestInvalid || status.ColumnGraphUnavailableReason != vectorIndexFallbackColumnGraphManifestInvalid {
		t.Fatalf("unexpected fallback reason: %+v", status)
	}
	if status.BytesDisk != 4096 || !status.RebuildNeeded {
		t.Fatalf("unexpected status accounting: %+v", status)
	}
}

func TestColumnGraphRebuildUnsupportedStatusPreservesReprobeReason(t *testing.T) {
	def := columnGraphVectorIndexTestDefinition()
	status := columnGraphRebuildUnsupportedStatus(def, columnGraphUnavailableLoadStatus(vectorIndexFallbackColumnGraphReprobeRequired))
	if status.ExactFallbackReason != vectorIndexFallbackColumnGraphReprobeRequired || status.ColumnGraphUnavailableReason != vectorIndexFallbackColumnGraphReprobeRequired {
		t.Fatalf("rebuild status reason=%q column reason=%q want %q: %+v", status.ExactFallbackReason, status.ColumnGraphUnavailableReason, vectorIndexFallbackColumnGraphReprobeRequired, status)
	}
	if !status.RebuildNeeded || status.ColumnGraphLoaded || status.PhysicalColumnAssetsSupported {
		t.Fatalf("unexpected rebuild status flags: %+v", status)
	}
}

func TestCollectionVectorIndexColumnGraphReportsManifestRootMismatch(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	def := columnGraphVectorIndexTestDefinition()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs", VectorIndexes: []VectorIndexDefinition{def}}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	active := ColumnManifestIdentity{Generation: 7, Version: columnManifestIdentityVersion, Checksum: 0x1111}
	rootIdentity := active
	rootIdentity.Checksum = 0x2222
	publishColumnStoreCatalogForTest(t, d, CollectionMeta{
		Name: "docs",
		Options: CollectionOptions{
			ColumnStore: columnGraphVectorIndexTestColumnStore(&active),
		},
		VectorIndexes: []VectorIndexDefinition{def},
	}, rootIdentity)

	graph, status, err := col.LoadColumnGraphVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("LoadColumnGraphVectorIndexSnapshot: %v", err)
	}
	if graph != nil {
		t.Fatalf("loaded graph despite manifest mismatch")
	}
	if status.RootID == 0 || status.ExactFallbackReason != vectorIndexFallbackColumnGraphManifestInvalid || status.ColumnGraphUnavailableReason != vectorIndexFallbackColumnGraphManifestInvalid {
		t.Fatalf("unexpected mismatch status: %+v", status)
	}
	rebuild, err := col.RebuildVectorIndex(def.Name)
	if err != nil {
		t.Fatalf("RebuildVectorIndex should report mismatch status, not error: %v", err)
	}
	if rebuild.ExactFallbackReason != vectorIndexFallbackColumnGraphManifestInvalid || rebuild.ColumnGraphUnavailableReason != vectorIndexFallbackColumnGraphManifestInvalid || !rebuild.RebuildNeeded {
		t.Fatalf("unexpected rebuild mismatch status: %+v", rebuild)
	}
}

func TestCollectionVectorIndexColumnGraphOptionDoesNotLoadNativeIndex(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	def := VectorIndexDefinition{
		Name:       "embedding",
		Field:      "embedding",
		Metric:     VectorMetricCosine,
		Dimensions: 2,
	}
	mgr := NewCollectionManager(d)
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
	if rebuild, err := col.RebuildVectorIndex(def.Name); err != nil || !rebuild.NativeRuntimeUsed || !rebuild.NativeRootLoaded {
		t.Fatalf("native rebuild status=%+v err=%v", rebuild, err)
	}
	nativeLoaded, nativeLoadStatus, err := col.LoadVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("native LoadVectorIndexSnapshot: %v", err)
	}
	if nativeLoaded == nil || nativeLoadStatus.Strategy != VectorIndexStrategyNativeRuntime || !nativeLoadStatus.NativeRuntimeUsed {
		t.Fatalf("native load status missing native strategy/runtime: loaded=%v status=%+v", nativeLoaded != nil, nativeLoadStatus)
	}

	loaded, status, err := col.LoadVectorIndexSnapshot(VectorIndexOptions{
		Name:       def.Name,
		Field:      def.Field,
		Metric:     def.Metric,
		Dimensions: def.Dimensions,
		Strategy:   VectorIndexStrategyColumnGraph,
	})
	if err != nil {
		t.Fatalf("LoadVectorIndexSnapshot column_graph option: %v", err)
	}
	if loaded != nil {
		t.Fatalf("column_graph option loaded native runtime")
	}
	if status.Strategy != VectorIndexStrategyColumnGraph || status.ExactFallbackReason != vectorIndexFallbackColumnGraphStrategyMissing || status.ColumnGraphUnavailableReason != vectorIndexFallbackColumnGraphStrategyMissing {
		t.Fatalf("unexpected column_graph strategy-missing status: %+v", status)
	}
}

func TestLoadVectorIndexSnapshotRejectsInvalidStrategyOption(t *testing.T) {
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

	loaded, status, err := col.LoadVectorIndexSnapshot(VectorIndexOptions{
		Name:       "embedding",
		Field:      "embedding",
		Metric:     VectorMetricCosine,
		Dimensions: 2,
		Strategy:   "bogus",
	})
	if err == nil || !strings.Contains(err.Error(), "unsupported vector index strategy") {
		t.Fatalf("LoadVectorIndexSnapshot err=%v want unsupported strategy", err)
	}
	if loaded != nil || status.Loaded {
		t.Fatalf("invalid strategy should not load an index: loaded=%v status=%+v", loaded != nil, status)
	}
}

func TestCollectionVectorIndexColumnGraphMutationsRemainRebuildNeeded(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	def := columnGraphVectorIndexTestDefinition()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:          "docs",
		VectorIndexes: []VectorIndexDefinition{def},
	}); err != nil {
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
	if _, modified, err := col.Update([]byte("a"), func([]byte) ([]byte, bool, error) {
		return []byte(`{"embedding":[0.9,0.1]}`), true, nil
	}); err != nil || !modified {
		t.Fatalf("update modified=%v err=%v", modified, err)
	}
	if deleted, err := col.DeleteDocument([]byte("b")); err != nil || !deleted {
		t.Fatalf("delete deleted=%v err=%v", deleted, err)
	}
	status, err := col.VectorIndexStatus(def.Name)
	if err != nil {
		t.Fatalf("VectorIndexStatus: %v", err)
	}
	assertColumnGraphUnavailableStatus(t, status, vectorIndexFallbackColumnGraphPhysicalMissing)
	if col.hasRegisteredVectorIndex(def.Name) {
		t.Fatalf("mutations registered a native runtime for column_graph")
	}
}

func TestCollectionVectorIndexColumnGraphStrategyJSON(t *testing.T) {
	meta, err := normalizeCollectionMeta(CollectionMeta{
		Name: "docs",
		VectorIndexes: []VectorIndexDefinition{{
			Name:       "embedding",
			Field:      "embedding",
			Metric:     VectorMetricCosine,
			Dimensions: 2,
			Strategy:   VectorIndexStrategyColumnGraph,
		}},
	})
	if err != nil {
		t.Fatalf("normalize meta: %v", err)
	}
	raw, err := json.Marshal(meta)
	if err != nil {
		t.Fatalf("marshal meta: %v", err)
	}
	if !strings.Contains(string(raw), `"strategy":"column_graph"`) {
		t.Fatalf("vector metadata JSON=%s want column_graph strategy", raw)
	}
	var decoded CollectionMeta
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal meta: %v", err)
	}
	decoded, err = normalizeCollectionMeta(decoded)
	if err != nil {
		t.Fatalf("normalize decoded meta: %v", err)
	}
	got, ok := findVectorIndex(decoded.VectorIndexes, "embedding")
	if !ok || got.Strategy != VectorIndexStrategyColumnGraph {
		t.Fatalf("decoded vector index=%+v ok=%v", got, ok)
	}
}

func columnGraphVectorIndexTestDefinition() VectorIndexDefinition {
	return VectorIndexDefinition{
		Name:       "embedding",
		Field:      "embedding",
		Metric:     VectorMetricCosine,
		Dimensions: 2,
		Strategy:   VectorIndexStrategyColumnGraph,
	}
}

func columnGraphVectorIndexTestColumnStore(active *ColumnManifestIdentity) *ColumnStoreConfig {
	var recovery *ColumnManifestIdentity
	if active != nil {
		copied := *active
		recovery = &copied
	}
	cfg := &ColumnStoreConfig{
		Enabled: true,
		Columns: []ColumnStoreColumn{
			{Name: "embedding", Path: "embedding", ValueType: ColumnStoreValueFloat32Vector, VectorDims: 2},
			{Name: "embedding_inv_norm", Path: "embedding_inv_norm", ValueType: ColumnStoreValueFloat32},
			{Name: "embedding_neighbors", Path: "embedding_neighbors", ValueType: ColumnStoreValueAdjacencyList},
		},
		ActiveManifest:                active,
		RecoveryAuthoritativeManifest: recovery,
	}
	if active != nil {
		cfg.RecoveryAuthoritativeAppliedCommandLSN = 1
	}
	return cfg
}

type testColumnVectorGraphIndexLoader struct {
	result ColumnVectorGraphIndexLoadResult
	err    error
}

func (l testColumnVectorGraphIndexLoader) LoadColumnVectorGraphIndex(ColumnVectorGraphIndexLoadInput) (ColumnVectorGraphIndexLoadResult, error) {
	return l.result, l.err
}

func newColumnGraphVectorIndexTestGraph(t *testing.T) *ColumnVectorGraph {
	t.Helper()
	graph, err := NewColumnVectorGraphFromColumns(ColumnVectorGraphColumns{
		DocumentIDs:     [][]byte{[]byte("a")},
		Vectors:         []float32{1, 0},
		InvNorms:        []float32{1},
		NeighborOffsets: []uint32{0, 0},
		Dimensions:      2,
		EntryPoint:      0,
		EfSearch:        8,
	})
	if err != nil {
		t.Fatalf("new column vector graph: %v", err)
	}
	return graph
}

func assertColumnGraphUnavailableStatus(t *testing.T, status VectorIndexStatus, reason string) {
	t.Helper()
	if status.Strategy != VectorIndexStrategyColumnGraph {
		t.Fatalf("strategy=%q want column_graph: %+v", status.Strategy, status)
	}
	if status.ColumnGraphLoaded || status.PhysicalColumnAssetsSupported {
		t.Fatalf("column_graph unexpectedly loaded/supported: %+v", status)
	}
	if status.ExactFallbackReason != reason || status.ColumnGraphUnavailableReason != reason {
		t.Fatalf("fallback reason=%q column reason=%q want %q: %+v", status.ExactFallbackReason, status.ColumnGraphUnavailableReason, reason, status)
	}
	if !status.RebuildNeeded {
		t.Fatalf("column_graph status should be rebuild-needed: %+v", status)
	}
}
