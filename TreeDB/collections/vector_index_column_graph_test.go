package collections

import (
	"context"
	"encoding/json"
	"errors"
	"os"
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

func TestBuildVectorIndexRejectsDeclaredColumnGraphWithoutStrategyOption(t *testing.T) {
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

	_, err = col.BuildVectorIndex(VectorIndexOptions{
		Name:       def.Name,
		Field:      def.Field,
		Metric:     def.Metric,
		Dimensions: def.Dimensions,
	})
	if err == nil || !strings.Contains(err.Error(), "column_graph vector indexes require the column-backed graph loader") {
		t.Fatalf("BuildVectorIndex err=%v want column_graph loader boundary", err)
	}
	if col.hasRegisteredVectorIndex(def.Name) {
		t.Fatalf("BuildVectorIndex registered native runtime for declared column_graph index")
	}
}

func TestColumnGraphUnavailableStatusPreservesPhysicalSupportForLoaderFailures(t *testing.T) {
	status := VectorIndexLoadStatus{
		Strategy:                      VectorIndexStrategyColumnGraph,
		PhysicalColumnAssetsSupported: true,
	}
	mergeColumnGraphLoadStatus(&status, VectorIndexLoadStatus{
		PhysicalColumnAssetsSupported: true,
		ColumnGraphUnavailableReason:  vectorIndexFallbackColumnGraphManifestRootMismatch,
		ColumnGraphUnavailableDetail:  "test detail",
		BytesDisk:                     4096,
	})
	if status.PhysicalColumnAssetsSupported == false {
		t.Fatalf("loader failure discarded physical column support: %+v", status)
	}
	if status.ExactFallbackReason != vectorIndexFallbackColumnGraphManifestRootMismatch || status.ColumnGraphUnavailableReason != vectorIndexFallbackColumnGraphManifestRootMismatch {
		t.Fatalf("unexpected fallback reason: %+v", status)
	}
	if status.BytesDisk != 4096 || !status.RebuildNeeded {
		t.Fatalf("unexpected status accounting: %+v", status)
	}
	if status.ColumnGraphUnavailableDetail != "test detail" {
		t.Fatalf("unavailable detail=%q want test detail", status.ColumnGraphUnavailableDetail)
	}
	mergeColumnGraphLoadStatus(&status, VectorIndexLoadStatus{
		PhysicalColumnAssetsSupported: true,
		ColumnGraphUnavailableReason:  vectorIndexFallbackColumnGraphInvalid,
	})
	if status.ColumnGraphUnavailableDetail != "" {
		t.Fatalf("unavailable detail=%q want cleared after detail-less update", status.ColumnGraphUnavailableDetail)
	}
}

func TestColumnGraphPhysicalLoadFailureRecordsDetail(t *testing.T) {
	state := columnVectorGraphPhysicalLoadState{}
	err := state.fail(vectorIndexFallbackColumnGraphInvalid, "invalid vector column value")
	if !errors.Is(err, errColumnVectorGraphPhysicalLoadUnavailable) {
		t.Fatalf("fail err=%v want errColumnVectorGraphPhysicalLoadUnavailable", err)
	}
	if state.unavailableReason != vectorIndexFallbackColumnGraphInvalid {
		t.Fatalf("unavailable reason=%q want %q", state.unavailableReason, vectorIndexFallbackColumnGraphInvalid)
	}
	if state.unavailableDetail != "invalid vector column value" {
		t.Fatalf("unavailable detail=%q want physical scan detail", state.unavailableDetail)
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
	if status.RootID == 0 || status.ExactFallbackReason != vectorIndexFallbackColumnGraphManifestRootMismatch || status.ColumnGraphUnavailableReason != vectorIndexFallbackColumnGraphManifestRootMismatch {
		t.Fatalf("unexpected mismatch status: %+v", status)
	}
	results, trace, err := col.SearchVectorIndex(def.Name, []float32{1, 0}, VectorIndexSearchOptions{TopK: 1})
	if err != nil {
		t.Fatalf("SearchVectorIndex fallback for mismatch: %v", err)
	}
	if len(results) != 0 || trace.Strategy != "vector_index_exact_fallback" || trace.ExactFallbackReason != vectorIndexFallbackColumnGraphManifestRootMismatch {
		t.Fatalf("fallback results=%+v trace=%+v want exact fallback for manifest mismatch", results, trace)
	}
	_, disabledTrace, err := col.SearchVectorIndex(def.Name, []float32{1, 0}, VectorIndexSearchOptions{
		TopK:                 1,
		DisableExactFallback: true,
	})
	if err == nil || disabledTrace.ExactFallbackReason != vectorIndexFallbackColumnGraphManifestRootMismatch {
		t.Fatalf("disabled fallback err=%v trace=%+v want manifest mismatch fallback reason", err, disabledTrace)
	}
	rebuild, err := col.RebuildVectorIndex(def.Name)
	if err != nil {
		t.Fatalf("RebuildVectorIndex should report mismatch status, not error: %v", err)
	}
	if rebuild.ExactFallbackReason != vectorIndexFallbackColumnGraphManifestRootMismatch || rebuild.ColumnGraphUnavailableReason != vectorIndexFallbackColumnGraphManifestRootMismatch || !rebuild.RebuildNeeded {
		t.Fatalf("unexpected rebuild mismatch status: %+v", rebuild)
	}
}

func TestCollectionVectorIndexColumnGraphExactFallbackRangeFilterBypassesColumnRootValidation(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	def := columnGraphVectorIndexTestDefinition()
	cityIndex := IndexDefinition{Name: "city_idx", Field: "city", ValueType: IndexValueString}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:          "docs",
		Indexes:       []IndexDefinition{cityIndex},
		VectorIndexes: []VectorIndexDefinition{def},
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
			[]byte(`{"embedding":[1,0],"city":"hnl"}`),
			[]byte(`{"embedding":[0,1],"city":"sea"}`),
			[]byte(`{"embedding":[0.9,0.1],"city":"hnl"}`),
		},
	); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := col.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	active := ColumnManifestIdentity{Generation: 7, Version: columnManifestIdentityVersion, Checksum: 0x1111}
	rootIdentity := active
	rootIdentity.Checksum = 0x2222
	columnStore := columnGraphVectorIndexTestColumnStore(&active)
	columnStore.RetainedPayload = ColumnRetainedPayloadFull
	publishColumnStoreCatalogForTest(t, d, CollectionMeta{
		Name:    "docs",
		Indexes: []IndexDefinition{cityIndex},
		Options: CollectionOptions{
			ColumnStore: columnStore,
		},
		VectorIndexes: []VectorIndexDefinition{def},
	}, rootIdentity)

	results, trace, err := col.SearchVectorIndex(def.Name, []float32{1, 0}, VectorIndexSearchOptions{
		TopK: 2,
		IndexRangeFilter: &VectorIndexRangeFilter{
			IndexName: cityIndex.Name,
			Range: IndexRangeOptions{
				Lower: IndexRangeBound{Value: "hnl", Inclusive: true},
				Upper: IndexRangeBound{Value: "hnl", Inclusive: true},
			},
		},
	})
	if err != nil {
		t.Fatalf("SearchVectorIndex fallback with range filter: %v", err)
	}
	requireVectorResultIDs(t, results, "a", "c")
	if trace.Strategy != "vector_index_exact_fallback" || trace.ExactFallbackReason != "column_graph_filter_requires_exact" || trace.ReturnedCount != 2 {
		t.Fatalf("trace=%+v want column_graph exact fallback with range-filter results", trace)
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

func TestCollectionVectorIndexColumnGraphLoadsPhysicalAssets(t *testing.T) {
	reopened, closeFn := openColumnGraphPhysicalAssetFixture(t)
	defer closeFn()

	def := columnGraphVectorIndexTestDefinition()
	graph, status, err := reopened.LoadColumnGraphVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("LoadColumnGraphVectorIndexSnapshot: %v", err)
	}
	if graph == nil {
		t.Fatalf("graph not loaded status=%+v", status)
	}
	if !status.Loaded || !status.ColumnGraphLoaded || !status.PhysicalColumnAssetsSupported || status.RebuildNeeded || status.ExactFallbackReason != "" {
		t.Fatalf("unexpected loaded status: %+v", status)
	}
	if status.BytesDisk <= 0 {
		t.Fatalf("loaded status bytes_disk=%d want physical bytes", status.BytesDisk)
	}
	if graph.Rows() != 2 || graph.Dims() != 2 || graph.Edges() != 2 {
		t.Fatalf("graph rows=%d dims=%d edges=%d", graph.Rows(), graph.Dims(), graph.Edges())
	}

	var scratch ColumnVectorGraphSearchScratch
	results, trace, err := graph.SearchCosine([]float32{1, 0}, ColumnVectorGraphSearchOptions{TopK: 2}, &scratch)
	if err != nil {
		t.Fatalf("SearchCosine: %v", err)
	}
	if len(results) != 2 || string(results[0].DocumentID) != "a" {
		t.Fatalf("results=%+v trace=%+v want doc a first", results, trace)
	}
	if trace.ReturnedCount != 2 || trace.CandidatesExamined == 0 || trace.EdgesVisited == 0 {
		t.Fatalf("unexpected trace: %+v", trace)
	}

	opStatus, err := reopened.VectorIndexStatus(def.Name)
	if err != nil {
		t.Fatalf("VectorIndexStatus: %v", err)
	}
	if !opStatus.ColumnGraphLoaded || !opStatus.PhysicalColumnAssetsSupported || opStatus.RebuildNeeded || opStatus.ExactFallbackReason != "" {
		t.Fatalf("unexpected operational status: %+v", opStatus)
	}
	if opStatus.Registered || opStatus.NativeRuntimeUsed || opStatus.NativeRootLoaded {
		t.Fatalf("column_graph status used native runtime/root: %+v", opStatus)
	}
}

func TestCollectionSearchVectorIndexUsesColumnGraphPhysicalAssets(t *testing.T) {
	reopened, closeFn := openColumnGraphPhysicalAssetFixture(t)
	defer closeFn()

	results, trace, err := reopened.SearchVectorIndex("embedding", []float32{1, 0}, VectorIndexSearchOptions{
		TopK:                 2,
		DisableExactFallback: true,
	})
	if err != nil {
		t.Fatalf("SearchVectorIndex: %v", err)
	}
	if trace.Strategy != columnVectorGraphStrategyCosine || trace.ExactFallbackReason != "" {
		t.Fatalf("trace=%+v want column graph without exact fallback", trace)
	}
	if len(results) != 2 || string(results[0].DocumentID) != "a" || len(results[0].Document) == 0 {
		t.Fatalf("results=%+v trace=%+v want materialized doc a first", results, trace)
	}
}

func TestCollectionSearchVectorIndexColumnGraphAllowsUnderfilledResultsWithFallbackDisabled(t *testing.T) {
	reopened, closeFn := openColumnGraphPhysicalAssetFixture(t)
	defer closeFn()

	results, trace, err := reopened.SearchVectorIndex("embedding", []float32{1, 0}, VectorIndexSearchOptions{
		TopK:                 10,
		DisableExactFallback: true,
	})
	if err != nil {
		t.Fatalf("SearchVectorIndex underfilled: %v", err)
	}
	if trace.Strategy != columnVectorGraphStrategyCosine || trace.ExactFallbackReason != "" || trace.ReturnedCount != 2 {
		t.Fatalf("trace=%+v want column graph with two returned rows and no exact fallback", trace)
	}
	if len(results) != 2 || string(results[0].DocumentID) != "a" || string(results[1].DocumentID) != "b" {
		t.Fatalf("underfilled results=%+v trace=%+v", results, trace)
	}
}

func TestCollectionVectorIndexColumnGraphPhysicalMutationManifestReportsRebuildNeeded(t *testing.T) {
	reopened, closeFn := openColumnGraphPhysicalAssetFixture(t)
	defer closeFn()

	def := columnGraphVectorIndexTestDefinition()
	if _, _, err := reopened.Update([]byte("a"), func([]byte) ([]byte, bool, error) {
		return []byte(`{"embedding":[1,0],"embedding_inv_norm":1,"embedding_neighbors":[1]}`), true, nil
	}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	status, err := reopened.VectorIndexStatus(def.Name)
	if err != nil {
		t.Fatalf("VectorIndexStatus: %v", err)
	}
	if status.Strategy != VectorIndexStrategyColumnGraph || !status.PhysicalColumnAssetsSupported || status.ColumnGraphLoaded || !status.RebuildNeeded {
		t.Fatalf("unexpected mutation status flags: %+v", status)
	}
	if status.ExactFallbackReason != vectorIndexFallbackColumnGraphVisibility || status.ColumnGraphUnavailableReason != vectorIndexFallbackColumnGraphVisibility {
		t.Fatalf("mutation fallback reason=%q column reason=%q want %q: %+v", status.ExactFallbackReason, status.ColumnGraphUnavailableReason, vectorIndexFallbackColumnGraphVisibility, status)
	}
	if status.Registered || status.NativeRuntimeUsed || status.NativeRootLoaded {
		t.Fatalf("mutation status used native runtime/root: %+v", status)
	}
}

func TestCollectionSearchVectorIndexColumnGraphFallsBackAfterMutation(t *testing.T) {
	reopened, closeFn := openColumnGraphPhysicalAssetFixture(t)
	defer closeFn()

	if _, _, err := reopened.Update([]byte("a"), func([]byte) ([]byte, bool, error) {
		return []byte(`{"embedding":[0,1],"embedding_inv_norm":1,"embedding_neighbors":[1]}`), true, nil
	}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	results, trace, err := reopened.SearchVectorIndex("embedding", []float32{1, 0}, VectorIndexSearchOptions{TopK: 2})
	if err != nil {
		t.Fatalf("SearchVectorIndex fallback: %v", err)
	}
	if trace.Strategy != "vector_index_exact_fallback" || trace.ExactFallbackReason != vectorIndexFallbackColumnGraphVisibility {
		t.Fatalf("trace=%+v want exact fallback for mutation-bearing column graph", trace)
	}
	if len(results) != 2 || string(results[0].DocumentID) != "a" || string(results[1].DocumentID) != "b" {
		t.Fatalf("fallback results=%+v trace=%+v", results, trace)
	}

	_, disabledTrace, err := reopened.SearchVectorIndex("embedding", []float32{1, 0}, VectorIndexSearchOptions{
		TopK:                 2,
		DisableExactFallback: true,
	})
	if err == nil || disabledTrace.ExactFallbackReason != vectorIndexFallbackColumnGraphVisibility {
		t.Fatalf("disabled fallback err=%v trace=%+v want column_graph visibility error", err, disabledTrace)
	}
}

func TestCollectionSearchVectorIndexColumnGraphSurvivesColumnAssetRewriteAndGC(t *testing.T) {
	dir, reopened, closeFn := openColumnGraphPhysicalAssetFixtureWithDir(t)
	closed := false
	defer func() {
		if !closed {
			closeFn()
		}
	}()

	beforeRefs := columnManifestAssetRefsForCollectionM12A(t, reopened.db, reopened)
	if len(beforeRefs) == 0 {
		t.Fatal("manifest refs empty, test requires live vector physical assets")
	}
	candidate := writeColumnGraphVectorAssetRewriteCandidate(t, reopened.db, reopened, 3, 99)
	if candidate.FileID != beforeRefs[0].FileID {
		t.Fatalf("candidate file_id=%d live file_id=%d, test requires mixed segment", candidate.FileID, beforeRefs[0].FileID)
	}
	oldSegmentPath, err := columnAssetSegmentPath(reopened.db.ColumnAssetRootDir(), candidate)
	if err != nil {
		t.Fatalf("columnAssetSegmentPath: %v", err)
	}

	rewrite, err := reopened.ColumnAssetRewrite(context.Background(), ColumnAssetRewriteOptions{
		Detailed:      true,
		CandidateRefs: []ColumnAssetRef{candidate},
	})
	if err != nil {
		t.Fatalf("ColumnAssetRewrite: %v", err)
	}
	if rewrite.SegmentsRewritten != 1 || rewrite.RefsRemapped != len(beforeRefs) {
		t.Fatalf("rewrite stats=%+v want one rewritten segment and %d remapped refs", rewrite, len(beforeRefs))
	}
	afterRefs := columnManifestAssetRefsForCollectionM12A(t, reopened.db, reopened)
	assertColumnAssetRefsRemappedM15C(t, beforeRefs, afterRefs)
	assertColumnGraphSearchUsesPhysicalAssets(t, reopened)
	if _, err := os.Stat(oldSegmentPath); err != nil {
		t.Fatalf("rewrite removed old mixed segment before GC: %v", err)
	}

	closeFn()
	closed = true
	reopen := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopen.Close() }()
	reopenedAfterRewrite, err := NewCollectionManager(reopen).OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection after rewrite: %v", err)
	}
	reopenRefs := columnManifestAssetRefsForCollectionM12A(t, reopen, reopenedAfterRewrite)
	assertColumnAssetRefsEqualM15C(t, afterRefs, reopenRefs)
	assertColumnGraphSearchUsesPhysicalAssets(t, reopenedAfterRewrite)

	gcStats, err := reopenedAfterRewrite.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{
		CandidateRefs: append(append([]ColumnAssetRef(nil), rewrite.SupersededRefs...), candidate),
	})
	if err != nil {
		t.Fatalf("ColumnAssetGC after vector rewrite: %v", err)
	}
	if gcStats.SegmentsDeleted != 1 || gcStats.BytesDeleted == 0 {
		t.Fatalf("gc stats=%+v want old mixed segment deleted after remap", gcStats)
	}
	if _, err := os.Stat(oldSegmentPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("old segment still exists or unexpected stat error: %v", err)
	}
	assertColumnGraphSearchUsesPhysicalAssets(t, reopenedAfterRewrite)
}

func TestCollectionVectorIndexColumnGraphReportsPhysicalSchemaMismatch(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	def := columnGraphVectorIndexTestDefinition()
	active := ColumnManifestIdentity{Generation: 7, Version: columnManifestIdentityVersion, Checksum: 0x1111}
	cfg := columnGraphVectorIndexTestColumnStore(&active)
	cfg.Columns = cfg.Columns[:1]
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs", VectorIndexes: []VectorIndexDefinition{def}}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	publishColumnStoreCatalogForTest(t, d, CollectionMeta{
		Name:          "docs",
		Options:       CollectionOptions{ColumnStore: cfg},
		VectorIndexes: []VectorIndexDefinition{def},
	}, active)

	graph, status, err := col.LoadColumnGraphVectorIndexSnapshot(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("LoadColumnGraphVectorIndexSnapshot: %v", err)
	}
	if graph != nil {
		t.Fatalf("loaded graph despite physical schema mismatch")
	}
	if !status.PhysicalColumnAssetsSupported || status.ExactFallbackReason != vectorIndexFallbackColumnGraphSchema || status.ColumnGraphUnavailableReason != vectorIndexFallbackColumnGraphSchema || !status.RebuildNeeded {
		t.Fatalf("unexpected schema mismatch status: %+v", status)
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

func openColumnGraphPhysicalAssetFixture(t *testing.T) (*Collection, func()) {
	t.Helper()
	_, reopened, closeFn := openColumnGraphPhysicalAssetFixtureWithDir(t)
	return reopened, closeFn
}

func openColumnGraphPhysicalAssetFixtureWithDir(t *testing.T) (string, *Collection, func()) {
	t.Helper()
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(t, dir)
	def := columnGraphVectorIndexTestDefinition()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "docs",
		Options: CollectionOptions{
			ColumnStore: columnGraphVectorIndexTestColumnStore(nil),
		},
		VectorIndexes: []VectorIndexDefinition{def},
	}); err != nil {
		_ = d.Close()
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		_ = d.Close()
		t.Fatalf("OpenCollection: %v", err)
	}
	if _, err := col.InsertBatch(
		[][]byte{[]byte("a"), []byte("b")},
		[][]byte{
			[]byte(`{"embedding":[1,0],"embedding_inv_norm":1,"embedding_neighbors":[1]}`),
			[]byte(`{"embedding":[0,1],"embedding_inv_norm":1,"embedding_neighbors":[0]}`),
		},
	); err != nil {
		_ = d.Close()
		t.Fatalf("InsertBatch: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		_ = d.Close()
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close seed DB: %v", err)
	}

	reopen := openCollectionCommandWALDB(t, dir)
	reopened, err := NewCollectionManager(reopen).OpenCollection("docs")
	if err != nil {
		_ = reopen.Close()
		t.Fatalf("OpenCollection reopened: %v", err)
	}
	return dir, reopened, func() { _ = reopen.Close() }
}

func writeColumnGraphVectorAssetRewriteCandidate(t testing.TB, d *backenddb.DB, col *Collection, generation, partID uint64) ColumnAssetRef {
	t.Helper()
	cfg := col.Meta().Options.ColumnStore
	if cfg == nil || cfg.AssetManager == nil {
		t.Fatalf("missing column store config: %+v", cfg)
	}
	encoded, _, err := encodeColumnPhysicalAsset(columnPhysicalAssetEncodeInput{
		Collection:        col.Meta().Name,
		Namespace:         cfg.AssetManager.Namespace,
		Generation:        generation,
		PartID:            partID,
		AppliedCommandLSN: d.State().AppliedCommandLSN + 1,
		Operation:         ColumnPublishOperationInsert,
		SchemaHash:        cfg.SchemaHash,
		Columns:           cfg.Columns,
		Rows: []columnDeclaredRow{{
			ID: []byte("candidate"),
			Values: []columnDeclaredValue{
				{Type: ColumnStoreValueFloat32Vector, Present: true, Float32Vector: []float32{1, 0}},
				{Type: ColumnStoreValueFloat32, Present: true, Float32: 1},
				{Type: ColumnStoreValueAdjacencyList, Present: true, AdjacencyList: []uint32{0}},
			},
		}},
	})
	if err != nil {
		t.Fatalf("encodeColumnPhysicalAsset: %v", err)
	}
	ref, err := writeColumnPhysicalAssetToManager(d.ColumnAssetRootDir(), *cfg, encoded, generation, partID)
	if err != nil {
		t.Fatalf("writeColumnPhysicalAssetToManager: %v", err)
	}
	return ref
}

func assertColumnGraphSearchUsesPhysicalAssets(t testing.TB, col *Collection) {
	t.Helper()
	results, trace, err := col.SearchVectorIndex("embedding", []float32{1, 0}, VectorIndexSearchOptions{
		TopK:                 2,
		DisableExactFallback: true,
	})
	if err != nil {
		t.Fatalf("SearchVectorIndex: %v", err)
	}
	if trace.Strategy != columnVectorGraphStrategyCosine || trace.ExactFallbackReason != "" {
		t.Fatalf("trace=%+v want column graph without exact fallback", trace)
	}
	if len(results) != 2 || string(results[0].DocumentID) != "a" || len(results[0].Document) == 0 {
		t.Fatalf("results=%+v trace=%+v want materialized doc a first", results, trace)
	}
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
