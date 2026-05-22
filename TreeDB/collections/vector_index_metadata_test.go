package collections

import (
	"encoding/json"
	"errors"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

var vectorIndexStatusBenchSink VectorIndexStatus

// TestCollectionMetaVersionIsV2 verifies that the collection metadata version
// written to disk is 2 after the PR downgrade from 3. It also verifies that a
// document with a different version is rejected on decode.
func TestCollectionMetaVersionIsV2(t *testing.T) {
	raw, err := encodeCollectionMeta(CollectionMeta{Name: "docs"})
	if err != nil {
		t.Fatalf("encodeCollectionMeta: %v", err)
	}
	var disk collectionMetaDisk
	if err := json.Unmarshal(raw, &disk); err != nil {
		t.Fatalf("unmarshal collectionMetaDisk: %v", err)
	}
	if disk.Version != 2 {
		t.Fatalf("collection metadata version=%d want 2", disk.Version)
	}
}

// TestCollectionMetaDecodeRejectsWrongVersionV2 verifies that decodeCollectionMeta
// returns an error when the on-disk version does not match collectionMetaVersion (2).
func TestCollectionMetaDecodeRejectsWrongVersionV2(t *testing.T) {
	for _, version := range []int{1, 3, 99} {
		wrong, err := json.Marshal(collectionMetaDisk{Version: version, Name: "docs"})
		if err != nil {
			t.Fatalf("marshal wrong version: %v", err)
		}
		if _, err := decodeCollectionMeta(wrong); err == nil {
			t.Fatalf("decodeCollectionMeta(version=%d) succeeded, want error", version)
		}
	}
}

// TestCollectionMetaVersionPreservedAfterRoundTripV2 verifies that
// creating and decoding a collection meta round-trips cleanly with version 2.
func TestCollectionMetaVersionPreservedAfterRoundTripV2(t *testing.T) {
	original := CollectionMeta{
		Name: "roundtrip",
		VectorIndexes: []VectorIndexDefinition{{
			Name:       "embedding_graph",
			Field:      "embedding",
			Metric:     VectorMetricCosine,
			Dimensions: 3,
			Strategy:   VectorIndexStrategyColumnGraph,
		}},
	}
	raw, err := encodeCollectionMeta(original)
	if err != nil {
		t.Fatalf("encodeCollectionMeta: %v", err)
	}
	decoded, err := decodeCollectionMeta(raw)
	if err != nil {
		t.Fatalf("decodeCollectionMeta: %v", err)
	}
	if decoded.Name != original.Name {
		t.Fatalf("decoded name=%q want %q", decoded.Name, original.Name)
	}
	if len(decoded.VectorIndexes) != 1 || decoded.VectorIndexes[0].Name != "embedding_graph" {
		t.Fatalf("decoded vector indexes=%+v", decoded.VectorIndexes)
	}
}

func TestColumnGraphVectorIndexMetadataCreateStatusDropReopenV2A(t *testing.T) {
	dir := t.TempDir()
	d, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "docs",
		Options: CollectionOptions{ColumnStore: &ColumnStoreConfig{
			Enabled: true,
			Columns: []ColumnStoreColumn{
				{Name: "embedding", Path: "embedding", ValueType: ColumnStoreValueFloat32Vector, VectorDims: 3},
				{Name: "embedding_inv_norm", Path: "embedding_inv_norm", ValueType: ColumnStoreValueFloat32},
				{Name: "embedding_neighbors", Path: "embedding_neighbors", ValueType: ColumnStoreValueAdjacencyList},
			},
		}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	meta, err := col.CreateVectorIndex(VectorIndexDefinition{
		Name:       "embedding_graph",
		Field:      "embedding",
		Metric:     VectorMetricCosine,
		Dimensions: 3,
		Strategy:   VectorIndexStrategyColumnGraph,
	})
	if err != nil {
		t.Fatalf("create vector index: %v", err)
	}
	def, ok := findVectorIndex(meta.VectorIndexes, "embedding_graph")
	if !ok {
		t.Fatalf("created meta missing vector index: %+v", meta.VectorIndexes)
	}
	if def.Encoding != VectorIndexEncodingFloat32 || def.M != defaultVectorIndexM || def.EfSearch != defaultVectorIndexEfSearch {
		t.Fatalf("unexpected normalized vector index: %+v", def)
	}

	meta.VectorIndexes[0].Name = "mutated"
	if got := col.Meta().VectorIndexes[0].Name; got != "embedding_graph" {
		t.Fatalf("Meta leaked vector index mutation: got %q want embedding_graph", got)
	}

	status, err := col.VectorIndexStatus("embedding_graph")
	if err != nil {
		t.Fatalf("vector index status: %v", err)
	}
	if status.Strategy != VectorIndexStrategyColumnGraph ||
		status.State != VectorIndexStateColumnGraphRebuildNeeded ||
		status.Reason != VectorIndexReasonColumnGraphRebuildNeeded ||
		!status.RebuildNeeded ||
		status.Loaded {
		t.Fatalf("unexpected column_graph status before build: %+v", status)
	}

	if err := d.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	reopenedDB, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer func() { _ = reopenedDB.Close() }()
	reopened, err := NewCollectionManager(reopenedDB).OpenCollection("docs")
	if err != nil {
		t.Fatalf("reopen collection: %v", err)
	}
	if _, ok := findVectorIndex(reopened.Meta().VectorIndexes, "embedding_graph"); !ok {
		t.Fatalf("reopened meta missing vector index: %+v", reopened.Meta().VectorIndexes)
	}

	dropped, err := reopened.DropVectorIndex("embedding_graph")
	if err != nil {
		t.Fatalf("drop vector index: %v", err)
	}
	if _, ok := findVectorIndex(dropped.VectorIndexes, "embedding_graph"); ok {
		t.Fatalf("dropped meta still has vector index: %+v", dropped.VectorIndexes)
	}
	if _, err := reopened.VectorIndexStatus("embedding_graph"); !errors.Is(err, ErrIndexNotFound) {
		t.Fatalf("status after drop err=%v want ErrIndexNotFound", err)
	}
}

func TestColumnGraphVectorIndexStatusReportsMissingPhysicalSupportV2A(t *testing.T) {
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
	if _, err := col.CreateVectorIndex(VectorIndexDefinition{
		Name:       "embedding_graph",
		Field:      "embedding",
		Dimensions: 3,
		Strategy:   VectorIndexStrategyColumnGraph,
	}); err != nil {
		t.Fatalf("create vector index: %v", err)
	}

	status, err := col.VectorIndexStatus("embedding_graph")
	if err != nil {
		t.Fatalf("vector index status: %v", err)
	}
	if status.State != VectorIndexStateColumnGraphUnavailable ||
		status.Reason != VectorIndexReasonPhysicalColumnAssetSupportMissing ||
		!status.RebuildNeeded ||
		status.Loaded {
		t.Fatalf("unexpected status without column store: %+v", status)
	}
}

func TestNativeRuntimeVectorIndexStatusClosedDBReturnsErrClosedV2A(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "docs",
		VectorIndexes: []VectorIndexDefinition{{
			Name:       "embedding_graph",
			Field:      "embedding",
			Dimensions: 3,
			Strategy:   VectorIndexStrategyNativeRuntime,
		}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}
	if _, err := col.VectorIndexStatus("embedding_graph"); !errors.Is(err, backenddb.ErrClosed) {
		t.Fatalf("VectorIndexStatus after close err=%v want ErrClosed", err)
	}
}

func TestScalarIndexMutationPreservesVectorIndexMetadataV2A(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "docs",
		Indexes: []IndexDefinition{{Name: "kind", Field: "kind", ValueType: IndexValueString}},
		VectorIndexes: []VectorIndexDefinition{{
			Name:       "embedding_graph",
			Field:      "embedding",
			Dimensions: 3,
			Strategy:   VectorIndexStrategyColumnGraph,
		}},
	}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}

	if _, err := col.DropIndex("kind"); err != nil {
		t.Fatalf("drop scalar index: %v", err)
	}
	if _, ok := findVectorIndex(col.Meta().VectorIndexes, "embedding_graph"); !ok {
		t.Fatalf("scalar DropIndex removed vector index metadata: %+v", col.Meta().VectorIndexes)
	}
}

func TestVectorIndexMetadataValidationV2A(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		meta, err := normalizeCollectionMeta(CollectionMeta{
			Name: "docs",
			VectorIndexes: []VectorIndexDefinition{{
				Field:      "embedding",
				Dimensions: 128,
				Strategy:   VectorIndexStrategyColumnGraph,
			}},
		})
		if err != nil {
			t.Fatalf("normalize: %v", err)
		}
		if len(meta.VectorIndexes) != 1 {
			t.Fatalf("vector indexes=%+v", meta.VectorIndexes)
		}
		def := meta.VectorIndexes[0]
		if def.Name != "embedding" ||
			def.Metric != VectorMetricCosine ||
			def.Encoding != VectorIndexEncodingFloat32 ||
			def.M != defaultVectorIndexM ||
			def.EfConstruction != defaultVectorIndexEfConstruction ||
			def.EfSearch != defaultVectorIndexEfSearch {
			t.Fatalf("unexpected defaults: %+v", def)
		}
	})

	cases := []struct {
		name string
		meta CollectionMeta
	}{
		{
			name: "duplicate scalar index name",
			meta: CollectionMeta{
				Name:    "docs",
				Indexes: []IndexDefinition{{Name: "embedding", Field: "kind", ValueType: IndexValueString}},
				VectorIndexes: []VectorIndexDefinition{{
					Name:       "embedding",
					Field:      "embedding",
					Dimensions: 3,
					Strategy:   VectorIndexStrategyColumnGraph,
				}},
			},
		},
		{
			name: "duplicate vector index name",
			meta: CollectionMeta{
				Name: "docs",
				VectorIndexes: []VectorIndexDefinition{
					{Name: "embedding", Field: "embedding", Dimensions: 3, Strategy: VectorIndexStrategyColumnGraph},
					{Name: "embedding", Field: "other_embedding", Dimensions: 3, Strategy: VectorIndexStrategyColumnGraph},
				},
			},
		},
		{
			name: "zero dimensions",
			meta: CollectionMeta{Name: "docs", VectorIndexes: []VectorIndexDefinition{{
				Name:     "embedding",
				Field:    "embedding",
				Strategy: VectorIndexStrategyColumnGraph,
			}}},
		},
		{
			name: "unsupported metric",
			meta: CollectionMeta{Name: "docs", VectorIndexes: []VectorIndexDefinition{{
				Name:       "embedding",
				Field:      "embedding",
				Metric:     VectorMetricL2,
				Dimensions: 3,
				Strategy:   VectorIndexStrategyColumnGraph,
			}}},
		},
		{
			name: "unsupported encoding",
			meta: CollectionMeta{Name: "docs", VectorIndexes: []VectorIndexDefinition{{
				Name:       "embedding",
				Field:      "embedding",
				Dimensions: 3,
				Encoding:   VectorIndexEncodingInt8,
				Strategy:   VectorIndexStrategyColumnGraph,
			}}},
		},
		{
			name: "unsupported strategy",
			meta: CollectionMeta{Name: "docs", VectorIndexes: []VectorIndexDefinition{{
				Name:       "embedding",
				Field:      "embedding",
				Dimensions: 3,
				Strategy:   VectorIndexStrategy("decoded_graph"),
			}}},
		},
		{
			name: "negative build parameter",
			meta: CollectionMeta{Name: "docs", VectorIndexes: []VectorIndexDefinition{{
				Name:       "embedding",
				Field:      "embedding",
				Dimensions: 3,
				M:          -1,
				Strategy:   VectorIndexStrategyColumnGraph,
			}}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := normalizeCollectionMeta(tc.meta); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func BenchmarkVectorIndexStatusV2A(b *testing.B) {
	d, err := backenddb.Open(backenddb.Options{Dir: b.TempDir()})
	if err != nil {
		b.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name: "docs",
		Options: CollectionOptions{ColumnStore: &ColumnStoreConfig{
			Enabled: true,
			Columns: []ColumnStoreColumn{
				{Name: "embedding", Path: "embedding", ValueType: ColumnStoreValueFloat32Vector, VectorDims: 3},
			},
		}},
		VectorIndexes: []VectorIndexDefinition{{
			Name:       "embedding_graph",
			Field:      "embedding",
			Dimensions: 3,
			Strategy:   VectorIndexStrategyColumnGraph,
		}},
	}); err != nil {
		b.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		b.Fatalf("open collection: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		status, err := col.VectorIndexStatus("embedding_graph")
		if err != nil {
			b.Fatalf("vector index status: %v", err)
		}
		vectorIndexStatusBenchSink = status
	}
}
