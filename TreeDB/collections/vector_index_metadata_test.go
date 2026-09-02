package collections

import (
	"encoding/json"
	"errors"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

var vectorIndexStatusBenchSink VectorIndexStatus

func TestColumnGraphVectorIndexMetadataUsesCollectionMetaVersionV2A(t *testing.T) {
	raw, err := encodeCollectionMeta(CollectionMeta{
		Name: "docs",
		VectorIndexes: []VectorIndexDefinition{{
			Name:       "embedding_graph",
			Field:      "embedding",
			Metric:     VectorMetricCosine,
			Dimensions: 3,
			Strategy:   VectorIndexStrategyColumnGraph,
		}},
	})
	if err != nil {
		t.Fatalf("encodeCollectionMeta: %v", err)
	}
	var disk collectionMetaDisk
	if err := json.Unmarshal(raw, &disk); err != nil {
		t.Fatalf("unmarshal collectionMetaDisk: %v", err)
	}
	if collectionMetaVersion != 6 {
		t.Fatalf("collectionMetaVersion=%d want compound-index metadata version 6", collectionMetaVersion)
	}
	if disk.Version != collectionMetaVersion {
		t.Fatalf("collection metadata version=%d want current compound-index metadata version %d", disk.Version, collectionMetaVersion)
	}
	if len(disk.VectorIndexes) != 1 || disk.VectorIndexes[0].Name != "embedding_graph" {
		t.Fatalf("encoded vector indexes=%+v", disk.VectorIndexes)
	}
}

func TestDecodeCollectionMetaAcceptsLegacyV5VectorMetadata(t *testing.T) {
	want, err := normalizeCollectionMeta(CollectionMeta{
		Name: "docs",
		VectorIndexes: []VectorIndexDefinition{{
			Name:       "embedding_graph",
			Field:      "embedding",
			Metric:     VectorMetricCosine,
			Dimensions: 3,
			Strategy:   VectorIndexStrategyColumnGraph,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := encodeNormalizedCollectionMeta(want)
	if err != nil {
		t.Fatal(err)
	}
	var disk collectionMetaDisk
	if err := json.Unmarshal(raw, &disk); err != nil {
		t.Fatal(err)
	}
	disk.Version = collectionMetaVersionV5
	raw, err = json.Marshal(disk)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeCollectionMeta(raw)
	if err != nil {
		t.Fatalf("decode v5 vector metadata: %v", err)
	}
	if !collectionMetaValuesEqual(got, want) {
		t.Fatalf("decoded v5 metadata=%+v want %+v", got, want)
	}

	disk.Indexes = []IndexDefinition{{Name: "compound", Field: "a", Components: []IndexComponent{{Field: "a", Direction: IndexDirectionAscending}}}}
	if raw, err = json.Marshal(disk); err != nil {
		t.Fatal(err)
	}
	if _, err := decodeCollectionMeta(raw); err == nil {
		t.Fatal("v5 metadata with v6 compound components decoded")
	}
	for _, version := range []int{collectionMetaVersionV5 - 1, collectionMetaVersion + 1} {
		disk.Version = version
		if raw, err = json.Marshal(disk); err != nil {
			t.Fatal(err)
		}
		if _, err := decodeCollectionMeta(raw); err == nil {
			t.Fatalf("unsupported metadata version %d decoded", version)
		}
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

func TestBuildVectorIndexDeclaredColumnGraphKeepsCompatibilityV2A(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()
	mgr := NewCollectionManager(d)
	def := VectorIndexDefinition{
		Name:       "embedding_graph",
		Field:      "embedding",
		Metric:     VectorMetricCosine,
		Dimensions: 2,
		Strategy:   VectorIndexStrategyColumnGraph,
	}
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "docs", VectorIndexes: []VectorIndexDefinition{def}}); err != nil {
		t.Fatalf("create collection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("open collection: %v", err)
	}
	if _, err := col.InsertBatch([][]byte{[]byte("a")}, [][]byte{[]byte(`{"embedding":[1,0]}`)}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	index, err := col.BuildVectorIndex(vectorIndexOptionsFromDefinition(def))
	if err != nil {
		t.Fatalf("build declared column graph: %v", err)
	}
	if col.registeredVectorIndex(def.Name) != index {
		t.Fatal("declared column graph build did not retain the compatible in-memory registration")
	}
	if index.isNativePersistent() {
		t.Fatal("declared column graph was promoted to the native persistent runtime")
	}
	if _, err := col.InsertBatch([][]byte{[]byte("b")}, [][]byte{[]byte(`{"embedding":[0,1]}`)}); err != nil {
		t.Fatalf("insert after column graph compatibility build: %v", err)
	}
	if col.registeredVectorIndex(def.Name) != index || index.isNativePersistent() {
		t.Fatal("column graph compatibility registration moved into the native registry after mutation")
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

func TestNativeRuntimeVectorIndexStatusMarksSnapshotForegroundRead(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = d.Close() }()

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

	begins, ends, active := 0, 0, 0
	unregister := d.RegisterForegroundReadObserver(func() {}, func() func() {
		begins++
		active++
		return func() {
			ends++
			active--
		}
	})
	defer unregister()

	if _, err := col.VectorIndexStatus("embedding_graph"); err != nil {
		t.Fatalf("VectorIndexStatus: %v", err)
	}
	if begins != 1 || ends != 1 || active != 0 {
		t.Fatalf("foreground begin/end/active=%d/%d/%d want 1/1/0", begins, ends, active)
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
