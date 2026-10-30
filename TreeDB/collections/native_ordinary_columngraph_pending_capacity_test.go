package collections

import (
	"bytes"
	"errors"
	"os"
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

func nativeOrdinaryPendingGraphMeta() CollectionMeta {
	return CollectionMeta{Name: "docs", Options: CollectionOptions{ColumnStore: &ColumnStoreConfig{
		Enabled: true, RetainedPayload: ColumnRetainedPayloadFull, RetainedPayloadEncoding: ColumnRetainedPayloadEncodingJSON,
		Columns: []ColumnStoreColumn{{Name: "embedding", Path: "embedding", ValueType: ColumnStoreValueFloat32Vector, VectorDims: 2, Owner: TypedStorageOwnerColumnPart}},
	}}, VectorIndexes: []VectorIndexDefinition{{Name: "vec", Field: "embedding", Dimensions: 2, Metric: VectorMetricCosine, Strategy: VectorIndexStrategyColumnGraph}}}
}

// Only controls that deliberately exercise the internal environment overrides
// opt into the maintained benchmark-relaxed policy. The public default-profile
// lifecycle below continues to use the untouched durable-only fixture.
func nativeOrdinaryPendingGraphBenchmarkMeta() CollectionMeta {
	meta := nativeOrdinaryPendingGraphMeta()
	meta.Options.ColumnStore.ProfileSupport = ColumnStoreProfileBenchmarkRelaxed
	return meta
}

func clearNativeOrdinaryPendingGraphCodecEnv(t *testing.T) {
	t.Helper()
	for _, name := range []string{
		typedColumnBenchmarkCompressionEnv, typedColumnBenchmarkSectionCompressionEnv,
		typedColumnBenchmarkLocatorCompressionEnv, typedColumnBenchmarkDictionaryCompressionEnv,
		typedColumnBenchmarkPruningCompressionEnv, typedColumnBenchmarkInt64EncodingEnv,
		typedColumnBenchmarkRowsPerGranuleEnv, typedColumnBenchmarkAdaptiveEnabledEnv,
		typedColumnBenchmarkAdaptiveTargetBytesEnv, typedColumnBenchmarkAdaptiveMinRowsEnv,
		typedColumnBenchmarkAdaptiveMaxRowsEnv,
	} {
		// Lookup presence matters to ADAPTIVE_ENABLED: even an empty value
		// is parsed as a bool, and false still activates benchmark policy.
		// t.Setenv registers restoration and testing's parallel-use guard;
		// removing the temporary value gives the real absent/default policy.
		t.Setenv(name, "")
		if err := os.Unsetenv(name); err != nil {
			t.Fatalf("unset %s: %v", name, err)
		}
	}
}

// This is the public immediate sequence that previously failed its first graph
// marker. There is no checkpoint, retry, cap override or large-budget fixture.
func TestNativeOrdinaryColumnGraphImmediateRebuildDefaultCapacity(t *testing.T) {
	clearNativeOrdinaryPendingGraphCodecEnv(t)
	dir := t.TempDir()
	opts := nativeTypedGroupOptions(dir)
	owner, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { owner.Close() })
	backend, err := owner.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager := NewCollectionManager(backend)
	t.Cleanup(func() { manager.Close() })
	meta := nativeOrdinaryPendingGraphMeta()
	if _, err = manager.CreateCollection(&meta); err != nil {
		t.Fatal(err)
	}
	col, err := manager.OpenCollection("docs")
	if err != nil {
		t.Fatal(err)
	}
	id, document := []byte("a"), []byte(`{"content":"owned","embedding":[1,0]}`)
	if _, _, err = col.InsertBatchWithStatsValidatedFloat32Projection([][]byte{id}, [][]byte{document}, "embedding", VectorMetricCosine, [][]float32{{1, 0}}); err != nil {
		t.Fatal(err)
	}
	held, err := col.nativeJSON.Capture("docs")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { held.Close() })
	if held.meta.Options.ColumnStore.ProfileSupport != ColumnStoreProfileDurableOnly {
		t.Fatalf("public default lifecycle lost durable-only policy: %q", held.meta.Options.ColumnStore.ProfileSupport)
	}
	budget := held.capture.NativeAllocator().(nativeHNSWAdmissionBudget)
	capture, err := backend.CaptureNativeSemanticState()
	if err != nil {
		t.Fatal(err)
	}
	state := capture.State().(*nativeEngineVersion)
	limits := state.maintenance.Limits()
	if limits.MaxInFlightBytes != 32<<20 || limits.MaxTotalBytes != 32<<20 {
		t.Fatalf("fixture lost the unchanged default maintenance cap: %+v", limits)
	}
	primary, _ := held.collection.domains.Get("primary")
	if primary == nil || primary.typed == nil {
		t.Fatal("first row was materialized before immediate rebuild")
	}
	if err = capture.Close(); err != nil {
		t.Fatal(err)
	}
	status, err := col.RebuildVectorIndex("vec")
	if err != nil {
		t.Fatalf("default-cap immediate rebuild: %v", err)
	}
	if !status.Loaded || status.State != VectorIndexStateColumnGraphLoaded || status.RebuildNeeded || status.Stats.RebuildNeeded || status.Stats.LiveDocs != 1 || status.Stats.BytesDisk <= 0 {
		t.Fatalf("rebuild did not install a current physical graph: %+v", status)
	}
	if got, e := held.Get(id); e != nil || !bytes.Equal(got, document) {
		t.Fatalf("held row changed: %s %v", got, e)
	}
	got, err := col.nativeJSON.Get("docs", id)
	if err != nil || !bytes.Equal(got, document) {
		t.Fatalf("current row changed: %s %v", got, err)
	}
	if held.meta.Options.ColumnStore.ActiveManifest != nil {
		t.Fatal("held pre-rebuild schema acquired a new physical manifest")
	}
	if err = held.Close(); err != nil {
		t.Fatal(err)
	}
	if err = manager.Close(); err != nil {
		t.Fatal(err)
	}
	if err = owner.Close(); err != nil {
		t.Fatal(err)
	}
	if stats := budget.Stats(); stats.TotalBytes != 0 || stats.ExternalLeases != 0 {
		t.Fatalf("full close retained original custody: %+v", stats)
	}
	owner, err = treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	backend, err = owner.NativeSemanticBackend()
	if err != nil {
		t.Fatal(err)
	}
	manager = NewCollectionManager(backend)
	col, err = manager.OpenCollection("docs")
	if err != nil {
		t.Fatal(err)
	}
	got, err = col.nativeJSON.Get("docs", id)
	if err != nil || !bytes.Equal(got, document) {
		t.Fatalf("reopened row changed: %s %v", got, err)
	}
	status, err = col.VectorIndexStatus("vec")
	if err != nil || !status.Loaded || status.Stats.LiveDocs != 1 || status.RebuildNeeded {
		t.Fatalf("reopened graph lost durable state: %+v %v", status, err)
	}
}

// Exercise the maintained producer and its actual emitted image, rather than
// asserting a hand-written codec table alone. Metadata-only ZSTD is skipped by
// the graph projection; column-data codecs come from the actual definitions.
func TestNativeOrdinaryColumnGraphPendingSourceCodecConcreteImage(t *testing.T) {
	for _, tc := range []struct {
		name, section, dictionary string
		wantDecoder               bool
	}{
		{name: "default"},
		{name: "raw_metadata_zstd_dictionary", section: "none", dictionary: "zstd"},
		{name: "lz4_metadata", section: "lz4"},
		{name: "zstd_metadata", section: "zstd"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clearNativeOrdinaryPendingGraphCodecEnv(t)
			t.Setenv(typedColumnBenchmarkSectionCompressionEnv, tc.section)
			t.Setenv(typedColumnBenchmarkDictionaryCompressionEnv, tc.dictionary)
			fixture := nativeOrdinaryPendingGraphMeta()
			if tc.section != "" || tc.dictionary != "" {
				fixture = nativeOrdinaryPendingGraphBenchmarkMeta()
			}
			if tc.dictionary == "zstd" {
				// This 120-byte identifier fits ValidateIndexName's 128-byte
				// limit and gives the real metadata dictionary repeated bytes.
				// The maintained compressor keeps ZSTD only when it is smaller;
				// assert that stored-size witness below. The field path is fixed.
				fixture.Options.ColumnStore.Columns[0].Name = strings.Repeat("embedding_", 12)
			}
			meta, err := normalizeCollectionMeta(fixture)
			if err != nil {
				t.Fatal(err)
			}
			codec, err := nativeOrdinaryGraphPendingSourceCodec(meta)
			if err != nil || codec == 0 || codec.needsZSTD() != tc.wantDecoder {
				t.Fatalf("pending codec=%d decoder=%t want=%t err=%v", codec, codec.needsZSTD(), tc.wantDecoder, err)
			}
			cfg := *meta.Options.ColumnStore
			fields := columnStoreTypedColumnPartFields(cfg)
			opts, err := typedColumnPublicationAdapterOptionsFromConfig(cfg, typedColumnPartAssetPartID, fields, nil)
			if err != nil {
				t.Fatal(err)
			}
			rows := make([]typedColumnAdapterRow, 256)
			for i := range rows {
				rows[i] = typedColumnAdapterRow{PrimaryID: int64(i), Values: map[string]columnDeclaredValue{"embedding": {Type: ColumnStoreValueFloat32Vector, Present: true, Float32Vector: []float32{1, 0}}}}
			}
			part, err := buildTypedColumnAdapterPart(opts, rows)
			if err != nil {
				t.Fatal(err)
			}
			image, err := part.buildImage()
			if err != nil {
				t.Fatal(err)
			}
			var dataZSTD, metadataZSTD bool
			for _, section := range image.Sections {
				dataZSTD = dataZSTD || section.Kind == typedcolumn.ColumnPartImageSectionColumnData && section.Compression == typedcolumn.CompressionZSTD
				metadataZSTD = metadataZSTD || section.Kind == typedcolumn.ColumnPartImageSectionDictionaries && section.Compression == typedcolumn.CompressionZSTD && section.Length < section.RawBytes
			}
			if dataZSTD != tc.wantDecoder {
				t.Fatalf("maintained output ZSTD data=%t want=%t", dataZSTD, tc.wantDecoder)
			}
			if tc.dictionary == "zstd" && !metadataZSTD {
				t.Fatal("fixture did not emit the required compressed dictionary witness")
			}
			if _, err = typedcolumn.ColumnPartFromImageWithOptions(image, typedcolumn.ColumnPartImageReadOptions{}); err != nil {
				t.Fatalf("maintained graph projection: %v", err)
			}
		})
	}
}

func TestNativeOrdinaryColumnGraphPendingSourceCodecChangeFailsBeforeConstruction(t *testing.T) {
	clearNativeOrdinaryPendingGraphCodecEnv(t)
	t.Setenv(typedColumnBenchmarkCompressionEnv, "none")
	meta, err := normalizeCollectionMeta(nativeOrdinaryPendingGraphBenchmarkMeta())
	if err != nil {
		t.Fatal(err)
	}
	codec, err := nativeOrdinaryGraphPendingSourceCodec(meta)
	if err != nil {
		t.Fatal(err)
	}
	recipe := &nativeVectorGraphRecipe{ordinary: true, schema: &nativeSchemaState{meta: &meta}, pendingSourceCodec: codec}
	delta := &nativeTypedDelta{appliedLSN: 1, data: &nativeTypedRows{graph: recipe}}
	work := &nativeTypedMaterialization{meta: meta, input: nativeTypedMaterializeInput{deltas: []*nativeTypedDelta{delta}}}
	validate, err := work.ordinaryGraphPendingSourceValidator()
	if err != nil || validate == nil {
		t.Fatalf("source contract: validator=%v err=%v", validate != nil, err)
	}
	cfg := *meta.Options.ColumnStore
	if _, err = prepareTypedColumnPartBatchFromSourceWithOptionsValidator(cfg, 71, typedColumnAdapterRowsSource{}, validate); err != nil {
		t.Fatalf("unchanged source policy: %v", err)
	}
	for _, policy := range []string{"lz4", "zstd", "unknown-codec"} {
		t.Run(policy, func(t *testing.T) {
			t.Setenv(typedColumnBenchmarkCompressionEnv, policy)
			// A nil row source is deliberate: policy must be rejected at the
			// actual producer boundary before batch allocation or row access.
			prepared, err := prepareTypedColumnPartBatchFromSourceWithOptionsValidator(cfg, 71, nil, validate)
			if err == nil || prepared != nil {
				t.Fatalf("changed producer policy escaped construction rejection: prepared=%v err=%v", prepared, err)
			}
		})
	}
}

func TestNativeOrdinaryColumnGraphPendingSourceProducerBinding(t *testing.T) {
	clearNativeOrdinaryPendingGraphCodecEnv(t)
	t.Setenv(typedColumnBenchmarkCompressionEnv, "none")
	meta, err := normalizeCollectionMeta(nativeOrdinaryPendingGraphBenchmarkMeta())
	if err != nil {
		t.Fatal(err)
	}
	codec, err := nativeOrdinaryGraphPendingSourceCodec(meta)
	if err != nil {
		t.Fatal(err)
	}
	recipe := &nativeVectorGraphRecipe{ordinary: true, schema: &nativeSchemaState{meta: &meta}, pendingSourceCodec: codec}
	work := &nativeTypedMaterialization{meta: meta, input: nativeTypedMaterializeInput{deltas: []*nativeTypedDelta{{appliedLSN: 1, data: &nativeTypedRows{graph: recipe}}}}}
	validate, err := work.ordinaryGraphPendingSourceValidator()
	if err != nil || validate == nil {
		t.Fatalf("source contract: validator=%v err=%v", validate != nil, err)
	}
	cfg := *meta.Options.ColumnStore
	rows := typedColumnAdapterRowsSource{{PrimaryID: 0, Values: map[string]columnDeclaredValue{"embedding": {Type: ColumnStoreValueFloat32Vector, Present: true, Float32Vector: []float32{1, 0}}}}}
	validations := 0
	prepared, err := prepareTypedColumnPartBatchFromSourceWithOptionsValidator(cfg, 71, rows, func(actual ColumnStoreConfig, opts typedColumnAdapterOptions, columns []typedColumnAdapterColumn) error {
		validations++
		if err := validate(actual, opts, columns); err != nil {
			return err
		}
		// Force the old guard/producer race deterministically. Construction
		// must consume the already validated options, never this new policy.
		t.Setenv(typedColumnBenchmarkCompressionEnv, "zstd")
		return nil
	})
	if err != nil || validations != 1 {
		t.Fatalf("producer validation count=%d err=%v", validations, err)
	}
	built, err := buildTypedColumnPartImageFromPreparedBatchWithResult(prepared, 71)
	if err != nil {
		t.Fatal(err)
	}
	image, err := typedcolumn.ParseColumnPartImage(built.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	for _, section := range image.Sections {
		if section.Kind == typedcolumn.ColumnPartImageSectionColumnData && section.Compression != typedcolumn.CompressionNone {
			t.Fatalf("producer reread changed policy: section=%+v", section)
		}
	}
	if _, err = nativeOrdinaryGraphPendingSourceCodec(meta); err == nil {
		t.Fatal("changed environment did not install the unsupported ZSTD policy")
	}
	t.Setenv(typedColumnBenchmarkCompressionEnv, "none")
	// Prepared batches have distinct option and column-definition headers.
	// Altering only the latter must also fail the actual constructor contract.
	prepared.Columns[0].Definition.Compression = typedcolumn.CompressionZSTD
	if err = validate(cfg, prepared.Options, prepared.Columns); err == nil {
		t.Fatal("prepared columns escaped their authenticated options")
	}
	for _, tc := range []struct {
		name   string
		change func(*CollectionMeta, *ColumnStoreConfig)
	}{
		{name: "actual_collection", change: func(meta *CollectionMeta, _ *ColumnStoreConfig) { meta.Name = "different" }},
		{name: "actual_schema", change: func(_ *CollectionMeta, cfg *ColumnStoreConfig) { cfg.SchemaHash++ }},
		{name: "actual_shape", change: func(_ *CollectionMeta, cfg *ColumnStoreConfig) { cfg.RetainedPayload = ColumnRetainedPayloadNone }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			actualMeta, actualCfg := meta, cfg
			tc.change(&actualMeta, &actualCfg)
			work.meta = actualMeta
			if prepared, err := prepareTypedColumnPartBatchFromSourceWithOptionsValidator(actualCfg, 72, nil, validate); err == nil || prepared != nil {
				t.Fatalf("actual producer schema mismatch escaped: prepared=%v err=%v", prepared, err)
			}
		})
	}
}

func TestNativeOrdinaryColumnGraphPendingSourceContractSummary(t *testing.T) {
	clearNativeOrdinaryPendingGraphCodecEnv(t)
	meta, err := normalizeCollectionMeta(nativeOrdinaryPendingGraphMeta())
	if err != nil {
		t.Fatal(err)
	}
	codec, err := nativeOrdinaryGraphPendingSourceCodec(meta)
	if err != nil {
		t.Fatal(err)
	}
	work := &nativeTypedMaterialization{meta: meta, input: nativeTypedMaterializeInput{deltas: []*nativeTypedDelta{{appliedLSN: 1}}}}
	if validate, err := work.ordinaryGraphPendingSourceValidator(); err != nil || validate != nil {
		t.Fatalf("non-graph producer acquired a graph callback: validator=%v err=%v", validate != nil, err)
	}
	// A supported scalar-index pure drop preserves the primary typed chain.
	// Earlier markers may therefore carry scalar declarations absent from the
	// collection metadata passed to physical materialization. Text declarations
	// likewise do not contribute any typed-column producer definitions.
	prior := meta
	prior.Indexes = []IndexDefinition{{Name: "content", Field: "content", ValueType: IndexValueString}}
	prior.TextIndexes = []TextIndexDefinition{{Name: "text", Fields: []TextIndexField{{Field: "content"}}}}
	earlier := &nativeVectorGraphRecipe{ordinary: true, schema: &nativeSchemaState{meta: &prior}, pendingSourceCodec: codec}
	latest := &nativeVectorGraphRecipe{ordinary: true, schema: &nativeSchemaState{meta: &meta}, pendingSourceCodec: codec}
	first := &nativeTypedDelta{appliedLSN: 1, data: &nativeTypedRows{graph: earlier}}
	last := &nativeTypedDelta{appliedLSN: 3, data: &nativeTypedRows{graph: latest}}
	work.input.deltas = []*nativeTypedDelta{first, {appliedLSN: 2}, last}
	validate, err := work.ordinaryGraphPendingSourceValidator()
	if err != nil || validate == nil {
		t.Fatalf("independent declaration change invalidated producer contract: validator=%v err=%v", validate != nil, err)
	}
	if _, err = prepareTypedColumnPartBatchFromSourceWithOptionsValidator(*meta.Options.ColumnStore, 71, typedColumnAdapterRowsSource{}, validate); err != nil {
		t.Fatalf("summarized actual producer: %v", err)
	}
	// Every bound marker contributes to the summary, including one before the
	// latest marker. A conflicting codec or physical schema fails closed.
	conflict := *earlier
	conflict.pendingSourceCodec ^= 1
	first.data = &nativeTypedRows{graph: &conflict}
	if validate, err := work.ordinaryGraphPendingSourceValidator(); err == nil || validate != nil {
		t.Fatal("summary ignored an earlier marker's incompatible codec")
	}
	conflict = *earlier
	other := *prior.copy()
	other.Options.ColumnStore.SchemaHash++
	conflict.schema = &nativeSchemaState{meta: &other}
	first.data = &nativeTypedRows{graph: &conflict}
	if validate, err := work.ordinaryGraphPendingSourceValidator(); err == nil || validate != nil {
		t.Fatal("summary ignored an earlier marker's incompatible physical schema")
	}
}

func TestNativeOrdinaryColumnGraphPendingValidationCapacity(t *testing.T) {
	clearNativeOrdinaryPendingGraphCodecEnv(t)
	meta, err := normalizeCollectionMeta(nativeOrdinaryPendingGraphMeta())
	if err != nil {
		t.Fatal(err)
	}
	codec, err := nativeOrdinaryGraphPendingSourceCodec(meta)
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := nativeOrdinaryColumnGraphMetadataPreflightCapacity(meta, meta.VectorIndexes[0], t.TempDir())
	if err != nil || envelope == 0 {
		t.Fatalf("source-bound metadata envelope=%d err=%v", envelope, err)
	}
	const original = uint64(12345)
	charged, err := nativeOrdinaryGraphBuildCapacityWithPendingValidation(original, envelope, codec)
	if err != nil || charged != original+envelope {
		t.Fatalf("validation did not add its complete independent envelope: original=%d envelope=%d charged=%d err=%v", original, envelope, charged, err)
	}
	// Follow that promise into the original typed work's finite cohort. The
	// original graph term and every existing retained control remain additive.
	delta := &nativeTypedDelta{data: &nativeTypedRows{graph: &nativeVectorGraphRecipe{ordinary: true, pendingSourceCodec: codec}}, buildCapacity: original}
	before, err := nativeTypedMaterializationCapacity(meta, []*nativeTypedDelta{delta})
	if err != nil {
		t.Fatal(err)
	}
	delta.buildCapacity = charged
	after, err := nativeTypedMaterializationCapacity(meta, []*nativeTypedDelta{delta})
	if err != nil || after-before != envelope {
		t.Fatalf("original work lost validation credit: before=%d after=%d envelope=%d err=%v", before, after, envelope, err)
	}
	max := ^uint64(0)
	if got, err := nativeOrdinaryGraphBuildCapacityWithPendingValidation(max-envelope, envelope, codec); err != nil || got != max {
		t.Fatalf("exact capacity boundary: got=%d err=%v", got, err)
	}
	if _, err := nativeOrdinaryGraphBuildCapacityWithPendingValidation(max-envelope+1, envelope, codec); !errors.Is(err, memtable.ErrCOWCapacity) {
		t.Fatalf("validation credit overflow did not fail closed: %v", err)
	}
	if _, err := nativeOrdinaryGraphBuildCapacityWithPendingValidation(original, 0, codec); err == nil {
		t.Fatal("bound producer accepted an absent validation envelope")
	}
	if got, err := nativeOrdinaryGraphBuildCapacityWithPendingValidation(original, envelope, 0); err != nil || got != original {
		t.Fatalf("unbound conservative graph changed: got=%d err=%v", got, err)
	}
}

// The ordinary vector mapper cannot currently emit ZSTD data. The environment
// override applies its codec to every declared field, and production validation
// refuses compression on float32_vector before any physical producer executes.
func TestNativeOrdinaryColumnGraphPendingSourceZSTDUnsupported(t *testing.T) {
	clearNativeOrdinaryPendingGraphCodecEnv(t)
	t.Setenv(typedColumnBenchmarkCompressionEnv, "zstd")
	meta, err := normalizeCollectionMeta(nativeOrdinaryPendingGraphBenchmarkMeta())
	if err != nil {
		t.Fatal(err)
	}
	_, err = nativeOrdinaryGraphPendingSourceCodec(meta)
	if !errors.Is(err, errTypedColumnProductionLayoutUnsupported) || !strings.Contains(err.Error(), "compression zstd is unsupported") || !strings.Contains(err.Error(), "float32_vector") {
		t.Fatalf("unsupported pending ZSTD did not retain the maintained mapper refusal: %v", err)
	}
}

// Existing physical sources have their own exact stored codec evidence. Build a
// real TCIM with ZSTD ordinal data and raw vectors through the generic maintained
// builder, then follow its actual immutable manifest ref through source census.
// This is an admitted-source control, not a claim that ordinary pending options
// can produce this layout or that the graph accepts every compressed layout.
func TestNativeOrdinaryColumnGraphExistingSourceZSTDDecoderCapacity(t *testing.T) {
	const rows = 256
	meta, def, construction, _, _ := nativeOrdinaryGraphPhaseFixture(t, rows)
	rootDir := t.TempDir()
	ordinals, vectors := make([]int64, rows), make([]float32, rows*def.Dimensions)
	for i := range ordinals {
		ordinals[i] = int64(i)
		vectors[i*def.Dimensions] = 1
	}
	part, err := typedcolumn.BuildColumnPart(71, typedcolumn.Options{
		SchemaVersion: uint32(meta.Options.ColumnStore.SchemaHash), SchemaMode: typedcolumn.ColumnSchemaFixed,
		Columns: []typedcolumn.ColumnDefinition{
			{Name: typedColumnAdapterPrimaryIDColumn, Type: typedcolumn.ColumnTypeInt64, Encoding: typedcolumn.EncodingDeltaVarint, Compression: typedcolumn.CompressionZSTD, CompressionSet: true, StatsDisabled: true},
			{Name: "embedding", Type: typedcolumn.ColumnTypeFloat32Vector, Encoding: typedcolumn.EncodingRawFloat32Vector, Compression: typedcolumn.CompressionNone, CompressionSet: true, FixedWidthElements: def.Dimensions},
		},
		LogicalPrimaryKey: typedcolumn.LogicalPrimaryKey{Columns: []string{typedColumnAdapterPrimaryIDColumn}},
		SortKey:           typedcolumn.SortKey{Columns: []typedcolumn.SortKeyColumn{{Column: typedColumnAdapterPrimaryIDColumn}}},
		Compression:       typedcolumn.ColumnCompressionPolicy{Default: typedcolumn.CompressionNone},
	}, typedcolumn.Batch{Rows: len(ordinals), Columns: map[string][]int64{typedColumnAdapterPrimaryIDColumn: ordinals}, Float32Vectors: map[string][]float32{"embedding": vectors}})
	if err != nil {
		t.Fatal(err)
	}
	compressed := false
	for _, block := range part.Columns[typedColumnAdapterPrimaryIDColumn].Blocks {
		compressed = compressed || block.Descriptor.Compression == typedcolumn.CompressionZSTD && block.Descriptor.StoredBytes < block.Descriptor.RawBytes
	}
	if !compressed {
		t.Fatal("maintained source builder emitted no compressed ZSTD block")
	}
	image, err := typedcolumn.BuildColumnPartImage(part, typedcolumn.ColumnPartImageOptions{})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, section := range image.Sections {
		found = found || section.Kind == typedcolumn.ColumnPartImageSectionColumnData && section.Compression == typedcolumn.CompressionZSTD
	}
	if !found {
		t.Fatal("maintained source builder emitted no ZSTD column-data witness")
	}
	ref, err := writeColumnAssetToManager(rootDir, *meta.Options.ColumnStore, image.Bytes, ColumnAssetKindTCS1TypedColumnPart, 1, image.PartID)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := encodeColumnManifestPartRecord(ColumnPreparedAsset{Ref: ref, Rows: len(ordinals), Bytes: ref.Length, Reason: string(ColumnPublishOperationInsert)})
	if err != nil {
		t.Fatal(err)
	}
	record := columnManifestRecord{key: columnManifestPartRecordKey(ref.Generation, ref.PartID), value: raw}
	var shape nativeTypedFamilyReadShape
	var admitted uint64
	// The selected immutable-record route reads only this real ref; the root
	// presence marker is never dereferenced, as in source point preflight.
	extra, _, err := nativeTypedFamilyReadCapacity(meta, nil, 1, rootDir, &shape, func(n uint64) error { admitted = max(admitted, n); return nil }, record)
	if err != nil || !shape.columnDataZSTD || admitted == 0 {
		t.Fatalf("stored ZSTD source escaped census: shape=%+v admitted=%d err=%v", shape, admitted, err)
	}
	fixed, err := nativeTypedDecoderFixedCapacity()
	if err != nil {
		t.Fatal(err)
	}
	source := nativeOrdinaryColumnGraphSourceCensus{TypedSourceParts: 1, TypedSourceStoredBytes: uint64(ref.Length), TypedSourceDecodedBytes: shape.decodedBytes, DictionarySlots: shape.dictionarySlots, TypedSourceReadPeak: admitted + extra}
	without, err := nativeOrdinaryColumnGraphPhaseCapacity(meta, def, rootDir, construction, source)
	if err != nil {
		t.Fatal(err)
	}
	if shape.columnDataZSTD {
		source.TypedSourceDecoderBytes = fixed
	}
	with, err := nativeOrdinaryColumnGraphPhaseCapacity(meta, def, rootDir, construction, source)
	if err != nil {
		t.Fatal(err)
	}
	if fixed == 0 || with.SourceReadBytes-without.SourceReadBytes != fixed || with.TotalBytes < with.PersistentBytes+with.SourceReadBytes || with.PersistentBytes != without.PersistentBytes || with.AssetsRetainedBytes != without.AssetsRetainedBytes {
		t.Fatalf("full existing-source decoder or original retained owners lost: fixed=%d without=%+v with=%+v", fixed, without, with)
	}
}
