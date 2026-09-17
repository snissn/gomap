package collections

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"sort"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/vectorops"
)

func cosineNormalizedF32V1TestMeta() CollectionMeta {
	meta := typedMinimaCollectionMeta()
	meta.VectorIndexes[0].Representation = VectorIndexRepresentationCosineNormalizedF32V1
	return meta
}

func cosineNormalizedF32V1TestColumns(vector []float32) []TypedColumnBatch {
	return []TypedColumnBatch{
		{Name: "embedding", Float32Vectors: [][]float32{vector}},
		{Name: "content", Strings: []string{"alpha"}},
		{Name: "user", Strings: []string{"u1"}},
		{Name: "path", Strings: []string{"file1"}},
	}
}

func requireFloat32BitsEqual(t testing.TB, got, want []float32) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("float32 lengths differ: got=%d want=%d", len(got), len(want))
	}
	for i := range want {
		if math.Float32bits(got[i]) != math.Float32bits(want[i]) {
			t.Fatalf("component %d bits=%08x want=%08x; got=%v want=%v", i, math.Float32bits(got[i]), math.Float32bits(want[i]), got, want)
		}
	}
}

func TestCosineNormalizedF32V1MetadataContract(t *testing.T) {
	valid := cosineNormalizedF32V1TestMeta()
	normalized, err := normalizeCollectionMeta(valid)
	if err != nil {
		t.Fatalf("normalize valid metadata: %v", err)
	}
	if got := normalized.VectorIndexes[0].Representation; got != VectorIndexRepresentationCosineNormalizedF32V1 {
		t.Fatalf("representation=%q want %q", got, VectorIndexRepresentationCosineNormalizedF32V1)
	}

	for name, mutate := range map[string]func(*CollectionMeta){
		"unsupported": func(meta *CollectionMeta) {
			meta.VectorIndexes[0].Representation = "future"
		},
		"wrong_strategy": func(meta *CollectionMeta) {
			meta.VectorIndexes[0].Strategy = VectorIndexStrategyNativeRuntime
		},
		"wrong_metric": func(meta *CollectionMeta) {
			meta.VectorIndexes[0].Metric = VectorMetricL2
		},
		"wrong_encoding": func(meta *CollectionMeta) {
			meta.VectorIndexes[0].Encoding = VectorIndexEncodingInt8
		},
		"multiple_vector_indexes": func(meta *CollectionMeta) {
			other := meta.VectorIndexes[0]
			other.Name = "other"
			other.Representation = ""
			meta.VectorIndexes = append(meta.VectorIndexes, other)
		},
		"unrelated_vector_index": func(meta *CollectionMeta) {
			other := meta.VectorIndexes[0]
			other.Name = "other"
			other.Field = "other_embedding"
			other.Strategy = VectorIndexStrategyNativeRuntime
			other.Representation = ""
			meta.VectorIndexes = append(meta.VectorIndexes, other)
		},
		"retained_full": func(meta *CollectionMeta) {
			meta.Options.ColumnStore.RetainedPayload = ColumnRetainedPayloadFull
		},
		"selected_row_asset": func(meta *CollectionMeta) {
			meta.Options.ColumnStore.Columns[0].Owner = TypedStorageOwnerRowAsset
		},
		"selected_nullable": func(meta *CollectionMeta) {
			meta.Options.ColumnStore.Columns[0].Nullable = true
		},
		"selected_dimensions": func(meta *CollectionMeta) {
			meta.Options.ColumnStore.Columns[0].VectorDims++
		},
	} {
		t.Run(name, func(t *testing.T) {
			meta := cosineNormalizedF32V1TestMeta()
			mutate(&meta)
			if _, err := normalizeCollectionMeta(meta); err == nil {
				t.Fatal("accepted incompatible normalized-vector metadata")
			}
		})
	}

	sibling := cosineNormalizedF32V1TestMeta()
	sibling.Options.ColumnStore.Columns = append(sibling.Options.ColumnStore.Columns, ColumnStoreColumn{
		Name: "aux_embedding", Path: "aux_embedding", ValueType: ColumnStoreValueFloat32Vector,
		Owner: TypedStorageOwnerColumnPart, VectorDims: 2,
	})
	if _, err := normalizeCollectionMeta(sibling); err == nil {
		t.Fatal("normalized representation accepted a sibling typed-column-part field")
	}
}

func TestCosineNormalizedF32V1StateRepresentationShape(t *testing.T) {
	def := cosineNormalizedF32V1TestMeta().VectorIndexes[0]
	pack := columnVectorIndexStateAssetSnapshot{
		Role: columnVectorIndexStateAssetRoleHNSWSearchPack, AssetID: columnVectorIndexStateHNSWTopologyPackAssetID,
		LogicalType: columnVectorIndexStateLogicalTypeSearchPack, PhysicalEncoding: columnVectorIndexStateEncodingHNSWSearchPackV2,
	}
	vector := columnVectorIndexStateAssetSnapshot{
		Role: columnVectorIndexStateAssetRoleNormalizedVectors, AssetID: columnVectorIndexStateNormalizedVectorsAssetID,
		LogicalType: columnVectorIndexStateLogicalTypeFloat32Vector, PhysicalEncoding: columnVectorIndexStateEncodingRawFloat32Vector,
	}
	valid := columnVectorIndexStateSnapshot{RowCount: 1, Assets: []columnVectorIndexStateAssetSnapshot{pack, vector}}
	if err := validateColumnVectorIndexStateRepresentationAssets(valid, def); err != nil {
		t.Fatalf("valid normalized state: %v", err)
	}
	for name, mutate := range map[string]func(*columnVectorIndexStateSnapshot, *VectorIndexDefinition){
		"missing_pack": func(state *columnVectorIndexStateSnapshot, _ *VectorIndexDefinition) {
			state.Assets = state.Assets[1:]
		},
		"duplicate_pack": func(state *columnVectorIndexStateSnapshot, _ *VectorIndexDefinition) {
			state.Assets = append(state.Assets, state.Assets[0])
		},
		"legacy_pack": func(state *columnVectorIndexStateSnapshot, _ *VectorIndexDefinition) {
			state.Assets[0].AssetID = columnVectorIndexStateHNSWSearchPackAssetID
			state.Assets[0].PhysicalEncoding = columnVectorIndexStateEncodingHNSWSearchPackV1
		},
		"duplicate_vector": func(state *columnVectorIndexStateSnapshot, _ *VectorIndexDefinition) {
			state.Assets = append(state.Assets, state.Assets[1])
		},
		"inverse_norm": func(state *columnVectorIndexStateSnapshot, _ *VectorIndexDefinition) {
			state.Assets = append(state.Assets, columnVectorIndexStateAssetSnapshot{Role: columnVectorIndexStateAssetRoleInverseNorm})
		},
		"empty_with_vector": func(state *columnVectorIndexStateSnapshot, _ *VectorIndexDefinition) {
			state.RowCount = 0
		},
		"topology_under_legacy": func(_ *columnVectorIndexStateSnapshot, def *VectorIndexDefinition) {
			def.Representation = ""
		},
	} {
		t.Run(name, func(t *testing.T) {
			state := valid
			state.Assets = append([]columnVectorIndexStateAssetSnapshot(nil), valid.Assets...)
			candidate := def
			mutate(&state, &candidate)
			if err := validateColumnVectorIndexStateRepresentationAssets(state, candidate); err == nil {
				t.Fatal("accepted incompatible representation/state asset shape")
			}
		})
	}
	empty := columnVectorIndexStateSnapshot{Assets: []columnVectorIndexStateAssetSnapshot{pack}}
	if err := validateColumnVectorIndexStateRepresentationAssets(empty, def); err != nil {
		t.Fatalf("valid empty normalized state: %v", err)
	}
}

func TestCosineNormalizedF32V1CreateRejectsPopulatedColumn(t *testing.T) {
	meta := typedMinimaCollectionMeta()
	def := meta.VectorIndexes[0]
	def.Representation = VectorIndexRepresentationCosineNormalizedF32V1
	meta.VectorIndexes = nil
	_, db, col := openTypedMinimaCollectionMeta(t, meta)
	defer db.Close()
	if _, _, err := col.InsertTypedBatchWithStats(
		[][]byte{[]byte("legacy")},
		[][]byte{[]byte(`{"id":"legacy"}`)},
		cosineNormalizedF32V1TestColumns([]float32{3, 4, 0, 0, 0, 0, 0, 0}),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := col.CreateVectorIndex(def); err == nil || !strings.Contains(err.Error(), "empty collection") {
		t.Fatalf("populated normalized conversion error=%v", err)
	}
	if len(col.Meta().VectorIndexes) != 0 {
		t.Fatalf("rejected conversion mutated metadata: %+v", col.Meta().VectorIndexes)
	}
}

func TestCosineNormalizedF32V1MetadataVersionGate(t *testing.T) {
	meta, err := normalizeCollectionMeta(cosineNormalizedF32V1TestMeta())
	if err != nil {
		t.Fatal(err)
	}
	raw, err := encodeNormalizedCollectionMeta(meta)
	if err != nil {
		t.Fatal(err)
	}
	var disk collectionMetaDisk
	if err := json.Unmarshal(raw, &disk); err != nil {
		t.Fatal(err)
	}
	for _, version := range []int{collectionMetaVersionV5, collectionMetaVersionV6} {
		disk.Version = version
		legacyRaw, err := json.Marshal(disk)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := decodeCollectionMeta(legacyRaw); err == nil || !strings.Contains(err.Error(), "cannot define vector representation") {
			t.Fatalf("version %d representation error=%v", version, err)
		}
	}
}

func TestCosineNormalizedF32V1TypedAdmissionOwnsCanonicalBytes(t *testing.T) {
	_, db, col := openTypedMinimaCollectionMeta(t, cosineNormalizedF32V1TestMeta())
	defer db.Close()
	input := []float32{3, 4, 0, 0, 0, 0, 0, 0}
	original := append([]float32(nil), input...)
	projection, err := newTrustedTypedProjection(col.Meta(), [][]byte{[]byte("a")}, [][]byte{[]byte(`{"id":"a"}`)}, cosineNormalizedF32V1TestColumns(input))
	if err != nil {
		t.Fatal(err)
	}
	requireFloat32BitsEqual(t, input, original)

	want, err := normalizeCosineNormalizedF32V1Reference(input, len(input))
	if err != nil {
		t.Fatal(err)
	}
	row := projection.typedRows["a"]
	requireFloat32BitsEqual(t, row[0].Float32Vector, want)
	input[0] = 30
	requireFloat32BitsEqual(t, row[0].Float32Vector, want)

	payload, err := typedCommandPayload(col.Meta(), []commitlog.CollectionDocument{{ID: []byte("a"), Document: []byte(`{"id":"a"}`)}}, projection)
	if err != nil {
		t.Fatal(err)
	}
	embeddingColumn := -1
	for i, column := range payload.Columns {
		if column.Name == "embedding" {
			embeddingColumn = i
			break
		}
	}
	if embeddingColumn < 0 {
		t.Fatal("typed command omitted embedding column")
	}
	requireFloat32BitsEqual(t, payload.Documents[0].Values[embeddingColumn].Vector, want)
	if _, _, _, err := typedProjectionFromPayload(col.Meta(), payload); err != nil {
		t.Fatalf("canonical typed command rejected: %v", err)
	}
	payload.Documents[0].Values[embeddingColumn].Vector = original
	if _, _, _, err := typedProjectionFromPayload(col.Meta(), payload); err == nil {
		t.Fatal("replay accepted non-canonical normalized-vector bytes")
	}
}

func TestCosineNormalizedF32V1TypedWALReplayPreservesCanonicalBits(t *testing.T) {
	dir, db, col := openTypedMinimaCollectionMeta(t, cosineNormalizedF32V1TestMeta())
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	replayDir := t.TempDir()
	copyColumnStoreCommandWALReplayBenchmarkDirM10C(t, dir, replayDir)
	baseline := collectionCommandWALFrames(t, dir)
	var baselineLSN uint64
	if len(baseline) > 0 {
		baselineLSN = baseline[len(baseline)-1].LSN
	}

	input := []float32{3, 4, 0, 0, 0, 0, 0, 0}
	want, err := normalizeCosineNormalizedF32V1Reference(input, len(input))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := col.InsertTypedBatchWithStats([][]byte{[]byte("a")}, [][]byte{[]byte(`{"id":"a"}`)}, cosineNormalizedF32V1TestColumns(input)); err != nil {
		t.Fatal(err)
	}
	frames := collectionCommandWALFrames(t, dir)
	if len(frames) != len(baseline)+1 {
		t.Fatalf("command WAL frames=%d want %d", len(frames), len(baseline)+1)
	}
	frame := frames[len(frames)-1]
	if frame.PayloadFormat != commitlog.PayloadFormatCollectionTypedBatchByIDV1 {
		t.Fatalf("payload format=%v", frame.PayloadFormat)
	}
	payload, err := commitlog.DecodeCollectionTypedBatchPayload(frame.Payload)
	if err != nil {
		t.Fatal(err)
	}
	for i, column := range payload.Columns {
		if column.Name == "embedding" {
			requireFloat32BitsEqual(t, payload.Documents[0].Values[i].Vector, want)
		}
	}
	if frame.LSN <= baselineLSN {
		t.Fatalf("mutation LSN=%d baseline=%d", frame.LSN, baselineLSN)
	}
	writeCollectionCommandWALFrame(t, replayDir, frame.LSN, frame.Kind, frame.PayloadFormat, frame.Payload)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	for _, path := range []string{dir, replayDir} {
		reopened := openTypedMinimaDB(t, path)
		reopenedCol, err := NewCollectionManager(reopened).OpenCollection("minima")
		if err != nil {
			reopened.Close()
			t.Fatal(err)
		}
		raw, err := reopenedCol.Get([]byte("a"))
		if err != nil {
			reopened.Close()
			t.Fatal(err)
		}
		var document struct {
			Embedding []float32 `json:"embedding"`
		}
		if err := json.Unmarshal(raw, &document); err != nil {
			reopened.Close()
			t.Fatal(err)
		}
		requireFloat32BitsEqual(t, document.Embedding, want)
		if err := reopened.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCosineNormalizedF32V1InvalidAdmissionPrecedesWAL(t *testing.T) {
	for name, vector := range map[string][]float32{
		"zero": {0, 0, 0, 0, 0, 0, 0, 0},
		"nan":  {float32(math.NaN()), 1, 0, 0, 0, 0, 0, 0},
		"inf":  {float32(math.Inf(1)), 1, 0, 0, 0, 0, 0, 0},
	} {
		t.Run(name, func(t *testing.T) {
			dir, db, col := openTypedMinimaCollectionMeta(t, cosineNormalizedF32V1TestMeta())
			defer db.Close()
			before := len(collectionCommandWALFrames(t, dir))
			if _, _, err := col.InsertTypedBatchWithStats([][]byte{[]byte("a")}, [][]byte{[]byte(`{"id":"a"}`)}, cosineNormalizedF32V1TestColumns(vector)); err == nil {
				t.Fatal("accepted invalid normalized-vector admission")
			}
			if after := len(collectionCommandWALFrames(t, dir)); after != before {
				t.Fatalf("invalid admission appended command WAL frame: before=%d after=%d", before, after)
			}
		})
	}
}

func TestCosineNormalizedF32V1GenericVectorWritesFailBeforeWAL(t *testing.T) {
	t.Run("insert requires typed admission", func(t *testing.T) {
		dir, db, col := openTypedMinimaCollectionMeta(t, cosineNormalizedF32V1TestMeta())
		defer db.Close()
		before := len(collectionCommandWALFrames(t, dir))
		document := []byte(`{"id":"a","embedding":[3,4,0,0,0,0,0,0],"content":"alpha","user":"u1","path":"file1"}`)
		if _, err := col.Insert([]byte("a"), document); err == nil || !strings.Contains(err.Error(), "typed vector admission") {
			t.Fatalf("generic insert error=%v", err)
		}
		if after := len(collectionCommandWALFrames(t, dir)); after != before {
			t.Fatalf("generic insert appended command WAL frame: before=%d after=%d", before, after)
		}
	})

	t.Run("update rejects noncanonical replacement", func(t *testing.T) {
		dir, db, col := openTypedMinimaCollectionMeta(t, cosineNormalizedF32V1TestMeta())
		defer db.Close()
		if _, _, err := col.InsertTypedBatchWithStats(
			[][]byte{[]byte("a")}, [][]byte{[]byte(`{"id":"a"}`)},
			cosineNormalizedF32V1TestColumns([]float32{3, 4, 0, 0, 0, 0, 0, 0}),
		); err != nil {
			t.Fatal(err)
		}
		before := len(collectionCommandWALFrames(t, dir))
		_, _, err := col.Update([]byte("a"), func(current []byte) ([]byte, bool, error) {
			var document map[string]json.RawMessage
			if err := json.Unmarshal(current, &document); err != nil {
				return nil, false, err
			}
			document["embedding"] = json.RawMessage(`[3,4,0,0,0,0,0,0]`)
			next, err := json.Marshal(document)
			return next, true, err
		})
		if err == nil || !strings.Contains(err.Error(), "canonical typed vector admission") {
			t.Fatalf("generic vector update error=%v", err)
		}
		if after := len(collectionCommandWALFrames(t, dir)); after != before {
			t.Fatalf("generic vector update appended command WAL frame: before=%d after=%d", before, after)
		}
	})
}

func TestCosineNormalizedF32V1DenormalAdmissionIsCanonical(t *testing.T) {
	_, db, col := openTypedMinimaCollectionMeta(t, cosineNormalizedF32V1TestMeta())
	defer db.Close()
	input := []float32{math.SmallestNonzeroFloat32, 0, 0, 0, 0, 0, 0, 0}
	if _, _, err := col.InsertTypedBatchWithStats([][]byte{[]byte("a")}, [][]byte{[]byte(`{"id":"a"}`)}, cosineNormalizedF32V1TestColumns(input)); err != nil {
		t.Fatal(err)
	}
	raw, err := col.Get([]byte("a"))
	if err != nil {
		t.Fatal(err)
	}
	var document struct {
		Embedding []float32 `json:"embedding"`
	}
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	requireFloat32BitsEqual(t, document.Embedding, []float32{1, 0, 0, 0, 0, 0, 0, 0})
}

func TestCosineNormalizedF32V1RebuildPublishesOneCanonicalFP32Plane(t *testing.T) {
	_, db, col := openTypedMinimaCollectionMeta(t, cosineNormalizedF32V1TestMeta())
	defer db.Close()
	ids := [][]byte{[]byte("c"), []byte("a"), []byte("b")}
	retained := [][]byte{[]byte(`{"id":"c"}`), []byte(`{"id":"a"}`), []byte(`{"id":"b"}`)}
	columns := []TypedColumnBatch{
		{Name: "embedding", Float32Vectors: [][]float32{{3, 4, 0, 0, 0, 0, 0, 0}, {0, 5, 12, 0, 0, 0, 0, 0}, {8, 0, 0, 15, 0, 0, 0, 0}}},
		{Name: "content", Strings: []string{"charlie", "alpha", "bravo"}},
		{Name: "user", Strings: []string{"u3", "u1", "u2"}},
		{Name: "path", Strings: []string{"file3", "file1", "file2"}},
	}
	if _, _, err := col.InsertTypedBatchWithStats(ids, retained, columns); err != nil {
		t.Fatal(err)
	}
	status, err := col.RebuildVectorIndex("embedding_graph")
	if err != nil {
		t.Fatal(err)
	}
	assertColumnGraphRebuildLoadedStatusV2A(t, status, "embedding_graph")

	records, cfg := loadColumnGraphRebuildManifestRecordsAndConfigV2A(t, db, "minima")
	def := col.Meta().VectorIndexes[0]
	graph := graphManifestFromRecords1918(t, records, def)
	stateRecord, ok := findColumnVectorIndexStateRecord(records, def.Name)
	if !ok {
		t.Fatal("vector-index state record missing")
	}
	state, err := decodeColumnVectorIndexStateRecord(stateRecord.value)
	if err != nil {
		t.Fatal(err)
	}
	typedRefs, err := typedColumnPartRefsByGenerationFromManifestRecords(records, cfg.AssetManager.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	typed, ok := typedRefs[graph.BaseManifestGeneration]
	if !ok || len(typedRefs) != 1 {
		t.Fatalf("typed vector owners=%+v", typedRefs)
	}
	var normalized columnVectorIndexStateAssetSnapshot
	normalizedCount := 0
	inverseNormCount := 0
	for _, asset := range state.Assets {
		if asset.Role == columnVectorIndexStateAssetRoleNormalizedVectors {
			normalized, normalizedCount = asset, normalizedCount+1
		}
		if asset.Role == columnVectorIndexStateAssetRoleInverseNorm {
			inverseNormCount++
		}
	}
	if normalizedCount != 1 || normalized.AssetID != columnVectorIndexStateNormalizedVectorsAssetID || normalized.Ref != typed.Ref {
		t.Fatalf("normalized asset=%+v count=%d typed=%+v", normalized, normalizedCount, typed.Ref)
	}
	if inverseNormCount != 0 {
		t.Fatalf("normalized representation retained %d inverse-norm assets", inverseNormCount)
	}
	packAsset, found, err := findColumnHNSWSearchPackStateAsset(state)
	if err != nil || !found {
		t.Fatalf("find topology pack: found=%t err=%v", found, err)
	}
	if packAsset.AssetID != columnVectorIndexStateHNSWTopologyPackAssetID || packAsset.PhysicalEncoding != columnVectorIndexStateEncodingHNSWSearchPackV2 {
		t.Fatalf("topology pack state=%+v", packAsset)
	}
	raw := readColumnHNSWSearchPackRawForTest2313(t, db, packAsset.Ref)
	pack, err := decodeColumnHNSWSearchPack(raw, columnHNSWSearchPackDecodeOptions{ExpectedBaseIdentity: columnHNSWSearchPackBaseIdentity{ManifestGeneration: graph.BaseManifestGeneration, ManifestChecksum: graph.BaseManifestChecksum, SchemaHash: graph.BaseSchemaHash}})
	if err != nil {
		t.Fatal(err)
	}
	if !pack.Header.ExternalNormalizedVectors || pack.Header.ExternalVectorDigest != columnHNSWSearchPackExternalVectorRefDigest(typed.Ref) || len(pack.NormalizedVectors) != 0 {
		t.Fatalf("decoded topology pack header=%+v vectors=%d", pack.Header, len(pack.NormalizedVectors))
	}
	if _, err := columnHNSWSearchPackFindSection(pack.Sections, columnHNSWSearchPackSectionNormalizedVectors, 0); err == nil {
		t.Fatal("published topology pack contains a normalized_vectors section")
	}
	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if reader.hnswSearchPack == nil || reader.hnswSearchPack.validateLive() != nil || len(reader.hnswSearchPack.NormalizedVectors) != graph.RowCount*def.Dimensions {
		t.Fatalf("bound topology pack unavailable: reader=%+v", reader.hnswSearchPack)
	}
	if len(reader.typedVectorSource.parts) != 1 || &reader.hnswSearchPack.NormalizedVectors[0] != &reader.typedVectorSource.parts[0].values[0] {
		t.Fatal("topology pack did not borrow the canonical typed-column vector slice")
	}
	if reader.invNormSource != nil {
		t.Fatal("normalized representation retained an inverse-norm source")
	}
	if reader.preparedSearch != nil && !reader.preparedSearch.norm.implicitUnit {
		t.Fatal("normalized prepared view did not use implicit unit norms")
	}
	if columnGraphTypedColumnMmapDirectViewSupportedForTest() && reader.preparedSearch == nil {
		t.Fatal("mmap platform did not admit the normalized prepared-search view")
	}
	originalDigest := reader.hnswSearchPack.Header.ExternalVectorDigest
	reader.hnswSearchPack.Header.ExternalVectorDigest[0]++
	err = reader.hnswSearchPack.bindExternalNormalizedVectors(state, records, *cfg, reader.typedVectorSource)
	reader.hnswSearchPack.Header.ExternalVectorDigest = originalDigest
	if err == nil {
		t.Fatal("topology pack accepted the wrong canonical vector identity")
	}
}

func TestCosineNormalizedF32V1ExactAndSQ8MatchExhaustivePackedTruth(t *testing.T) {
	requireTypedGraphPublicServingTest(t)
	meta := cosineNormalizedF32V1TestMeta()
	meta.VectorIndexes[0].QuantizedIndexes = []QuantizedVectorIndexDefinition{{Name: "embedding.scalar_u8.legacy"}}
	_, db, col := openTypedMinimaCollectionMeta(t, meta)
	defer db.Close()

	ids := [][]byte{[]byte("tie-b"), []byte("near"), []byte("tie-a"), []byte("far"), []byte("opposite")}
	vectors := [][]float32{
		{2, 2e-4, 0, 0, 0, 0, 0, 0},
		{1, 3e-4, 0, 0, 0, 0, 0, 0},
		{1, 1e-4, 0, 0, 0, 0, 0, 0},
		{0, 1, 0, 0, 0, 0, 0, 0},
		{-1, 0, 0, 0, 0, 0, 0, 0},
	}
	retained := make([][]byte, len(ids))
	content, user, path := make([]string, len(ids)), make([]string, len(ids)), make([]string, len(ids))
	for i := range ids {
		retained[i] = []byte(`{"id":"` + string(ids[i]) + `"}`)
		content[i], user[i], path[i] = string(ids[i]), "u", "p"
	}
	columns := []TypedColumnBatch{
		{Name: "embedding", Float32Vectors: vectors},
		{Name: "content", Strings: content},
		{Name: "user", Strings: user},
		{Name: "path", Strings: path},
	}
	if _, _, err := col.InsertTypedBatchWithStats(ids, retained, columns); err != nil {
		t.Fatal(err)
	}
	if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
		t.Fatal(err)
	}
	if err := col.EnsureColumnGraphServing(context.Background(), "embedding_graph", typedGraphPublicTestOptions()); errors.Is(err, errColumnVectorGraphSharedPreparedSearchNotEligible) {
		t.Skip("typed shared prepared holder is unavailable on this host")
	} else if err != nil {
		t.Fatal(err)
	}

	query := []float32{7, 7e-4, 0, 0, 0, 0, 0, 0}
	canonicalQuery, err := normalizeCosineNormalizedF32V1Reference(query, len(query))
	if err != nil {
		t.Fatal(err)
	}
	want := make([]VectorIndexSearchResult, len(ids))
	for i := range ids {
		canonical, err := normalizeCosineNormalizedF32V1Reference(vectors[i], len(vectors[i]))
		if err != nil {
			t.Fatal(err)
		}
		want[i] = VectorIndexSearchResult{ID: ids[i], Score: clampCosineNormalizedF32V1Score(float64(vectorops.DotFloat32(canonical, canonicalQuery)))}
	}
	sort.Slice(want, func(i, j int) bool { return vectorIndexSearchResultBefore(want[i], want[j]) })

	search := func(mode VectorIndexQueryMode) VectorIndexSearchResponse {
		t.Helper()
		opts := VectorIndexSearchOptions{
			IndexName: "embedding_graph", Query: query, QueryMode: mode,
			TopK: len(ids), EfSearch: len(ids), StatsMode: VectorIndexSearchStatsModeMinimal,
		}
		if mode == VectorIndexQueryModeQuantizedRerank {
			opts.QuantizedIndexName = "embedding.scalar_u8.legacy"
			opts.QuantizedRerankCandidates = len(ids)
		}
		var buffer VectorIndexSearchBuffer
		response, view, err := col.SearchVectorIndexWithBufferReadView(opts, &buffer)
		if err != nil {
			t.Fatal(err)
		}
		if err := view.Close(); err != nil {
			t.Fatal(err)
		}
		return response
	}
	for _, mode := range []VectorIndexQueryMode{VectorIndexQueryModeExact, VectorIndexQueryModeQuantizedRerank} {
		response := search(mode)
		if len(response.Results) != len(want) {
			t.Fatalf("mode=%s results=%+v want=%+v", mode, response.Results, want)
		}
		for i := range want {
			if string(response.Results[i].ID) != string(want[i].ID) || math.Float64bits(response.Results[i].Score) != math.Float64bits(want[i].Score) {
				t.Fatalf("mode=%s rank=%d result=%+v want=%+v", mode, i, response.Results[i], want[i])
			}
		}
		if response.Stats.NormBytesRead != 0 {
			t.Fatalf("mode=%s read persisted norms: %+v", mode, response.Stats)
		}
		if mode == VectorIndexQueryModeQuantizedRerank {
			proof := response.Stats.ColumnGraphWork.ScorePlane
			if proof.PackedScoreBatchCalls != 1 || proof.PackedScoreCandidates != uint64(len(ids)) || proof.ForbiddenStableScoreCalls != 0 {
				t.Fatalf("sq8 exhaustive packed proof=%+v", proof)
			}
		}
	}

	for name, invalidQuery := range map[string][]float32{
		"zero": {0, 0, 0, 0, 0, 0, 0, 0},
		"nan":  {float32(math.NaN()), 1, 0, 0, 0, 0, 0, 0},
		"inf":  {float32(math.Inf(1)), 1, 0, 0, 0, 0, 0, 0},
	} {
		t.Run("invalid_query_"+name, func(t *testing.T) {
			for _, mode := range []VectorIndexQueryMode{VectorIndexQueryModeExact, VectorIndexQueryModeQuantizedRerank} {
				opts := VectorIndexSearchOptions{
					IndexName: "embedding_graph", Query: invalidQuery, QueryMode: mode,
					TopK: len(ids), EfSearch: len(ids), StatsMode: VectorIndexSearchStatsModeMinimal,
				}
				if mode == VectorIndexQueryModeQuantizedRerank {
					opts.QuantizedIndexName = "embedding.scalar_u8.legacy"
					opts.QuantizedRerankCandidates = len(ids)
				}
				var buffer VectorIndexSearchBuffer
				response, view, err := col.SearchVectorIndexWithBufferReadView(opts, &buffer)
				if view != nil {
					_ = view.Close()
				}
				if err == nil || len(response.Results) != 0 {
					t.Fatalf("mode=%s response=%+v err=%v", mode, response, err)
				}
			}
		})
	}
}

func TestCosineNormalizedF32V1EmptyRebuildPublishesTopologyOnlyPack(t *testing.T) {
	_, db, col := openTypedMinimaCollectionMeta(t, cosineNormalizedF32V1TestMeta())
	defer db.Close()
	status, err := col.RebuildVectorIndex("embedding_graph")
	if err != nil {
		t.Fatal(err)
	}
	assertColumnGraphRebuildLoadedStatusV2A(t, status, "embedding_graph")
	records, cfg := loadColumnGraphRebuildManifestRecordsAndConfigV2A(t, db, "minima")
	stateRecord, ok := findColumnVectorIndexStateRecord(records, "embedding_graph")
	if !ok {
		t.Fatal("vector-index state record missing")
	}
	state, err := decodeColumnVectorIndexStateRecord(stateRecord.value)
	if err != nil {
		t.Fatal(err)
	}
	for _, asset := range state.Assets {
		if asset.Role == columnVectorIndexStateAssetRoleNormalizedVectors || asset.Role == columnVectorIndexStateAssetRoleInverseNorm {
			t.Fatalf("empty normalized base retained row state: %+v", asset)
		}
	}
	packAsset, found, err := findColumnHNSWSearchPackStateAsset(state)
	if err != nil || !found {
		t.Fatalf("find topology pack: found=%t err=%v", found, err)
	}
	raw := readColumnHNSWSearchPackRawForTest2313(t, db, packAsset.Ref)
	pack, err := decodeColumnHNSWSearchPack(raw, columnHNSWSearchPackDecodeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if pack.Header.Rows != 0 || !pack.Header.ExternalNormalizedVectors || pack.Header.ExternalVectorDigest != columnHNSWSearchPackEmptyExternalVectorDigest() || len(pack.NormalizedVectors) != 0 {
		t.Fatalf("empty topology pack=%+v vectors=%d", pack.Header, len(pack.NormalizedVectors))
	}
	reader, err := col.openColumnVectorGraphPhysicalRowReader("embedding_graph", columnVectorGraphPhysicalRowReaderOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if reader.hnswSearchPack == nil || reader.hnswSearchPack.validateLive() != nil {
		t.Fatal("empty topology pack was not bound and validated")
	}
	originalDigest := reader.hnswSearchPack.Header.ExternalVectorDigest
	reader.hnswSearchPack.Header.ExternalVectorDigest[0]++
	err = reader.hnswSearchPack.bindExternalNormalizedVectors(state, records, *cfg, nil)
	reader.hnswSearchPack.Header.ExternalVectorDigest = originalDigest
	if err == nil {
		t.Fatal("empty topology pack accepted the wrong canonical vector sentinel")
	}
}

func TestCosineNormalizedF32V1PublicSQ8UsesPackedCanonicalRerank(t *testing.T) {
	requireTypedGraphPublicServingTest(t)
	meta := cosineNormalizedF32V1TestMeta()
	meta.VectorIndexes[0].QuantizedIndexes = []QuantizedVectorIndexDefinition{{Name: "embedding.scalar_u8.legacy"}}
	_, db, col := openTypedMinimaCollectionMeta(t, meta)
	defer db.Close()

	ids := [][]byte{[]byte("b"), []byte("a"), []byte("c"), []byte("d"), []byte("e"), []byte("f")}
	vectors := [][]float32{
		{6, 8, 0, 0, 0, 0, 0, 0},
		{3, 4, 0, 0, 0, 0, 0, 0},
		{4, 3, 0, 0, 0, 0, 0, 0},
		{0, 1, 0, 0, 0, 0, 0, 0},
		{-3, -4, 0, 0, 0, 0, 0, 0},
		{0, 0, 1, 0, 0, 0, 0, 0},
	}
	retained := make([][]byte, len(ids))
	content, user, path := make([]string, len(ids)), make([]string, len(ids)), make([]string, len(ids))
	for i := range ids {
		retained[i] = []byte(`{"id":"` + string(ids[i]) + `"}`)
		content[i], user[i], path[i] = string(ids[i]), "u", "p"
	}
	columns := []TypedColumnBatch{
		{Name: "embedding", Float32Vectors: vectors},
		{Name: "content", Strings: content},
		{Name: "user", Strings: user},
		{Name: "path", Strings: path},
	}
	if _, _, err := col.InsertTypedBatchWithStats(ids, retained, columns); err != nil {
		t.Fatal(err)
	}
	if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
		t.Fatal(err)
	}
	if err := col.EnsureColumnGraphServing(context.Background(), "embedding_graph", typedGraphPublicTestOptions()); errors.Is(err, errColumnVectorGraphSharedPreparedSearchNotEligible) {
		t.Skip("typed shared prepared holder is unavailable on this host")
	} else if err != nil {
		t.Fatal(err)
	}

	opts := VectorIndexSearchOptions{
		IndexName:                 "embedding_graph",
		Query:                     []float32{30, 40, 0, 0, 0, 0, 0, 0},
		QueryMode:                 VectorIndexQueryModeQuantizedRerank,
		QuantizedIndexName:        "embedding.scalar_u8.legacy",
		QuantizedRerankCandidates: len(ids),
		TopK:                      2,
		EfSearch:                  len(ids),
		StatsMode:                 VectorIndexSearchStatsModeMinimal,
	}
	var buffer VectorIndexSearchBuffer
	response, view, err := col.SearchVectorIndexWithBufferReadView(opts, &buffer)
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	if len(response.Results) != 2 || string(response.Results[0].ID) != "a" || string(response.Results[1].ID) != "b" {
		t.Fatalf("results=%+v want deterministic tie [a b]", response.Results)
	}
	for _, result := range response.Results {
		if result.Score < -1 || result.Score > 1 {
			t.Fatalf("unclamped public score=%v", result.Score)
		}
	}
	proof := response.Stats.ColumnGraphWork.ScorePlane
	wantBytes := uint64(len(ids) * meta.VectorIndexes[0].Dimensions * 4)
	if proof.ActualRerankCandidates != uint64(len(ids)) || proof.PackedScoreBatchCalls != 1 || proof.PackedScoreCandidates != uint64(len(ids)) || proof.PackedVectorBytesRead != wantBytes || proof.ExactBaseVectorBytesRead != wantBytes || proof.ForbiddenStableScoreCalls != 0 {
		t.Fatalf("packed proof=%+v want candidates=%d bytes=%d", proof, len(ids), wantBytes)
	}
	if response.Stats.NormBytesRead != 0 || response.Stats.QuantizedRerankCandidates != uint64(len(ids)) || response.Stats.QuantizedRerankExactScoreCalls != uint64(len(ids)) {
		t.Fatalf("normalized packed stats=%+v", response.Stats)
	}

	// A singleton shortlist still crosses the strict indexed vectorops seam;
	// it must not be relabeled after scalar scoreOrdinal fallback.
	opts.TopK, opts.EfSearch, opts.QuantizedRerankCandidates = 1, 1, 1
	var singletonBuffer VectorIndexSearchBuffer
	singleton, singletonView, err := col.SearchVectorIndexWithBufferReadView(opts, &singletonBuffer)
	if err != nil {
		t.Fatal(err)
	}
	defer singletonView.Close()
	singletonProof := singleton.Stats.ColumnGraphWork.ScorePlane
	if len(singleton.Results) != 1 || singletonProof.ActualRerankCandidates != 1 || singletonProof.PackedScoreBatchCalls != 1 || singletonProof.PackedScoreCandidates != 1 || singletonProof.PackedVectorBytesRead != uint64(meta.VectorIndexes[0].Dimensions*4) || singletonProof.ForbiddenStableScoreCalls != 0 {
		t.Fatalf("singleton packed response=%+v proof=%+v", singleton.Results, singletonProof)
	}
}

func TestCosineNormalizedF32V1MutableFoldPinProjectionAndReopen(t *testing.T) {
	requireTypedGraphPublicServingTest(t)
	meta := cosineNormalizedF32V1TestMeta()
	meta.VectorIndexes[0].QuantizedIndexes = []QuantizedVectorIndexDefinition{{Name: "embedding.scalar_u8.legacy"}}
	dir, db, col := openTypedMinimaCollectionMeta(t, meta)

	ids := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
	retained := [][]byte{[]byte(`{"id":"a"}`), []byte(`{"id":"b"}`), []byte(`{"id":"c"}`)}
	hostileRenormalization := []float32{
		math.Float32frombits(0x400d2bea), math.Float32frombits(0x3aa936dd),
		math.Float32frombits(0xc11e5ca6), math.Float32frombits(0xc0fed934),
		math.Float32frombits(0x40c3de24), math.Float32frombits(0x3fe25b65),
		math.Float32frombits(0xc0e55e89), math.Float32frombits(0xbf43214c),
	}
	columns := []TypedColumnBatch{
		{Name: "embedding", Float32Vectors: [][]float32{hostileRenormalization, {4, 3, 0, 0, 0, 0, 0, 0}, {0, 1, 0, 0, 0, 0, 0, 0}}},
		{Name: "content", Strings: []string{"alpha", "bravo", "charlie"}},
		{Name: "user", Strings: []string{"u", "u", "u"}},
		{Name: "path", Strings: []string{"p", "p", "p"}},
	}
	if _, _, err := col.InsertTypedBatchWithStats(ids, retained, columns); err != nil {
		db.Close()
		t.Fatal(err)
	}
	readEmbedding := func(collection *Collection, id []byte) []float32 {
		t.Helper()
		raw, err := collection.Get(id)
		if err != nil {
			t.Fatal(err)
		}
		var document struct {
			Embedding []float32 `json:"embedding"`
		}
		if err := json.Unmarshal(raw, &document); err != nil {
			t.Fatal(err)
		}
		return document.Embedding
	}
	canonicalA := append([]float32(nil), readEmbedding(col, ids[0])...)
	renormalizedA, err := normalizeCosineNormalizedF32V1(canonicalA, len(canonicalA))
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	renormalizationChanged := false
	for i := range canonicalA {
		renormalizationChanged = renormalizationChanged || math.Float32bits(canonicalA[i]) != math.Float32bits(renormalizedA[i])
	}
	if !renormalizationChanged {
		db.Close()
		t.Fatal("metadata-only update fixture cannot detect a forbidden second normalization")
	}
	matched, modified, err := col.Update(ids[0], func(current []byte) ([]byte, bool, error) {
		var document map[string]json.RawMessage
		if err := json.Unmarshal(current, &document); err != nil {
			return nil, false, err
		}
		document["content"] = json.RawMessage(`"metadata-only"`)
		next, err := json.Marshal(document)
		return next, true, err
	})
	if err != nil || !matched || !modified {
		db.Close()
		t.Fatalf("metadata-only update matched=%t modified=%t err=%v", matched, modified, err)
	}
	requireFloat32BitsEqual(t, readEmbedding(col, ids[0]), canonicalA)

	if _, err := col.RebuildVectorIndex("embedding_graph"); err != nil {
		db.Close()
		t.Fatal(err)
	}
	serving := typedGraphPublicTestOptions()
	if err := col.EnsureColumnGraphServing(context.Background(), "embedding_graph", serving); errors.Is(err, errColumnVectorGraphSharedPreparedSearchNotEligible) {
		db.Close()
		t.Skip("typed shared prepared holder is unavailable on this host")
	} else if err != nil {
		db.Close()
		t.Fatal(err)
	}
	search := func(collection *Collection) (VectorIndexSearchResponse, *CollectionReadView) {
		t.Helper()
		var buffer VectorIndexSearchBuffer
		response, view, err := collection.SearchVectorIndexWithBufferReadView(VectorIndexSearchOptions{
			IndexName:                 "embedding_graph",
			Query:                     hostileRenormalization,
			QueryMode:                 VectorIndexQueryModeQuantizedRerank,
			QuantizedIndexName:        "embedding.scalar_u8.legacy",
			QuantizedRerankCandidates: 3,
			TopK:                      3,
			EfSearch:                  3,
			StatsMode:                 VectorIndexSearchStatsModeMinimal,
		}, &buffer)
		if err != nil || view == nil {
			t.Fatalf("selected search response=%+v view=%v err=%v", response, view, err)
		}
		return response, view
	}
	held, heldView := search(col)
	if held.Stats.VectorBytesRead == 0 || held.Stats.ColumnGraphWork.ScorePlane.ForbiddenStableScoreCalls != 0 {
		heldView.Close()
		db.Close()
		t.Fatalf("held selected scoring stats=%+v proof=%+v", held.Stats, held.Stats.ColumnGraphWork.ScorePlane)
	}

	if _, err := col.ReplaceTypedBatch(ids[2:3], retained[2:3], []TypedColumnBatch{
		{Name: "embedding", Float32Vectors: [][]float32{{5, 0, 0, 0, 0, 0, 0, 0}}},
		{Name: "content", Strings: []string{"charlie-new"}}, {Name: "user", Strings: []string{"u"}}, {Name: "path", Strings: []string{"p"}},
	}); err != nil {
		heldView.Close()
		db.Close()
		t.Fatal(err)
	}
	if err := col.Delete(ids[1]); err != nil {
		heldView.Close()
		db.Close()
		t.Fatal(err)
	}
	if _, _, err := col.InsertTypedBatchWithStats(
		[][]byte{[]byte("d")}, [][]byte{[]byte(`{"id":"d"}`)},
		[]TypedColumnBatch{
			{Name: "embedding", Float32Vectors: [][]float32{{0, -2, 0, 0, 0, 0, 0, 0}}},
			{Name: "content", Strings: []string{"delta"}}, {Name: "user", Strings: []string{"u"}}, {Name: "path", Strings: []string{"p"}},
		},
	); err != nil {
		heldView.Close()
		db.Close()
		t.Fatal(err)
	}

	withoutEmbedding, err := heldView.FetchDocumentsForVectorIndexSearchResults(held.Results, DocumentFetchOptions{ExcludePaths: []string{"embedding"}})
	if err != nil {
		heldView.Close()
		db.Close()
		t.Fatal(err)
	}
	if withoutEmbedding.Stats.TypedColumnRows != 0 {
		heldView.Close()
		db.Close()
		t.Fatalf("return_embedding=false read vector typed column: %+v", withoutEmbedding.Stats)
	}
	for _, result := range withoutEmbedding.Results {
		var document map[string]json.RawMessage
		if err := json.Unmarshal(result.Document, &document); err != nil {
			t.Fatal(err)
		}
		if _, ok := document["embedding"]; ok {
			t.Fatalf("excluded embedding returned in %s", result.Document)
		}
	}
	withEmbedding, err := heldView.FetchDocumentsForVectorIndexSearchResults(held.Results, DocumentFetchOptions{})
	if err != nil || withEmbedding.Stats.TypedColumnRows != uint64(len(held.Results)) {
		heldView.Close()
		db.Close()
		t.Fatalf("return_embedding=true stats=%+v err=%v", withEmbedding.Stats, err)
	}
	var heldA struct {
		ID        string    `json:"id"`
		Embedding []float32 `json:"embedding"`
	}
	for _, result := range withEmbedding.Results {
		var document struct {
			ID        string    `json:"id"`
			Embedding []float32 `json:"embedding"`
		}
		if err := json.Unmarshal(result.Document, &document); err != nil {
			t.Fatal(err)
		}
		if document.ID == "a" {
			heldA = document
		}
	}
	requireFloat32BitsEqual(t, heldA.Embedding, canonicalA)

	current, currentView := search(col)
	if len(current.Results) != 3 || string(current.Results[0].ID) != "a" || string(current.Results[1].ID) != "c" || string(current.Results[2].ID) != "d" {
		currentView.Close()
		heldView.Close()
		db.Close()
		t.Fatalf("current mutable results=%+v want [a c d]", current.Results)
	}
	proof := current.Stats.ColumnGraphWork.ScorePlane
	if proof.PackedScoreCandidates == 0 || proof.ExactSuffixScoreCalls != 2 || proof.ForbiddenStableScoreCalls != 0 || current.Stats.NormBytesRead != 0 {
		currentView.Close()
		heldView.Close()
		db.Close()
		t.Fatalf("mutable packed/suffix proof=%+v stats=%+v", proof, current.Stats)
	}
	if err := currentView.Close(); err != nil {
		t.Fatal(err)
	}

	if err := col.foldTypedGraph(context.Background(), serving.Owners.Cold, serving.FoldRows, serving.CandidateOutput, nil); err != nil {
		heldView.Close()
		db.Close()
		t.Fatal(err)
	}
	col.retireTypedGraphCapturedBaseKeepers("embedding_graph")
	pinned, err := col.renewTypedGraphWorkEpoch(context.Background(), serving.Maintenance)
	if err != nil {
		heldView.Close()
		db.Close()
		t.Fatalf("pinned normalized owner renewal=%+v err=%v", pinned.Columns, err)
	}
	stillHeld, err := heldView.FetchDocumentsForVectorIndexSearchResults(held.Results, DocumentFetchOptions{})
	if err != nil || len(stillHeld.Results) != len(withEmbedding.Results) {
		heldView.Close()
		db.Close()
		t.Fatalf("pinned normalized generation became unreadable: results=%+v err=%v", stillHeld.Results, err)
	}
	for i := range stillHeld.Results {
		if string(stillHeld.Results[i].Document) != string(withEmbedding.Results[i].Document) {
			heldView.Close()
			db.Close()
			t.Fatalf("pinned document %d changed before=%q after=%q", i, withEmbedding.Results[i].Document, stillHeld.Results[i].Document)
		}
	}
	if err := heldView.Close(); err != nil {
		db.Close()
		t.Fatal(err)
	}
	advanceColumnAssetDurableFallbackM15C(t, db)
	released, err := col.renewTypedGraphWorkEpoch(context.Background(), serving.Maintenance)
	if err != nil || released.Columns.SegmentsDeleted == 0 || released.Columns.BytesRetained >= pinned.Columns.BytesRetained {
		db.Close()
		t.Fatalf("released normalized owner renewal pinned=%+v released=%+v err=%v", pinned.Columns, released.Columns, err)
	}

	if err := db.Checkpoint(); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := openTypedMinimaDB(t, dir)
	defer reopened.Close()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("minima")
	if err != nil {
		t.Fatal(err)
	}
	if err := reopenedCol.EnsureColumnGraphServing(context.Background(), "embedding_graph", serving); err != nil {
		t.Fatal(err)
	}
	reopenedResponse, reopenedView := search(reopenedCol)
	defer reopenedView.Close()
	if len(reopenedResponse.Results) != 3 || reopenedResponse.Stats.ColumnGraphWork.ScorePlane.PackedScoreCandidates == 0 || reopenedResponse.Stats.ColumnGraphWork.ScorePlane.ExactSuffixScoreCalls != 0 || reopenedResponse.Stats.NormBytesRead != 0 {
		t.Fatalf("reopened selected response=%+v", reopenedResponse)
	}
	requireFloat32BitsEqual(t, readEmbedding(reopenedCol, ids[0]), canonicalA)
}
