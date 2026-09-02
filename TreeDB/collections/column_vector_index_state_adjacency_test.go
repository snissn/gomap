package collections

import (
	"encoding/binary"
	"fmt"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

func TestColumnGraphRebuildPublishesUint32ListAdjacencyState1987(t *testing.T) {
	t.Run("empty_collection", func(t *testing.T) {
		dir, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 0, nil)
		defer func() { _ = d.Close() }()
		if _, err := col.RebuildVectorIndex(def.Name); err != nil {
			t.Fatalf("RebuildVectorIndex: %v", err)
		}
		records, cfg := loadColumnGraphRebuildManifestRecordsAndConfigV2A(t, d, "docs")
		state := columnVectorIndexStateFromRecords1987(t, records, def)
		assets := columnVectorIndexStateAdjacencyAssetsByLayer1987(t, state)
		if len(assets) != 1 {
			t.Fatalf("adjacency state assets=%d want one layer-0 asset", len(assets))
		}
		list := loadColumnVectorIndexStateAdjacencyList1987(t, d, "docs", cfg, def, state, 0)
		if list.Rows != 0 || len(list.Offsets) != 1 || list.Offsets[0] != 0 || len(list.Values) != 0 {
			t.Fatalf("empty state list=%+v want rows=0 offsets=[0] values=[]", list)
		}
		if err := d.Checkpoint(); err != nil {
			t.Fatalf("Checkpoint: %v", err)
		}
		if err := d.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		reopened := openCollectionCommandWALDB(t, dir)
		defer func() { _ = reopened.Close() }()
		reopenedCol, err := NewCollectionManager(reopened).OpenCollection("docs")
		if err != nil {
			t.Fatalf("OpenCollection reopen: %v", err)
		}
		status, err := reopenedCol.VectorIndexStatus(def.Name)
		if err != nil {
			t.Fatalf("VectorIndexStatus reopen: %v", err)
		}
		assertColumnGraphRebuildLoadedStatusV2A(t, status, def.Name)
	})

	t.Run("single_empty_list", func(t *testing.T) {
		rows := []columnGraphRebuildInputRowV2A{{id: "solo", vector: []float32{1, 0, 0}}}
		_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 0, rows)
		defer func() { _ = d.Close() }()
		if _, err := col.RebuildVectorIndex(def.Name); err != nil {
			t.Fatalf("RebuildVectorIndex: %v", err)
		}
		records, cfg := loadColumnGraphRebuildManifestRecordsAndConfigV2A(t, d, "docs")
		state := columnVectorIndexStateFromRecords1987(t, records, def)
		list := loadColumnVectorIndexStateAdjacencyList1987(t, d, "docs", cfg, def, state, 0)
		if list.Rows != 1 || len(list.Offsets) != 2 || list.Offsets[0] != 0 || list.Offsets[1] != 0 || len(list.Values) != 0 {
			t.Fatalf("single-row state list=%+v want one empty row", list)
		}
	})

	t.Run("multi_layer", func(t *testing.T) {
		rows := columnGraphRebuildSyntheticRowsV2A(96, 3)
		_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 2, rows)
		defer func() { _ = d.Close() }()
		if _, err := col.RebuildVectorIndex(def.Name); err != nil {
			t.Fatalf("RebuildVectorIndex: %v", err)
		}
		graph, scanned := loadAndScanColumnGraphRebuildRowsV2A(t, d, "docs", def)
		records, cfg := loadColumnGraphRebuildManifestRecordsAndConfigV2A(t, d, "docs")
		state := columnVectorIndexStateFromRecords1987(t, records, def)
		assertColumnVectorIndexStateAdjacencyAssetsMatchScanned1987(t, d, "docs", cfg, def, graph, state, scanned)
	})
}

func TestBuildColumnVectorIndexStateAdjacencyListsPresizesValues4521(t *testing.T) {
	rows := []columnVectorGraphAssetRow{
		{Adjacency: []uint32{7, 8}},
		{Adjacency: []uint32{columnVectorGraphLayeredAdjacencyMagic, 2, 2, 9, 10, 1, 11, 0}},
		{Adjacency: []uint32{12}},
		{Adjacency: []uint32{columnVectorGraphLayeredAdjacencyMagic, 1, 1, 13, 2, 14, 15}},
	}
	want := []typedcolumn.Uint32List{
		{Rows: 4, Offsets: []uint64{0, 2, 4, 5, 6}, Values: []uint32{7, 8, 9, 10, 12, 13}},
		{Rows: 4, Offsets: []uint64{0, 0, 1, 1, 3}, Values: []uint32{11, 14, 15}},
		{Rows: 4, Offsets: []uint64{0, 0, 0, 0, 0}},
	}
	got, err := buildColumnVectorIndexStateAdjacencyLists(rows)
	if err != nil {
		t.Fatalf("buildColumnVectorIndexStateAdjacencyLists: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("layers=%d want %d", len(got), len(want))
	}
	for layer := range want {
		if cap(got[layer].Values) != len(got[layer].Values) {
			t.Fatalf("layer %d values cap=%d len=%d want exact capacity", layer, cap(got[layer].Values), len(got[layer].Values))
		}
		gotBytes, err := typedcolumn.EncodeRawUint32OffsetsListPayload(nil, got[layer].Rows, got[layer].Offsets, got[layer].Values)
		if err != nil {
			t.Fatalf("encode layer %d got: %v", layer, err)
		}
		wantBytes, err := typedcolumn.EncodeRawUint32OffsetsListPayload(nil, want[layer].Rows, want[layer].Offsets, want[layer].Values)
		if err != nil {
			t.Fatalf("encode layer %d want: %v", layer, err)
		}
		if string(gotBytes) != string(wantBytes) {
			t.Fatalf("layer %d bytes/value/offset parity mismatch: got rows=%d offsets=%v values=%v; want rows=%d offsets=%v values=%v", layer, got[layer].Rows, got[layer].Offsets, got[layer].Values, want[layer].Rows, want[layer].Offsets, want[layer].Values)
		}
	}
}

func TestColumnVectorIndexStateAdjacencyStatusValidation1987(t *testing.T) {
	t.Run("loaded_without_legacy_column_graph_adjacency_sources", func(t *testing.T) {
		d := openCollectionCommandWALDB(t, t.TempDir())
		defer func() { _ = d.Close() }()
		ctx := makeColumnVectorIndexStateAdjacencyStatusContext1987(t, d)
		if len(ctx.graph.AdjacencyLayerSources) != 0 || ctx.graph.Layer0AdjacencySource.Present {
			t.Fatalf("test graph unexpectedly has legacy adjacency sources: %+v", ctx.graph)
		}
		publishColumnVectorIndexStateAdjacencyContext1987(t, d, ctx)
		col, err := NewCollectionManager(d).OpenCollection("docs")
		if err != nil {
			t.Fatalf("OpenCollection: %v", err)
		}
		status, err := col.VectorIndexStatus(ctx.def.Name)
		if err != nil {
			t.Fatalf("VectorIndexStatus: %v", err)
		}
		assertColumnGraphRebuildLoadedStatusV2A(t, status, ctx.def.Name)
		if got, want := status.Stats.BytesDisk, columnVectorGraphStorageBytesWithState(ctx.graph, ctx.state); got != want {
			t.Fatalf("status bytes_disk=%d want graph+state assets=%d", got, want)
		}
	})

	t.Run("missing_adjacency_assets", func(t *testing.T) {
		d := openCollectionCommandWALDB(t, t.TempDir())
		defer func() { _ = d.Close() }()
		ctx := makeColumnVectorIndexStateAdjacencyStatusContext1987(t, d)
		ctx.state.Assets = nil
		ctx.records, ctx.identity = appendVectorIndexStateRecordForTest1986(t, *ctx.cfg, ctx.records, ctx.identity, ctx.state)
		publishColumnVectorIndexStateAdjacencyContext1987(t, d, ctx)
		assertColumnVectorIndexStateAdjacencyStatusCorrupt1987(t, d, ctx.def, "missing adjacency uint32_list assets")
	})

	t.Run("adjacency_layer_gap", func(t *testing.T) {
		d := openCollectionCommandWALDB(t, t.TempDir())
		defer func() { _ = d.Close() }()
		ctx := makeColumnVectorIndexStateAdjacencyStatusContext1987(t, d)
		layer1 := writeUncheckedColumnVectorIndexStateAdjacencyAsset1987(t, d, *ctx.cfg, ctx.def, ctx.identity.Generation, 503, 1, typedcolumn.Uint32List{Rows: 2, Offsets: []uint64{0, 1, 2}, Values: []uint32{1, 0}}, nil)
		ctx.state.Assets = []columnVectorIndexStateAssetSnapshot{layer1}
		if got, want := ctx.state.AdjacencyLayerCount, 1; got != want {
			t.Fatalf("state adjacency layer count=%d want %d", got, want)
		}
		ctx.records, ctx.identity = appendVectorIndexStateRecordForTest1986(t, *ctx.cfg, ctx.records, ctx.identity, ctx.state)
		publishColumnVectorIndexStateAdjacencyContext1987(t, d, ctx)
		assertColumnVectorIndexStateAdjacencyStatusCorrupt1987(t, d, ctx.def, "missing adjacency layer 0")
	})

	t.Run("missing_highest_adjacency_layer_preserves_expected_count", func(t *testing.T) {
		d := openCollectionCommandWALDB(t, t.TempDir())
		defer func() { _ = d.Close() }()
		assetRows1989 := []columnVectorGraphAssetRow{
			{ID: []byte("doc-a"), Vector: []float32{1, 0, 0}, InvNorm: 1, Adjacency: []uint32{columnVectorGraphLayeredAdjacencyMagic, 1, 1, 1, 1, 1}},
			{ID: []byte("doc-b"), Vector: []float32{0, 1, 0}, InvNorm: 1, Adjacency: []uint32{columnVectorGraphLayeredAdjacencyMagic, 1, 1, 0, 1, 0}},
		}
		ctx := makeColumnVectorIndexStateAdjacencyStatusContextWithRows1987(t, d, assetRows1989)
		wantLayers := columnVectorGraphExpectedAdjacencyLayerCountFromAssetRows1989(t, assetRows1989)
		if got := ctx.state.AdjacencyLayerCount; got != wantLayers {
			t.Fatalf("state adjacency layer count=%d want %d", got, wantLayers)
		}
		ctx.state.Assets = ctx.state.Assets[:1]
		ctx.records, ctx.identity = appendVectorIndexStateRecordForTest1986(t, *ctx.cfg, ctx.records, ctx.identity, ctx.state)
		publishColumnVectorIndexStateAdjacencyContext1987(t, d, ctx)
		assertColumnVectorIndexStateAdjacencyStatusCorrupt1987(t, d, ctx.def, fmt.Sprintf("adjacency layers=1 want %d", wantLayers))
	})

	t.Run("out_of_bounds_ordinal", func(t *testing.T) {
		d := openCollectionCommandWALDB(t, t.TempDir())
		defer func() { _ = d.Close() }()
		ctx := makeColumnVectorIndexStateAdjacencyStatusContext1987(t, d)
		bad := writeUncheckedColumnVectorIndexStateAdjacencyAsset1987(t, d, *ctx.cfg, ctx.def, ctx.identity.Generation, 500, 0, typedcolumn.Uint32List{Rows: 2, Offsets: []uint64{0, 1, 1}, Values: []uint32{2}}, nil)
		ctx.state.Assets[0] = bad
		ctx.records, ctx.identity = appendVectorIndexStateRecordForTest1986(t, *ctx.cfg, ctx.records, ctx.identity, ctx.state)
		publishColumnVectorIndexStateAdjacencyContext1987(t, d, ctx)
		assertColumnVectorIndexStateAdjacencyOpenCorrupt1987(t, d, ctx.def, "outside row_count")
	})

	t.Run("row_count_mismatch", func(t *testing.T) {
		d := openCollectionCommandWALDB(t, t.TempDir())
		defer func() { _ = d.Close() }()
		ctx := makeColumnVectorIndexStateAdjacencyStatusContext1987(t, d)
		bad := writeUncheckedColumnVectorIndexStateAdjacencyAsset1987(t, d, *ctx.cfg, ctx.def, ctx.identity.Generation, 501, 0, typedcolumn.Uint32List{Rows: 1, Offsets: []uint64{0, 0}}, nil)
		bad.RowCount = ctx.state.RowCount
		ctx.state.Assets[0] = bad
		ctx.records, ctx.identity = appendVectorIndexStateRecordForTest1986(t, *ctx.cfg, ctx.records, ctx.identity, ctx.state)
		publishColumnVectorIndexStateAdjacencyContext1987(t, d, ctx)
		assertColumnVectorIndexStateAdjacencyOpenCorrupt1987(t, d, ctx.def, "image part/rows")
	})

	t.Run("final_offset_value_count_mismatch", func(t *testing.T) {
		d := openCollectionCommandWALDB(t, t.TempDir())
		defer func() { _ = d.Close() }()
		ctx := makeColumnVectorIndexStateAdjacencyStatusContext1987(t, d)
		bad := writeUncheckedColumnVectorIndexStateAdjacencyAsset1987(t, d, *ctx.cfg, ctx.def, ctx.identity.Generation, 502, 0, typedcolumn.Uint32List{Rows: 2, Offsets: []uint64{0, 1, 1}, Values: []uint32{1}}, func(payload []byte, image typedcolumn.ColumnPartImage, adapterColumn typedColumnAdapterColumn) {
			offsetsSection, _, ok := image.ColumnOffsetsListSections(adapterColumn.Definition.Name)
			if !ok {
				t.Fatalf("missing offsets-list sections")
			}
			binary.LittleEndian.PutUint64(payload[offsetsSection.Offset+offsetsSection.Length-8:], 2)
		})
		ctx.state.Assets[0] = bad
		ctx.records, ctx.identity = appendVectorIndexStateRecordForTest1986(t, *ctx.cfg, ctx.records, ctx.identity, ctx.state)
		publishColumnVectorIndexStateAdjacencyContext1987(t, d, ctx)
		assertColumnVectorIndexStateAdjacencyOpenCorrupt1987(t, d, ctx.def, "final offset=2 values=1")
	})

	t.Run("embedded_schema_version_mismatch", func(t *testing.T) {
		d := openCollectionCommandWALDB(t, t.TempDir())
		defer func() { _ = d.Close() }()
		ctx := makeColumnVectorIndexStateAdjacencyStatusContext1987(t, d)
		bad := writeUncheckedColumnVectorIndexStateAdjacencyAsset1987(t, d, *ctx.cfg, ctx.def, ctx.identity.Generation, 504, 0, typedcolumn.Uint32List{Rows: 2, Offsets: []uint64{0, 1, 2}, Values: []uint32{1, 0}}, func(payload []byte, image typedcolumn.ColumnPartImage, adapterColumn typedColumnAdapterColumn) {
			_ = adapterColumn
			for _, section := range image.Sections {
				if section.Kind != typedcolumn.ColumnPartImageSectionDescriptor {
					continue
				}
				if section.Length < 14 {
					t.Fatalf("descriptor section length=%d too short", section.Length)
				}
				// Offset 10 within the descriptor section is schema_version (uint32).
				current := binary.LittleEndian.Uint32(payload[section.Offset+10 : section.Offset+14])
				binary.LittleEndian.PutUint32(payload[section.Offset+10:section.Offset+14], current+1)
				return
			}
			t.Fatalf("descriptor section missing")
		})
		ctx.state.Assets[0] = bad
		ctx.records, ctx.identity = appendVectorIndexStateRecordForTest1986(t, *ctx.cfg, ctx.records, ctx.identity, ctx.state)
		publishColumnVectorIndexStateAdjacencyContext1987(t, d, ctx)
		assertColumnVectorIndexStateAdjacencyOpenCorrupt1987(t, d, ctx.def, "schema_version")
	})

	t.Run("stale_base_identity", func(t *testing.T) {
		d := openCollectionCommandWALDB(t, t.TempDir())
		defer func() { _ = d.Close() }()
		ctx := makeColumnVectorIndexStateAdjacencyStatusContext1987(t, d)
		ctx.state.BaseManifestChecksum++
		ctx.records, ctx.identity = appendVectorIndexStateRecordForTest1986(t, *ctx.cfg, ctx.records, ctx.identity, ctx.state)
		publishColumnVectorIndexStateAdjacencyContext1987(t, d, ctx)
		col, err := NewCollectionManager(d).OpenCollection("docs")
		if err != nil {
			t.Fatalf("OpenCollection: %v", err)
		}
		status, err := col.VectorIndexStatus(ctx.def.Name)
		if err != nil {
			t.Fatalf("VectorIndexStatus stale identity err=%v, want rebuild-needed status", err)
		}
		if status.State != VectorIndexStateColumnGraphRebuildNeeded || status.Reason != VectorIndexReasonColumnGraphAssetMismatch || !status.RebuildNeeded {
			t.Fatalf("status=%+v want stale state rebuild-needed", status)
		}
	})
}

type columnVectorIndexStateAdjacencyStatusContext1987 struct {
	cfg      *ColumnStoreConfig
	def      VectorIndexDefinition
	identity ColumnManifestIdentity
	records  []columnManifestRecord
	graph    columnVectorGraphManifestSnapshot
	state    columnVectorIndexStateSnapshot
}

func makeColumnVectorIndexStateAdjacencyStatusContext1987(tb testing.TB, d *backenddb.DB) columnVectorIndexStateAdjacencyStatusContext1987 {
	return makeColumnVectorIndexStateAdjacencyStatusContextWithRows1987(tb, d, []columnVectorGraphAssetRow{
		{ID: []byte("doc-a"), Vector: []float32{1, 0, 0}, InvNorm: 1, Adjacency: []uint32{1}},
		{ID: []byte("doc-b"), Vector: []float32{0, 1, 0}, InvNorm: 1, Adjacency: []uint32{0}},
	})
}

func makeColumnVectorIndexStateAdjacencyStatusContextWithRows1987(tb testing.TB, d *backenddb.DB, rows []columnVectorGraphAssetRow) columnVectorIndexStateAdjacencyStatusContext1987 {
	tb.Helper()
	baseCfg, err := normalizeColumnStoreConfig("docs", testColumnGraphBaseColumnStoreConfigV2A())
	if err != nil {
		tb.Fatalf("normalize base column store: %v", err)
	}
	def := testColumnGraphVectorIndexDefinitionV2A()
	prepared, err := prepareColumnVectorGraphPhysicalAsset(d.ColumnAssetRootDir(), "docs", *baseCfg, def, 2, 1, 1, rows)
	if err != nil {
		tb.Fatalf("prepareColumnVectorGraphPhysicalAsset: %v", err)
	}
	identity := ColumnManifestIdentity{Generation: 2, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 0x1234}
	records, identity := testColumnGraphManifestRecordsV2A(tb, *baseCfg, def, identity, prepared.Ref, prepared.Bytes, prepared.RowCount)
	graph := graphManifestFromRecords1918(tb, records, def)
	statePartID := nextColumnVectorGraphPartIDAfter(prepared.Ref.PartID, prepared.Ref.PartID)
	preparedStateAssets, err := prepareColumnVectorIndexStateAdjacencyAssets(d.ColumnAssetRootDir(), "docs", *baseCfg, def, graph.BaseManifestGeneration, statePartID, rows)
	if err != nil {
		tb.Fatalf("prepareColumnVectorIndexStateAdjacencyAssets: %v", err)
	}
	state := columnVectorIndexStateSnapshotFromGraph(graph)
	state.AdjacencyLayerCount = len(preparedStateAssets)
	state.Assets = columnVectorIndexStateAdjacencyAssetsFromPrepared(preparedStateAssets)
	records, identity = appendVectorIndexStateRecordForTest1986(tb, *baseCfg, records, identity, state)
	baseCfg.ActiveManifest = &identity
	baseCfg.RecoveryAuthoritativeManifest = &identity
	return columnVectorIndexStateAdjacencyStatusContext1987{cfg: baseCfg, def: def, identity: identity, records: records, graph: graph, state: state}
}

func publishColumnVectorIndexStateAdjacencyContext1987(tb testing.TB, d *backenddb.DB, ctx columnVectorIndexStateAdjacencyStatusContext1987) {
	tb.Helper()
	meta := CollectionMeta{
		Name:          "docs",
		Options:       CollectionOptions{ColumnStore: testColumnGraphBaseColumnStoreConfigV2A()},
		VectorIndexes: []VectorIndexDefinition{ctx.def},
	}
	publishColumnGraphCatalogForTestV2A(tb, d, meta, ctx.identity, ctx.records)
}

func appendCompleteVectorIndexStateForGraphTest1987(tb testing.TB, d *backenddb.DB, collection string, cfg ColumnStoreConfig, def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot, records []columnManifestRecord, identity ColumnManifestIdentity, input ColumnPublishManifestEncodeInput, rows []columnVectorGraphAssetRow) ([]columnManifestRecord, ColumnManifestIdentity) {
	tb.Helper()
	invNormPartID := nextColumnVectorGraphPartIDAfterGraphSnapshot1987(graph)
	preparedInvNorm, err := prepareColumnVectorGraphInvNormStateAsset(d.ColumnAssetRootDir(), collection, cfg, def, graph.BaseManifestGeneration, invNormPartID, rows)
	if err != nil {
		tb.Fatalf("prepareColumnVectorGraphInvNormStateAsset: %v", err)
	}
	adjacencyPartID := invNormPartID
	if preparedInvNorm.Present {
		adjacencyPartID = nextColumnVectorGraphPartIDAfter(adjacencyPartID, preparedInvNorm.Ref.PartID)
	}
	preparedAdjacency, err := prepareColumnVectorIndexStateAdjacencyAssets(d.ColumnAssetRootDir(), collection, cfg, def, graph.BaseManifestGeneration, adjacencyPartID, rows)
	if err != nil {
		tb.Fatalf("prepareColumnVectorIndexStateAdjacencyAssets: %v", err)
	}
	state := columnVectorIndexStateSnapshotFromGraph(graph)
	state.AdjacencyLayerCount = len(preparedAdjacency)
	state.Assets = columnVectorIndexStateAdjacencyAssetsFromPrepared(preparedAdjacency)
	if invNormAsset, ok := columnVectorGraphInvNormStateAssetSnapshot(preparedInvNorm); ok {
		state.Assets = append(state.Assets, invNormAsset)
	}
	raw, err := encodeColumnVectorIndexStateRecord(state)
	if err != nil {
		tb.Fatalf("encodeColumnVectorIndexStateRecord: %v", err)
	}
	out, err := replaceColumnVectorGraphManifestRecord(records, identity.Generation, columnManifestRecord{key: columnVectorIndexStateRecordKey(def.Name), value: raw})
	if err != nil {
		tb.Fatalf("replace vector-index state record: %v", err)
	}
	identity.Checksum = checksumColumnManifestRecords(input, identity.Generation, out)
	return out, identity
}

func nextColumnVectorGraphPartIDAfterGraphSnapshot1987(graph columnVectorGraphManifestSnapshot) uint64 {
	next := nextColumnVectorGraphPartIDAfter(graph.AssetRef.PartID, graph.AssetRef.PartID)
	for _, source := range graph.AdjacencyLayerSources {
		if source.Present {
			next = nextColumnVectorGraphPartIDAfter(next, source.Ref.PartID)
		}
	}
	if graph.Layer0AdjacencySource.Present {
		next = nextColumnVectorGraphPartIDAfter(next, graph.Layer0AdjacencySource.Ref.PartID)
	}
	return next
}

func columnVectorGraphExpectedAdjacencyLayerCountFromAssetRows1989(tb testing.TB, rows []columnVectorGraphAssetRow) int {
	tb.Helper()
	expectedLayers := 1
	for rowIdx := range rows {
		maxLayer, err := columnVectorGraphAdjacencyMaxLayer(rows[rowIdx].Adjacency)
		if err != nil {
			tb.Fatalf("asset row %d adjacency max layer: %v", rowIdx, err)
		}
		if maxLayer+1 > expectedLayers {
			expectedLayers = maxLayer + 1
		}
	}
	return expectedLayers
}

func columnVectorGraphExpectedAdjacencyLayerCountFromScannedRows1989(tb testing.TB, rows []columnGraphRebuildScannedRowV2A) int {
	tb.Helper()
	expectedLayers := 1
	for rowIdx := range rows {
		maxLayer, err := columnVectorGraphAdjacencyMaxLayer(rows[rowIdx].adjacency)
		if err != nil {
			tb.Fatalf("scanned row %d adjacency max layer: %v", rowIdx, err)
		}
		if maxLayer+1 > expectedLayers {
			expectedLayers = maxLayer + 1
		}
	}
	return expectedLayers
}

func columnVectorGraphStateRowsForTest1987(rows []columnVectorGraphAssetRow, rowCount, dims, layerCount int) []columnVectorGraphAssetRow {
	if rowCount < 0 {
		rowCount = 0
	}
	out := make([]columnVectorGraphAssetRow, rowCount)
	for i := range out {
		if i < len(rows) {
			out[i].ID = append([]byte(nil), rows[i].ID...)
			out[i].Vector = append([]float32(nil), rows[i].Vector...)
			if err := validateColumnVectorGraphAdjacency("vector-index-state-test", i, rows[i].Adjacency, rowCount); err == nil {
				out[i].Adjacency = append([]uint32(nil), rows[i].Adjacency...)
			}
		}
		if len(out[i].ID) == 0 {
			out[i].ID = []byte("state-row")
		}
		if len(out[i].Vector) != dims {
			out[i].Vector = make([]float32, dims)
		}
		if _, err := columnVectorGraphInvNorm(out[i].Vector); err != nil && dims > 0 {
			out[i].Vector[0] = 1
		}
		if len(out[i].Adjacency) == 0 && layerCount > 1 {
			out[i].Adjacency = make([]uint32, 2+layerCount)
			out[i].Adjacency[0] = columnVectorGraphLayeredAdjacencyMagic
			out[i].Adjacency[1] = uint32(layerCount - 1)
		}
	}
	return out
}

func assertColumnVectorIndexStateAdjacencyStatusCorrupt1987(tb testing.TB, d *backenddb.DB, def VectorIndexDefinition, wantErr string) {
	tb.Helper()
	col, err := NewCollectionManager(d).OpenCollection("docs")
	if err != nil {
		tb.Fatalf("OpenCollection: %v", err)
	}
	status, err := col.VectorIndexStatus(def.Name)
	if err == nil || !strings.Contains(err.Error(), wantErr) {
		tb.Fatalf("VectorIndexStatus err=%v want containing %q", err, wantErr)
	}
	if status.State != VectorIndexStateColumnGraphUnavailable || status.Reason != VectorIndexReasonColumnGraphCorrupt || !status.RebuildNeeded {
		tb.Fatalf("status=%+v want corrupt/unavailable", status)
	}
}

func assertColumnVectorIndexStateAdjacencyOpenCorrupt1987(tb testing.TB, d *backenddb.DB, def VectorIndexDefinition, wantErr string) {
	tb.Helper()
	col, err := NewCollectionManager(d).OpenCollection("docs")
	if err != nil {
		tb.Fatalf("OpenCollection: %v", err)
	}
	status, err := col.VectorIndexStatus(def.Name)
	if err != nil || status.State != VectorIndexStateColumnGraphLoaded || !status.Loaded || status.RebuildNeeded {
		tb.Fatalf("VectorIndexStatus status=%+v err=%v want cheap loaded status before payload validation", status, err)
	}
	reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err == nil {
		_ = reader.Close()
		tb.Fatalf("openColumnVectorGraphPhysicalRowReader err=nil want containing %q", wantErr)
	}
	if !strings.Contains(err.Error(), wantErr) {
		tb.Fatalf("openColumnVectorGraphPhysicalRowReader err=%v want containing %q", err, wantErr)
	}
}

func loadColumnGraphRebuildScannedRowsOnly1987(tb testing.TB, d *backenddb.DB, collection string, def VectorIndexDefinition) []columnGraphRebuildScannedRowV2A {
	tb.Helper()
	_, rows := loadAndScanColumnGraphRebuildRowsV2A(tb, d, collection, def)
	return rows
}

func columnVectorIndexStateFromRecords1987(tb testing.TB, records []columnManifestRecord, def VectorIndexDefinition) columnVectorIndexStateSnapshot {
	tb.Helper()
	record, ok := findColumnVectorIndexStateRecord(records, def.Name)
	if !ok {
		tb.Fatalf("vector-index state record %q missing", def.Name)
	}
	state, err := decodeColumnVectorIndexStateRecord(record.value)
	if err != nil {
		tb.Fatalf("decodeColumnVectorIndexStateRecord: %v", err)
	}
	return state
}

func columnVectorIndexStateAdjacencyAssetsByLayer1987(tb testing.TB, state columnVectorIndexStateSnapshot) map[int]columnVectorIndexStateAssetSnapshot {
	tb.Helper()
	assets := make(map[int]columnVectorIndexStateAssetSnapshot)
	for _, asset := range state.Assets {
		if asset.Role != columnVectorIndexStateAssetRoleAdjacency {
			continue
		}
		if asset.LogicalType != columnVectorIndexStateLogicalTypeUint32List || asset.PhysicalEncoding != columnVectorIndexStateEncodingRawUint32List {
			tb.Fatalf("adjacency asset type/encoding=(%q,%q), want generic uint32_list/raw_uint32_offsets_list", asset.LogicalType, asset.PhysicalEncoding)
		}
		layer, err := columnVectorIndexStateAdjacencyLayerFromAssetID(asset.AssetID)
		if err != nil {
			tb.Fatalf("adjacency asset id %q: %v", asset.AssetID, err)
		}
		if _, exists := assets[layer]; exists {
			tb.Fatalf("duplicate adjacency state layer %d", layer)
		}
		assets[layer] = asset
	}
	return assets
}

func loadColumnVectorIndexStateAdjacencyList1987(tb testing.TB, d *backenddb.DB, collection string, cfg *ColumnStoreConfig, def VectorIndexDefinition, state columnVectorIndexStateSnapshot, layer int) typedcolumn.Uint32List {
	tb.Helper()
	assets := columnVectorIndexStateAdjacencyAssetsByLayer1987(tb, state)
	asset, ok := assets[layer]
	if !ok {
		tb.Fatalf("state missing adjacency layer %d assets=%+v", layer, assets)
	}
	if err := validateColumnVectorIndexStateAdjacencyAsset(d.ColumnAssetRootDir(), collection, *cfg, def, state, asset, layer); err != nil {
		tb.Fatalf("validateColumnVectorIndexStateAdjacencyAsset layer %d: %v", layer, err)
	}
	sourceCfg, adapterColumn, err := columnVectorIndexStateAdjacencyColumnStoreConfig(collection, *cfg, def, layer)
	if err != nil {
		tb.Fatalf("columnVectorIndexStateAdjacencyColumnStoreConfig: %v", err)
	}
	if len(sourceCfg.Columns) != 1 || sourceCfg.Columns[0].ValueType != ColumnStoreValueUint32List || sourceCfg.Columns[0].AdjacencyLayout != ColumnAdjacencyListLayoutFixedDense {
		tb.Fatalf("state adjacency config column=%+v want uint32_list with default fixed-dense/no explicit adjacency_layout", sourceCfg.Columns)
	}
	if adapterColumn.Definition.Type != typedcolumn.ColumnTypeUint32List || adapterColumn.Definition.Encoding != typedcolumn.EncodingRawUint32OffsetsList {
		tb.Fatalf("state adjacency adapter definition=%+v want uint32_list raw offsets-list", adapterColumn.Definition)
	}
	raw, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), asset.Ref)
	if err != nil {
		tb.Fatalf("read state adjacency asset: %v", err)
	}
	image, err := typedcolumn.ParseColumnPartImage(raw)
	if err != nil {
		tb.Fatalf("ParseColumnPartImage: %v", err)
	}
	certification, err := typedcolumn.CertifyColumnPartLayoutContractFromImage(image)
	if err != nil {
		tb.Fatalf("CertifyColumnPartLayoutContractFromImage: %v", err)
	}
	certColumn, ok := certification.Column(adapterColumn.Definition.Name)
	if !ok {
		tb.Fatalf("missing cert column %q", adapterColumn.Definition.Name)
	}
	if certColumn.LogicalType != columnVectorIndexStateLogicalTypeUint32List || certColumn.Type != typedcolumn.ColumnTypeUint32List || certColumn.Encoding != typedcolumn.EncodingRawUint32OffsetsList {
		tb.Fatalf("cert column logical/type/encoding=(%q,%s,%s), want uint32_list/raw_uint32_offsets_list", certColumn.LogicalType, certColumn.Type, certColumn.Encoding)
	}
	offsetsSection, valuesSection, ok := image.ColumnOffsetsListSections(adapterColumn.Definition.Name)
	if !ok {
		tb.Fatalf("missing offsets-list sections for %q", adapterColumn.Definition.Name)
	}
	offsetsRaw, err := image.SectionBytes(offsetsSection)
	if err != nil {
		tb.Fatalf("offsets SectionBytes: %v", err)
	}
	valuesRaw, err := image.SectionBytes(valuesSection)
	if err != nil {
		tb.Fatalf("values SectionBytes: %v", err)
	}
	list, err := typedcolumn.DecodeRawUint32OffsetsListFallback(nil, nil, offsetsRaw, valuesRaw, state.RowCount)
	if err != nil {
		tb.Fatalf("DecodeRawUint32OffsetsListFallback: %v", err)
	}
	return list
}

func assertColumnVectorIndexStateAdjacencyAssetsMatchScanned1987(tb testing.TB, d *backenddb.DB, collection string, cfg *ColumnStoreConfig, def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot, state columnVectorIndexStateSnapshot, rows []columnGraphRebuildScannedRowV2A) {
	tb.Helper()
	if !columnVectorIndexStateMatchesGraph(state, graph) {
		tb.Fatalf("state=%+v does not match graph=%+v", state, graph)
	}
	assets := columnVectorIndexStateAdjacencyAssetsByLayer1987(tb, state)
	expectedLayers := columnVectorGraphExpectedAdjacencyLayerCountFromScannedRows1989(tb, rows)
	if state.AdjacencyLayerCount > 0 {
		if state.AdjacencyLayerCount != expectedLayers {
			tb.Fatalf("state adjacency layer count=%d want %d from scanned rows", state.AdjacencyLayerCount, expectedLayers)
		}
		expectedLayers = state.AdjacencyLayerCount
	} else if graph.AdjacencyLayerCount > 0 {
		expectedLayers = graph.AdjacencyLayerCount
	}
	if len(assets) != expectedLayers || expectedLayers == 0 {
		tb.Fatalf("state adjacency assets=%d expected layers=%d graph legacy layers=%d state layers=%d", len(assets), expectedLayers, graph.AdjacencyLayerCount, state.AdjacencyLayerCount)
	}
	for layer := 0; layer < expectedLayers; layer++ {
		asset, ok := assets[layer]
		if !ok {
			tb.Fatalf("missing state adjacency layer %d assets=%+v", layer, assets)
		}
		if asset.RowCount != graph.RowCount || asset.Ref.Kind != ColumnAssetKindTCS1TypedColumnPart {
			tb.Fatalf("asset layer %d=%+v graph rows=%d", layer, asset, graph.RowCount)
		}
		list := loadColumnVectorIndexStateAdjacencyList1987(tb, d, collection, cfg, def, state, layer)
		want := adjacencyLayerSourceFromScannedRows1920(tb, rows, layer)
		assertRawUint32OffsetsListEqual1918(tb, list, want)
		col, err := NewCollectionManager(d).OpenCollection(collection)
		if err != nil {
			tb.Fatalf("OpenCollection: %v", err)
		}
		assertColumnAssetReachabilityProtectsGraphRefV2A(tb, col, asset.Ref)
	}
}

func writeUncheckedColumnVectorIndexStateAdjacencyAsset1987(tb testing.TB, d *backenddb.DB, cfg ColumnStoreConfig, def VectorIndexDefinition, generation, partID uint64, layer int, list typedcolumn.Uint32List, mutate func([]byte, typedcolumn.ColumnPartImage, typedColumnAdapterColumn)) columnVectorIndexStateAssetSnapshot {
	tb.Helper()
	sourceCfg, adapterColumn, err := columnVectorIndexStateAdjacencyColumnStoreConfig("docs", cfg, def, layer)
	if err != nil {
		tb.Fatalf("columnVectorIndexStateAdjacencyColumnStoreConfig: %v", err)
	}
	primaryIDs := make([]int64, list.Rows)
	for rowIdx := range primaryIDs {
		primaryIDs[rowIdx] = int64(rowIdx)
	}
	part, err := typedcolumn.BuildColumnPart(partID, typedcolumn.Options{
		SchemaVersion: uint32(sourceCfg.SchemaHash),
		SchemaMode:    typedcolumn.ColumnSchemaFixed,
		Columns: []typedcolumn.ColumnDefinition{
			{Name: typedColumnAdapterPrimaryIDColumn, Type: typedcolumn.ColumnTypeInt64, Encoding: typedcolumn.EncodingRawInt64, Compression: typedcolumn.CompressionNone, CompressionSet: true, StatsDisabled: true},
			adapterColumn.Definition,
		},
		LogicalPrimaryKey: typedcolumn.LogicalPrimaryKey{Columns: []string{typedColumnAdapterPrimaryIDColumn}},
		SortKey:           typedcolumn.SortKey{Columns: []typedcolumn.SortKeyColumn{{Column: typedColumnAdapterPrimaryIDColumn}}},
		PartPolicy:        typedcolumn.ColumnPartPolicy{RowsPerGranule: typedcolumn.DefaultRowsPerGranule},
		Compression:       typedcolumn.ColumnCompressionPolicy{Default: typedcolumn.CompressionNone},
	}, typedcolumn.Batch{
		Rows: list.Rows,
		Columns: map[string][]int64{
			typedColumnAdapterPrimaryIDColumn: primaryIDs,
		},
		Uint32OffsetsLists: map[string]typedcolumn.RawUint32OffsetsList{
			adapterColumn.Definition.Name: list,
		},
	})
	if err != nil {
		tb.Fatalf("BuildColumnPart: %v", err)
	}
	logicalTypes := map[string]string{adapterColumn.Definition.Name: columnVectorIndexStateLogicalTypeUint32List}
	image, err := typedcolumn.BuildColumnPartImage(part, typedcolumn.ColumnPartImageOptions{Dictionaries: typedColumnAdapterDictionaries([]typedColumnAdapterColumn{adapterColumn}), LayoutLogicalTypes: logicalTypes})
	if err != nil {
		tb.Fatalf("BuildColumnPartImage: %v", err)
	}
	payload := append([]byte(nil), image.Bytes...)
	parsed, err := typedcolumn.ParseColumnPartImage(payload)
	if err != nil {
		tb.Fatalf("ParseColumnPartImage before mutate: %v", err)
	}
	if mutate != nil {
		mutate(payload, parsed, adapterColumn)
	}
	appender, err := newNextColumnPhysicalAssetSegmentAppender(d.ColumnAssetRootDir(), sourceCfg)
	if err != nil {
		tb.Fatalf("newNextColumnPhysicalAssetSegmentAppender: %v", err)
	}
	alignment := columnAssetSegmentPayloadAlignment(ColumnAssetKindTCS1TypedColumnPart, sourceCfg)
	ref, appendErr := appender.appendKindWithAlignment(payload, ColumnAssetKindTCS1TypedColumnPart, generation, partID, alignment)
	closeErr := appender.close()
	if appendErr != nil {
		tb.Fatalf("append state asset: %v close=%v", appendErr, closeErr)
	}
	if closeErr != nil {
		tb.Fatalf("close state asset appender: %v", closeErr)
	}
	return columnVectorIndexStateAssetSnapshot{
		Role:             columnVectorIndexStateAssetRoleAdjacency,
		AssetID:          columnVectorIndexStateAdjacencyAssetID(layer),
		LogicalType:      columnVectorIndexStateLogicalTypeUint32List,
		PhysicalEncoding: columnVectorIndexStateEncodingRawUint32List,
		RowCount:         list.Rows,
		SourceSchemaHash: sourceCfg.SchemaHash,
		Ref:              ref,
		AssetBytes:       ref.Length,
	}
}
