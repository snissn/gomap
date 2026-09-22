package collections

import (
	"encoding/binary"
	"math"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/mappedresource"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
	"github.com/snissn/gomap/TreeDB/internal/typeddecode"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestColumnGraphRebuildPublishesInvNormState1992(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{3, 4, 0}},
		{id: "doc-b", vector: []float32{0, 2, 0}},
		{id: "doc-c", vector: []float32{0, 0, 5}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 2, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	fixture := loadColumnGraphInvNormStateFixture1992(t, d, col, def)
	if fixture.asset.Role != columnVectorIndexStateAssetRoleInverseNorm || fixture.asset.AssetID != columnVectorGraphInvNormStateAssetID {
		t.Fatalf("asset=%+v want inverse_norm/%s", fixture.asset, columnVectorGraphInvNormStateAssetID)
	}
	if fixture.asset.LogicalType != columnVectorIndexStateLogicalTypeFloat32 || fixture.asset.PhysicalEncoding != columnVectorIndexStateEncodingRawFloat32 {
		t.Fatalf("asset type/encoding=(%q,%q), want raw_float32", fixture.asset.LogicalType, fixture.asset.PhysicalEncoding)
	}
	if fixture.asset.RowCount != fixture.graph.RowCount || fixture.asset.RowCount != len(fixture.scanned) {
		t.Fatalf("asset rows=%d graph=%d scanned=%d", fixture.asset.RowCount, fixture.graph.RowCount, len(fixture.scanned))
	}
	assertColumnAssetReachabilityProtectsGraphRefV2A(t, col, fixture.asset.Ref)
	source := openColumnGraphInvNormStateSourceForFixture1992(t, fixture)
	defer func() { _ = source.Close() }()
	for ordinal, row := range fixture.scanned {
		got, _, _, ok := source.invNormForOrdinal(ordinal)
		if !ok {
			t.Fatalf("source missing ordinal %d", ordinal)
		}
		want, err := columnVectorGraphInvNorm(row.vector)
		if err != nil {
			t.Fatalf("columnVectorGraphInvNorm row %d: %v", ordinal, err)
		}
		if got != want || got != row.invNorm {
			t.Fatalf("ordinal %d inv_norm=%v want computed=%v graph_row=%v", ordinal, got, want, row.invNorm)
		}
	}
}

func TestColumnGraphInvNormStateReopenSearchParityAndCounters1992(t *testing.T) {
	rows := columnGraphRebuildSyntheticRowsV2A(48, 8)
	dir, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 8, 4, rows)
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		_ = d.Close()
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		_ = d.Close()
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

	query := append([]float32(nil), rows[7].vector...)
	stateReader, err := reopenedCol.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
	if err != nil {
		t.Fatalf("open state reader: %v", err)
	}
	defer func() { _ = stateReader.Close() }()
	if !stateReader.usesInvNormStateSource() {
		t.Fatal("reopened reader did not bind inverse-norm state source")
	}
	var stateScratch columnVectorGraphNativeSearchScratch
	stateResults, stateStats, err := stateReader.SearchCosine(query, columnVectorGraphNativeSearchOptions{TopK: 8, EfSearch: 24}, &stateScratch)
	if err != nil {
		t.Fatalf("state SearchCosine: %v", err)
	}
	if len(stateResults) == 0 {
		t.Fatal("state SearchCosine returned no results")
	}
	if stateStats.NormMmapDirectViews+stateStats.NormHeapCopyTypedViews+stateStats.NormScratchDecodes != stateStats.CandidateFetches {
		t.Fatalf("state stats=%+v want one norm state read per candidate", stateStats)
	}
	if stateStats.NormSourceFallbacks != 0 || stateStats.NormSourceUnavailable != 0 {
		t.Fatalf("state stats=%+v want healthy norm state without fallback", stateStats)
	}

	publicResponse, err := reopenedCol.SearchVectorIndex(VectorIndexSearchOptions{IndexName: def.Name, Query: query, TopK: 8, EfSearch: 24, StatsMode: VectorIndexSearchStatsModeBenchmarkDebug})
	if err != nil {
		t.Fatalf("SearchVectorIndex: %v", err)
	}
	if publicResponse.Stats.NormBytesRead == 0 || publicResponse.Stats.NormMmapDirectViews+publicResponse.Stats.NormHeapCopyTypedViews+publicResponse.Stats.NormScratchDecodes == 0 {
		t.Fatalf("public stats=%+v want norm source counters surfaced", publicResponse.Stats)
	}
}

func TestColumnGraphInvNormStateValidationFailures1992(t *testing.T) {
	t.Run("row_count_wrong_type_encoding_and_stale_base", func(t *testing.T) {
		_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 2, []columnGraphRebuildInputRowV2A{
			{id: "doc-a", vector: []float32{1, 0, 0}},
			{id: "doc-b", vector: []float32{0, 2, 0}},
		})
		defer func() { _ = d.Close() }()
		if _, err := col.RebuildVectorIndex(def.Name); err != nil {
			t.Fatalf("RebuildVectorIndex: %v", err)
		}
		fixture := loadColumnGraphInvNormStateFixture1992(t, d, col, def)
		cases := []struct {
			name    string
			mutate  func(testing.TB, *columnVectorIndexStateSnapshot)
			message string
		}{
			{
				name: "row_count",
				mutate: func(tb testing.TB, state *columnVectorIndexStateSnapshot) {
					state.RowCount++
					mutateColumnGraphInvNormStateAsset1992(tb, state, func(asset *columnVectorIndexStateAssetSnapshot) {
						asset.RowCount++
					})
				},
				message: "identity mismatch",
			},
			{
				name: "wrong_logical_type",
				mutate: func(tb testing.TB, state *columnVectorIndexStateSnapshot) {
					mutateColumnGraphInvNormStateAsset1992(tb, state, func(asset *columnVectorIndexStateAssetSnapshot) {
						asset.LogicalType = columnVectorIndexStateLogicalTypeFloat32Vector
					})
				},
				message: "asset contract mismatch",
			},
			{
				name: "wrong_physical_encoding",
				mutate: func(tb testing.TB, state *columnVectorIndexStateSnapshot) {
					mutateColumnGraphInvNormStateAsset1992(tb, state, func(asset *columnVectorIndexStateAssetSnapshot) {
						asset.PhysicalEncoding = columnVectorIndexStateEncodingRawFloat32Vector
					})
				},
				message: "asset contract mismatch",
			},
			{
				name: "stale_base",
				mutate: func(_ testing.TB, state *columnVectorIndexStateSnapshot) {
					state.BaseManifestGeneration++
				},
				message: "identity mismatch",
			},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				state := cloneColumnVectorIndexStateSnapshot1992(fixture.state)
				tc.mutate(t, &state)
				if err := validateColumnVectorGraphInvNormStateAssetIfPresent(d.ColumnAssetRootDir(), "docs", *fixture.cfg, def, fixture.graph, state); err == nil || !strings.Contains(err.Error(), tc.message) {
					t.Fatalf("validate err=%v want %q", err, tc.message)
				}
			})
		}
	})

	t.Run("nan_inf_non_positive_values", func(t *testing.T) {
		cases := []struct {
			name  string
			value float32
		}{
			{name: "nan", value: float32(math.NaN())},
			{name: "inf", value: float32(math.Inf(1))},
			{name: "zero", value: 0},
			{name: "negative", value: -1},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 2, []columnGraphRebuildInputRowV2A{
					{id: "doc-a", vector: []float32{1, 0, 0}},
					{id: "doc-b", vector: []float32{0, 2, 0}},
				})
				defer func() { _ = d.Close() }()
				if _, err := col.RebuildVectorIndex(def.Name); err != nil {
					t.Fatalf("RebuildVectorIndex: %v", err)
				}
				fixture := loadColumnGraphInvNormStateFixture1992(t, d, col, def)
				state := corruptColumnGraphInvNormStateValue1992(t, d, fixture, 1, tc.value)
				if _, _, err := newColumnVectorGraphInvNormStateSourceFromRoot(d.ColumnAssetRootDir(), "docs", *fixture.cfg, def, fixture.graph, state); err == nil || !strings.Contains(err.Error(), "invalid inv_norm state value") {
					t.Fatalf("open err=%v want invalid inv_norm state value", err)
				}
			})
		}
	})

	t.Run("nullable_and_compressed_section", func(t *testing.T) {
		fixture := newColumnGraphInvNormStateViewFixture1992(t, [][]float32{{1, 0, 0}, {0, 2, 0}})
		cases := []struct {
			name    string
			mutate  func(*typedcolumn.ColumnPart, *typedcolumn.ColumnPartImageSection, *typedcolumn.ColumnPartLayoutContractColumn)
			message string
		}{
			{
				name: "wrong_type",
				mutate: func(part *typedcolumn.ColumnPart, _ *typedcolumn.ColumnPartImageSection, _ *typedcolumn.ColumnPartLayoutContractColumn) {
					column := part.Columns[columnVectorGraphInvNormStateColumnName]
					column.Definition.Type = typedcolumn.ColumnTypeInt64
					part.Columns[columnVectorGraphInvNormStateColumnName] = column
				},
				message: "schema mismatch",
			},
			{
				name: "wrong_encoding",
				mutate: func(part *typedcolumn.ColumnPart, _ *typedcolumn.ColumnPartImageSection, _ *typedcolumn.ColumnPartLayoutContractColumn) {
					column := part.Columns[columnVectorGraphInvNormStateColumnName]
					column.Definition.Encoding = typedcolumn.EncodingRawInt64
					part.Columns[columnVectorGraphInvNormStateColumnName] = column
				},
				message: "schema mismatch",
			},
			{
				name: "nullable",
				mutate: func(_ *typedcolumn.ColumnPart, _ *typedcolumn.ColumnPartImageSection, cert *typedcolumn.ColumnPartLayoutContractColumn) {
					cert.NullMaskPresent = true
					cert.NullCount = 1
				},
				message: string(typeddecode.ReasonNullableWrapper),
			},
			{
				name: "compressed",
				mutate: func(_ *typedcolumn.ColumnPart, section *typedcolumn.ColumnPartImageSection, cert *typedcolumn.ColumnPartLayoutContractColumn) {
					section.Compression = typedcolumn.CompressionSnappy
					cert.Compression = typedcolumn.CompressionSnappy
				},
				message: "compression=snappy",
			},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				part := cloneColumnPartForInvNormValidation1992(fixture.part)
				section := fixture.section
				cert := fixture.certColumn
				tc.mutate(part, &section, &cert)
				if err := validateColumnVectorGraphInvNormStateSection(part, section, cert, columnVectorGraphInvNormStateColumnName, fixture.rows); err == nil || !strings.Contains(err.Error(), tc.message) {
					t.Fatalf("validate section err=%v want %q", err, tc.message)
				}
			})
		}
	})
}

func TestColumnGraphInvNormStateMissingFallbackAndCorruptFailClosed1992(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
		{id: "doc-c", vector: []float32{0, 0, 1}},
	}
	t.Run("missing_state_asset_fails_closed_without_graph_rows", func(t *testing.T) {
		_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 2, rows)
		defer func() { _ = d.Close() }()
		if _, err := col.RebuildVectorIndex(def.Name); err != nil {
			t.Fatalf("RebuildVectorIndex: %v", err)
		}
		reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
		if err != nil {
			t.Fatalf("open reader: %v", err)
		}
		defer func() { _ = reader.Close() }()
		if reader.invNormSource != nil {
			if err := reader.invNormSource.Close(); err != nil {
				t.Fatalf("close inv_norm source: %v", err)
			}
			reader.invNormSource = nil
		}
		reader.preparedSearch = nil
		reader.invNormStateUnavailable = true
		var scratch columnVectorGraphNativeSearchScratch
		_, _, err = reader.SearchCosine([]float32{1, 0, 0}, columnVectorGraphNativeSearchOptions{TopK: 2, EfSearch: 3}, &scratch)
		if err == nil || !strings.Contains(err.Error(), "inverse-norm state") {
			t.Fatalf("SearchCosine err=%v want fail-closed missing inverse-norm state", err)
		}
	})

	t.Run("legacy_physical_asset_norm_fallback_counts", func(t *testing.T) {
		legacyRows := []columnVectorGraphAssetRow{
			{ID: []byte("doc-a"), Vector: []float32{1, 0, 0}, InvNorm: 1, Adjacency: []uint32{1, 2}},
			{ID: []byte("doc-b"), Vector: []float32{0, 1, 0}, InvNorm: 1, Adjacency: []uint32{0, 2}},
			{ID: []byte("doc-c"), Vector: []float32{0, 0, 1}, InvNorm: 1, Adjacency: []uint32{0, 1}},
		}
		d, col, def := publishColumnVectorGraphPhysicalReaderTestAssetV2B(t, legacyRows)
		defer func() { _ = d.Close() }()
		reader, err := col.openColumnVectorGraphPhysicalRowReader(def.Name, columnVectorGraphPhysicalRowReaderOptions{MaxDecodedBlocks: 1})
		if err != nil {
			t.Fatalf("open legacy reader: %v", err)
		}
		defer func() { _ = reader.Close() }()
		if reader.invNormSource != nil {
			if err := reader.invNormSource.Close(); err != nil {
				t.Fatalf("close inv_norm source: %v", err)
			}
			reader.invNormSource = nil
		}
		reader.invNormStateUnavailable = true
		var scratch columnVectorGraphNativeSearchScratch
		got, stats, err := reader.SearchCosine([]float32{1, 0, 0}, columnVectorGraphNativeSearchOptions{TopK: 2, EfSearch: 3}, &scratch)
		if err != nil {
			t.Fatalf("legacy SearchCosine fallback: %v", err)
		}
		if len(got) == 0 || stats.NormSourceUnavailable != 1 || stats.NormSourceFallbacks != 1 || stats.NormMmapDirectViews+stats.NormHeapCopyTypedViews+stats.NormScratchDecodes != 0 {
			t.Fatalf("results=%d stats=%+v want explicit legacy graph-row norm fallback", len(got), stats)
		}
	})

	t.Run("corrupt_state_asset_fails_closed", func(t *testing.T) {
		_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 2, rows)
		defer func() { _ = d.Close() }()
		if _, err := col.RebuildVectorIndex(def.Name); err != nil {
			t.Fatalf("RebuildVectorIndex: %v", err)
		}
		fixture := loadColumnGraphInvNormStateFixture1992(t, d, col, def)
		state := cloneColumnVectorIndexStateSnapshot1992(fixture.state)
		mutateColumnGraphInvNormStateAsset1992(t, &state, func(asset *columnVectorIndexStateAssetSnapshot) {
			asset.Ref.Checksum++
		})
		if err := validateColumnVectorGraphInvNormStateAssetIfPresent(d.ColumnAssetRootDir(), "docs", *fixture.cfg, def, fixture.graph, state); err == nil || !strings.Contains(err.Error(), "checksum") {
			t.Fatalf("validate err=%v want checksum fail-closed", err)
		}
	})
}

func TestColumnGraphInvNormStateCounterPaths1992(t *testing.T) {
	fixture := newColumnGraphInvNormStateViewFixture1992(t, [][]float32{{1, 0, 0}, {0, 2, 0}})

	t.Run("heap_copy_typed_view", func(t *testing.T) {
		manager := mappedresource.NewManager()
		handle := acquireColumnGraphInvNormStateBytesHandle1992(t, manager, fixture, mappedresource.SourceHeapCopy, append([]byte(nil), fixture.sectionBytes...))
		values, retained, outcome, reason, err := columnVectorGraphInvNormStateValuesFromHandle(manager, handle, fixture.directReq, fixture.rows)
		if err != nil {
			t.Fatalf("values from heap handle: %v", err)
		}
		defer func() { _ = retained.Release() }()
		if retained != handle || outcome != columnVectorGraphInvNormStateOutcomeHeapCopyTypedView || reason != "" {
			t.Fatalf("retained=%v outcome=%s reason=%s want heap-copy typed view", retained != nil, outcome, reason)
		}
		if !float32SlicesEqual1782(values, []float32{1, 0.5}) {
			t.Fatalf("values=%v want [1 0.5]", values)
		}
		var stats columnVectorGraphNativeSearchStats
		recordColumnVectorGraphInvNormSourceStats(&stats, outcome, reason)
		if stats.NormHeapCopyTypedViews != 1 || stats.NormMmapDirectViews != 0 || stats.NormScratchDecodes != 0 {
			t.Fatalf("stats=%+v want heap-copy norm counter", stats)
		}
	})

	t.Run("scratch_decode_fallback", func(t *testing.T) {
		manager := mappedresource.NewManager()
		misaligned := append([]byte{0}, fixture.sectionBytes...)
		handle := acquireColumnGraphInvNormStateBytesHandle1992(t, manager, fixture, mappedresource.SourceMapped, misaligned[1:])
		values, retained, outcome, reason, err := columnVectorGraphInvNormStateValuesFromHandle(manager, handle, fixture.directReq, fixture.rows)
		if err != nil {
			t.Fatalf("values from misaligned handle: %v", err)
		}
		if retained != nil || outcome != columnVectorGraphInvNormStateOutcomeScratchDecode || reason != typeddecode.ReasonActualPointerUnaligned {
			t.Fatalf("retained=%v outcome=%s reason=%s want scratch decode from actual pointer unaligned", retained != nil, outcome, reason)
		}
		if !float32SlicesEqual1782(values, []float32{1, 0.5}) {
			t.Fatalf("values=%v want [1 0.5]", values)
		}
		var stats columnVectorGraphNativeSearchStats
		recordColumnVectorGraphInvNormSourceStats(&stats, outcome, reason)
		if stats.NormScratchDecodes != 1 || stats.NormActualPointerUnaligned != 1 || stats.NormMmapDirectViews != 0 || stats.NormHeapCopyTypedViews != 0 {
			t.Fatalf("stats=%+v want scratch norm fallback counters", stats)
		}
	})

	t.Run("validation_failure_counter", func(t *testing.T) {
		var stats columnVectorGraphNativeSearchStats
		recordColumnVectorGraphInvNormFallbackReasonStats(&stats, typeddecode.ReasonValidationFailed)
		if stats.NormValidationFailures != 1 {
			t.Fatalf("stats=%+v want validation failure counter", stats)
		}
	})

	t.Run("stale_handle_falls_back", func(t *testing.T) {
		source := &columnVectorGraphInvNormStateSource{rows: 1, values: []float32{1}, outcome: columnVectorGraphInvNormStateOutcomeMmapDirect, handle: nil}
		reader := &columnVectorGraphPhysicalRowReader{invNormSource: source}
		if got, _, _, ok := reader.invNormForOrdinal(0); !ok || got != 1 {
			t.Fatalf("fresh invNormForOrdinal got=%v ok=%v", got, ok)
		}
		source.closed = true
		_, _, reason, ok := reader.invNormForOrdinal(0)
		if ok || reason != typeddecode.ReasonStaleHandle {
			t.Fatalf("stale reason=%s ok=%v want stale handle", reason, ok)
		}
		var stats columnVectorGraphNativeSearchStats
		recordColumnVectorGraphInvNormFallbackReasonStats(&stats, reason)
		if stats.NormStaleHandles != 1 {
			t.Fatalf("stats=%+v want stale norm counter", stats)
		}
	})
}

type columnGraphInvNormStateFixture1992 struct {
	d       *backenddb.DB
	col     *Collection
	def     VectorIndexDefinition
	records []columnManifestRecord
	cfg     *ColumnStoreConfig
	graph   columnVectorGraphManifestSnapshot
	scanned []columnGraphRebuildScannedRowV2A
	state   columnVectorIndexStateSnapshot
	asset   columnVectorIndexStateAssetSnapshot
}

func loadColumnGraphInvNormStateFixture1992(tb testing.TB, d *backenddb.DB, col *Collection, def VectorIndexDefinition) columnGraphInvNormStateFixture1992 {
	tb.Helper()
	graph, scanned := loadAndScanColumnGraphRebuildRowsV2A(tb, d, "docs", def)
	records, cfg := loadColumnGraphRebuildManifestRecordsAndConfigV2A(tb, d, "docs")
	stateRecord, ok := findColumnVectorIndexStateRecord(records, def.Name)
	if !ok {
		tb.Fatalf("state record %q missing", def.Name)
	}
	state, err := decodeColumnVectorIndexStateRecord(stateRecord.value)
	if err != nil {
		tb.Fatalf("decode state record: %v", err)
	}
	asset, ok := findColumnVectorGraphInvNormStateAsset(state)
	if !ok {
		tb.Fatalf("inv_norm state asset missing from %+v", state.Assets)
	}
	return columnGraphInvNormStateFixture1992{d: d, col: col, def: def, records: records, cfg: cfg, graph: graph, scanned: scanned, state: state, asset: asset}
}

func openColumnGraphInvNormStateSourceForFixture1992(tb testing.TB, fixture columnGraphInvNormStateFixture1992) *columnVectorGraphInvNormStateSource {
	tb.Helper()
	source, _, err := newColumnVectorGraphInvNormStateSourceFromRoot(fixture.d.ColumnAssetRootDir(), "docs", *fixture.cfg, fixture.def, fixture.graph, fixture.state)
	if err != nil {
		tb.Fatalf("newColumnVectorGraphInvNormStateSourceFromRoot: %v", err)
	}
	if source == nil {
		tb.Fatal("source=nil")
	}
	return source
}

func corruptColumnGraphInvNormStateValue1992(tb testing.TB, d *backenddb.DB, fixture columnGraphInvNormStateFixture1992, ordinal int, value float32) columnVectorIndexStateSnapshot {
	tb.Helper()
	raw, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), fixture.asset.Ref)
	if err != nil {
		tb.Fatalf("read inv_norm state asset: %v", err)
	}
	image, err := typedcolumn.ParseColumnPartImage(raw)
	if err != nil {
		tb.Fatalf("ParseColumnPartImage: %v", err)
	}
	sourceCfg, adapterColumn, err := columnVectorGraphInvNormStateColumnStoreConfig("docs", *fixture.cfg, fixture.def)
	if err != nil {
		tb.Fatalf("columnVectorGraphInvNormStateColumnStoreConfig: %v", err)
	}
	fields := columnStoreTypedColumnPartFields(sourceCfg)
	adapterPart, err := typedColumnAdapterPartFromImage(typedColumnAdapterOptions{Fields: fields, SchemaVersion: uint32(sourceCfg.SchemaHash)}, image)
	if err != nil {
		tb.Fatalf("typedColumnAdapterPartFromImage: %v", err)
	}
	if ordinal < 0 || ordinal >= fixture.asset.RowCount {
		tb.Fatalf("ordinal=%d rows=%d", ordinal, fixture.asset.RowCount)
	}
	column := adapterPart.Part.Columns[adapterColumn.Definition.Name]
	patched := false
	for i := range column.Blocks {
		block := column.Blocks[i]
		first := block.Descriptor.FirstRow
		last := first + block.Descriptor.RowCount
		if ordinal < first || ordinal >= last {
			continue
		}
		rel := (ordinal - first) * 4
		payload := append([]byte(nil), block.Granule.Payload...)
		if rel < 0 || rel+4 > len(payload) {
			tb.Fatalf("block payload offset=%d len=%d", rel, len(payload))
		}
		binary.LittleEndian.PutUint32(payload[rel:rel+4], math.Float32bits(value))
		block.Granule.Payload = payload
		column.Blocks[i] = block
		patched = true
		break
	}
	if !patched {
		tb.Fatalf("ordinal=%d not found in inv_norm state blocks", ordinal)
	}
	adapterPart.Part.Columns[adapterColumn.Definition.Name] = column
	newImage, err := typedcolumn.BuildColumnPartImage(adapterPart.Part, typedcolumn.ColumnPartImageOptions{
		Dictionaries: typedColumnAdapterDictionaries([]typedColumnAdapterColumn{adapterColumn}),
		LayoutLogicalTypes: map[string]string{
			typedColumnAdapterPrimaryIDColumn: string(typedcolumn.ColumnTypeInt64),
			adapterColumn.Definition.Name:     string(ColumnStoreValueFloat32),
		},
	})
	if err != nil {
		tb.Fatalf("BuildColumnPartImage: %v", err)
	}
	appender, err := newNextColumnPhysicalAssetSegmentAppender(d.ColumnAssetRootDir(), sourceCfg)
	if err != nil {
		tb.Fatalf("new appender: %v", err)
	}
	alignment := columnAssetSegmentPayloadAlignment(ColumnAssetKindTCS1TypedColumnPart, sourceCfg)
	ref, appendErr := appender.appendKindWithAlignment(newImage.Bytes, ColumnAssetKindTCS1TypedColumnPart, fixture.graph.BaseManifestGeneration, fixture.asset.Ref.PartID, alignment)
	closeErr := appender.close()
	if appendErr != nil {
		tb.Fatalf("append corrupt inv_norm state asset: %v", appendErr)
	}
	if closeErr != nil {
		tb.Fatalf("close appender: %v", closeErr)
	}
	state := cloneColumnVectorIndexStateSnapshot1992(fixture.state)
	mutateColumnGraphInvNormStateAsset1992(tb, &state, func(asset *columnVectorIndexStateAssetSnapshot) {
		asset.Ref = ref
		asset.AssetBytes = ref.Length
	})
	return state
}

type columnGraphInvNormStateViewFixture1992 struct {
	rows         int
	part         *typedcolumn.ColumnPart
	image        typedcolumn.ColumnPartImage
	section      typedcolumn.ColumnPartImageSection
	certColumn   typedcolumn.ColumnPartLayoutContractColumn
	sectionBytes []byte
	directReq    typeddecode.DirectViewColumnRequest
}

func newColumnGraphInvNormStateViewFixture1992(tb testing.TB, vectors [][]float32) columnGraphInvNormStateViewFixture1992 {
	tb.Helper()
	if len(vectors) == 0 {
		tb.Fatal("vectors required")
	}
	dims := len(vectors[0])
	baseCfg, err := normalizeColumnStoreConfig("docs", columnGraphRebuildColumnStoreConfigV2A(dims))
	if err != nil {
		tb.Fatalf("normalize base cfg: %v", err)
	}
	def := columnGraphRebuildVectorIndexDefinitionV2A(dims, 2)
	rows := make([]columnVectorGraphAssetRow, len(vectors))
	for i, vector := range vectors {
		rows[i] = columnVectorGraphAssetRow{ID: []byte{byte('a' + i)}, Vector: vector}
	}
	payload, err := prepareColumnVectorGraphInvNormStatePayload("docs", *baseCfg, def, 9, rows)
	if err != nil {
		tb.Fatalf("prepare inv_norm payload: %v", err)
	}
	image, err := typedcolumn.ParseColumnPartImage(payload.Payload)
	if err != nil {
		tb.Fatalf("ParseColumnPartImage: %v", err)
	}
	sourceCfg, adapterColumn, err := columnVectorGraphInvNormStateColumnStoreConfig("docs", *baseCfg, def)
	if err != nil {
		tb.Fatalf("columnVectorGraphInvNormStateColumnStoreConfig: %v", err)
	}
	fields := columnStoreTypedColumnPartFields(sourceCfg)
	adapterPart, err := typedColumnAdapterPartFromImageWithoutRowLocators(typedColumnAdapterOptions{Fields: fields, SchemaVersion: uint32(sourceCfg.SchemaHash)}, image)
	if err != nil {
		tb.Fatalf("typedColumnAdapterPartFromImageWithoutRowLocators: %v", err)
	}
	section, err := columnVectorGraphInvNormStateSection(image, adapterColumn.Definition.Name)
	if err != nil {
		tb.Fatalf("section: %v", err)
	}
	sectionBytes, err := image.SectionBytes(section)
	if err != nil {
		tb.Fatalf("SectionBytes: %v", err)
	}
	certification, err := typedcolumn.CertifyColumnPartLayoutContractFromImage(image)
	if err != nil {
		tb.Fatalf("CertifyColumnPartLayoutContractFromImage: %v", err)
	}
	certColumn, ok := certification.Column(adapterColumn.Definition.Name)
	if !ok {
		tb.Fatalf("missing certification for %q", adapterColumn.Definition.Name)
	}
	plan := typeddecode.Float32ScalarPlan(certColumn)
	directReq := typeddecode.DirectViewColumnRequest{Plan: plan, Certification: certColumn, Rows: image.Rows, PayloadBytes: section.Length, AssetOffset: 0, HasAssetOffset: true}
	if status := typeddecode.ValidateDirectViewColumn(directReq); !status.Direct() {
		tb.Fatalf("ValidateDirectViewColumn: %s", status.String())
	}
	return columnGraphInvNormStateViewFixture1992{rows: image.Rows, part: adapterPart.Part, image: image, section: section, certColumn: certColumn, sectionBytes: sectionBytes, directReq: directReq}
}

func acquireColumnGraphInvNormStateBytesHandle1992(tb testing.TB, manager *mappedresource.Manager, fixture columnGraphInvNormStateViewFixture1992, source mappedresource.Source, data []byte) *mappedresource.Handle {
	tb.Helper()
	key := mappedresource.Key{
		Class:      mappedresource.ClassTypedColumnAsset,
		Namespace:  "test",
		Kind:       string(ColumnAssetKindTCS1TypedColumnPart),
		Generation: 1,
		PartID:     9,
		FileID:     1,
		Offset:     0,
		Length:     int64(len(data)),
		Checksum:   uint64(page.Checksum(data)),
		Version:    fixture.image.Version,
		Encoding:   fixture.section.Encoding.String(),
		Section: mappedresource.Section{
			Kind:     string(fixture.section.Kind),
			Category: string(fixture.section.Category),
			Column:   fixture.section.Column,
		},
	}
	scope := mappedresource.Scope{Kind: mappedresource.ScopeColumnPartReader, ID: columnVectorGraphInvNormStateScopeID, Collection: "docs", Namespace: "test", Generation: 1, Reason: "column_graph inv_norm state test"}
	handle, err := manager.AcquireBytes(key, scope, source, data, mappedresource.AcquireOptions{Reason: "column_graph inv_norm state test", ValidationMode: mappedresource.ValidationVerify})
	if err != nil {
		tb.Fatalf("AcquireBytes: %v", err)
	}
	return handle
}

func cloneColumnVectorIndexStateSnapshot1992(state columnVectorIndexStateSnapshot) columnVectorIndexStateSnapshot {
	state.Assets = append([]columnVectorIndexStateAssetSnapshot(nil), state.Assets...)
	return state
}

func mutateColumnGraphInvNormStateAsset1992(tb testing.TB, state *columnVectorIndexStateSnapshot, mutate func(*columnVectorIndexStateAssetSnapshot)) {
	tb.Helper()
	if state == nil {
		tb.Fatal("nil vector-index state snapshot")
	}
	for i := range state.Assets {
		asset := &state.Assets[i]
		if asset.Role == columnVectorIndexStateAssetRoleInverseNorm && asset.AssetID == columnVectorGraphInvNormStateAssetID {
			mutate(asset)
			return
		}
	}
	tb.Fatalf("inv_norm state asset missing from %+v", state.Assets)
}

func cloneColumnPartForInvNormValidation1992(part *typedcolumn.ColumnPart) *typedcolumn.ColumnPart {
	clone := *part
	clone.Columns = make(map[string]typedcolumn.ColumnPartColumn, len(part.Columns))
	for name, column := range part.Columns {
		clone.Columns[name] = column
	}
	return &clone
}

func assertColumnGraphSearchResultsEqual1992(tb testing.TB, left, right []columnVectorGraphNativeSearchResult) {
	tb.Helper()
	if len(left) != len(right) {
		tb.Fatalf("results len=%d want %d\nleft=%+v\nright=%+v", len(right), len(left), left, right)
	}
	for i := range left {
		if left[i].Ordinal != right[i].Ordinal || string(left[i].ID) != string(right[i].ID) || math.Abs(left[i].Score-right[i].Score) > 1e-6 {
			tb.Fatalf("result[%d]=%+v want %+v\nleft=%+v\nright=%+v", i, right[i], left[i], left, right)
		}
	}
}
