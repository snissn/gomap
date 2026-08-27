package collections

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/internal/typedcolumn"
)

var columnGraphRebuildBenchSinkV2A VectorIndexStatus

func TestColumnGraphRebuildPinsIndexNamespaceUntilPublication4259(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online index vacuum is not supported on Windows")
	}
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
		{id: "doc-c", vector: []float32{0, 0, 1}},
		{id: "doc-d", vector: []float32{0.5, 0.5, 0}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 2, rows)
	defer func() { _ = d.Close() }()

	beforeBuild := make(chan struct{})
	resumeBuild := make(chan struct{})
	restore := setColumnVectorGraphRebuildBeforeBuildTestHook(func() {
		close(beforeBuild)
		<-resumeBuild
	})
	defer restore()

	type rebuildResult struct {
		status VectorIndexStatus
		err    error
	}
	rebuildDone := make(chan rebuildResult, 1)
	go func() {
		status, err := col.RebuildVectorIndex(def.Name)
		rebuildDone <- rebuildResult{status: status, err: err}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	select {
	case <-beforeBuild:
	case <-ctx.Done():
		close(resumeBuild)
		t.Fatalf("waiting for paused vector rebuild: %v", ctx.Err())
	}
	vacuumErr := d.VacuumIndexOnline(ctx)
	close(resumeBuild)
	var result rebuildResult
	select {
	case result = <-rebuildDone:
	case <-ctx.Done():
		t.Fatalf("waiting for vector rebuild after release: %v", ctx.Err())
	}
	if !errors.Is(vacuumErr, rootpublication.ErrResourcePinned) {
		t.Fatalf("VacuumIndexOnline during paused rebuild=%v, want %v (rebuild err=%v)", vacuumErr, rootpublication.ErrResourcePinned, result.err)
	}
	if result.err != nil {
		t.Fatalf("RebuildVectorIndex after rejected vacuum: %v", result.err)
	}
	assertColumnGraphRebuildLoadedStatusV2A(t, result.status, def.Name)
	if err := d.VacuumIndexOnline(ctx); err != nil {
		t.Fatalf("VacuumIndexOnline after rebuild publication: %v", err)
	}
}

func TestColumnGraphRebuildVectorIndexUsesTypedColumnRows4254(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-z", vector: []float32{1, 0}},
		{id: "doc-a", vector: []float32{0, 1}},
		{id: "doc-m", vector: []float32{0.5, 0.5}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 2, 1, nil)
	defer func() { _ = d.Close() }()
	for _, row := range rows {
		insertColumnGraphRebuildRowsV2A(t, col, []columnGraphRebuildInputRowV2A{row})
	}
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	catalog, err := col.catalogForSnapshot(snap)
	if err != nil {
		_ = snap.Close()
		t.Fatalf("catalogForSnapshot: %v", err)
	}
	cfg := *catalog.meta.Options.ColumnStore
	records, err := loadColumnManifestRecordsFromRoot(snap, catalog.rootID(collectionColumnManifestRootName(catalog.meta.Name)))
	if err != nil {
		_ = snap.Close()
		t.Fatalf("loadColumnManifestRecordsFromRoot: %v", err)
	}
	manifest, err := decodeColumnManifestSnapshotForScan(records)
	if err != nil {
		_ = snap.Close()
		t.Fatalf("decodeColumnManifestSnapshotForScan: %v", err)
	}
	typedRows, usedTypedColumns, err := col.columnVectorGraphRowsFromTypedColumnCatalogSnapshot(snap, catalog, cfg, records, manifest, def)
	if err != nil || !usedTypedColumns {
		_ = snap.Close()
		t.Fatalf("typed rebuild rows used=%v err=%v", usedTypedColumns, err)
	}
	canonicalRows, err := col.columnVectorGraphRowsFromCatalogSnapshot(snap, catalog, def)
	if err == nil {
		err = col.assignColumnVectorGraphRowRefsFromBaseManifest(catalog.meta.Name, cfg, records, manifest.Generation, canonicalRows)
	}
	if closeErr := snap.Close(); closeErr != nil {
		t.Fatalf("snapshot close: %v", closeErr)
	}
	if err != nil {
		t.Fatalf("canonical row refs: %v", err)
	}
	if len(typedRows) != len(canonicalRows) {
		t.Fatalf("typed rows=%d canonical rows=%d", len(typedRows), len(canonicalRows))
	}
	for i := range typedRows {
		typedRef, canonicalRef := typedRows[i].BaseRowRef, canonicalRows[i].BaseRowRef
		if string(typedRows[i].ID) != string(canonicalRows[i].ID) || !slices.Equal(typedRows[i].Vector, canonicalRows[i].Vector) || typedRows[i].InvNorm != canonicalRows[i].InvNorm || !slices.Equal(typedRef.DocumentID, canonicalRef.DocumentID) || typedRef.Generation != canonicalRef.Generation || typedRef.PartID != canonicalRef.PartID || typedRef.RowIndex != canonicalRef.RowIndex || typedRef.AppliedCommandLSN != canonicalRef.AppliedCommandLSN {
			t.Fatalf("row[%d] typed=%+v canonical=%+v want exact source/row-ref parity", i, typedRows[i], canonicalRows[i])
		}
	}
	called := false
	restore := setColumnVectorGraphCanonicalRowsTestHook(func() { called = true })
	defer restore()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	if called {
		t.Fatal("RebuildVectorIndex selected canonical JSON extraction despite certified typed columns")
	}
}

func assertColumnGraphNoLegacyAdjacencySources1989(tb testing.TB, graph columnVectorGraphManifestSnapshot) {
	tb.Helper()
	if columnVectorGraphManifestHasPhysicalAsset(graph) {
		tb.Fatalf("graph manifest has physical row asset %+v; new graph builds must use TVIS/base typed-column state", graph.AssetRef)
	}
	if graph.Layer0AdjacencySource.Present || graph.AdjacencyLayerCount != 0 || len(graph.AdjacencyLayerSources) != 0 {
		tb.Fatalf("graph manifest has legacy adjacency sources layer0=%+v count=%d sources=%d; new graph builds must use vector-index state uint32_list assets", graph.Layer0AdjacencySource, graph.AdjacencyLayerCount, len(graph.AdjacencyLayerSources))
	}
}

func TestColumnGraphRebuildVectorIndexPublishesPhysicalManifestV2A(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0.9, 0.1, 0}},
		{id: "doc-c", vector: []float32{0, 1, 0}},
		{id: "doc-d", vector: []float32{0, 0, 1}},
	}
	dir, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 0, rows)
	defer func() { _ = d.Close() }()

	status, err := col.VectorIndexStatus(def.Name)
	if err != nil {
		t.Fatalf("VectorIndexStatus before rebuild: %v", err)
	}
	if status.State != VectorIndexStateColumnGraphRebuildNeeded || !status.RebuildNeeded || status.Loaded {
		t.Fatalf("status before rebuild=%+v, want rebuild-needed", status)
	}

	baseLSN := d.State().AppliedCommandLSN
	status, err = col.RebuildVectorIndex(def.Name)
	if err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	assertColumnGraphRebuildLoadedStatusV2A(t, status, def.Name)
	rebuildLSN := d.State().AppliedCommandLSN
	if rebuildLSN <= baseLSN {
		t.Fatalf("rebuild AppliedCommandLSN=%d want > base %d", rebuildLSN, baseLSN)
	}
	frames := collectionCommandWALFrames(t, dir)
	if len(frames) == 0 || frames[len(frames)-1].Kind != commitlog.CommandKindCollectionRebuildVectorIndex {
		t.Fatalf("last command WAL frame=%+v, want vector-index rebuild", frames)
	}

	graph, scanned := loadAndScanColumnGraphRebuildRowsV2A(t, d, "docs", def)
	if graph.RowCount != len(rows) {
		t.Fatalf("graph row count=%d want %d", graph.RowCount, len(rows))
	}
	records, cfg := loadColumnGraphRebuildManifestRecordsAndConfigV2A(t, d, "docs")
	manifest, err := decodeColumnManifestSnapshotForScan(records)
	if err != nil {
		t.Fatalf("decodeColumnManifestSnapshotForScan: %v", err)
	}
	if manifest.AppliedCommandLSN != rebuildLSN {
		t.Fatalf("manifest AppliedCommandLSN=%d want rebuild LSN %d", manifest.AppliedCommandLSN, rebuildLSN)
	}
	if cfg.RecoveryAuthoritativeAppliedCommandLSN != rebuildLSN {
		t.Fatalf("recovery AppliedCommandLSN=%d want rebuild LSN %d", cfg.RecoveryAuthoritativeAppliedCommandLSN, rebuildLSN)
	}
	assertColumnAssetReachabilityProtectsGraphRefV2A(t, col, graph.AssetRef)
	if len(scanned) != len(rows) {
		t.Fatalf("scanned graph rows=%d want %d", len(scanned), len(rows))
	}
	wantGraph := columnGraphRebuildNativeGraphLayoutV2A(t, def, rows)
	if got := strings.Join(columnGraphRebuildScannedIDsV2A(scanned), ","); got != strings.Join(wantGraph.ids, ",") {
		t.Fatalf("scanned graph ids=%s, want native locality order %s", got, strings.Join(wantGraph.ids, ","))
	}
	for i := range wantGraph.ids {
		if got := scanned[i].adjacency; !uint32SlicesEqual(got, wantGraph.adjacency[i]) {
			t.Fatalf("%s adjacency=%v, want native layered adjacency %v", wantGraph.ids[i], got, wantGraph.adjacency[i])
		}
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
	status, err = reopenedCol.VectorIndexStatus(def.Name)
	if err != nil {
		t.Fatalf("VectorIndexStatus reopen: %v", err)
	}
	assertColumnGraphRebuildLoadedStatusV2A(t, status, def.Name)
	_, scanned = loadAndScanColumnGraphRebuildRowsV2A(t, reopened, "docs", def)
	if len(scanned) != len(rows) {
		t.Fatalf("reopened graph rows=%d want %d", len(scanned), len(rows))
	}
}

func TestColumnGraphRebuildVectorIndexPublishesEmptyPhysicalManifestV2A(t *testing.T) {
	dir, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 0, nil)
	defer func() { _ = d.Close() }()

	status, err := col.RebuildVectorIndex(def.Name)
	if err != nil {
		t.Fatalf("RebuildVectorIndex empty collection: %v", err)
	}
	assertColumnGraphRebuildLoadedStatusV2A(t, status, def.Name)
	timing := status.ColumnGraphBuild
	if timing.Total == 0 || timing.Snapshot == 0 || timing.RowExtraction == 0 || timing.AssetPreparation == 0 || timing.ManifestFinalization == 0 || timing.Publication == 0 {
		t.Fatalf("empty rebuild timing=%+v want completed stages", timing)
	}
	if timing.AdjacencyBuild != 0 || timing.LocalityRemap != 0 || timing.Total < timing.Snapshot+timing.RowExtraction+timing.Publication || timing.Publication < timing.AssetPreparation {
		t.Fatalf("empty rebuild timing=%+v has invalid reconciliation", timing)
	}
	status, err = col.RebuildVectorIndex(def.Name)
	if err != nil {
		t.Fatalf("second RebuildVectorIndex empty collection: %v", err)
	}
	assertColumnGraphRebuildLoadedStatusV2A(t, status, def.Name)
	closure, err := col.PrepareVectorIndexStableClosure(def.Name)
	if err != nil {
		t.Fatalf("PrepareVectorIndexStableClosure empty collection: %v", err)
	}
	closure.Release()
	frames := collectionCommandWALFrames(t, dir)
	if len(frames) == 0 || frames[len(frames)-1].Kind != commitlog.CommandKindCollectionRebuildVectorIndex {
		t.Fatalf("last command WAL frame=%+v, want vector-index rebuild", frames)
	}

	graph, scanned := loadAndScanColumnGraphRebuildRowsV2A(t, d, "docs", def)
	if graph.RowCount != 0 {
		t.Fatalf("empty graph row count=%d want 0", graph.RowCount)
	}
	if len(scanned) != 0 {
		t.Fatalf("empty graph scanned rows=%d want 0", len(scanned))
	}
	records, cfg := loadColumnGraphRebuildManifestRecordsAndConfigV2A(t, d, "docs")
	state := columnVectorIndexStateFromRecords1987(t, records, def)
	list := loadColumnVectorIndexStateAdjacencyList1987(t, d, "docs", cfg, def, state, 0)
	if list.Rows != 0 || len(list.Offsets) != 1 || list.Offsets[0] != 0 || len(list.Values) != 0 {
		t.Fatalf("empty state layer-0 list=%+v want rows=0 offsets=[0] values=[]", list)
	}
	assertColumnGraphNoLegacyAdjacencySources1989(t, graph)
	assertColumnAssetReachabilityProtectsGraphRefV2A(t, col, graph.AssetRef)
	for _, asset := range columnVectorIndexStateAdjacencyAssetsByLayer1987(t, state) {
		assertColumnAssetReachabilityProtectsGraphRefV2A(t, col, asset.Ref)
	}
}

func TestColumnGraphRebuildPublishesUint32ListAdjacencyStateNotLegacySource1989(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0.95, 0.05, 0}},
		{id: "doc-c", vector: []float32{0, 1, 0}},
		{id: "doc-d", vector: []float32{0, 0.1, 0.9}},
		{id: "doc-e", vector: []float32{0.1, 0, 0.9}},
	}
	dir, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 2, rows)
	defer func() { _ = d.Close() }()
	status, err := col.RebuildVectorIndex(def.Name)
	if err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	assertColumnGraphRebuildLoadedStatusV2A(t, status, def.Name)

	graph, scanned := loadAndScanColumnGraphRebuildRowsV2A(t, d, "docs", def)
	assertColumnGraphNoLegacyAdjacencySources1989(t, graph)
	records, cfg := loadColumnGraphRebuildManifestRecordsAndConfigV2A(t, d, "docs")
	state := columnVectorIndexStateFromRecords1987(t, records, def)
	if status.Stats.BytesDisk != columnVectorGraphStorageBytesWithState(graph, state) {
		t.Fatalf("status bytes_disk=%d want graph+state=%d", status.Stats.BytesDisk, columnVectorGraphStorageBytesWithState(graph, state))
	}
	assertColumnVectorIndexStateAdjacencyAssetsMatchScanned1987(t, d, "docs", cfg, def, graph, state, scanned)
	assertColumnAssetReachabilityProtectsGraphRefV2A(t, col, graph.AssetRef)

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
	reopenedStatus, err := reopenedCol.VectorIndexStatus(def.Name)
	if err != nil {
		t.Fatalf("VectorIndexStatus reopen: %v", err)
	}
	assertColumnGraphRebuildLoadedStatusV2A(t, reopenedStatus, def.Name)
	reopenedGraph, reopenedRows := loadAndScanColumnGraphRebuildRowsV2A(t, reopened, "docs", def)
	assertColumnGraphNoLegacyAdjacencySources1989(t, reopenedGraph)
	reopenedRecords, reopenedCfg := loadColumnGraphRebuildManifestRecordsAndConfigV2A(t, reopened, "docs")
	reopenedState := columnVectorIndexStateFromRecords1987(t, reopenedRecords, def)
	assertColumnVectorIndexStateAdjacencyAssetsMatchScanned1987(t, reopened, "docs", reopenedCfg, def, reopenedGraph, reopenedState, reopenedRows)
}

func TestColumnGraphRebuildPublishesMultiLayerUint32ListAdjacencyState1989(t *testing.T) {
	rows := columnGraphRebuildSyntheticRowsV2A(96, 3)
	dir, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 2, rows)
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		_ = d.Close()
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	graph, scanned := loadAndScanColumnGraphRebuildRowsV2A(t, d, "docs", def)
	assertColumnGraphNoLegacyAdjacencySources1989(t, graph)
	records, cfg := loadColumnGraphRebuildManifestRecordsAndConfigV2A(t, d, "docs")
	state := columnVectorIndexStateFromRecords1987(t, records, def)
	assertColumnVectorIndexStateAdjacencyAssetsMatchScanned1987(t, d, "docs", cfg, def, graph, state, scanned)
	if err := d.Checkpoint(); err != nil {
		_ = d.Close()
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	reopened := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopened.Close() }()
	reopenedGraph, reopenedRows := loadAndScanColumnGraphRebuildRowsV2A(t, reopened, "docs", def)
	assertColumnGraphNoLegacyAdjacencySources1989(t, reopenedGraph)
	reopenedRecords, reopenedCfg := loadColumnGraphRebuildManifestRecordsAndConfigV2A(t, reopened, "docs")
	reopenedState := columnVectorIndexStateFromRecords1987(t, reopenedRecords, def)
	assertColumnVectorIndexStateAdjacencyAssetsMatchScanned1987(t, reopened, "docs", reopenedCfg, def, reopenedGraph, reopenedState, reopenedRows)
}

func TestColumnGraphRebuildPublishesEmptyNeighborUint32ListState1989(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{{id: "solo", vector: []float32{1, 0, 0}}}
	_, d, _, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 2, rows)
	defer func() { _ = d.Close() }()
	col, err := NewCollectionManager(d).OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	graph, scanned := loadAndScanColumnGraphRebuildRowsV2A(t, d, "docs", def)
	if len(scanned) != 1 || len(scanned[0].adjacency) != 0 {
		t.Fatalf("single-row graph adjacency=%v want empty", scanned)
	}
	assertColumnGraphNoLegacyAdjacencySources1989(t, graph)
	records, cfg := loadColumnGraphRebuildManifestRecordsAndConfigV2A(t, d, "docs")
	state := columnVectorIndexStateFromRecords1987(t, records, def)
	list := loadColumnVectorIndexStateAdjacencyList1987(t, d, "docs", cfg, def, state, 0)
	if list.Rows != 1 || len(list.Offsets) != 2 || list.Offsets[0] != 0 || list.Offsets[1] != 0 || len(list.Values) != 0 {
		t.Fatalf("single-row state layer-0 list=%+v want one empty row", list)
	}
}

func TestColumnGraphLegacyAllLayerAdjacencySourcesCompatibility1920(t *testing.T) {
	d, cfg, def, graph, rows := prepareManualColumnGraphAllLayerSources1920(t)
	defer func() { _ = d.Close() }()
	if graph.AdjacencyLayerCount != 3 || len(graph.AdjacencyLayerSources) != 3 {
		t.Fatalf("adjacency layer count=%d sources=%d want 3 layers", graph.AdjacencyLayerCount, len(graph.AdjacencyLayerSources))
	}
	if graph.Layer0AdjacencySource != graph.AdjacencyLayerSources[0] {
		t.Fatalf("layer-0 alias=%+v want source[0]=%+v", graph.Layer0AdjacencySource, graph.AdjacencyLayerSources[0])
	}
	if err := validateColumnVectorGraphAdjacencyLayerSourcesAssets(d.ColumnAssetRootDir(), "docs", *cfg, def, graph); err != nil {
		t.Fatalf("validate all-layer sources: %v", err)
	}
	var sourceBytes int64
	for layer, source := range graph.AdjacencyLayerSources {
		if source.Layer != layer || source.ColumnName != columnVectorGraphAdjacencySourceColumnName(layer) {
			t.Fatalf("source[%d]=%+v has wrong identity", layer, source)
		}
		list := loadColumnGraphAdjacencyLayerSourceList1920(t, d, "docs", cfg, def, graph, layer)
		want := adjacencyLayerSourceFromAssetRows1920(t, rows, layer)
		assertRawUint32OffsetsListEqual1918(t, list, want)
		if source.OffsetsBytes != int64(len(want.Offsets))*8 || source.ValuesBytes != int64(len(want.Values))*4 {
			t.Fatalf("layer %d byte accounting offsets=%d values=%d want offsets=%d values=%d", layer, source.OffsetsBytes, source.ValuesBytes, len(want.Offsets)*8, len(want.Values)*4)
		}
		if source.PaddingBytes < 0 || source.AssetBytes <= source.OffsetsBytes+source.ValuesBytes {
			t.Fatalf("layer %d storage accounting=%+v", layer, source)
		}
		sourceBytes += source.AssetBytes
	}
	if got, want := columnVectorGraphStorageBytes(graph), graph.AssetBytes+sourceBytes; got != want {
		t.Fatalf("storage bytes=%d want %d", got, want)
	}
}

func TestColumnGraphLegacyAllLayerAdjacencySourcesRejectSingleLayerDefect1920(t *testing.T) {
	t.Run("missing_single_layer", func(t *testing.T) {
		d, cfg, def, graph, _ := prepareManualColumnGraphAllLayerSources1920(t)
		defer func() { _ = d.Close() }()
		if err := validateColumnVectorGraphAdjacencyLayerSourcesAssets(d.ColumnAssetRootDir(), "docs", *cfg, def, graph); err != nil {
			t.Fatalf("validate all-layer sources before removal: %v", err)
		}
		broken := graph
		broken.AdjacencyLayerSources = append([]columnVectorGraphLayer0AdjacencySourceSnapshot(nil), graph.AdjacencyLayerSources...)
		broken.AdjacencyLayerSources[1].Ref.FileID += 1000
		if err := validateColumnVectorGraphAdjacencyLayerSourcesAssets(d.ColumnAssetRootDir(), "docs", *cfg, def, broken); err == nil || !strings.Contains(err.Error(), "layer 1") {
			t.Fatalf("validate all-layer sources after missing layer err=%v, want layer-1 failure", err)
		}
	})

	t.Run("empty_advertised_layers", func(t *testing.T) {
		d, cfg, def, graph, _ := prepareManualColumnGraphAllLayerSources1920(t)
		defer func() { _ = d.Close() }()
		broken := graph
		broken.AdjacencyLayerCount = graph.AdjacencyLayerCount
		broken.AdjacencyLayerSources = nil
		if err := validateColumnVectorGraphAdjacencyLayerSourcesAssets(d.ColumnAssetRootDir(), "docs", *cfg, def, broken); err == nil || !strings.Contains(err.Error(), "sources count") {
			t.Fatalf("validate all-layer sources with empty advertised layers err=%v, want count failure", err)
		}
	})

	t.Run("layer0_alias_mismatch", func(t *testing.T) {
		d, cfg, def, graph, _ := prepareManualColumnGraphAllLayerSources1920(t)
		defer func() { _ = d.Close() }()
		broken := graph
		broken.Layer0AdjacencySource = graph.AdjacencyLayerSources[1]
		if err := validateColumnVectorGraphAdjacencyLayerSourcesAssets(d.ColumnAssetRootDir(), "docs", *cfg, def, broken); err == nil || !strings.Contains(err.Error(), "alias") {
			t.Fatalf("validate all-layer sources with layer-0 alias mismatch err=%v, want alias failure", err)
		}
	})

	t.Run("corrupt_single_layer", func(t *testing.T) {
		d, cfg, def, graph, _ := prepareManualColumnGraphAllLayerSources1920(t)
		defer func() { _ = d.Close() }()
		source := graph.AdjacencyLayerSources[1]
		raw, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), source.Ref)
		if err != nil {
			t.Fatalf("read layer-1 source: %v", err)
		}
		image, err := typedcolumn.ParseColumnPartImage(raw)
		if err != nil {
			t.Fatalf("ParseColumnPartImage: %v", err)
		}
		offsetsSection, _, ok := image.ColumnOffsetsListSections(columnVectorGraphAdjacencySourceColumnName(1))
		if !ok || offsetsSection.Length < 16 {
			t.Fatalf("offsets section=%+v ok=%v", offsetsSection, ok)
		}
		path, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), source.Ref)
		if err != nil {
			t.Fatalf("columnAssetSegmentPath: %v", err)
		}
		file, err := os.OpenFile(path, os.O_WRONLY, 0)
		if err != nil {
			t.Fatalf("OpenFile source: %v", err)
		}
		corrupt := []byte{0xff}
		if _, err := file.WriteAt(corrupt, source.Ref.Offset+int64(offsetsSection.Offset+8)); err != nil {
			_ = file.Close()
			t.Fatalf("WriteAt corrupt source: %v", err)
		}
		if err := file.Close(); err != nil {
			t.Fatalf("Close corrupt source: %v", err)
		}
		if err := validateColumnVectorGraphAdjacencyLayerSourcesAssets(d.ColumnAssetRootDir(), "docs", *cfg, def, graph); err == nil || !strings.Contains(err.Error(), "layer 1") {
			t.Fatalf("validate all-layer sources after corrupt layer err=%v, want layer-1 failure", err)
		}
	})
}

func TestColumnGraphAllLayerManifestRejectsMalformedSourceBlock1920(t *testing.T) {
	d, _, _, graph, _ := prepareManualColumnGraphAllLayerSources1920(t)
	defer func() { _ = d.Close() }()
	encoded, err := encodeColumnVectorGraphManifestRecord(graph)
	if err != nil {
		t.Fatalf("encodeColumnVectorGraphManifestRecord: %v", err)
	}
	magic := make([]byte, 4)
	binary.BigEndian.PutUint32(magic, columnVectorGraphAdjacencyLayerSourcesMagic)
	pos := bytes.Index(encoded, magic)
	if pos < 0 {
		t.Fatal("encoded graph manifest missing adjacency layer sources block")
	}
	if len(encoded)-pos < 4+2+8+8 {
		t.Fatalf("encoded graph manifest too short for layer source header at %d", pos)
	}
	sourceCountOffset := pos + 4 + 2 + 8
	for _, tc := range []struct {
		name        string
		rewrite     func([]byte)
		wantMessage string
	}{
		{
			name: "source_count_mismatch",
			rewrite: func(raw []byte) {
				binary.BigEndian.PutUint64(raw[sourceCountOffset:], uint64(graph.AdjacencyLayerCount+1))
			},
			wantMessage: "source_count",
		},
		{
			name: "implausible_source_count",
			rewrite: func(raw []byte) {
				binary.BigEndian.PutUint64(raw[pos+4+2:], uint64(1<<20))
				binary.BigEndian.PutUint64(raw[sourceCountOffset:], uint64(1<<20))
			},
			wantMessage: "remaining record bytes",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			corrupt := append([]byte(nil), encoded...)
			tc.rewrite(corrupt)
			if _, err := decodeColumnVectorGraphManifestRecord(corrupt); err == nil || !strings.Contains(err.Error(), tc.wantMessage) {
				t.Fatalf("decode malformed all-layer block err=%v, want %q", err, tc.wantMessage)
			}
		})
	}
	t.Run("legacy_block_before_all_layer_block", func(t *testing.T) {
		var legacy bytes.Buffer
		encodeColumnVectorGraphLayer0AdjacencySource(&legacy, graph.AdjacencyLayerSources[0])
		corrupt := make([]byte, 0, len(encoded)+legacy.Len())
		corrupt = append(corrupt, encoded[:pos]...)
		corrupt = append(corrupt, legacy.Bytes()...)
		corrupt = append(corrupt, encoded[pos:]...)
		if _, err := decodeColumnVectorGraphManifestRecord(corrupt); err == nil || !strings.Contains(err.Error(), "both legacy layer-0 and all-layer") {
			t.Fatalf("decode manifest with legacy+all-layer blocks err=%v, want duplicate-block failure", err)
		}
	})
	t.Run("malformed_legacy_block_before_all_layer_block", func(t *testing.T) {
		badLegacy := make([]byte, 6)
		binary.BigEndian.PutUint32(badLegacy, columnVectorGraphLayer0SourceMagic)
		binary.BigEndian.PutUint16(badLegacy[4:], columnVectorGraphLayer0SourceVersion+1)
		corrupt := make([]byte, 0, len(encoded)+len(badLegacy))
		corrupt = append(corrupt, encoded[:pos]...)
		corrupt = append(corrupt, badLegacy...)
		corrupt = append(corrupt, encoded[pos:]...)
		if _, err := decodeColumnVectorGraphManifestRecord(corrupt); err == nil || !strings.Contains(err.Error(), "malformed legacy layer-0") {
			t.Fatalf("decode manifest with malformed legacy before all-layer err=%v, want fail-closed malformed legacy failure", err)
		}
	})
	t.Run("padded_malformed_legacy_block_before_all_layer_block", func(t *testing.T) {
		badLegacy := make([]byte, 7)
		binary.BigEndian.PutUint32(badLegacy, columnVectorGraphLayer0SourceMagic)
		binary.BigEndian.PutUint16(badLegacy[4:], columnVectorGraphLayer0SourceVersion+1)
		badLegacy[6] = 0xa5
		corrupt := make([]byte, 0, len(encoded)+len(badLegacy))
		corrupt = append(corrupt, encoded[:pos]...)
		corrupt = append(corrupt, badLegacy...)
		corrupt = append(corrupt, encoded[pos:]...)
		if _, err := decodeColumnVectorGraphManifestRecord(corrupt); err == nil || !strings.Contains(err.Error(), "malformed legacy layer-0") {
			t.Fatalf("decode manifest with padded malformed legacy before all-layer err=%v, want fail-closed malformed legacy failure", err)
		}
	})
	t.Run("malformed_legacy_only_magic_payload_ignored", func(t *testing.T) {
		badLegacy := make([]byte, 14)
		binary.BigEndian.PutUint32(badLegacy, columnVectorGraphLayer0SourceMagic)
		binary.BigEndian.PutUint16(badLegacy[4:], columnVectorGraphLayer0SourceVersion+1)
		binary.BigEndian.PutUint32(badLegacy[10:], columnVectorGraphAdjacencyLayerSourcesMagic)
		corrupt := append(append([]byte(nil), encoded[:pos]...), badLegacy...)
		decoded, err := decodeColumnVectorGraphManifestRecord(corrupt)
		if err != nil {
			t.Fatalf("decode malformed legacy-only trailer with embedded TCGL magic: %v", err)
		}
		if decoded.Layer0AdjacencySource.Present || len(decoded.AdjacencyLayerSources) != 0 || decoded.AdjacencyLayerCount != 0 {
			t.Fatalf("decoded malformed legacy-only trailer sources=%+v count=%d layer0=%+v, want ignored optional legacy metadata", decoded.AdjacencyLayerSources, decoded.AdjacencyLayerCount, decoded.Layer0AdjacencySource)
		}
	})
	t.Run("encode_rejects_empty_advertised_layers", func(t *testing.T) {
		broken := graph
		broken.AdjacencyLayerSources = nil
		if _, err := encodeColumnVectorGraphManifestRecord(broken); err == nil || !strings.Contains(err.Error(), "layer sources=0") {
			t.Fatalf("encode graph with advertised layers but no sources err=%v, want count failure", err)
		}
	})
	t.Run("encode_rejects_missing_layer0_alias", func(t *testing.T) {
		broken := graph
		broken.Layer0AdjacencySource = columnVectorGraphLayer0AdjacencySourceSnapshot{}
		if _, err := encodeColumnVectorGraphManifestRecord(broken); err == nil || !strings.Contains(err.Error(), "missing layer-0") {
			t.Fatalf("encode graph with missing layer-0 alias err=%v, want alias failure", err)
		}
	})
}

func TestColumnGraphReachabilityProtectsLegacyAllLayerAdjacencySourceRefs1920(t *testing.T) {
	d, cfg, def, graph, _ := prepareManualColumnGraphAllLayerSources1920(t)
	defer func() { _ = d.Close() }()
	identity := ColumnManifestIdentity{Generation: graph.BaseManifestGeneration, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 0x1234}
	records, identity := testColumnGraphManifestRecordsFromSnapshot1920(t, *cfg, graph, identity)
	cfgForMeta := cfg.copy()
	publishColumnGraphCatalogForTestV2A(t, d, CollectionMeta{
		Name:          "docs",
		Options:       CollectionOptions{DocumentFormat: DocumentFormatJSON, ColumnStore: &cfgForMeta},
		VectorIndexes: []VectorIndexDefinition{def},
	}, identity, records)
	col, err := NewCollectionManager(d).OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	assertColumnAssetReachabilityProtectsGraphRefV2A(t, col, graph.AssetRef)
	for _, source := range graph.AdjacencyLayerSources {
		assertColumnAssetReachabilityProtectsGraphRefV2A(t, col, source.Ref)
	}
}

func TestColumnGraphLegacyAdjacencySourceCorruptionFallsBack1919(t *testing.T) {
	t.Run("missing_source", func(t *testing.T) {
		d, col, def, graph, _ := openManualColumnGraphAllLayerSourceSearchFixture1921(t, nil)
		defer func() { _ = d.Close() }()
		path, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), graph.Layer0AdjacencySource.Ref)
		if err != nil {
			t.Fatalf("columnAssetSegmentPath: %v", err)
		}
		if err := os.Remove(path); err != nil {
			t.Fatalf("remove legacy source segment: %v", err)
		}
		status, err := col.VectorIndexStatus(def.Name)
		if err != nil {
			t.Fatalf("VectorIndexStatus with missing optional legacy source: %v", err)
		}
		assertColumnGraphRebuildLoadedStatusV2A(t, status, def.Name)
	})

	t.Run("wrong_row_count_identity", func(t *testing.T) {
		d, _, def, _, _ := openManualColumnGraphAllLayerSourceSearchFixture1921(t, nil)
		defer func() { _ = d.Close() }()
		records, cfg := loadColumnGraphRebuildManifestRecordsAndConfigV2A(t, d, "docs")
		manifest, err := decodeColumnManifestSnapshotForScan(records)
		if err != nil {
			t.Fatalf("decodeColumnManifestSnapshotForScan: %v", err)
		}
		graph := graphManifestFromRecords1918(t, records, def)
		graph.Layer0AdjacencySource.RowCount++
		if !columnVectorGraphManifestMatchesDefinition("docs", graph, def, *cfg, manifest, records) {
			t.Fatal("graph manifest mismatch for stale optional legacy layer-0 source row_count; vector-index state should remain loaded")
		}
	})

	t.Run("corrupt_offsets_payload", func(t *testing.T) {
		d, col, def, graph, _ := openManualColumnGraphAllLayerSourceSearchFixture1921(t, nil)
		defer func() { _ = d.Close() }()
		raw, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), graph.Layer0AdjacencySource.Ref)
		if err != nil {
			t.Fatalf("read legacy source: %v", err)
		}
		image, err := typedcolumn.ParseColumnPartImage(raw)
		if err != nil {
			t.Fatalf("ParseColumnPartImage: %v", err)
		}
		offsetsSection, _, ok := image.ColumnOffsetsListSections(columnVectorGraphLayer0AdjacencySourceColumnName)
		if !ok || offsetsSection.Length < 16 {
			t.Fatalf("legacy offsets section=%+v ok=%v", offsetsSection, ok)
		}
		corrupt := append([]byte(nil), raw...)
		corrupt[offsetsSection.Offset+8] ^= 0xff
		path, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), graph.Layer0AdjacencySource.Ref)
		if err != nil {
			t.Fatalf("columnAssetSegmentPath: %v", err)
		}
		file, err := os.OpenFile(path, os.O_WRONLY, 0)
		if err != nil {
			t.Fatalf("OpenFile legacy source: %v", err)
		}
		if _, err := file.WriteAt(corrupt, graph.Layer0AdjacencySource.Ref.Offset); err != nil {
			_ = file.Close()
			t.Fatalf("WriteAt corrupt legacy source: %v", err)
		}
		if err := file.Close(); err != nil {
			t.Fatalf("Close corrupt legacy source: %v", err)
		}
		status, err := col.VectorIndexStatus(def.Name)
		if err != nil {
			t.Fatalf("VectorIndexStatus with corrupt optional legacy source: %v", err)
		}
		assertColumnGraphRebuildLoadedStatusV2A(t, status, def.Name)
	})
}

func TestColumnGraphRebuildEmptyInitialManifestWithoutCommandWALFailsClosedV2A(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{
		Dir:                    t.TempDir(),
		Durability:             backenddb.DurabilityWALOffRelaxed,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	cfg := columnGraphRebuildColumnStoreConfigV2A(3)
	cfg.ProfileSupport = ColumnStoreProfileBenchmarkRelaxed
	def := columnGraphRebuildVectorIndexDefinitionV2A(3, 1)
	meta := CollectionMeta{
		Name: "docs",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
			ColumnStore:    cfg,
		},
		VectorIndexes: []VectorIndexDefinition{def},
	}
	if _, err := NewCollectionManager(d).CreateCollection(&meta); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := NewCollectionManager(d).OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	_, err = col.RebuildVectorIndex(def.Name)
	if !errors.Is(err, backenddb.ErrCommandWALRejected) || !strings.Contains(err.Error(), "requires command WAL") {
		t.Fatalf("RebuildVectorIndex empty without command WAL err=%v, want command WAL rejection", err)
	}
	status, statusErr := col.VectorIndexStatus(def.Name)
	if statusErr != nil {
		t.Fatalf("VectorIndexStatus: %v", statusErr)
	}
	if status.Loaded || !status.RebuildNeeded {
		t.Fatalf("status after rejected empty rebuild=%+v, want not loaded/rebuild-needed", status)
	}
}

func TestColumnGraphRebuildNonEmptyWithoutManifestRootFailsClosedV2A(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{
		Dir:                    t.TempDir(),
		Durability:             backenddb.DurabilityWALOffRelaxed,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()
	meta := CollectionMeta{
		Name: "docs",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
		},
	}
	if _, err := NewCollectionManager(d).CreateCollection(&meta); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := NewCollectionManager(d).OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	insertColumnGraphRebuildRowsV2A(t, col, []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
	})

	def := columnGraphRebuildVectorIndexDefinitionV2A(3, 1)
	cfg := columnGraphRebuildColumnStoreConfigV2A(3)
	cfg.ProfileSupport = ColumnStoreProfileBenchmarkRelaxed
	publishColumnGraphRebuildMetadataWithoutManifestRootV2A(t, d, "docs", cfg, []VectorIndexDefinition{def})
	col, err = NewCollectionManager(d).OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection after metadata update: %v", err)
	}
	_, err = col.RebuildVectorIndex(def.Name)
	if err == nil || !strings.Contains(err.Error(), "requires an initial physical column manifest root") {
		t.Fatalf("RebuildVectorIndex err=%v want initial manifest root failure", err)
	}
	status, statusErr := col.VectorIndexStatus(def.Name)
	if statusErr != nil {
		t.Fatalf("VectorIndexStatus: %v", statusErr)
	}
	if status.Loaded || !status.RebuildNeeded || status.Reason != VectorIndexReasonColumnGraphRebuildNeeded {
		t.Fatalf("status after rejected non-empty rebuild=%+v, want rebuild-needed", status)
	}
}

func TestColumnGraphRebuildVectorIndexFailsClosedOnZeroVectorV2A(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 0, 0}},
	}
	dir, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 0, rows)
	defer func() { _ = d.Close() }()
	framesBefore := countCollectionCommandWALFrames(t, dir)

	_, err := col.RebuildVectorIndex(def.Name)
	if err == nil || !strings.Contains(err.Error(), "non-zero") {
		t.Fatalf("RebuildVectorIndex err=%v, want non-zero vector failure", err)
	}
	if got := countCollectionCommandWALFrames(t, dir); got != framesBefore {
		t.Fatalf("command WAL frames after failed rebuild=%d want %d", got, framesBefore)
	}
	status, statusErr := col.VectorIndexStatus(def.Name)
	if statusErr != nil {
		t.Fatalf("VectorIndexStatus after failed rebuild: %v", statusErr)
	}
	if status.Loaded || !status.RebuildNeeded {
		t.Fatalf("status after failed rebuild=%+v, want rebuild-needed and not loaded", status)
	}
}

func TestColumnGraphRebuildVectorIndexFailsClosedOnInvNormOverflowV2A(t *testing.T) {
	_, err := columnVectorGraphInvNorm([]float32{math.SmallestNonzeroFloat32})
	if err == nil || !strings.Contains(err.Error(), "fit float32") {
		t.Fatalf("columnVectorGraphInvNorm err=%v, want float32 overflow failure", err)
	}

	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{math.SmallestNonzeroFloat32, 0, 0}},
	}
	dir, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 0, rows)
	defer func() { _ = d.Close() }()
	framesBefore := countCollectionCommandWALFrames(t, dir)

	_, err = col.RebuildVectorIndex(def.Name)
	if err == nil || !strings.Contains(err.Error(), "fit float32") {
		t.Fatalf("RebuildVectorIndex err=%v, want float32 overflow failure", err)
	}
	if got := countCollectionCommandWALFrames(t, dir); got != framesBefore {
		t.Fatalf("command WAL frames after failed rebuild=%d want %d", got, framesBefore)
	}
}

func TestColumnGraphRebuildVectorIndexMarksStaleAfterMutationV2A(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
		{id: "doc-c", vector: []float32{0, 0, 1}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 0, rows)
	defer func() { _ = d.Close() }()
	status, err := col.RebuildVectorIndex(def.Name)
	if err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	assertColumnGraphRebuildLoadedStatusV2A(t, status, def.Name)

	insertColumnGraphRebuildRowsV2A(t, col, []columnGraphRebuildInputRowV2A{
		{id: "doc-d", vector: []float32{0.7, 0.7, 0}},
	})
	status, err = col.VectorIndexStatus(def.Name)
	if err != nil {
		t.Fatalf("VectorIndexStatus after mutation: %v", err)
	}
	if status.Loaded || !status.RebuildNeeded {
		t.Fatalf("status after mutation=%+v, want stale/rebuild-needed", status)
	}
	switch status.Reason {
	case VectorIndexReasonColumnGraphRebuildNeeded, VectorIndexReasonColumnGraphAssetMismatch, VectorIndexReasonColumnGraphUnsupportedVisibility:
	default:
		t.Fatalf("status reason after mutation=%q, want rebuild-needed, asset-mismatch, or unsupported visibility", status.Reason)
	}
}

func TestColumnGraphRebuildVectorIndexExpectedPartsUsesActiveGenerationV2A(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 1, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("first RebuildVectorIndex: %v", err)
	}
	insertColumnGraphRebuildRowsV2A(t, col, []columnGraphRebuildInputRowV2A{
		{id: "doc-c", vector: []float32{0, 0, 1}},
	})
	status, err := col.RebuildVectorIndex(def.Name)
	if err != nil {
		t.Fatalf("second RebuildVectorIndex: %v", err)
	}
	assertColumnGraphRebuildLoadedStatusV2A(t, status, def.Name)

	records, _ := loadColumnGraphRebuildManifestRecordsAndConfigV2A(t, d, "docs")
	manifest, err := decodeColumnManifestSnapshotForScan(records)
	if err != nil {
		t.Fatalf("decodeColumnManifestSnapshotForScan: %v", err)
	}
	activeGenerationParts := 0
	totalLineageParts := 0
	for _, record := range records {
		if !bytes.HasPrefix(record.key, columnManifestPartRecordPrefixBytes) {
			continue
		}
		totalLineageParts++
		generation, err := columnManifestPartGenerationFromRecordKeyForScan(record.key)
		if err != nil {
			t.Fatalf("column manifest part generation: %v", err)
		}
		if generation == manifest.Generation {
			activeGenerationParts++
		}
	}
	if totalLineageParts <= activeGenerationParts {
		t.Fatalf("test did not create retained prior-generation parts: total=%d active=%d generation=%d", totalLineageParts, activeGenerationParts, manifest.Generation)
	}
	if got, want := manifest.ExpectedParts, uint64(activeGenerationParts); got != want {
		t.Fatalf("manifest ExpectedParts=%d want active-generation part count %d", got, want)
	}
}

func TestColumnGraphRebuildVectorIndexAdjacencyUsesFlattenedNativeGraphV2A(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0}},
		{id: "doc-b", vector: []float32{0.99, 0.01}},
		{id: "doc-c", vector: []float32{0.75, 0.25}},
		{id: "doc-d", vector: []float32{0, 1}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 2, 2, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	graph, scanned := loadAndScanColumnGraphRebuildRowsV2A(t, d, "docs", def)
	if columnVectorGraphManifestHasPhysicalAsset(graph) {
		t.Fatalf("graph manifest has physical row asset %+v; current rebuild should use TVIS/base typed-column state", graph.AssetRef)
	}
	wantGraph := columnGraphRebuildNativeGraphLayoutV2A(t, def, rows)
	for i := range rows {
		if gotID := string(scanned[i].id); gotID != wantGraph.ids[i] {
			t.Fatalf("scanned[%d] id=%q, want native locality order %q", i, gotID, wantGraph.ids[i])
		}
		if got := scanned[i].adjacency; !uint32SlicesEqual(got, wantGraph.adjacency[i]) {
			t.Fatalf("%s adjacency=%v, want native layered adjacency %v", wantGraph.ids[i], got, wantGraph.adjacency[i])
		}
	}
}

func TestColumnGraphRebuildVectorIndexAllocatesPartIDsAcrossGraphIndexesV2A(t *testing.T) {
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(t, dir)
	defer func() { _ = d.Close() }()

	cfg := columnGraphRebuildColumnStoreConfigV2A(2)
	cfg.Columns = append(cfg.Columns, ColumnStoreColumn{
		Name:       "other_embedding",
		Path:       "other_embedding",
		Owner:      TypedStorageOwnerColumnPart,
		ValueType:  ColumnStoreValueFloat32Vector,
		VectorDims: 2,
	})
	defA := columnGraphRebuildVectorIndexDefinitionV2A(2, 1)
	defB, err := normalizeVectorIndexDefinition(VectorIndexDefinition{
		Name:       "other_embedding_graph",
		Field:      "other_embedding",
		Metric:     VectorMetricCosine,
		Dimensions: 2,
		M:          1,
		Strategy:   VectorIndexStrategyColumnGraph,
	})
	if err != nil {
		t.Fatalf("normalizeVectorIndexDefinition defB: %v", err)
	}
	meta := CollectionMeta{
		Name: "docs",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
			ColumnStore:    cfg,
		},
		VectorIndexes: []VectorIndexDefinition{defA, defB},
	}
	if _, err := NewCollectionManager(d).CreateCollection(&meta); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := NewCollectionManager(d).OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	insertColumnGraphRebuildDualVectorRowsV2A(t, col, []columnGraphRebuildDualInputRowV2A{
		{id: "doc-a", embedding: []float32{1, 0}, otherEmbedding: []float32{0, 1}},
		{id: "doc-b", embedding: []float32{0, 1}, otherEmbedding: []float32{1, 0}},
	})
	if _, err := col.RebuildVectorIndex(defA.Name); err != nil {
		t.Fatalf("RebuildVectorIndex defA: %v", err)
	}
	if _, err := col.RebuildVectorIndex(defB.Name); err != nil {
		t.Fatalf("RebuildVectorIndex defB: %v", err)
	}
	graphA, _ := loadAndScanColumnGraphRebuildRowsV2A(t, d, "docs", defA)
	graphB, _ := loadAndScanColumnGraphRebuildRowsV2A(t, d, "docs", defB)
	if columnVectorGraphManifestHasPhysicalAsset(graphA) || columnVectorGraphManifestHasPhysicalAsset(graphB) {
		t.Fatalf("current graph rebuilds should not publish physical row assets: graphA=%+v graphB=%+v", graphA.AssetRef, graphB.AssetRef)
	}
	records, _ := loadColumnGraphRebuildManifestRecordsAndConfigV2A(t, d, "docs")
	stateA := columnVectorIndexStateFromRecords1987(t, records, defA)
	stateB := columnVectorIndexStateFromRecords1987(t, records, defB)
	seen := make(map[uint64]string)
	for _, asset := range stateA.Assets {
		seen[asset.Ref.PartID] = defA.Name + ":" + asset.Role + ":" + asset.AssetID
	}
	for _, asset := range stateB.Assets {
		if previous := seen[asset.Ref.PartID]; previous != "" {
			t.Fatalf("vector-index state assets reused part_id=%d between %s and %s:%s:%s", asset.Ref.PartID, previous, defB.Name, asset.Role, asset.AssetID)
		}
	}
}

func TestColumnGraphRebuildVectorIndexHeapTopKV2A(t *testing.T) {
	const rows = 45
	degree := columnVectorGraphNeighborInsertionLimit + 1
	input := make([]columnVectorGraphAssetRow, rows)
	input[0] = columnVectorGraphAssetRow{
		ID:      []byte("doc-00"),
		Vector:  []float32{1, 0},
		InvNorm: 1,
	}
	for i := 1; i < rows; i++ {
		vector := []float32{float32(i), 1}
		invNorm, err := columnVectorGraphInvNorm(vector)
		if err != nil {
			t.Fatalf("columnVectorGraphInvNorm row %d: %v", i, err)
		}
		input[i] = columnVectorGraphAssetRow{
			ID:      []byte(fmt.Sprintf("doc-%02d", i)),
			Vector:  vector,
			InvNorm: invNorm,
		}
	}

	got, err := topColumnVectorGraphNeighbors(input, 0, degree)
	if err != nil {
		t.Fatalf("topColumnVectorGraphNeighbors: %v", err)
	}
	if len(got) != degree {
		t.Fatalf("neighbors=%d want %d", len(got), degree)
	}
	for i, neighbor := range got {
		wantOrdinal := rows - 1 - i
		wantScore := columnVectorGraphCosine(input[0], input[wantOrdinal])
		if neighbor.ordinal != wantOrdinal || math.Abs(neighbor.score-wantScore) > 1e-12 {
			t.Fatalf("neighbor[%d]=%+v want ordinal=%d score=%.12f", i, neighbor, wantOrdinal, wantScore)
		}
	}
}

func TestColumnGraphRebuildVectorIndexAdjacencyValidatesAllDimsBeforeScoringV2A(t *testing.T) {
	def := columnGraphRebuildVectorIndexDefinitionV2A(3, 1)
	rows := []columnVectorGraphAssetRow{
		{ID: []byte("doc-a"), Vector: []float32{1, 0, 0}, InvNorm: 1},
		{ID: []byte("doc-b"), Vector: []float32{1}, InvNorm: 1},
		{ID: []byte("doc-c"), Vector: []float32{0, 1, 0}, InvNorm: 1},
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			t.Fatalf("buildColumnVectorGraphAdjacency panicked before validating all row dimensions: %v", recovered)
		}
	}()
	err := buildColumnVectorGraphAdjacency(rows, def)
	if err == nil || !strings.Contains(err.Error(), "row[1] vector dims=1 want 3") {
		t.Fatalf("buildColumnVectorGraphAdjacency error=%v, want row[1] dimension failure", err)
	}
}

// This compact ordered fixture reproduces the directed entry-reachability
// collapse observed in retained partition-local packs. The auxiliary channel
// must repair that structural path without changing the native HNSW graph.
func TestVectorPartitionLocalAuxiliaryNavigationKeepsNativeLayer0AndEntryReachable(t *testing.T) {
	const count = 4096
	def := columnGraphRebuildVectorIndexDefinitionV2A(16, 16)
	rows := make([]columnVectorGraphAssetRow, count)
	for i := range rows {
		vector := make([]float32, 16)
		cluster := (i / 97) % 4
		vector[cluster] = 1
		for d := 4; d < len(vector); d++ {
			vector[d] = float32(((i+1)*(d+3)+1)%31) / 310
		}
		rows[i] = columnVectorGraphAssetRow{ID: []byte(fmt.Sprintf("ordered-%05d", i)), Vector: vector, InvNorm: 1}
	}
	native := cloneColumnVectorGraphAssetRows3999(rows)
	repeated := cloneColumnVectorGraphAssetRows3999(rows)
	if err := buildColumnVectorGraphAdjacency(native, def); err != nil {
		t.Fatalf("buildColumnVectorGraphAdjacency: %v", err)
	}
	if got, err := vectorPartitionLayer0Reachability3999(native); err != nil {
		t.Fatalf("native layer-0 reachability: %v", err)
	} else if got >= len(native) {
		t.Fatalf("native layer-0 entry reaches %d/%d rows; fixture no longer reproduces the base failure", got, len(native))
	}
	if err := buildColumnVectorGraphAdjacency(repeated, def); err != nil {
		t.Fatalf("repeat buildColumnVectorGraphAdjacency: %v", err)
	}
	before := cloneColumnVectorGraphAssetRows3999(native)
	auxiliary, err := buildVectorPartitionLocalAuxiliaryNavigationV1(native, 0)
	if err != nil {
		t.Fatalf("build auxiliary: %v", err)
	}
	repeatedAuxiliary, err := buildVectorPartitionLocalAuxiliaryNavigationV1(repeated, 0)
	if err != nil {
		t.Fatalf("repeat build auxiliary: %v", err)
	}
	if err := validateVectorPartitionLocalAuxiliaryNavigationV1(native, 0, auxiliary); err != nil {
		t.Fatalf("validate auxiliary: %v", err)
	}
	if len(auxiliary.Neighbors) == 0 {
		t.Fatalf("auxiliary directed bridge count=%d", len(auxiliary.Neighbors))
	}
	for row := range native {
		if degree := auxiliary.Offsets[row+1] - auxiliary.Offsets[row]; degree > vectorPartitionLocalNavigationBranchV1+1 {
			t.Fatalf("auxiliary row=%d degree=%d exceeds cap", row, degree)
		}
	}
	malformed := auxiliary
	malformed.Neighbors = append([]uint32(nil), auxiliary.Neighbors...)
	malformed.Neighbors[0] = uint32((int(malformed.Neighbors[0]) + 1) % len(native))
	if err := validateVectorPartitionLocalAuxiliaryNavigationV1(native, 0, malformed); err == nil {
		t.Fatal("malformed auxiliary navigation was accepted")
	}
	if !slices.Equal(auxiliary.Offsets, repeatedAuxiliary.Offsets) || !slices.Equal(auxiliary.Neighbors, repeatedAuxiliary.Neighbors) {
		t.Fatal("auxiliary navigation is nondeterministic")
	}
	if got, err := vectorPartitionLayer0AuxiliaryReachability3999(native, auxiliary); err != nil {
		t.Fatalf("native plus auxiliary reachability: %v", err)
	} else if got != len(native) {
		t.Fatalf("native plus auxiliary entry reaches %d/%d rows", got, len(native))
	}
	upperSeeds := 0
	for ordinal, row := range native {
		level, err := columnVectorGraphAdjacencyMaxLayer(row.Adjacency)
		if err != nil || level == 0 {
			continue
		}
		upperSeeds++
		if got, err := vectorPartitionLayer0AuxiliaryReachabilityFrom3999(native, auxiliary, ordinal); err != nil || got != len(native) {
			t.Fatalf("native plus auxiliary upper seed=%d reaches %d/%d err=%v", ordinal, got, len(native), err)
		}
	}
	if upperSeeds == 0 {
		t.Fatal("fixture has no upper-layer seed")
	}
	saturated := false
	for row := range native {
		layer0, suffix, err := vectorPartitionLayer0AdjacencySplitV1(native[row].Adjacency)
		if err != nil || len(layer0) > 2*def.M {
			t.Fatalf("native row=%d layer-0 degree=%d err=%v", row, len(layer0), err)
		}
		if len(layer0) == 2*def.M {
			saturated = true
		}
		_, beforeSuffix, beforeErr := vectorPartitionLayer0AdjacencySplitV1(before[row].Adjacency)
		if beforeErr != nil || !slices.Equal(native[row].Adjacency, before[row].Adjacency) || !slices.Equal(suffix, beforeSuffix) {
			t.Fatalf("auxiliary changed native adjacency row=%d", row)
		}
	}
	if !saturated {
		t.Fatal("fixture no longer contains a saturated native layer-0 row")
	}
}

func cloneColumnVectorGraphAssetRows3999(in []columnVectorGraphAssetRow) []columnVectorGraphAssetRow {
	out := make([]columnVectorGraphAssetRow, len(in))
	for i := range in {
		out[i] = in[i]
		out[i].ID = append([]byte(nil), in[i].ID...)
		out[i].Vector = append([]float32(nil), in[i].Vector...)
	}
	return out
}

func vectorPartitionLayer0Reachability3999(rows []columnVectorGraphAssetRow) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	seen := make([]bool, len(rows))
	queue := []int{0}
	seen[0] = true
	for head := 0; head < len(queue); head++ {
		layer0, _, err := vectorPartitionLayer0AdjacencySplitV1(rows[queue[head]].Adjacency)
		if err != nil {
			return 0, err
		}
		for _, neighbor := range layer0 {
			if int(neighbor) >= len(rows) {
				return 0, fmt.Errorf("neighbor=%d out of range rows=%d", neighbor, len(rows))
			}
			if !seen[neighbor] {
				seen[neighbor] = true
				queue = append(queue, int(neighbor))
			}
		}
	}
	return len(queue), nil
}

func vectorPartitionLayer0AuxiliaryReachability3999(rows []columnVectorGraphAssetRow, auxiliary vectorPartitionLocalAuxiliaryNavigationV1) (int, error) {
	return vectorPartitionLayer0AuxiliaryReachabilityFrom3999(rows, auxiliary, 0)
}

func vectorPartitionLayer0AuxiliaryReachabilityFrom3999(rows []columnVectorGraphAssetRow, auxiliary vectorPartitionLocalAuxiliaryNavigationV1, entryOrdinal int) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	if entryOrdinal < 0 || entryOrdinal >= len(rows) {
		return 0, errors.New("auxiliary entry ordinal")
	}
	if len(auxiliary.Offsets) != len(rows)+1 || auxiliary.Offsets[0] != 0 || auxiliary.Offsets[len(rows)] != uint64(len(auxiliary.Neighbors)) {
		return 0, errors.New("auxiliary csr shape")
	}
	seen := make([]bool, len(rows))
	queue := []int{entryOrdinal}
	seen[entryOrdinal] = true
	for head := 0; head < len(queue); head++ {
		ordinal := queue[head]
		layer0, _, err := vectorPartitionLayer0AdjacencySplitV1(rows[ordinal].Adjacency)
		if err != nil {
			return 0, err
		}
		start, end := auxiliary.Offsets[ordinal], auxiliary.Offsets[ordinal+1]
		if end < start || end > uint64(len(auxiliary.Neighbors)) {
			return 0, errors.New("auxiliary csr offsets")
		}
		neighbors := append(append([]uint32(nil), layer0...), auxiliary.Neighbors[start:end]...)
		for _, neighbor := range neighbors {
			if int(neighbor) >= len(rows) || neighbor == uint32(ordinal) {
				return 0, fmt.Errorf("neighbor=%d out of range rows=%d", neighbor, len(rows))
			}
			if !seen[neighbor] {
				seen[neighbor] = true
				queue = append(queue, int(neighbor))
			}
		}
	}
	return len(queue), nil
}

func TestColumnGraphRebuildVectorIndexReachabilityReclaimsSupersededGraphSegmentV2A(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0.9, 0.1, 0}},
		{id: "doc-c", vector: []float32{0, 1, 0}},
		{id: "doc-d", vector: []float32{0, 0, 1}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 2, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("first RebuildVectorIndex: %v", err)
	}
	firstRecords, _ := loadColumnGraphRebuildManifestRecordsAndConfigV2A(t, d, "docs")
	firstState := columnVectorIndexStateFromRecords1987(t, firstRecords, def)
	if len(firstState.Assets) == 0 {
		t.Fatal("first rebuild produced no vector-index state assets")
	}
	firstRef := firstState.Assets[0].Ref
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("second RebuildVectorIndex: %v", err)
	}
	secondRecords, _ := loadColumnGraphRebuildManifestRecordsAndConfigV2A(t, d, "docs")
	secondState := columnVectorIndexStateFromRecords1987(t, secondRecords, def)
	if len(secondState.Assets) == 0 {
		t.Fatal("second rebuild produced no vector-index state assets")
	}
	secondRef := secondState.Assets[0].Ref
	if firstRef == secondRef {
		t.Fatalf("second rebuild reused vector-index state asset ref %+v", firstRef)
	}
	if firstRef.FileID == secondRef.FileID {
		t.Fatalf("second rebuild reused vector-index state segment file_id=%d; want fresh segment for whole-segment reclaim", firstRef.FileID)
	}

	plan, err := col.PlanColumnAssetReachability(context.Background(), ColumnAssetReachabilityOptions{Detailed: true})
	if err != nil {
		t.Fatalf("PlanColumnAssetReachability: %v", err)
	}
	if !plan.Complete {
		t.Fatalf("reachability plan incomplete: refs=%+v segments=%+v", plan.Refs, plan.Segments)
	}
	assertColumnGraphReachabilityEntryV2A(t, plan, secondRef)
	assertColumnGraphSegmentStatusV2A(t, plan, firstRef.FileID, ColumnAssetReachabilitySegmentReclaimable)
}

func TestColumnGraphReachabilityPrunesDroppedVectorIndexGraphRefV2A(t *testing.T) {
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()
	baseCfg, err := normalizeColumnStoreConfig("docs", columnGraphRebuildColumnStoreConfigV2A(2))
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	def := columnGraphRebuildVectorIndexDefinitionV2A(2, 1)
	prepared, err := prepareColumnVectorGraphPhysicalAsset(d.ColumnAssetRootDir(), "docs", *baseCfg, def, 2, 1, 1, []columnVectorGraphAssetRow{
		{ID: []byte("doc-a"), Vector: []float32{1, 0}, InvNorm: 1, Adjacency: []uint32{1}},
		{ID: []byte("doc-b"), Vector: []float32{0, 1}, InvNorm: 1, Adjacency: []uint32{0}},
	})
	if err != nil {
		t.Fatalf("prepareColumnVectorGraphPhysicalAsset: %v", err)
	}
	identity := ColumnManifestIdentity{Generation: 2, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 0x1234}
	records, identity := testColumnGraphManifestRecordsV2A(t, *baseCfg, def, identity, prepared.Ref, prepared.Bytes, prepared.RowCount)
	// Model a metadata-only drop: the stale graph manifest record remains, but
	// the current catalog no longer declares the column_graph index.
	publishColumnGraphCatalogForTestV2A(t, d, CollectionMeta{
		Name:    "docs",
		Options: CollectionOptions{DocumentFormat: DocumentFormatJSON, ColumnStore: columnGraphRebuildColumnStoreConfigV2A(2)},
	}, identity, records)
	col, err := NewCollectionManager(d).OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}

	plan, err := col.PlanColumnAssetReachability(context.Background(), ColumnAssetReachabilityOptions{Detailed: true})
	if err != nil {
		t.Fatalf("PlanColumnAssetReachability: %v", err)
	}
	if !plan.Complete {
		t.Fatalf("reachability plan incomplete: refs=%+v segments=%+v", plan.Refs, plan.Segments)
	}
	assertColumnGraphReachabilityRefAbsentV2A(t, plan, prepared.Ref)
	assertColumnGraphSegmentStatusV2A(t, plan, prepared.Ref.FileID, ColumnAssetReachabilitySegmentReclaimable)
}

func TestColumnGraphManifestRetentionPrunesDroppedVectorIndexV2A(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0}},
		{id: "doc-b", vector: []float32{0, 1}},
	}
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(t, 2, 1, rows)
	defer func() { _ = d.Close() }()
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("RebuildVectorIndex: %v", err)
	}
	graph, _ := loadAndScanColumnGraphRebuildRowsV2A(t, d, "docs", def)
	records := loadColumnGraphRebuildManifestRecordsV2A(t, d, "docs")

	retainedWithIndex, err := retainedColumnManifestRecordsForWrite(records, graph.BaseManifestGeneration+1, true, []VectorIndexDefinition{def})
	if err != nil {
		t.Fatalf("retained active vector graph record: %v", err)
	}
	if _, ok := findColumnVectorGraphManifestRecord(retainedWithIndex, def.Name); !ok {
		t.Fatalf("active vector index graph record was pruned: records=%d", len(retainedWithIndex))
	}

	retainedAfterDrop, err := retainedColumnManifestRecordsForWrite(records, graph.BaseManifestGeneration+1, true, nil)
	if err != nil {
		t.Fatalf("retained dropped vector graph record: %v", err)
	}
	if _, ok := findColumnVectorGraphManifestRecord(retainedAfterDrop, def.Name); ok {
		t.Fatalf("dropped vector index graph record was retained: records=%+v", retainedAfterDrop)
	}
}

func TestColumnGraphManifestRetentionRejectsCorruptVectorGraphRecordV2A(t *testing.T) {
	records, def, identity := testColumnGraphRetentionRecordsV2A(t)
	corrupted := false
	for i := range records {
		if bytes.Equal(records[i].key, columnVectorGraphManifestRecordKey(def.Name)) {
			records[i].value = append(bytes.Clone(records[i].value), 0xff)
			corrupted = true
			break
		}
	}
	if !corrupted {
		t.Fatalf("test vector graph record %q missing", def.Name)
	}

	_, err := retainedColumnManifestRecordsForWrite(records, identity.Generation+1, true, []VectorIndexDefinition{def})
	if err == nil {
		t.Fatalf("retained corrupt vector graph record without error")
	}
	if !strings.Contains(err.Error(), "trailing bytes in column vector graph manifest record") {
		t.Fatalf("error=%v, want trailing vector graph manifest error", err)
	}
}

func TestColumnGraphManifestRetentionSkipsFutureVectorGraphRecordV2A(t *testing.T) {
	records, def, identity := testColumnGraphRetentionRecordsV2A(t)

	retained, err := retainedColumnManifestRecordsForWrite(records, identity.Generation, true, []VectorIndexDefinition{def})
	if err != nil {
		t.Fatalf("retained future vector graph record: %v", err)
	}
	if _, ok := findColumnVectorGraphManifestRecord(retained, def.Name); ok {
		t.Fatalf("future vector graph record was retained: records=%+v", retained)
	}
}

func TestColumnGraphManifestRetentionRejectsMismatchedVectorGraphGenerationV2A(t *testing.T) {
	records, def, identity := testColumnGraphRetentionRecordsV2A(t)
	mutated := false
	for i := range records {
		if !bytes.Equal(records[i].key, columnVectorGraphManifestRecordKey(def.Name)) {
			continue
		}
		graph, err := decodeColumnVectorGraphManifestRecord(records[i].value)
		if err != nil {
			t.Fatalf("decodeColumnVectorGraphManifestRecord: %v", err)
		}
		graph.BaseManifestGeneration--
		encoded, err := encodeColumnVectorGraphManifestRecord(graph)
		if err != nil {
			t.Fatalf("encodeColumnVectorGraphManifestRecord: %v", err)
		}
		records[i].value = encoded
		mutated = true
		break
	}
	if !mutated {
		t.Fatalf("test vector graph record %q missing", def.Name)
	}

	_, err := retainedColumnManifestRecordsForWrite(records, identity.Generation+1, true, []VectorIndexDefinition{def})
	if err == nil {
		t.Fatalf("retained mismatched vector graph generation without error")
	}
	if !strings.Contains(err.Error(), "base manifest generation") {
		t.Fatalf("error=%v, want base manifest generation mismatch", err)
	}
}

func TestColumnGraphRebuildManifestRecordsPrunesDroppedVectorGraphsV2A(t *testing.T) {
	cfg := columnGraphRebuildColumnStoreConfigV2A(2)
	var err error
	cfg, err = normalizeColumnStoreConfig("docs", cfg)
	if err != nil {
		t.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	defA := columnGraphRebuildVectorIndexDefinitionV2A(2, 1)
	defB, err := normalizeVectorIndexDefinition(VectorIndexDefinition{
		Name:       "other_embedding_graph",
		Field:      "embedding",
		Metric:     VectorMetricCosine,
		Dimensions: 2,
		M:          1,
		Strategy:   VectorIndexStrategyColumnGraph,
	})
	if err != nil {
		t.Fatalf("normalizeVectorIndexDefinition: %v", err)
	}
	identity := ColumnManifestIdentity{Generation: 7, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 0x1234}
	refA := ColumnAssetRef{Kind: ColumnAssetKindTCS1PartImage, Namespace: cfg.AssetManager.Namespace, Generation: identity.Generation, FileID: 1, PartID: 1, Length: 1}
	refB := ColumnAssetRef{Kind: ColumnAssetKindTCS1PartImage, Namespace: cfg.AssetManager.Namespace, Generation: identity.Generation, FileID: 2, PartID: 2, Length: 1}
	recordsA, _ := testColumnGraphManifestRecordsV2A(t, *cfg, defA, identity, refA, 1, 1)
	recordsB, _ := testColumnGraphManifestRecordsV2A(t, *cfg, defB, identity, refB, 1, 1)
	combined := append([]columnManifestRecord(nil), recordsA...)
	graphB, ok := findColumnVectorGraphManifestRecord(recordsB, defB.Name)
	if !ok {
		t.Fatalf("test graph record %q missing", defB.Name)
	}
	combined = append(combined, graphB)
	sortColumnManifestRecords(combined)
	manifest, err := decodeColumnManifestSnapshotForScan(combined)
	if err != nil {
		t.Fatalf("decodeColumnManifestSnapshotForScan: %v", err)
	}
	got, err := columnVectorGraphManifestRecordsWithAppliedCommandLSN(manifest, combined, *cfg, []VectorIndexDefinition{defA}, 99)
	if err != nil {
		t.Fatalf("columnVectorGraphManifestRecordsWithAppliedCommandLSN: %v", err)
	}
	if _, ok := findColumnVectorGraphManifestRecord(got, defA.Name); !ok {
		t.Fatalf("active graph record %q was pruned", defA.Name)
	}
	if _, ok := findColumnVectorGraphManifestRecord(got, defB.Name); ok {
		t.Fatalf("dropped graph record %q was retained: records=%+v", defB.Name, got)
	}
}

func testColumnGraphRetentionRecordsV2A(tb testing.TB) ([]columnManifestRecord, VectorIndexDefinition, ColumnManifestIdentity) {
	tb.Helper()
	cfg := columnGraphRebuildColumnStoreConfigV2A(2)
	var err error
	cfg, err = normalizeColumnStoreConfig("docs", cfg)
	if err != nil {
		tb.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	def := columnGraphRebuildVectorIndexDefinitionV2A(2, 1)
	identity := ColumnManifestIdentity{Generation: 7, Format: columnManifestFormatTCS1, Version: columnManifestIdentityVersion, Checksum: 0x1234}
	ref := ColumnAssetRef{
		Kind:       ColumnAssetKindTCS1PartImage,
		Namespace:  cfg.AssetManager.Namespace,
		Generation: identity.Generation,
		FileID:     1,
		PartID:     1,
		Length:     128,
	}
	records, identity := testColumnGraphManifestRecordsV2A(tb, *cfg, def, identity, ref, ref.Length, 2)
	return records, def, identity
}

func TestRebuildVectorIndexPublishesCommandWALV2A(t *testing.T) {
	tests := []struct {
		name           string
		strategy       VectorIndexStrategy
		wantState      VectorIndexState
		wantReason     VectorIndexReason
		rebuildNeeded  bool
		wantNativeRoot bool
	}{
		{
			name:           "native_strategy",
			strategy:       VectorIndexStrategyNativeRuntime,
			wantState:      VectorIndexStateNativeRuntime,
			wantReason:     VectorIndexReasonNativeRuntime,
			wantNativeRoot: true,
		},
		{
			name:          "column_graph_physical_support_missing",
			strategy:      VectorIndexStrategyColumnGraph,
			wantState:     VectorIndexStateColumnGraphUnavailable,
			wantReason:    VectorIndexReasonPhysicalColumnAssetSupportMissing,
			rebuildNeeded: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			def, err := normalizeVectorIndexDefinition(VectorIndexDefinition{
				Name:       "embedding_graph",
				Field:      "embedding",
				Metric:     VectorMetricCosine,
				Dimensions: 3,
				Strategy:   tt.strategy,
			})
			if err != nil {
				t.Fatalf("normalizeVectorIndexDefinition: %v", err)
			}
			dir := prepareCollectionCommandWALDir(t, CollectionMeta{
				Name: "docs",
				Options: CollectionOptions{
					DocumentFormat: DocumentFormatJSON,
				},
				VectorIndexes: []VectorIndexDefinition{def},
			})
			d := openCollectionCommandWALDB(t, dir)
			defer func() { _ = d.Close() }()
			col, err := NewCollectionManager(d).OpenCollection("docs")
			if err != nil {
				t.Fatalf("OpenCollection: %v", err)
			}
			framesBefore := countCollectionCommandWALFrames(t, dir)
			baseLSN := d.State().AppliedCommandLSN

			status, err := col.RebuildVectorIndex(def.Name)
			if err != nil {
				t.Fatalf("RebuildVectorIndex: %v", err)
			}
			if status.State != tt.wantState || status.Reason != tt.wantReason || status.RebuildNeeded != tt.rebuildNeeded {
				t.Fatalf("status=%+v want state=%q reason=%q rebuild=%t", status, tt.wantState, tt.wantReason, tt.rebuildNeeded)
			}
			if tt.wantNativeRoot && (!status.Loaded || status.RootID == 0 || !status.NativeRootLoaded) {
				t.Fatalf("native rebuild status=%+v want published root", status)
			}
			if got := d.State().AppliedCommandLSN; got <= baseLSN {
				t.Fatalf("AppliedCommandLSN after rebuild=%d want > %d", got, baseLSN)
			}
			frames := collectionCommandWALFrames(t, dir)
			if len(frames) != framesBefore+1 {
				t.Fatalf("command WAL frames after rebuild=%d want %d", len(frames), framesBefore+1)
			}
			frame := frames[len(frames)-1]
			if frame.Kind != commitlog.CommandKindCollectionRebuildVectorIndex || frame.PayloadFormat != commitlog.PayloadFormatCollectionRebuildVectorIndexV1 {
				t.Fatalf("last command WAL frame kind=%d format=%d, want rebuild vector index v1", frame.Kind, frame.PayloadFormat)
			}
			payload, err := commitlog.DecodeCollectionRebuildVectorIndexPayload(frame.Payload)
			if err != nil {
				t.Fatalf("DecodeCollectionRebuildVectorIndexPayload: %v", err)
			}
			if payload.Collection != "docs" || payload.IndexName != def.Name {
				t.Fatalf("rebuild payload=%+v want collection=docs index=%s", payload, def.Name)
			}
		})
	}
}

func TestColumnGraphRebuildVectorIndexCommandWALReplayV2A(t *testing.T) {
	rows := []columnGraphRebuildInputRowV2A{
		{id: "doc-a", vector: []float32{1, 0, 0}},
		{id: "doc-b", vector: []float32{0, 1, 0}},
		{id: "doc-c", vector: []float32{0, 0, 1}},
	}
	dir, d, _, def := openColumnGraphRebuildTestCollectionV2A(t, 3, 0, rows)
	baseLSN := d.State().AppliedCommandLSN
	if baseLSN == 0 {
		_ = d.Close()
		t.Fatal("setup AppliedCommandLSN=0, want create/insert command WAL frames")
	}
	if err := d.Checkpoint(); err != nil {
		_ = d.Close()
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close setup DB: %v", err)
	}
	payload, err := commitlog.EncodeCollectionRebuildVectorIndexPayload("docs", def.Name)
	if err != nil {
		t.Fatalf("EncodeCollectionRebuildVectorIndexPayload: %v", err)
	}
	writeCollectionCommandWALFrame(t, dir, baseLSN+1, commitlog.CommandKindCollectionRebuildVectorIndex, commitlog.PayloadFormatCollectionRebuildVectorIndexV1, payload)

	reopened := openCollectionCommandWALDB(t, dir)
	defer func() { _ = reopened.Close() }()
	if got := reopened.State().AppliedCommandLSN; got != baseLSN+1 {
		t.Fatalf("AppliedCommandLSN after rebuild replay=%d want %d", got, baseLSN+1)
	}
	col, err := NewCollectionManager(reopened).OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	status, err := col.VectorIndexStatus(def.Name)
	if err != nil {
		t.Fatalf("VectorIndexStatus: %v", err)
	}
	assertColumnGraphRebuildLoadedStatusV2A(t, status, def.Name)
	graph, scanned := loadAndScanColumnGraphRebuildRowsV2A(t, reopened, "docs", def)
	if graph.RowCount != len(rows) || len(scanned) != len(rows) {
		t.Fatalf("replayed graph row count=%d scanned=%d want %d", graph.RowCount, len(scanned), len(rows))
	}
}

func TestRebuildVectorIndexCommandWALReplayAdvancesLSNV2A(t *testing.T) {
	tests := []struct {
		name           string
		strategy       VectorIndexStrategy
		wantState      VectorIndexState
		wantReason     VectorIndexReason
		rebuildNeeded  bool
		wantNativeRoot bool
	}{
		{
			name:           "native_strategy",
			strategy:       VectorIndexStrategyNativeRuntime,
			wantState:      VectorIndexStateNativeRuntime,
			wantReason:     VectorIndexReasonNativeRuntime,
			wantNativeRoot: true,
		},
		{
			name:          "column_graph_physical_support_missing",
			strategy:      VectorIndexStrategyColumnGraph,
			wantState:     VectorIndexStateColumnGraphUnavailable,
			wantReason:    VectorIndexReasonPhysicalColumnAssetSupportMissing,
			rebuildNeeded: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			def, err := normalizeVectorIndexDefinition(VectorIndexDefinition{
				Name:       "embedding_graph",
				Field:      "embedding",
				Metric:     VectorMetricCosine,
				Dimensions: 3,
				Strategy:   tt.strategy,
			})
			if err != nil {
				t.Fatalf("normalizeVectorIndexDefinition: %v", err)
			}
			dir := prepareCollectionCommandWALDir(t, CollectionMeta{
				Name: "docs",
				Options: CollectionOptions{
					DocumentFormat: DocumentFormatJSON,
				},
				VectorIndexes: []VectorIndexDefinition{def},
			})
			payload, err := commitlog.EncodeCollectionRebuildVectorIndexPayload("docs", def.Name)
			if err != nil {
				t.Fatalf("EncodeCollectionRebuildVectorIndexPayload: %v", err)
			}
			writeCollectionCommandWALFrame(t, dir, 1, commitlog.CommandKindCollectionRebuildVectorIndex, commitlog.PayloadFormatCollectionRebuildVectorIndexV1, payload)

			reopened := openCollectionCommandWALDB(t, dir)
			defer func() { _ = reopened.Close() }()
			if got := reopened.State().AppliedCommandLSN; got != 1 {
				t.Fatalf("AppliedCommandLSN after rebuild replay=%d want 1", got)
			}
			col, err := NewCollectionManager(reopened).OpenCollection("docs")
			if err != nil {
				t.Fatalf("OpenCollection: %v", err)
			}
			status, err := col.VectorIndexStatus(def.Name)
			if err != nil {
				t.Fatalf("VectorIndexStatus: %v", err)
			}
			if status.State != tt.wantState || status.Reason != tt.wantReason || status.RebuildNeeded != tt.rebuildNeeded {
				t.Fatalf("status=%+v want state=%q reason=%q rebuild=%t", status, tt.wantState, tt.wantReason, tt.rebuildNeeded)
			}
			if tt.wantNativeRoot && (status.RootID == 0 || !status.NativeRootLoaded) {
				t.Fatalf("native replay status=%+v want published root", status)
			}
		})
	}
}

func BenchmarkColumnGraphRebuildVectorIndexV2A(b *testing.B) {
	const (
		rows = 128
		dims = 8
		m    = 8
	)
	input := columnGraphRebuildSyntheticRowsV2A(rows, dims)
	_, d, col, def := openColumnGraphRebuildTestCollectionV2A(b, dims, m, input)
	defer func() { _ = d.Close() }()
	b.ReportAllocs()
	b.ReportMetric(float64(rows), "rows/op")
	b.ReportMetric(float64(dims), "dims")
	b.ReportMetric(float64(m), "M")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		status, err := col.RebuildVectorIndex(def.Name)
		if err != nil {
			b.Fatalf("RebuildVectorIndex: %v", err)
		}
		if !status.Loaded || status.RebuildNeeded {
			b.Fatalf("status=%+v, want loaded", status)
		}
		columnGraphRebuildBenchSinkV2A = status
	}
	b.StopTimer()
	elapsed := b.Elapsed().Seconds()
	if elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed, "ops/sec")
	}
	graph, _ := loadAndScanColumnGraphRebuildRowsV2A(b, d, "docs", def)
	records, _ := loadColumnGraphRebuildManifestRecordsAndConfigV2A(b, d, "docs")
	state := columnVectorIndexStateFromRecords1987(b, records, def)
	b.ReportMetric(float64(graph.AssetBytes), "graph_asset_B/op")
	b.ReportMetric(float64(columnVectorIndexStateAssetsStorageBytes(state)), "state_assets_B/op")
	reportColumnVectorIndexStateAdjacencyStorageMetrics1990(b, d, state)
	reportColumnVectorGraphDocumentIDStateStorageMetrics2013(b, d, state)
	b.ReportMetric(float64(columnVectorGraphStorageBytesWithState(graph, state)), "graph_total_storage_B/op")
	if len(graph.AdjacencyLayerSources) > 0 {
		b.ReportMetric(float64(graph.AdjacencyLayerCount), "adjacency_layer_count")
		var sourceBytes, offsetsBytes, valuesBytes, paddingBytes int64
		for layer, source := range graph.AdjacencyLayerSources {
			sourceBytes += source.AssetBytes
			offsetsBytes += source.OffsetsBytes
			valuesBytes += source.ValuesBytes
			paddingBytes += source.PaddingBytes
			b.ReportMetric(float64(source.AssetBytes), fmt.Sprintf("layer%d_source_B/op", layer))
			b.ReportMetric(float64(source.OffsetsBytes), fmt.Sprintf("layer%d_offsets_B/op", layer))
			b.ReportMetric(float64(source.ValuesBytes), fmt.Sprintf("layer%d_values_B/op", layer))
			b.ReportMetric(float64(source.PaddingBytes), fmt.Sprintf("layer%d_padding_B/op", layer))
		}
		b.ReportMetric(float64(sourceBytes), "adjacency_sources_B/op")
		b.ReportMetric(float64(offsetsBytes), "adjacency_offsets_B/op")
		b.ReportMetric(float64(valuesBytes), "adjacency_values_B/op")
		b.ReportMetric(float64(paddingBytes), "adjacency_padding_B/op")
	} else if graph.Layer0AdjacencySource.Present {
		b.ReportMetric(1, "adjacency_layer_count")
		b.ReportMetric(float64(graph.Layer0AdjacencySource.AssetBytes), "layer0_source_B/op")
		b.ReportMetric(float64(graph.Layer0AdjacencySource.OffsetsBytes), "layer0_offsets_B/op")
		b.ReportMetric(float64(graph.Layer0AdjacencySource.ValuesBytes), "layer0_values_B/op")
		b.ReportMetric(float64(graph.Layer0AdjacencySource.PaddingBytes), "layer0_padding_B/op")
	}
}

func reportColumnVectorGraphDocumentIDStateStorageMetrics2013(b *testing.B, d *backenddb.DB, state columnVectorIndexStateSnapshot) {
	b.Helper()
	asset, found, err := findColumnVectorGraphDocumentIDStateAsset(state)
	if err != nil {
		b.Fatalf("document-id state asset: %v", err)
	}
	if !found {
		return
	}
	raw, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), asset.Ref)
	if err != nil {
		b.Fatalf("read document-id state asset: %v", err)
	}
	image, err := typedcolumn.ParseColumnPartImage(raw)
	if err != nil {
		b.Fatalf("parse document-id state asset: %v", err)
	}
	offsetsSection, valuesSection, ok := image.ColumnOffsetsListSections(columnVectorGraphDocumentIDStateColumnName)
	if !ok {
		b.Fatalf("document-id state missing offsets/value sections for %q", columnVectorGraphDocumentIDStateColumnName)
	}
	b.ReportMetric(float64(asset.AssetBytes), "document_id_state_asset_B/op")
	b.ReportMetric(float64(offsetsSection.Length), "document_id_state_offsets_B/op")
	b.ReportMetric(float64(valuesSection.Length), "document_id_state_values_B/op")
	b.ReportMetric(float64(image.PaddingBytes()), "document_id_state_padding_B/op")
	if asset.RowCount > 0 {
		b.ReportMetric(float64(asset.AssetBytes)/float64(asset.RowCount), "document_id_state_asset_B/row")
		b.ReportMetric(float64(valuesSection.Length)/float64(asset.RowCount), "document_id_state_values_B/row")
	}
}

func reportColumnVectorIndexStateAdjacencyStorageMetrics1990(b *testing.B, d *backenddb.DB, state columnVectorIndexStateSnapshot) {
	b.Helper()
	var layers, offsetsBytes, valuesBytes, valuesCount, assetBytes, paddingBytes int64
	for _, asset := range state.Assets {
		if asset.Role != columnVectorIndexStateAssetRoleAdjacency {
			continue
		}
		if asset.LogicalType != columnVectorIndexStateLogicalTypeUint32List || asset.PhysicalEncoding != columnVectorIndexStateEncodingRawUint32List {
			b.Fatalf("state adjacency asset %q type/encoding=(%q,%q), want uint32_list/raw_uint32_offsets_list", asset.AssetID, asset.LogicalType, asset.PhysicalEncoding)
		}
		layer, err := columnVectorIndexStateAdjacencyLayerFromAssetID(asset.AssetID)
		if err != nil {
			b.Fatalf("state adjacency asset id %q: %v", asset.AssetID, err)
		}
		raw, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), asset.Ref)
		if err != nil {
			b.Fatalf("read state adjacency asset %q: %v", asset.AssetID, err)
		}
		image, err := typedcolumn.ParseColumnPartImage(raw)
		if err != nil {
			b.Fatalf("parse state adjacency asset %q: %v", asset.AssetID, err)
		}
		offsetsSection, valuesSection, ok := image.ColumnOffsetsListSections(columnVectorIndexStateAdjacencyColumnName)
		if !ok {
			b.Fatalf("state adjacency asset %q missing offsets/value sections for %q", asset.AssetID, columnVectorIndexStateAdjacencyColumnName)
		}
		wantOffsetsBytes, err := columnVectorIndexStateAdjacencyOffsetsBytes(asset.RowCount)
		if err != nil {
			b.Fatalf("state adjacency asset %q offsets bytes: %v", asset.AssetID, err)
		}
		if offsetsSection.Length != wantOffsetsBytes {
			b.Fatalf("state adjacency asset %q offsets bytes=%d want %d", asset.AssetID, offsetsSection.Length, wantOffsetsBytes)
		}
		if valuesSection.Length%4 != 0 {
			b.Fatalf("state adjacency asset %q values bytes=%d not uint32 aligned", asset.AssetID, valuesSection.Length)
		}
		layers++
		offsetsBytes += int64(offsetsSection.Length)
		valuesBytes += int64(valuesSection.Length)
		valuesCount += int64(valuesSection.Length / 4)
		assetBytes += asset.AssetBytes
		paddingBytes += int64(image.PaddingBytes())
		b.ReportMetric(float64(offsetsSection.Length), fmt.Sprintf("state_layer%d_offsets_B/op", layer))
		b.ReportMetric(float64(valuesSection.Length), fmt.Sprintf("state_layer%d_values_B/op", layer))
		b.ReportMetric(float64(valuesSection.Length/4), fmt.Sprintf("state_layer%d_values/op", layer))
		b.ReportMetric(float64(asset.AssetBytes), fmt.Sprintf("state_layer%d_asset_B/op", layer))
	}
	b.ReportMetric(float64(layers), "state_adjacency_layers/op")
	b.ReportMetric(float64(offsetsBytes), "state_adjacency_offsets_B/op")
	b.ReportMetric(float64(valuesBytes), "state_adjacency_values_B/op")
	b.ReportMetric(float64(valuesCount), "state_adjacency_values/op")
	b.ReportMetric(float64(assetBytes), "state_adjacency_assets_B/op")
	b.ReportMetric(float64(paddingBytes), "state_adjacency_padding_B/op")
}

type columnGraphRebuildInputRowV2A struct {
	id     string
	vector []float32
}

type columnGraphRebuildDualInputRowV2A struct {
	id             string
	embedding      []float32
	otherEmbedding []float32
}

type columnGraphRebuildScannedRowV2A struct {
	id        string
	vector    []float32
	invNorm   float32
	adjacency []uint32
}

func openColumnGraphRebuildTestCollectionV2A(tb testing.TB, dims, m int, rows []columnGraphRebuildInputRowV2A) (string, *backenddb.DB, *Collection, VectorIndexDefinition) {
	tb.Helper()
	dir := tb.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		tb.Fatalf("SaveFormatConfig: %v", err)
	}
	d := openCollectionCommandWALDB(tb, dir)
	def := columnGraphRebuildVectorIndexDefinitionV2A(dims, m)
	meta := CollectionMeta{
		Name: "docs",
		Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
			ColumnStore:    columnGraphRebuildColumnStoreConfigV2A(dims),
		},
		VectorIndexes: []VectorIndexDefinition{def},
	}
	if _, err := NewCollectionManager(d).CreateCollection(&meta); err != nil {
		_ = d.Close()
		tb.Fatalf("CreateCollection: %v", err)
	}
	col, err := NewCollectionManager(d).OpenCollection("docs")
	if err != nil {
		_ = d.Close()
		tb.Fatalf("OpenCollection: %v", err)
	}
	if len(rows) != 0 {
		insertColumnGraphRebuildRowsV2A(tb, col, rows)
	}
	return dir, d, col, def
}

func columnGraphRebuildColumnStoreConfigV2A(dims int) *ColumnStoreConfig {
	cfg := testColumnStoreConfig(nil)
	cfg.Columns = append(append([]ColumnStoreColumn(nil), cfg.Columns...), ColumnStoreColumn{
		Name:       "embedding",
		Path:       "embedding",
		Owner:      TypedStorageOwnerColumnPart,
		ValueType:  ColumnStoreValueFloat32Vector,
		VectorDims: dims,
	})
	return cfg
}

func columnGraphRebuildVectorIndexDefinitionV2A(dims, m int) VectorIndexDefinition {
	def, err := normalizeVectorIndexDefinition(VectorIndexDefinition{
		Name:       "embedding_graph",
		Field:      "embedding",
		Metric:     VectorMetricCosine,
		Dimensions: dims,
		M:          m,
		Strategy:   VectorIndexStrategyColumnGraph,
	})
	if err != nil {
		panic(err)
	}
	return def
}

type columnGraphRebuildNativeGraphLayoutResultV2A struct {
	ids       []string
	adjacency [][]uint32
}

func columnGraphRebuildNativeGraphLayoutV2A(tb testing.TB, def VectorIndexDefinition, rows []columnGraphRebuildInputRowV2A) columnGraphRebuildNativeGraphLayoutResultV2A {
	tb.Helper()
	index, err := newVectorIndex(nil, vectorIndexOptionsFromDefinition(def))
	if err != nil {
		tb.Fatalf("newVectorIndex: %v", err)
	}
	index.mu.Lock()
	defer index.mu.Unlock()
	for i := range rows {
		if err := index.insertVectorLocked([]byte(rows[i].id), rows[i].vector); err != nil {
			tb.Fatalf("insertVectorLocked row %d: %v", i, err)
		}
	}
	inputOrdinalByNode := make([]int, len(index.nodes))
	for i := range inputOrdinalByNode {
		inputOrdinalByNode[i] = -1
	}
	for i := range rows {
		nodeID, ok := index.currentNode[rows[i].id]
		if !ok {
			tb.Fatalf("native graph missing row %q", rows[i].id)
		}
		inputOrdinalByNode[nodeID] = i
	}
	order := columnVectorGraphNativeLocalityOrder(index)
	nodeOrdinal := make([]int, len(index.nodes))
	for i := range nodeOrdinal {
		nodeOrdinal[i] = -1
	}
	ids := make([]string, len(order))
	for ordinal, nodeID := range order {
		if nodeID < 0 || nodeID >= len(inputOrdinalByNode) || inputOrdinalByNode[nodeID] < 0 {
			tb.Fatalf("native graph locality node %d missing input row", nodeID)
		}
		nodeOrdinal[nodeID] = ordinal
		ids[ordinal] = rows[inputOrdinalByNode[nodeID]].id
	}
	adjacency := make([][]uint32, len(rows))
	for ordinal, nodeID := range order {
		encoded, err := columnVectorGraphLayeredAdjacencyFromNativeNode(&index.nodes[nodeID], nodeOrdinal)
		if err != nil {
			tb.Fatalf("columnVectorGraphLayeredAdjacencyFromNativeNode ordinal %d: %v", ordinal, err)
		}
		adjacency[ordinal] = encoded
	}
	return columnGraphRebuildNativeGraphLayoutResultV2A{ids: ids, adjacency: adjacency}
}

func uint32SlicesEqual(a, b []uint32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func insertColumnGraphRebuildRowsV2A(tb testing.TB, col *Collection, rows []columnGraphRebuildInputRowV2A) {
	tb.Helper()
	ids := make([][]byte, len(rows))
	docs := make([][]byte, len(rows))
	for i, row := range rows {
		raw, err := json.Marshal(map[string]any{
			"time_us":   int64(i + 1),
			"kind":      "vector",
			"did":       row.id,
			"embedding": row.vector,
		})
		if err != nil {
			tb.Fatalf("json.Marshal row %q: %v", row.id, err)
		}
		ids[i] = []byte(row.id)
		docs[i] = raw
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		tb.Fatalf("InsertBatch: %v", err)
	}
	if err := col.Flush(); err != nil {
		tb.Fatalf("Flush: %v", err)
	}
}

func insertColumnGraphRebuildDualVectorRowsV2A(tb testing.TB, col *Collection, rows []columnGraphRebuildDualInputRowV2A) {
	tb.Helper()
	ids := make([][]byte, len(rows))
	docs := make([][]byte, len(rows))
	for i, row := range rows {
		raw, err := json.Marshal(map[string]any{
			"time_us":         int64(i + 1),
			"kind":            "vector",
			"did":             row.id,
			"embedding":       row.embedding,
			"other_embedding": row.otherEmbedding,
		})
		if err != nil {
			tb.Fatalf("json.Marshal row %q: %v", row.id, err)
		}
		ids[i] = []byte(row.id)
		docs[i] = raw
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		tb.Fatalf("InsertBatch: %v", err)
	}
	if err := col.Flush(); err != nil {
		tb.Fatalf("Flush: %v", err)
	}
}

func publishColumnGraphRebuildMetadataWithoutManifestRootV2A(tb testing.TB, d *backenddb.DB, collection string, cfg *ColumnStoreConfig, vectorIndexes []VectorIndexDefinition) {
	tb.Helper()
	snap := d.AcquireSnapshot()
	if snap == nil {
		tb.Fatal("AcquireSnapshot returned nil")
	}
	catalog, err := loadCollectionCatalog(snap, collection)
	_ = snap.Close()
	if err != nil {
		tb.Fatalf("loadCollectionCatalog: %v", err)
	}
	if catalog == nil {
		tb.Fatalf("collection %q missing", collection)
	}
	meta := copyCollectionMeta(catalog.meta)
	meta.Options.ColumnStore = cfg
	meta.VectorIndexes = append([]VectorIndexDefinition(nil), vectorIndexes...)
	normalized, err := normalizeCollectionMeta(meta)
	if err != nil {
		tb.Fatalf("normalizeCollectionMeta: %v", err)
	}
	encoded, err := encodeCollectionMeta(normalized)
	if err != nil {
		tb.Fatalf("encodeCollectionMeta: %v", err)
	}
	_, _, err = d.PublishOrderedRootGroupWithSystemBuilder(nil, func([]uint64) (iterator.UnsafeIterator, error) {
		current := d.AcquireSnapshot()
		if current == nil {
			return nil, backenddb.ErrClosed
		}
		defer func() { _ = current.Close() }()
		return buildSystemTargetIterator(current, map[string][]byte{
			systemCollectionMetaKey(normalized.Name): encoded,
		})
	})
	if err != nil {
		tb.Fatalf("publish collection metadata without manifest root: %v", err)
	}
}

func loadAndScanColumnGraphRebuildRowsV2A(tb testing.TB, d *backenddb.DB, collection string, def VectorIndexDefinition) (columnVectorGraphManifestSnapshot, []columnGraphRebuildScannedRowV2A) {
	tb.Helper()
	records, cfg := loadColumnGraphRebuildManifestRecordsAndConfigV2A(tb, d, collection)
	record, ok := findColumnVectorGraphManifestRecord(records, def.Name)
	if !ok {
		tb.Fatalf("graph manifest record %q missing from %d records", def.Name, len(records))
	}
	graph, err := decodeColumnVectorGraphManifestRecord(record.value)
	if err != nil {
		tb.Fatalf("decodeColumnVectorGraphManifestRecord: %v", err)
	}
	if !columnVectorGraphManifestHasPhysicalAsset(graph) {
		return graph, loadColumnGraphRebuildRowsFromStateV2A(tb, d, collection, cfg, def, graph, records)
	}
	graphCfg, err := columnVectorGraphPhysicalColumnStoreConfig(collection, *cfg, def)
	if err != nil {
		tb.Fatalf("columnVectorGraphPhysicalColumnStoreConfig: %v", err)
	}
	raw, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), graph.AssetRef)
	if err != nil {
		tb.Fatalf("read graph asset: %v", err)
	}
	if err := validateColumnPhysicalAssetForManifest(raw, graph.AssetRef, graphCfg); err != nil {
		tb.Fatalf("validate graph asset: %v", err)
	}
	projection, err := newColumnPhysicalScanProjection(graphCfg, []string{
		columnVectorGraphVectorColumnName,
		columnVectorGraphInvNormColumnName,
		columnVectorGraphAdjacencyColumnName,
	})
	if err != nil {
		tb.Fatalf("newColumnPhysicalScanProjection: %v", err)
	}
	var rows []columnGraphRebuildScannedRowV2A
	summary, err := scanColumnPhysicalAssetRows(raw, graph.AssetRef, collection, &graphCfg, projection, func(row columnPhysicalScanRowView) error {
		if len(row.Values) != 3 {
			return fmt.Errorf("graph row values=%d want 3", len(row.Values))
		}
		rows = append(rows, columnGraphRebuildScannedRowV2A{
			id:        string(row.ID),
			vector:    append([]float32(nil), row.Values[0].Float32Vector...),
			invNorm:   row.Values[1].Float32,
			adjacency: append([]uint32(nil), row.Values[2].AdjacencyList...),
		})
		return nil
	})
	if err != nil {
		tb.Fatalf("scanColumnPhysicalAssetRows: %v", err)
	}
	if summary.rows != graph.RowCount {
		tb.Fatalf("scan summary rows=%d graph rows=%d", summary.rows, graph.RowCount)
	}
	for _, row := range rows {
		if row.invNorm <= 0 || math.IsNaN(float64(row.invNorm)) || math.IsInf(float64(row.invNorm), 0) {
			tb.Fatalf("row %q invNorm=%v, want finite positive", row.id, row.invNorm)
		}
	}
	return graph, rows
}

func loadColumnGraphRebuildRowsFromStateV2A(tb testing.TB, d *backenddb.DB, collection string, cfg *ColumnStoreConfig, def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot, records []columnManifestRecord) []columnGraphRebuildScannedRowV2A {
	tb.Helper()
	state := columnVectorIndexStateFromRecords1987(tb, records, def)
	documentIDs, _, err := newColumnVectorGraphDocumentIDStateSourceFromRoot(d.ColumnAssetRootDir(), collection, *cfg, def, graph, state)
	if err != nil {
		tb.Fatalf("open document-id state source: %v", err)
	}
	if documentIDs != nil {
		defer func() { _ = documentIDs.Close() }()
	}
	invNorms, _, err := newColumnVectorGraphInvNormStateSourceFromRoot(d.ColumnAssetRootDir(), collection, *cfg, def, graph, state)
	if err != nil {
		tb.Fatalf("open inv_norm state source: %v", err)
	}
	if invNorms != nil {
		defer func() { _ = invNorms.Close() }()
	}
	layers := make([]typedcolumn.RawUint32OffsetsList, state.AdjacencyLayerCount)
	for layer := range layers {
		layers[layer] = loadColumnVectorIndexStateAdjacencyList1987(tb, d, collection, cfg, def, state, layer)
	}
	col, err := NewCollectionManager(d).OpenCollection(collection)
	if err != nil {
		tb.Fatalf("OpenCollection for state rows: %v", err)
	}
	rows := make([]columnGraphRebuildScannedRowV2A, graph.RowCount)
	for ordinal := range rows {
		id, ok := documentIDs.documentIDForOrdinal(ordinal)
		if !ok {
			tb.Fatalf("document-id state missing ordinal %d", ordinal)
		}
		document, err := col.Get(id)
		if err != nil {
			tb.Fatalf("Get %q: %v", string(id), err)
		}
		vector := columnGraphRebuildVectorFromDocumentV2A(tb, collection, def, id, document)
		invNorm, _, _, ok := invNorms.invNormForOrdinal(ordinal)
		if !ok {
			tb.Fatalf("inv_norm state missing ordinal %d", ordinal)
		}
		rows[ordinal] = columnGraphRebuildScannedRowV2A{
			id:        string(id),
			vector:    vector,
			invNorm:   invNorm,
			adjacency: columnGraphRebuildAdjacencyFromStateLayersV2A(tb, layers, ordinal),
		}
	}
	return rows
}

func columnGraphRebuildVectorFromDocumentV2A(tb testing.TB, collection string, def VectorIndexDefinition, id []byte, document []byte) []float32 {
	tb.Helper()
	vectorCfg, err := normalizeColumnStoreConfig(collection, &ColumnStoreConfig{
		Enabled: true,
		Columns: []ColumnStoreColumn{{
			Name:       columnVectorGraphVectorColumnName,
			Path:       def.Field,
			ValueType:  ColumnStoreValueFloat32Vector,
			VectorDims: def.Dimensions,
		}},
	})
	if err != nil {
		tb.Fatalf("normalize vector config: %v", err)
	}
	declared, err := extractColumnDeclaredRowsFromJSONDocuments(*vectorCfg, []columnWriteDocument{{ID: id, Document: document}})
	if err != nil {
		tb.Fatalf("extract vector for %q: %v", string(id), err)
	}
	if len(declared) != 1 || len(declared[0].Values) != 1 {
		values := 0
		if len(declared) != 0 {
			values = len(declared[0].Values)
		}
		tb.Fatalf("extract vector rows=%d values=%d want 1 row and 1 value", len(declared), values)
	}
	value := declared[0].Values[0]
	if !value.Present || value.Null || len(value.Float32Vector) != def.Dimensions {
		tb.Fatalf("extract vector for %q present/null/dims=(%t,%t,%d) want present non-null dims=%d", string(id), value.Present, value.Null, len(value.Float32Vector), def.Dimensions)
	}
	return append([]float32(nil), value.Float32Vector...)
}

func columnGraphRebuildAdjacencyFromStateLayersV2A(tb testing.TB, layers []typedcolumn.RawUint32OffsetsList, ordinal int) []uint32 {
	tb.Helper()
	if len(layers) == 0 {
		return nil
	}
	if len(layers) == 1 {
		row, err := layers[0].Row(ordinal)
		if err != nil {
			tb.Fatalf("adjacency layer 0 row %d: %v", ordinal, err)
		}
		return append([]uint32(nil), row...)
	}
	maxLayer := 0
	layerRows := make([][]uint32, len(layers))
	for layer := range layers {
		row, err := layers[layer].Row(ordinal)
		if err != nil {
			tb.Fatalf("adjacency layer %d row %d: %v", layer, ordinal, err)
		}
		layerRows[layer] = row
		if len(row) > 0 {
			maxLayer = layer
		}
	}
	if maxLayer == 0 {
		return append([]uint32(nil), layerRows[0]...)
	}
	total := 2 + maxLayer + 1
	for layer := 0; layer <= maxLayer; layer++ {
		total += len(layerRows[layer])
	}
	encoded := make([]uint32, 0, total)
	encoded = append(encoded, columnVectorGraphLayeredAdjacencyMagic, uint32(maxLayer))
	for _, row := range layerRows[:maxLayer+1] {
		encoded = append(encoded, uint32(len(row)))
		encoded = append(encoded, row...)
	}
	return encoded
}

func loadColumnGraphLayer0AdjacencySourceList1918(tb testing.TB, d *backenddb.DB, collection string, cfg *ColumnStoreConfig, def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot) typedcolumn.RawUint32OffsetsList {
	tb.Helper()
	if !graph.Layer0AdjacencySource.Present {
		tb.Fatalf("graph %q missing layer-0 adjacency source", def.Name)
	}
	list, _, err := decodeColumnVectorGraphLayer0AdjacencySourceAsset(d.ColumnAssetRootDir(), collection, *cfg, def, graph)
	if err != nil {
		tb.Fatalf("decodeColumnVectorGraphLayer0AdjacencySourceAsset: %v", err)
	}
	return list
}

func loadColumnGraphAdjacencyLayerSourceList1920(tb testing.TB, d *backenddb.DB, collection string, cfg *ColumnStoreConfig, def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot, layer int) typedcolumn.RawUint32OffsetsList {
	tb.Helper()
	list, _, err := decodeColumnVectorGraphAdjacencyLayerSourceAsset(d.ColumnAssetRootDir(), collection, *cfg, def, graph, layer)
	if err != nil {
		tb.Fatalf("decodeColumnVectorGraphAdjacencyLayerSourceAsset layer %d: %v", layer, err)
	}
	return list
}

func layer0AdjacencySourceFromScannedRows1918(tb testing.TB, rows []columnGraphRebuildScannedRowV2A) typedcolumn.RawUint32OffsetsList {
	tb.Helper()
	return adjacencyLayerSourceFromScannedRows1920(tb, rows, 0)
}

func adjacencyLayerSourceFromScannedRows1920(tb testing.TB, rows []columnGraphRebuildScannedRowV2A, layer int) typedcolumn.RawUint32OffsetsList {
	tb.Helper()
	offsets := make([]uint64, len(rows)+1)
	values := make([]uint32, 0)
	for i, row := range rows {
		layerAdjacency, err := columnVectorGraphAdjacencyLayer(row.adjacency, layer)
		if err != nil {
			tb.Fatalf("columnVectorGraphAdjacencyLayer row %d layer %d: %v", i, layer, err)
		}
		values = append(values, layerAdjacency...)
		offsets[i+1] = uint64(len(values))
	}
	return typedcolumn.RawUint32OffsetsList{Rows: len(rows), Offsets: offsets, Values: values}
}

func adjacencyLayerSourceFromAssetRows1920(tb testing.TB, rows []columnVectorGraphAssetRow, layer int) typedcolumn.RawUint32OffsetsList {
	tb.Helper()
	offsets := make([]uint64, len(rows)+1)
	values := make([]uint32, 0)
	for i, row := range rows {
		layerAdjacency, err := columnVectorGraphAdjacencyLayer(row.Adjacency, layer)
		if err != nil {
			tb.Fatalf("columnVectorGraphAdjacencyLayer row %d layer %d: %v", i, layer, err)
		}
		values = append(values, layerAdjacency...)
		offsets[i+1] = uint64(len(values))
	}
	return typedcolumn.RawUint32OffsetsList{Rows: len(rows), Offsets: offsets, Values: values}
}

func assertColumnGraphAllLayerSourcesMatchScanned1920(tb testing.TB, d *backenddb.DB, collection string, cfg *ColumnStoreConfig, def VectorIndexDefinition, graph columnVectorGraphManifestSnapshot, rows []columnGraphRebuildScannedRowV2A) {
	tb.Helper()
	if graph.AdjacencyLayerCount != len(graph.AdjacencyLayerSources) || graph.AdjacencyLayerCount == 0 {
		tb.Fatalf("graph adjacency layer count=%d sources=%d, want non-empty matching all-layer metadata", graph.AdjacencyLayerCount, len(graph.AdjacencyLayerSources))
	}
	if graph.Layer0AdjacencySource != graph.AdjacencyLayerSources[0] {
		tb.Fatalf("layer-0 source alias=%+v want all-layer source[0]=%+v", graph.Layer0AdjacencySource, graph.AdjacencyLayerSources[0])
	}
	if err := validateColumnVectorGraphAdjacencyLayerSourcesAssets(d.ColumnAssetRootDir(), collection, *cfg, def, graph); err != nil {
		tb.Fatalf("validateColumnVectorGraphAdjacencyLayerSourcesAssets: %v", err)
	}
	var accounted int64
	for layer, source := range graph.AdjacencyLayerSources {
		if !source.Present || source.Layer != layer {
			tb.Fatalf("source[%d]=%+v want present matching layer", layer, source)
		}
		list := loadColumnGraphAdjacencyLayerSourceList1920(tb, d, collection, cfg, def, graph, layer)
		want := adjacencyLayerSourceFromScannedRows1920(tb, rows, layer)
		assertRawUint32OffsetsListEqual1918(tb, list, want)
		if source.OffsetsBytes != int64(len(want.Offsets))*8 || source.ValuesBytes != int64(len(want.Values))*4 {
			tb.Fatalf("layer %d source bytes offsets=%d values=%d want offsets=%d values=%d", layer, source.OffsetsBytes, source.ValuesBytes, len(want.Offsets)*8, len(want.Values)*4)
		}
		if source.PaddingBytes < 0 || source.AssetBytes <= source.OffsetsBytes+source.ValuesBytes {
			tb.Fatalf("layer %d source storage accounting=%+v", layer, source)
		}
		accounted += source.AssetBytes
	}
	if got, want := columnVectorGraphStorageBytes(graph), graph.AssetBytes+accounted; got != want {
		tb.Fatalf("graph storage bytes=%d want graph asset %d + layer sources %d = %d", got, graph.AssetBytes, accounted, want)
	}
}

func assertRawUint32OffsetsListEqual1918(tb testing.TB, got, want typedcolumn.RawUint32OffsetsList) {
	tb.Helper()
	if got.Rows != want.Rows || !uint64SlicesEqual(got.Offsets, want.Offsets) || !uint32SlicesEqual(got.Values, want.Values) {
		tb.Fatalf("offsets-list got rows=%d offsets=%v values=%v want rows=%d offsets=%v values=%v", got.Rows, got.Offsets, got.Values, want.Rows, want.Offsets, want.Values)
	}
}

func graphManifestFromRecords1918(tb testing.TB, records []columnManifestRecord, def VectorIndexDefinition) columnVectorGraphManifestSnapshot {
	tb.Helper()
	record, ok := findColumnVectorGraphManifestRecord(records, def.Name)
	if !ok {
		tb.Fatalf("graph manifest record %q missing", def.Name)
	}
	graph, err := decodeColumnVectorGraphManifestRecord(record.value)
	if err != nil {
		tb.Fatalf("decodeColumnVectorGraphManifestRecord: %v", err)
	}
	return graph
}

func prepareManualColumnGraphAllLayerSources1920(tb testing.TB) (*backenddb.DB, *ColumnStoreConfig, VectorIndexDefinition, columnVectorGraphManifestSnapshot, []columnVectorGraphAssetRow) {
	tb.Helper()
	d, err := backenddb.Open(backenddb.Options{Dir: tb.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		tb.Fatalf("Open: %v", err)
	}
	baseCfg, err := normalizeColumnStoreConfig("docs", columnGraphRebuildColumnStoreConfigV2A(3))
	if err != nil {
		_ = d.Close()
		tb.Fatalf("normalizeColumnStoreConfig: %v", err)
	}
	def := columnGraphRebuildVectorIndexDefinitionV2A(3, 2)
	rows := []columnVectorGraphAssetRow{
		{ID: []byte("doc-a"), Vector: []float32{1, 0, 0}, Adjacency: []uint32{columnVectorGraphLayeredAdjacencyMagic, 2, 2, 1, 2, 1, 2, 1, 3}},
		{ID: []byte("doc-b"), Vector: []float32{0, 1, 0}, Adjacency: []uint32{0, 2}},
		{ID: []byte("doc-c"), Vector: []float32{0, 0, 1}, Adjacency: []uint32{columnVectorGraphLayeredAdjacencyMagic, 2, 2, 0, 1, 0, 1, 3}},
		{ID: []byte("doc-d"), Vector: []float32{0.5, 0.5, 0}, Adjacency: []uint32{columnVectorGraphLayeredAdjacencyMagic, 1, 0, 1, 0}},
	}
	for i := range rows {
		invNorm, err := columnVectorGraphInvNorm(rows[i].Vector)
		if err != nil {
			_ = d.Close()
			tb.Fatalf("columnVectorGraphInvNorm row %d: %v", i, err)
		}
		rows[i].InvNorm = invNorm
	}
	const generation = uint64(7)
	input := ColumnPublishManifestEncodeInput{
		Collection:        "docs",
		ColumnStore:       *baseCfg,
		Operation:         ColumnPublishOperationInsert,
		AppliedCommandLSN: 1,
		Prepared: ColumnPublishPreparedAssets{
			RowCount:           0,
			ColumnPayloadBytes: 0,
		},
	}
	header, err := encodeColumnManifestHeaderRecord(input, generation)
	if err != nil {
		_ = d.Close()
		tb.Fatalf("encodeColumnManifestHeaderRecord: %v", err)
	}
	baseRecords := []columnManifestRecord{{key: []byte(columnManifestHeaderRecordKey), value: header}}
	sortColumnManifestRecords(baseRecords)
	baseChecksum := checksumColumnManifestRecords(input, generation, baseRecords)
	prepared, err := prepareColumnVectorGraphPhysicalAsset(d.ColumnAssetRootDir(), "docs", *baseCfg, def, generation, 1, 1, rows)
	if err != nil {
		_ = d.Close()
		tb.Fatalf("prepareColumnVectorGraphPhysicalAsset: %v", err)
	}
	graphCfg, err := columnVectorGraphPhysicalColumnStoreConfig("docs", *baseCfg, def)
	if err != nil {
		_ = d.Close()
		tb.Fatalf("columnVectorGraphPhysicalColumnStoreConfig: %v", err)
	}
	graph := columnVectorGraphManifestSnapshot{
		IndexName:              def.Name,
		Field:                  def.Field,
		Metric:                 def.Metric,
		Encoding:               def.Encoding,
		Dimensions:             def.Dimensions,
		M:                      def.M,
		EfConstruction:         def.EfConstruction,
		EfSearch:               def.EfSearch,
		BaseManifestGeneration: generation,
		BaseManifestChecksum:   baseChecksum,
		BaseSchemaHash:         baseCfg.SchemaHash,
		GraphSchemaHash:        graphCfg.SchemaHash,
		RowCount:               prepared.RowCount,
		AssetRef:               prepared.Ref,
		AssetBytes:             prepared.Bytes,
	}
	legacyPartID := nextColumnVectorGraphPartIDAfter(prepared.Ref.PartID, prepared.Ref.PartID)
	legacySources, err := prepareColumnVectorGraphAdjacencySourcesAssets(d.ColumnAssetRootDir(), "docs", *baseCfg, def, generation, legacyPartID, rows)
	if err != nil {
		_ = d.Close()
		tb.Fatalf("prepare legacy column_graph adjacency sources: %v", err)
	}
	graph.AdjacencyLayerCount = len(legacySources)
	graph.AdjacencyLayerSources = columnVectorGraphAdjacencyLayerSourcesFromPrepared(graph, legacySources)
	if len(graph.AdjacencyLayerSources) > 0 {
		graph.Layer0AdjacencySource = graph.AdjacencyLayerSources[0]
	}
	return d, baseCfg, def, graph, rows
}

func testColumnGraphManifestRecordsFromSnapshot1920(tb testing.TB, baseCfg ColumnStoreConfig, graph columnVectorGraphManifestSnapshot, identity ColumnManifestIdentity) ([]columnManifestRecord, ColumnManifestIdentity) {
	tb.Helper()
	input := ColumnPublishManifestEncodeInput{
		Collection:        "docs",
		ColumnStore:       baseCfg,
		Operation:         ColumnPublishOperationInsert,
		AppliedCommandLSN: 1,
		Prepared: ColumnPublishPreparedAssets{
			RowCount:           0,
			ColumnPayloadBytes: 0,
		},
	}
	header, err := encodeColumnManifestHeaderRecord(input, identity.Generation)
	if err != nil {
		tb.Fatalf("encodeColumnManifestHeaderRecord: %v", err)
	}
	baseRecords := []columnManifestRecord{{key: []byte(columnManifestHeaderRecordKey), value: header}}
	sortColumnManifestRecords(baseRecords)
	baseChecksum := checksumColumnManifestRecords(input, identity.Generation, baseRecords)
	graph.BaseManifestGeneration = identity.Generation
	graph.BaseManifestChecksum = baseChecksum
	for idx := range graph.AdjacencyLayerSources {
		graph.AdjacencyLayerSources[idx].BaseManifestGeneration = graph.BaseManifestGeneration
		graph.AdjacencyLayerSources[idx].BaseManifestChecksum = graph.BaseManifestChecksum
	}
	if len(graph.AdjacencyLayerSources) > 0 {
		graph.Layer0AdjacencySource = graph.AdjacencyLayerSources[0]
	} else if graph.Layer0AdjacencySource.Present {
		graph.Layer0AdjacencySource.BaseManifestGeneration = graph.BaseManifestGeneration
		graph.Layer0AdjacencySource.BaseManifestChecksum = graph.BaseManifestChecksum
	}
	encoded, err := encodeColumnVectorGraphManifestRecord(graph)
	if err != nil {
		tb.Fatalf("encodeColumnVectorGraphManifestRecord: %v", err)
	}
	records := []columnManifestRecord{
		{key: []byte(columnManifestHeaderRecordKey), value: header},
		{key: columnVectorGraphManifestRecordKey(graph.IndexName), value: encoded},
	}
	sortColumnManifestRecords(records)
	identity.Checksum = checksumColumnManifestRecords(input, identity.Generation, records)
	return records, identity
}

func loadColumnGraphRebuildManifestRecordsV2A(tb testing.TB, d *backenddb.DB, collection string) []columnManifestRecord {
	tb.Helper()
	records, _ := loadColumnGraphRebuildManifestRecordsAndConfigV2A(tb, d, collection)
	return records
}

func loadColumnGraphRebuildManifestRecordsAndConfigV2A(tb testing.TB, d *backenddb.DB, collection string) ([]columnManifestRecord, *ColumnStoreConfig) {
	tb.Helper()
	snap := d.AcquireSnapshot()
	if snap == nil {
		tb.Fatal("AcquireSnapshot returned nil")
	}
	defer func() { _ = snap.Close() }()
	catalog, err := loadCollectionCatalog(snap, collection)
	if err != nil {
		tb.Fatalf("loadCollectionCatalog: %v", err)
	}
	if catalog == nil {
		tb.Fatalf("collection %q missing", collection)
	}
	cfg := catalog.meta.Options.ColumnStore
	if cfg == nil || cfg.ActiveManifest == nil {
		tb.Fatalf("column store metadata missing active manifest: %+v", catalog.meta.Options.ColumnStore)
	}
	records, err := loadColumnManifestRecordsFromRoot(snap, catalog.rootID(collectionColumnManifestRootName(collection)))
	if err != nil {
		tb.Fatalf("loadColumnManifestRecordsFromRoot: %v", err)
	}
	return records, cfg
}

func assertColumnGraphRebuildLoadedStatusV2A(tb testing.TB, status VectorIndexStatus, indexName string) {
	tb.Helper()
	if status.Name != indexName ||
		status.Strategy != VectorIndexStrategyColumnGraph ||
		status.State != VectorIndexStateColumnGraphLoaded ||
		!status.Loaded ||
		status.RebuildNeeded {
		tb.Fatalf("status=%+v, want loaded column_graph index %q", status, indexName)
	}
}

func assertColumnAssetReachabilityProtectsGraphRefV2A(tb testing.TB, col *Collection, graphRef ColumnAssetRef) {
	tb.Helper()
	if graphRef.Kind == "" {
		return
	}
	plan, err := col.PlanColumnAssetReachability(context.Background(), ColumnAssetReachabilityOptions{Detailed: true})
	if err != nil {
		tb.Fatalf("PlanColumnAssetReachability: %v", err)
	}
	if !plan.Complete {
		tb.Fatalf("reachability plan incomplete: refs=%+v segments=%+v", plan.Refs, plan.Segments)
	}
	assertColumnGraphReachabilityEntryV2A(tb, plan, graphRef)
}

func assertColumnGraphReachabilityEntryV2A(tb testing.TB, plan ColumnAssetReachabilityPlan, graphRef ColumnAssetRef) {
	tb.Helper()
	for _, entry := range plan.Entries {
		if entry.Ref != graphRef {
			continue
		}
		if entry.Status != ColumnAssetReachabilityProtected {
			tb.Fatalf("graph ref reachability status=%q want protected", entry.Status)
		}
		if !columnGraphReachabilitySourcesContainV2A(entry.Sources, ColumnAssetReachabilitySourceActiveManifest) ||
			!columnGraphReachabilitySourcesContainV2A(entry.Sources, ColumnAssetReachabilitySourceRecoveryManifest) {
			tb.Fatalf("graph ref sources=%v want active and recovery manifest", entry.Sources)
		}
		return
	}
	tb.Fatalf("graph ref %+v missing from reachability entries=%+v", graphRef, plan.Entries)
}

func assertColumnGraphReachabilityRefAbsentV2A(tb testing.TB, plan ColumnAssetReachabilityPlan, graphRef ColumnAssetRef) {
	tb.Helper()
	for _, entry := range plan.Entries {
		if entry.Ref == graphRef {
			tb.Fatalf("graph ref %+v unexpectedly retained in reachability entries=%+v", graphRef, plan.Entries)
		}
	}
}

func assertColumnGraphSegmentStatusV2A(tb testing.TB, plan ColumnAssetReachabilityPlan, fileID uint32, want ColumnAssetReachabilitySegmentStatus) {
	tb.Helper()
	for _, entry := range plan.SegmentEntries {
		if entry.FileID != fileID {
			continue
		}
		if entry.Status != want {
			tb.Fatalf("segment file_id=%d status=%q want %q entry=%+v", fileID, entry.Status, want, entry)
		}
		if want == ColumnAssetReachabilitySegmentReclaimable && (entry.ProtectedBytes != 0 || entry.UnknownBytes != 0 || entry.ReclaimableBytes != entry.Bytes) {
			tb.Fatalf("segment file_id=%d reclaim accounting=%+v, want whole segment reclaimable", fileID, entry)
		}
		return
	}
	tb.Fatalf("segment file_id=%d missing from reachability segment entries=%+v", fileID, plan.SegmentEntries)
}

func columnGraphReachabilitySourcesContainV2A(sources []ColumnAssetReachabilitySource, want ColumnAssetReachabilitySource) bool {
	for _, source := range sources {
		if source == want {
			return true
		}
	}
	return false
}

func columnGraphRebuildScannedIDsV2A(rows []columnGraphRebuildScannedRowV2A) []string {
	ids := make([]string, len(rows))
	for i := range rows {
		ids[i] = rows[i].id
	}
	return ids
}

func columnGraphRebuildSyntheticRowsV2A(n, dims int) []columnGraphRebuildInputRowV2A {
	rows := make([]columnGraphRebuildInputRowV2A, n)
	for i := range rows {
		vector := make([]float32, dims)
		for j := range vector {
			vector[j] = float32(((i+3)*(j+5))%23+1) / 23
		}
		rows[i] = columnGraphRebuildInputRowV2A{
			id:     fmt.Sprintf("doc-%04d", i),
			vector: vector,
		}
	}
	return rows
}
