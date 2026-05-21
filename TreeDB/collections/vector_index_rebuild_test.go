package collections

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/commitlog"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
)

var columnGraphRebuildBenchSinkV2A VectorIndexStatus

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
	if got := strings.Join(columnGraphRebuildScannedIDsV2A(scanned), ","); got != "doc-a,doc-b,doc-c,doc-d" {
		t.Fatalf("scanned graph ids=%s", got)
	}
	if got := scanned[0].adjacency; len(got) != 3 || got[0] != 1 || got[1] != 2 || got[2] != 3 {
		t.Fatalf("doc-a adjacency=%v, want [1 2 3]", got)
	}
	if got := scanned[1].adjacency; len(got) != 3 || got[0] != 0 || got[1] != 2 || got[2] != 3 {
		t.Fatalf("doc-b adjacency=%v, want [0 2 3]", got)
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
	assertColumnAssetReachabilityProtectsGraphRefV2A(t, col, graph.AssetRef)
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

func TestColumnGraphRebuildVectorIndexAdjacencyUsesBoundedTopKV2A(t *testing.T) {
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
	if graph.AssetRef.Namespace != col.Meta().Options.ColumnStore.AssetManager.Namespace {
		t.Fatalf("graph namespace=%q want base namespace=%q", graph.AssetRef.Namespace, col.Meta().Options.ColumnStore.AssetManager.Namespace)
	}
	if got := scanned[0].adjacency; len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("doc-a adjacency=%v, want bounded top-k [1 2]", got)
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
	if graphA.AssetRef.Namespace != graphB.AssetRef.Namespace {
		t.Fatalf("graph namespaces differ %q vs %q, test requires shared namespace", graphA.AssetRef.Namespace, graphB.AssetRef.Namespace)
	}
	if graphA.AssetRef.PartID == graphB.AssetRef.PartID {
		t.Fatalf("graph indexes reused part_id=%d in namespace %q", graphA.AssetRef.PartID, graphA.AssetRef.Namespace)
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
	first, _ := loadAndScanColumnGraphRebuildRowsV2A(t, d, "docs", def)
	if _, err := col.RebuildVectorIndex(def.Name); err != nil {
		t.Fatalf("second RebuildVectorIndex: %v", err)
	}
	second, _ := loadAndScanColumnGraphRebuildRowsV2A(t, d, "docs", def)
	if first.AssetRef == second.AssetRef {
		t.Fatalf("second rebuild reused graph asset ref %+v", first.AssetRef)
	}
	if first.AssetRef.FileID == second.AssetRef.FileID {
		t.Fatalf("second rebuild reused graph segment file_id=%d; want fresh segment for whole-segment reclaim", first.AssetRef.FileID)
	}

	plan, err := col.PlanColumnAssetReachability(context.Background(), ColumnAssetReachabilityOptions{Detailed: true})
	if err != nil {
		t.Fatalf("PlanColumnAssetReachability: %v", err)
	}
	if !plan.Complete {
		t.Fatalf("reachability plan incomplete: refs=%+v segments=%+v", plan.Refs, plan.Segments)
	}
	assertColumnGraphReachabilityEntryV2A(t, plan, second.AssetRef)
	assertColumnGraphSegmentStatusV2A(t, plan, first.AssetRef.FileID, ColumnAssetReachabilitySegmentReclaimable)
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

func TestColumnGraphRebuildVectorIndexNoopPublishesCommandWALV2A(t *testing.T) {
	tests := []struct {
		name          string
		strategy      VectorIndexStrategy
		wantState     VectorIndexState
		wantReason    VectorIndexReason
		rebuildNeeded bool
	}{
		{
			name:       "native_strategy",
			strategy:   VectorIndexStrategyNativeRuntime,
			wantState:  VectorIndexStateNativeRuntime,
			wantReason: VectorIndexReasonNativeRuntime,
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
			if got := d.State().AppliedCommandLSN; got <= baseLSN {
				t.Fatalf("AppliedCommandLSN after no-op rebuild=%d want > %d", got, baseLSN)
			}
			frames := collectionCommandWALFrames(t, dir)
			if len(frames) != framesBefore+1 {
				t.Fatalf("command WAL frames after no-op rebuild=%d want %d", len(frames), framesBefore+1)
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

func TestColumnGraphRebuildVectorIndexCommandWALReplayNoopAdvancesLSNV2A(t *testing.T) {
	tests := []struct {
		name          string
		strategy      VectorIndexStrategy
		wantState     VectorIndexState
		wantReason    VectorIndexReason
		rebuildNeeded bool
	}{
		{
			name:       "native_strategy",
			strategy:   VectorIndexStrategyNativeRuntime,
			wantState:  VectorIndexStateNativeRuntime,
			wantReason: VectorIndexReasonNativeRuntime,
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
				t.Fatalf("AppliedCommandLSN after rebuild no-op replay=%d want 1", got)
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
