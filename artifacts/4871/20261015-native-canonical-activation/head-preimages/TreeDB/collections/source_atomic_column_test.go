package collections

import (
	"testing"

	"github.com/snissn/gomap/TreeDB/collections/chunking"
	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestSourceReplacementPreparesDeleteAndTypedInsertAssetsInOneGeneration(t *testing.T) {
	cfg, err := normalizeColumnStoreConfig("docs", &ColumnStoreConfig{
		Enabled: true,
		Columns: []ColumnStoreColumn{
			{Name: "ordinal", Path: "ordinal", ValueType: ColumnStoreValueInt64},
			{Name: "kind", Path: "kind", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerColumnPart, Dictionary: true},
		},
		SortKey: []ColumnSortKey{{Column: "kind"}},
	})
	if err != nil {
		t.Fatalf("normalize column store: %v", err)
	}
	d, err := backenddb.Open(backenddb.Options{Dir: t.TempDir(), DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = d.Close() }()
	col := &Collection{db: d}
	input, err := prepareColumnWritePublishInputBeforeCommandWAL(columnWritePublishInput{
		meta: CollectionMeta{Name: "docs", Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
			ColumnStore:    cfg,
		}},
		operation:             ColumnPublishOperationUpdate,
		documents:             []columnWriteDocument{{ID: []byte("src#0"), Document: []byte(`{"ordinal":0,"kind":"chunk"}`)}},
		sourceDeleteDocuments: []columnWriteDocument{{ID: []byte("src#0")}, {ID: []byte("src#1")}},
		rows:                  1,
	})
	if err != nil {
		t.Fatalf("prepare input: %v", err)
	}
	prepared, err := col.prepareColumnPhysicalAssetsForCommand(input, ColumnPublishAssetPrepareInput{
		Collection: "docs", ColumnStore: *cfg, Operation: ColumnPublishOperationUpdate, AppliedCommandLSN: 7,
	})
	if err != nil {
		t.Fatalf("prepare source assets: %v", err)
	}
	if prepared.stableResources != nil {
		defer prepared.stableResources.Release()
	}
	if prepared.RowCount != 3 {
		t.Fatalf("prepared rows=%d want 3", prepared.RowCount)
	}
	manifest, err := encodeColumnManifestIdentityForWrite(ColumnPublishManifestEncodeInput{
		Collection: "docs", ColumnStore: *cfg, Operation: ColumnPublishOperationUpdate,
		AppliedCommandLSN: 7, Prepared: prepared,
	})
	if err != nil {
		t.Fatalf("encode mixed source manifest: %v", err)
	}
	if manifest.Identity.Generation != 1 {
		t.Fatalf("manifest generation=%d want 1", manifest.Identity.Generation)
	}
	var deleteRow, insertRow, typedInsert bool
	for _, asset := range prepared.Assets {
		switch {
		case asset.Ref.Kind == ColumnAssetKindTCS1PartImage && asset.Ref.PartID == columnPhysicalRowAssetPartID:
			deleteRow = asset.Reason == string(ColumnPublishOperationDelete) && asset.PartRole == ColumnManifestPartRoleTombstone
		case asset.Ref.Kind == ColumnAssetKindTCS1PartImage && asset.Ref.PartID == columnPhysicalRowAssetPartID+(1<<32):
			insertRow = asset.Reason == string(ColumnPublishOperationInsert) && asset.PartRole == ColumnManifestPartRoleBase
		case asset.Ref.Kind == ColumnAssetKindTCS1TypedColumnPart && asset.Ref.PartID == typedColumnPartAssetPartID:
			typedInsert = asset.Reason == string(ColumnPublishOperationInsert) && asset.PartRole == ColumnManifestPartRoleBase
		}
	}
	if !deleteRow || !insertRow || !typedInsert {
		t.Fatalf("mixed source assets delete=%t insert=%t typed=%t assets=%+v", deleteRow, insertRow, typedInsert, prepared.Assets)
	}
}

func TestIngestSourcesAtomicTypedColumnReplacementReopen(t *testing.T) {
	dir := t.TempDir()
	cfg := testColumnStoreConfig(nil)
	cfg.Columns = []ColumnStoreColumn{{
		Name: "chunk_kind", Path: chunking.MetaFieldKind, ValueType: ColumnStoreValueString,
		Owner: TypedStorageOwnerColumnPart, Dictionary: true, Nullable: true,
	}}
	cfg.SortKey = nil
	cfg.AggregateMetadata = nil
	setup, err := backenddb.Open(backenddb.Options{
		Dir: dir, Durability: backenddb.DurabilityDurable, DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatalf("open setup: %v", err)
	}
	mgr := NewCollectionManager(setup)
	if _, err := mgr.CreateCollection(&CollectionMeta{
		Name:    "docs",
		Options: CollectionOptions{ColumnStore: cfg},
		VectorIndexes: []VectorIndexDefinition{{
			Name: "embedding", Field: "embedding", Metric: VectorMetricCosine, Dimensions: 64,
		}},
	}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := mgr.OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection setup: %v", err)
	}
	if _, _, err := col.CreateTextIndex(TextIndexDefinition{
		Name: "lexical", Version: TextIndexVersionV1, Fields: []TextIndexField{{Field: "body"}}, StorePositions: true,
	}); err != nil {
		t.Fatalf("CreateTextIndex: %v", err)
	}
	if err := setup.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint setup: %v", err)
	}
	if err := setup.Close(); err != nil {
		t.Fatalf("Close setup: %v", err)
	}
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}

	d, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("open command WAL: %v", err)
	}
	old := ingestTestSource("src-typed", 0)
	newSource := ingestTestSource("src-typed", 7)
	live, err := NewCollectionManager(d).OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	mustIngest(t, live, []SourceDocument{old}, ingestTestCfg(64))
	replaced := mustIngest(t, live, []SourceDocument{newSource}, ingestTestCfg(64))
	if len(replaced.Ingested) != 1 {
		t.Fatalf("replacement outcomes=%d want 1", len(replaced.Ingested))
	}
	assertAtomicTypedSourceState(t, live, len(replaced.Ingested[0].ChildIDs))
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint replacement: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close replacement: %v", err)
	}

	reopened, err := backenddb.Open(backenddb.Options{Dir: dir, DisableBackgroundPrune: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	reopenedCol, err := NewCollectionManager(reopened).OpenCollection("docs")
	if err != nil {
		t.Fatalf("OpenCollection reopen: %v", err)
	}
	assertAtomicTypedSourceState(t, reopenedCol, len(replaced.Ingested[0].ChildIDs))
}

func assertAtomicTypedSourceState(t *testing.T, col *Collection, wantChunks int) {
	t.Helper()
	result, err := col.RunColumnPhysicalQuery(ColumnPhysicalQueryRequest{
		Kind: ColumnPhysicalQueryGroupCount, GroupColumn: "chunk_kind",
	})
	if err != nil {
		t.Fatalf("RunColumnPhysicalQuery: %v", err)
	}
	if result.Diagnostics.StorageSource != ColumnPhysicalQueryStorageSourceTypedColumnPartSection ||
		result.Diagnostics.FallbackReason != ColumnPhysicalQueryFallbackNone ||
		result.Diagnostics.TypedColumnPartSections == 0 {
		t.Fatalf("typed-column diagnostics=%+v", result.Diagnostics)
	}
	gotChunks := 0
	for _, group := range result.Groups {
		if group.Key == chunking.KindChunk {
			gotChunks += group.Count
		}
	}
	if gotChunks != wantChunks {
		t.Fatalf("typed-column chunk rows=%d want %d groups=%+v", gotChunks, wantChunks, result.Groups)
	}
	oldHits, err := col.SearchText(TextSearchOptions{IndexName: "lexical", Query: "tok0", TopK: wantChunks + 8})
	if err != nil {
		t.Fatalf("SearchText old: %v", err)
	}
	if len(oldHits.Results) != 0 {
		t.Fatalf("old source text remains visible: %d hits", len(oldHits.Results))
	}
	newHits, err := col.SearchText(TextSearchOptions{IndexName: "lexical", Query: "tok7", TopK: wantChunks + 8})
	if err != nil || len(newHits.Results) == 0 {
		t.Fatalf("new source text hits=%d err=%v", len(newHits.Results), err)
	}
}
