package collections

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

func TestColumnRetainedPayloadValueLogPlacementReopen(t *testing.T) {
	dir := t.TempDir()
	enableColumnRetainedPlacementCommandWAL(t, dir)

	d := openColumnRetainedPlacementDB(t, dir, backenddb.Options{})
	col := createColumnRetainedPlacementCollection(t, d, "events")
	doc := retainedPlacementDocument("durable-reopen", 1)
	if _, err := col.InsertBatch([][]byte{[]byte("doc-1")}, [][]byte{doc}); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	ptr := requireColumnRetainedPlacementPointer(t, d, "events", []byte("doc-1"))
	if ptr.FileID == 0 || !page.IsValueLogFileID(ptr.FileID) {
		t.Fatalf("retained payload pointer file id=%d want value-log pointer", ptr.FileID)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopen := openColumnRetainedPlacementDB(t, dir, backenddb.Options{})
	defer func() { _ = reopen.Close() }()
	reopenedCol := openColumnRetainedPlacementCollection(t, reopen, "events")
	got, err := reopenedCol.Get([]byte("doc-1"))
	if err != nil {
		t.Fatalf("Get after reopen: %v", err)
	}
	assertJSONMapEqual1875(t, got, map[string]any{"row_id": float64(1), "kind": "kind-1", "payload": strings.Repeat("durable-reopen", 10)})
	reopenedPtr := requireColumnRetainedPlacementPointer(t, reopen, "events", []byte("doc-1"))
	if reopenedPtr != ptr {
		t.Fatalf("retained payload pointer changed after reopen: got=%+v want=%+v", reopenedPtr, ptr)
	}
}

func TestColumnRetainedPayloadValueLogPlacementGCRewrite(t *testing.T) {
	dir := t.TempDir()
	enableColumnRetainedPlacementCommandWAL(t, dir)

	d := openColumnRetainedPlacementDB(t, dir, backenddb.Options{})
	col := createColumnRetainedPlacementCollection(t, d, "events")
	if _, err := col.InsertBatch([][]byte{[]byte("doc-a")}, [][]byte{retainedPlacementDocument("gc-stale", 1)}); err != nil {
		t.Fatalf("InsertBatch doc-a: %v", err)
	}
	ptrA := requireColumnRetainedPlacementPointer(t, d, "events", []byte("doc-a"))
	pathA := valueLogPathForFileID(t, dir, ptrA.FileID)
	if err := d.Close(); err != nil {
		t.Fatalf("Close after doc-a: %v", err)
	}

	d = openColumnRetainedPlacementDB(t, dir, backenddb.Options{})
	col = openColumnRetainedPlacementCollection(t, d, "events")
	if _, err := col.InsertBatch([][]byte{[]byte("doc-b")}, [][]byte{retainedPlacementDocument("rewrite-live", 2)}); err != nil {
		_ = d.Close()
		t.Fatalf("InsertBatch doc-b: %v", err)
	}
	ptrB := requireColumnRetainedPlacementPointer(t, d, "events", []byte("doc-b"))
	pathB := valueLogPathForFileID(t, dir, ptrB.FileID)
	if ptrA.FileID == ptrB.FileID {
		_ = d.Close()
		t.Fatalf("test expected separate retained payload segments, both pointers use file %d", ptrA.FileID)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close after doc-b: %v", err)
	}

	// Reopen and append another retained payload so doc-b's segment is no longer
	// the command-WAL appender's active segment when the rewrite cleanup runs.
	d = openColumnRetainedPlacementDB(t, dir, backenddb.Options{})
	col = openColumnRetainedPlacementCollection(t, d, "events")
	if _, err := col.InsertBatch([][]byte{[]byte("doc-c")}, [][]byte{retainedPlacementDocument("active-tail", 3)}); err != nil {
		_ = d.Close()
		t.Fatalf("InsertBatch doc-c: %v", err)
	}
	_ = requireColumnRetainedPlacementPointer(t, d, "events", []byte("doc-c"))
	if err := col.Delete([]byte("doc-a")); err != nil {
		_ = d.Close()
		t.Fatalf("Delete doc-a: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		_ = d.Close()
		t.Fatalf("settle deletion before durable slot rollover: %v", err)
	}
	// Advance the alternate durable slot so neither selectable closure retains
	// doc-a's value-log segment before destructive GC.
	if _, err := col.InsertBatch([][]byte{[]byte("doc-d")}, [][]byte{retainedPlacementDocument("slot-rollover", 4)}); err != nil {
		_ = d.Close()
		t.Fatalf("InsertBatch doc-d slot rollover: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		_ = d.Close()
		t.Fatalf("wait durable doc-d slot rollover: %v", err)
	}

	gcStats, err := d.ValueLogGC(context.Background(), backenddb.ValueLogGCOptions{})
	if err != nil {
		_ = d.Close()
		t.Fatalf("ValueLogGC: %v", err)
	}
	if gcStats.SegmentsDeleted == 0 {
		_ = d.Close()
		t.Fatalf("ValueLogGC deleted no segments after deleting retained payload: %+v", gcStats)
	}
	if _, err := os.Stat(pathA); err == nil || !os.IsNotExist(err) {
		_ = d.Close()
		t.Fatalf("stale retained payload segment %s stat err=%v, want removed", pathA, err)
	}
	if got, err := col.Get([]byte("doc-b")); err != nil {
		_ = d.Close()
		t.Fatalf("Get doc-b after GC: %v", err)
	} else {
		assertJSONMapEqual1875(t, got, map[string]any{"row_id": float64(2), "kind": "kind-2", "payload": strings.Repeat("rewrite-live", 10)})
	}

	rewriteStats, err := d.ValueLogRewriteOnline(context.Background(), backenddb.ValueLogRewriteOnlineOptions{
		SourceFileIDs: []uint32{ptrB.FileID},
		BatchSize:     1,
	})
	if err != nil {
		_ = d.Close()
		t.Fatalf("ValueLogRewriteOnline: %v", err)
	}
	if rewriteStats.RecordsCopied == 0 {
		_ = d.Close()
		t.Fatalf("ValueLogRewriteOnline copied no retained payload records: %+v", rewriteStats)
	}
	rewrittenPtr := requireColumnRetainedPlacementPointer(t, d, "events", []byte("doc-b"))
	if rewrittenPtr.FileID == ptrB.FileID {
		_ = d.Close()
		t.Fatalf("retained payload pointer still references rewrite source segment %d", ptrB.FileID)
	}
	if got, err := col.Get([]byte("doc-b")); err != nil {
		_ = d.Close()
		t.Fatalf("Get doc-b after rewrite: %v", err)
	} else {
		assertJSONMapEqual1875(t, got, map[string]any{"row_id": float64(2), "kind": "kind-2", "payload": strings.Repeat("rewrite-live", 10)})
	}
	// The rewrite source is no longer reachable from the current root, but the
	// other selectable durable slot still owns its exact closure.
	postRewriteGC, err := d.ValueLogGC(context.Background(), backenddb.ValueLogGCOptions{})
	if err != nil {
		_ = d.Close()
		t.Fatalf("ValueLogGC after rewrite: %v", err)
	}
	if postRewriteGC.SegmentsDeleted != 0 || rewriteStats.SourceSegmentsReclaimed != 0 {
		_ = d.Close()
		t.Fatalf("rewrite reclaimed fallback-slot source: rewrite=%+v gc=%+v", rewriteStats, postRewriteGC)
	}
	if _, err := os.Stat(pathB); err != nil {
		_ = d.Close()
		t.Fatalf("fallback-slot rewrite source %s stat err=%v, want retained", pathB, err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close after rewrite: %v", err)
	}

	reopen := openColumnRetainedPlacementDB(t, dir, backenddb.Options{})
	defer func() { _ = reopen.Close() }()
	reopenedCol := openColumnRetainedPlacementCollection(t, reopen, "events")
	if got, err := reopenedCol.Get([]byte("doc-b")); err != nil {
		t.Fatalf("Get doc-b after rewrite reopen: %v", err)
	} else {
		assertJSONMapEqual1875(t, got, map[string]any{"row_id": float64(2), "kind": "kind-2", "payload": strings.Repeat("rewrite-live", 10)})
	}
	reopenedPtr := requireColumnRetainedPlacementPointer(t, reopen, "events", []byte("doc-b"))
	if reopenedPtr != rewrittenPtr {
		t.Fatalf("rewritten retained payload pointer changed after reopen: got=%+v want=%+v", reopenedPtr, rewrittenPtr)
	}
}

func TestColumnRetainedPayloadLeafLogSyntheticShrink(t *testing.T) {
	const docs = 512
	const marker = "retained-leaf-vlog-marker-2357"

	rowDir := t.TempDir()
	rowLeafBytes := writeRetainedPlacementSynthetic(t, rowDir, "row_docs", nil, docs, marker)
	if !filesUnderDirContain(t, backenddb.LeafLogDirPath(rowDir), []byte(marker)) {
		t.Fatalf("row-store baseline leaf_vlog did not contain marker %q", marker)
	}

	retainedDir := t.TempDir()
	retainedCfg := &ColumnStoreConfig{
		Enabled: true,
		Columns: []ColumnStoreColumn{
			{Name: "row_id", Path: "row_id", ValueType: ColumnStoreValueInt64, Owner: TypedStorageOwnerRowAsset},
			{Name: "kind", Path: "kind", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerColumnPart, Dictionary: true},
		},
		RetainedPayload:         ColumnRetainedPayloadNonColumn,
		RetainedPayloadEncoding: ColumnRetainedPayloadEncodingTemplateV1,
		Reconstruction:          ColumnReconstructionRetainedPayloadAndColumns,
	}
	retainedLeafBytes := writeRetainedPlacementSynthetic(t, retainedDir, "retained_docs", retainedCfg, docs, marker)
	if filesUnderDirContain(t, backenddb.LeafLogDirPath(retainedDir), []byte(marker)) {
		t.Fatalf("retained payload marker %q remained inline in leaf_vlog", marker)
	}
	if !filesUnderDirContain(t, backenddb.ValueLogDirPath(retainedDir), []byte(marker)) {
		t.Fatalf("retained payload marker %q was not found in value_vlog", marker)
	}

	if retainedLeafBytes >= rowLeafBytes/2 {
		t.Fatalf("retained leaf_vlog did not shrink enough: retained=%d row_baseline=%d", retainedLeafBytes, rowLeafBytes)
	}
	t.Logf("synthetic leaf_vlog bytes: retained=%d row_baseline=%d shrink=%.1f%%", retainedLeafBytes, rowLeafBytes, 100*(1-float64(retainedLeafBytes)/float64(rowLeafBytes)))
}

func TestColumnRetainedPayloadTemplateV1PointerizePacksByTemplateID(t *testing.T) {
	const docs = 32
	stored := retainedPlacementTemplateV1StoredDocuments(t, docs)
	table := newCollectionRunTable(docs)
	for i := 0; i < docs; i++ {
		table.SetEntrySteal([]byte(fmt.Sprintf("doc-%06d", i)), bytes.Clone(stored[i]), page.ValuePtr{}, node.FlagInline)
	}
	table.Freeze()

	db := &backenddb.DB{}
	appender := &retainedPlacementTemplatePackAppender{}
	db.SetValueLogAppender(appender)
	defer db.SetValueLogAppender(nil)

	cfg := &ColumnStoreConfig{
		Enabled:                 true,
		RetainedPayload:         ColumnRetainedPayloadNonColumn,
		RetainedPayloadEncoding: ColumnRetainedPayloadEncodingTemplateV1,
		Reconstruction:          ColumnReconstructionRetainedPayloadAndColumns,
	}
	meta := CollectionMeta{Name: "events", Options: CollectionOptions{DocumentFormat: DocumentFormatJSON, ColumnStore: cfg}}
	out, pointerized, err := pointerizeCollectionRunTableValuesForRoot(db, meta, collectionPrimaryRootName("events"), table)
	if err != nil {
		t.Fatalf("pointerizeCollectionRunTableValuesForRoot: %v", err)
	}
	if !pointerized {
		t.Fatalf("pointerizeCollectionRunTableValuesForRoot did not pointerize retained payloads")
	}
	if len(appender.values) != docs {
		t.Fatalf("appended values=%d want %d", len(appender.values), docs)
	}

	lastTemplateID := uint64(0)
	transitions := 0
	for i, value := range appender.values {
		root, err := parseTemplateV1StoredDocument(value)
		if err != nil {
			t.Fatalf("appended value %d parse template-v1 stored document: %v", i, err)
		}
		if i > 0 && root.templateID != lastTemplateID {
			transitions++
			if root.templateID < lastTemplateID {
				t.Fatalf("appended template ids are not grouped/sorted at %d: got %d after %d", i, root.templateID, lastTemplateID)
			}
		}
		lastTemplateID = root.templateID
	}
	if transitions != 1 {
		t.Fatalf("template packing transitions=%d want 1 for two alternating templates", transitions)
	}

	ptrOffsetByDocument := make(map[int]uint64, docs)
	for appendedIdx, value := range appender.values {
		sourceIdx := -1
		for i := range stored {
			if bytes.Equal(value, stored[i]) {
				sourceIdx = i
				break
			}
		}
		if sourceIdx < 0 {
			t.Fatalf("appended value %d did not match any source document", appendedIdx)
		}
		ptrOffsetByDocument[sourceIdx] = uint64(appendedIdx + 1)
	}
	for i := 0; i < docs; i++ {
		key := []byte(fmt.Sprintf("doc-%06d", i))
		value, ptr, flags, found := out.GetEntry(key)
		if !found {
			t.Fatalf("pointerized table missing %q", key)
		}
		if len(value) != 0 || flags&node.FlagPointer == 0 {
			t.Fatalf("pointerized entry %q value_len=%d flags=%#x want pointer-only", key, len(value), flags)
		}
		if ptr.Offset != ptrOffsetByDocument[i] {
			t.Fatalf("pointerized entry %q offset=%d want appended offset %d", key, ptr.Offset, ptrOffsetByDocument[i])
		}
	}
}

func TestColumnRetainedPayloadTemplateV1PackingImprovesGroupedBlockStorage(t *testing.T) {
	const docs = 128
	stored := retainedPlacementTemplateV1CompressibleStoredDocuments(t, docs)
	order, ok := collectionRetainedTemplateV1PackOrder(stored)
	if !ok {
		t.Fatalf("collectionRetainedTemplateV1PackOrder returned no packing order for interleaved templates")
	}
	packed := make([][]byte, len(stored))
	for packedIdx, sourceIdx := range order {
		packed[packedIdx] = stored[sourceIdx]
	}

	mixedStats := retainedPlacementTemplateV1BlockFrameStats(t, stored)
	packedStats := retainedPlacementTemplateV1BlockFrameStats(t, packed)
	if !mixedStats.Kept || !packedStats.Kept {
		t.Fatalf("block compression not kept: mixed=%+v packed=%+v", mixedStats, packedStats)
	}
	if packedStats.RawPayloadBytes != mixedStats.RawPayloadBytes {
		t.Fatalf("raw payload bytes changed: mixed=%d packed=%d", mixedStats.RawPayloadBytes, packedStats.RawPayloadBytes)
	}
	if packedStats.StoredPayloadBytes >= mixedStats.StoredPayloadBytes {
		t.Fatalf("packed template-v1 frame stored bytes=%d want below mixed=%d", packedStats.StoredPayloadBytes, mixedStats.StoredPayloadBytes)
	}
	shrinkPct := 100 * (1 - float64(packedStats.StoredPayloadBytes)/float64(mixedStats.StoredPayloadBytes))
	if shrinkPct < 2 {
		t.Fatalf("packed template-v1 frame shrink %.2f%% below guardrail: mixed=%d packed=%d", shrinkPct, mixedStats.StoredPayloadBytes, packedStats.StoredPayloadBytes)
	}
	t.Logf("template-v1 retained grouped-block bytes: mixed=%d packed=%d shrink=%.2f%%",
		mixedStats.StoredPayloadBytes, packedStats.StoredPayloadBytes, shrinkPct)
}

func TestColumnRetainedPayloadTemplateV1PackedAppendMismatchFailsClosed(t *testing.T) {
	const docs = 32
	stored := retainedPlacementTemplateV1StoredDocuments(t, docs)
	db := &backenddb.DB{}
	appender := &retainedPlacementTemplatePackMismatchedAppender{}
	db.SetValueLogAppender(appender)
	defer db.SetValueLogAppender(nil)

	ptrs, err := appendCollectionPointerizedBatchValues(db, stored, true)
	if err == nil {
		t.Fatalf("appendCollectionPointerizedBatchValues returned nil error for mismatched pointer count")
	}
	if len(ptrs) != 0 {
		t.Fatalf("appendCollectionPointerizedBatchValues returned %d ptrs on mismatch", len(ptrs))
	}
	if len(appender.values) != docs {
		t.Fatalf("appended values=%d want %d", len(appender.values), docs)
	}
}

func TestColumnRetainedPayloadSemanticStreamV1InsertBatchRoundTripReopen(t *testing.T) {
	dir := t.TempDir()
	enableColumnRetainedPlacementCommandWAL(t, dir)

	d := openColumnRetainedPlacementDB(t, dir, backenddb.Options{})
	col := createColumnRetainedSemanticStreamCollection(t, d, "events")
	ids, docs := retainedSemanticStreamDocuments(96)
	if _, err := col.InsertBatch(ids, docs); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	stats := col.LastInsertStats()
	if stats.RetainedPayloadRows != len(docs) {
		t.Fatalf("retained payload rows=%d want %d", stats.RetainedPayloadRows, len(docs))
	}
	if stats.RetainedPayloadDeclaredRows != len(docs) {
		t.Fatalf("retained payload declared rows=%d want %d", stats.RetainedPayloadDeclaredRows, len(docs))
	}
	if stats.RetainedPayloadSemanticStreamBlocks != 1 {
		t.Fatalf("retained payload semantic-stream blocks=%d want 1", stats.RetainedPayloadSemanticStreamBlocks)
	}
	if stats.ColumnPublishRows != len(docs) {
		t.Fatalf("column publish rows=%d want %d", stats.ColumnPublishRows, len(docs))
	}
	if stats.ColumnPublishPreparedAssets == 0 || stats.ColumnPublishRequiredAssetBytes == 0 || stats.ColumnPublishManifestBytes == 0 {
		t.Fatalf("column publish asset counters missing: %+v", stats)
	}
	if stats.ColumnPublishBuildColumnDelta <= 0 || stats.ColumnPublishCommit <= 0 {
		t.Fatalf("column publish callback/commit timings missing: %+v", stats)
	}
	// Individual sub-millisecond phases can measure as zero on Windows'
	// coarser monotonic clock. Ordered-root apply plus the nonzero exclusive
	// total still establish that the scoped publication timings were recorded.
	if stats.ColumnPublishOrderedRootApply <= 0 || stats.ColumnPublishCommitExclusiveTotal() <= 0 {
		t.Fatalf("column publish exclusive timings missing: %+v", stats)
	}
	if stats.ColumnPublishCommitExclusiveTotal() > stats.ColumnPublishCommit {
		t.Fatalf("column publish exclusive phases exceed commit wall time: exclusive=%s commit=%s", stats.ColumnPublishCommitExclusiveTotal(), stats.ColumnPublishCommit)
	}
	finalizeChildren := stats.ColumnPublishFinalizePrepareDurability +
		stats.ColumnPublishFinalizeCandidateBuild +
		stats.ColumnPublishFinalizeEnqueueActivation +
		stats.ColumnPublishFinalizeAdmissionWait +
		stats.ColumnPublishFinalizeDurabilityWait
	if finalizeChildren < 0 || finalizeChildren > stats.ColumnPublishFinalize {
		t.Fatalf("column publish finalize children=%s finalize=%s", finalizeChildren, stats.ColumnPublishFinalize)
	}
	t.Logf("column publish phase evidence: commit=%s exclusive=%s append=%s root_apply=%s system_apply=%s finalize=%s finalize_prepare=%s finalize_candidate=%s finalize_enqueue=%s finalize_admission=%s", stats.ColumnPublishCommit, stats.ColumnPublishCommitExclusiveTotal(), stats.ColumnPublishCommandWALAppend, stats.ColumnPublishOrderedRootApply, stats.ColumnPublishSystemRootApply, stats.ColumnPublishFinalize, stats.ColumnPublishFinalizePrepareDurability, stats.ColumnPublishFinalizeCandidateBuild, stats.ColumnPublishFinalizeEnqueueActivation, stats.ColumnPublishFinalizeAdmissionWait)
	if stats.ColumnPublishAssetPreparation <= 0 {
		t.Fatalf("column publish asset-preparation timing missing: %+v", stats)
	}
	if stats.ColumnPublishManifestEncode < 0 || stats.ColumnPublishRootDeltaConstruction < 0 {
		t.Fatalf("column publish plan timings invalid: %+v", stats)
	}
	_, whitespaceRow, _ := requireColumnRetainedSemanticStreamLocatorAndBlockPointer(t, d, "events", ids[9])
	if whitespaceRow != 9 {
		t.Fatalf("semantic locator row=%d want whitespace-varint row 9", whitespaceRow)
	}
	blockKey, row, ptr := requireColumnRetainedSemanticStreamLocatorAndBlockPointer(t, d, "events", ids[17])
	if len(blockKey) == 0 || row != 17 {
		t.Fatalf("semantic locator block_key_len=%d row=%d want row 17", len(blockKey), row)
	}
	if !page.IsValueLogFileID(ptr.FileID) {
		t.Fatalf("semantic stream block pointer file id=%d is not value-log backed", ptr.FileID)
	}
	storedBlock := requireColumnRetainedSemanticStreamStoredBlock(t, d, "events", blockKey)
	rawBlock, err := decodeColumnRetainedSemanticStreamV1StoredBlock(storedBlock)
	if err != nil {
		t.Fatalf("decode stored semantic stream block: %v", err)
	}
	if !bytes.HasPrefix(storedBlock, columnRetainedSemanticStreamV1BlockZSTDMagic) || len(storedBlock) >= len(rawBlock) {
		t.Fatalf("semantic stream block stored_len=%d raw_len=%d magic=%q want compressed zstd wrapper", len(storedBlock), len(rawBlock), storedBlock[:len(columnRetainedSemanticStreamV1BlockMagic)])
	}
	if got, err := col.Get(ids[17]); err != nil {
		t.Fatalf("Get semantic stream doc: %v", err)
	} else {
		assertRetainedSemanticStreamDocument(t, got, 17)
	}
	if got, err := col.Get(ids[9]); err != nil {
		t.Fatalf("Get semantic stream whitespace-row doc: %v", err)
	} else {
		assertRetainedSemanticStreamDocument(t, got, 9)
	}
	view, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatalf("OpenCollectionReadView: %v", err)
	}
	response, err := view.FetchDocumentsByID([][]byte{ids[9], ids[17], ids[42]}, DocumentFetchOptions{})
	closeErr := view.Close()
	if err != nil {
		t.Fatalf("FetchDocumentsByID: %v", err)
	}
	if closeErr != nil {
		t.Fatalf("Close read view: %v", closeErr)
	}
	if len(response.Results) != 3 || !response.Results[0].Found || !response.Results[1].Found || !response.Results[2].Found {
		t.Fatalf("FetchDocumentsByID results=%+v", response.Results)
	}
	assertRetainedSemanticStreamDocument(t, response.Results[0].Document, 9)
	assertRetainedSemanticStreamDocument(t, response.Results[1].Document, 17)
	assertRetainedSemanticStreamDocument(t, response.Results[2].Document, 42)
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopen := openColumnRetainedPlacementDB(t, dir, backenddb.Options{})
	defer func() { _ = reopen.Close() }()
	reopenedCol := openColumnRetainedPlacementCollection(t, reopen, "events")
	if got, err := reopenedCol.Get(ids[17]); err != nil {
		t.Fatalf("Get semantic stream doc after reopen: %v", err)
	} else {
		assertRetainedSemanticStreamDocument(t, got, 17)
	}
	if got, err := reopenedCol.Get(ids[9]); err != nil {
		t.Fatalf("Get semantic stream whitespace-row doc after reopen: %v", err)
	} else {
		assertRetainedSemanticStreamDocument(t, got, 9)
	}
	_, reopenedRow, reopenedPtr := requireColumnRetainedSemanticStreamLocatorAndBlockPointer(t, reopen, "events", ids[17])
	if reopenedRow != row || reopenedPtr != ptr {
		t.Fatalf("semantic stream locator changed after reopen: row/ptr got %d/%+v want %d/%+v", reopenedRow, reopenedPtr, row, ptr)
	}
}

func TestColumnRetainedPayloadSemanticStreamV1StoredBlockEncoderReuse(t *testing.T) {
	_, docsA := retainedSemanticStreamDocuments(96)
	_, docsB := retainedSemanticStreamDocumentsFrom(96, 96)
	rawA, err := encodeColumnRetainedSemanticStreamV1RawBlock(docsA)
	if err != nil {
		t.Fatalf("encode raw block A: %v", err)
	}
	rawB, err := encodeColumnRetainedSemanticStreamV1RawBlock(docsB)
	if err != nil {
		t.Fatalf("encode raw block B: %v", err)
	}

	encoder, err := newColumnRetainedSemanticStreamV1StoredBlockEncoder()
	if err != nil {
		t.Fatalf("new stored block encoder: %v", err)
	}
	defer encoder.close()
	for name, raw := range map[string][]byte{"A": rawA, "B": rawB} {
		block, err := encoder.encodeWithRawLimit(raw, maxColumnRetainedSemanticStreamV1CompressedRawBlockBytes)
		if err != nil {
			t.Fatalf("encode reused block %s: %v", name, err)
		}
		if !bytes.HasPrefix(block, columnRetainedSemanticStreamV1BlockZSTDMagic) {
			t.Fatalf("reused block %s magic=%q want zstd wrapper", name, block[:len(columnRetainedSemanticStreamV1BlockMagic)])
		}
		decoded, err := decodeColumnRetainedSemanticStreamV1StoredBlock(block)
		if err != nil {
			t.Fatalf("decode reused block %s: %v", name, err)
		}
		if !bytes.Equal(decoded, raw) {
			t.Fatalf("decoded reused block %s differs from raw block", name)
		}
	}
}

func TestColumnRetainedPayloadSemanticStreamV1StoredBlockEncoderRawFallbackDoesNotAliasScratch3221(t *testing.T) {
	_, docsA := retainedSemanticStreamDocuments(96)
	_, docsB := retainedSemanticStreamDocumentsFrom(96, 96)
	streamsA := retainedSemanticStreamTestStreamsFromDocuments(t, docsA)
	streamsB := retainedSemanticStreamTestStreamsFromDocuments(t, docsB)

	encoder, err := newColumnRetainedSemanticStreamV1StoredBlockEncoder()
	if err != nil {
		t.Fatalf("new stored block encoder: %v", err)
	}
	defer encoder.close()

	blockA, err := encoder.encodeStreamsWithRawLimit(len(docsA), streamsA, 1)
	if err != nil {
		t.Fatalf("encode raw fallback block A: %v", err)
	}
	if !bytes.HasPrefix(blockA, columnRetainedSemanticStreamV1BlockMagic) {
		t.Fatalf("block A magic=%q want raw semantic-stream block", blockA[:len(columnRetainedSemanticStreamV1BlockMagic)])
	}
	blockACopy := append([]byte(nil), blockA...)

	if _, err := encoder.encodeStreamsWithRawLimit(len(docsB), streamsB, 1); err != nil {
		t.Fatalf("encode raw fallback block B: %v", err)
	}
	if !bytes.Equal(blockA, blockACopy) {
		t.Fatal("raw fallback block A aliases reusable encoder scratch and changed after block B")
	}
	decodedA, err := decodeColumnRetainedSemanticStreamV1StoredBlock(blockA)
	if err != nil {
		t.Fatalf("decode raw fallback block A after reuse: %v", err)
	}
	if !bytes.Equal(decodedA, blockACopy) {
		t.Fatal("decoded raw fallback block A differs after encoder scratch reuse")
	}
}

func TestColumnRetainedPayloadSemanticStreamV1StoredBlockEncoderRawFallbackDoesNotAliasPooledScratch3223(t *testing.T) {
	previousPool := columnRetainedSemanticStreamV1RawBlockScratchPool
	columnRetainedSemanticStreamV1RawBlockScratchPool = make(chan []byte, columnRetainedSemanticStreamV1RawBlockScratchPoolSlots)
	t.Cleanup(func() {
		columnRetainedSemanticStreamV1RawBlockScratchPool = previousPool
	})

	_, docsA := retainedSemanticStreamDocuments(96)
	_, docsB := retainedSemanticStreamDocumentsFrom(96, 96)
	streamsA := retainedSemanticStreamTestStreamsFromDocuments(t, docsA)
	streamsB := retainedSemanticStreamTestStreamsFromDocuments(t, docsB)

	encoderA, err := newColumnRetainedSemanticStreamV1StoredBlockEncoder()
	if err != nil {
		t.Fatalf("new stored block encoder A: %v", err)
	}
	blockA, err := encoderA.encodeStreamsWithRawLimit(len(docsA), streamsA, 1)
	if err != nil {
		encoderA.close()
		t.Fatalf("encode raw fallback block A: %v", err)
	}
	if !bytes.HasPrefix(blockA, columnRetainedSemanticStreamV1BlockMagic) {
		encoderA.close()
		t.Fatalf("block A magic=%q want raw semantic-stream block", blockA[:len(columnRetainedSemanticStreamV1BlockMagic)])
	}
	blockACopy := append([]byte(nil), blockA...)
	returnedScratchCap := cap(encoderA.rawBlockScratch)
	if returnedScratchCap == 0 {
		encoderA.close()
		t.Fatal("encoder A did not retain raw block scratch")
	}
	encoderA.close()

	encoderB, err := newColumnRetainedSemanticStreamV1StoredBlockEncoder()
	if err != nil {
		t.Fatalf("new stored block encoder B: %v", err)
	}
	defer encoderB.close()
	if cap(encoderB.rawBlockScratch) != returnedScratchCap {
		t.Fatalf("encoder B scratch cap=%d want pooled cap %d", cap(encoderB.rawBlockScratch), returnedScratchCap)
	}
	if _, err := encoderB.encodeStreamsWithRawLimit(len(docsB), streamsB, 1); err != nil {
		t.Fatalf("encode raw fallback block B: %v", err)
	}
	if !bytes.Equal(blockA, blockACopy) {
		t.Fatal("raw fallback block A aliases pooled encoder scratch and changed after block B")
	}
	decodedA, err := decodeColumnRetainedSemanticStreamV1StoredBlock(blockA)
	if err != nil {
		t.Fatalf("decode raw fallback block A after pooled reuse: %v", err)
	}
	if !bytes.Equal(decodedA, blockACopy) {
		t.Fatal("decoded raw fallback block A differs after pooled scratch reuse")
	}
}

func TestColumnRetainedPayloadSemanticStreamV1RootFastPathPreparesDeclaredRows(t *testing.T) {
	cfg := ColumnStoreConfig{
		Enabled: true,
		Columns: []ColumnStoreColumn{
			{Name: "row_id", Path: "row_id", ValueType: ColumnStoreValueInt64, Owner: TypedStorageOwnerRowAsset},
			{Name: "kind", Path: "kind", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerColumnPart, Dictionary: true},
		},
		RetainedPayload:         ColumnRetainedPayloadNonColumn,
		RetainedPayloadEncoding: ColumnRetainedPayloadEncodingSemanticStreamV1,
		Reconstruction:          ColumnReconstructionRetainedPayloadAndColumns,
	}
	ids, docs := retainedSemanticStreamDocuments(12)
	docs[5] = []byte(`{"row_id":5,"kind":"kind-5","payload":"payload-005","note":"quote \" slash \\ smile \u263a","commit":{"cid":"bafy-test-000005"}}`)
	docs[6] = []byte(`{"row_id":6,"kind":"old-kind","kind":"kind-6","payload":{"old":true},"payload":{"kept":false,"kept":true,"inner":{"old":1,"old":2}},"note":"old-note","note":["kept-note"],"commit":{"cid":"old-cid"},"commit":"kept-commit"}`)
	prepared, err := prepareColumnRetainedPayloadInsertBatchStorageDocumentsWithIDs(cfg, ids, docs, nil)
	if err != nil {
		t.Fatalf("prepare semantic-stream-v1 documents with ids: %v", err)
	}
	if !prepared.declaredRowsReady {
		t.Fatal("semantic-stream-v1 root fast path did not prepare declared rows")
	}
	if len(prepared.declaredRows) != len(docs) {
		t.Fatalf("declared rows=%d want %d", len(prepared.declaredRows), len(docs))
	}
	row := prepared.declaredRows[5]
	if got := string(row.ID); got != "doc-000005" {
		t.Fatalf("declared row id=%q want doc-000005", got)
	}
	if got := row.Values[0].Int64; got != 5 {
		t.Fatalf("declared row_id=%d want 5", got)
	}
	if got := row.Values[1].String; got != "kind-5" {
		t.Fatalf("declared kind=%q want kind-5", got)
	}
	duplicateRow := prepared.declaredRows[6]
	if got := duplicateRow.Values[1].String; got != "kind-6" {
		t.Fatalf("duplicate declared kind=%q want last duplicate kind-6", got)
	}
	if prepared.semanticStreamBlocks == nil {
		t.Fatal("semantic-stream-v1 root fast path did not return block table")
	}
	defer resetCollectionRunTable(prepared.semanticStreamBlocks)
	iter := prepared.semanticStreamBlocks.NewIterator(nil, nil)
	defer func() { _ = iter.Close() }()
	if !iter.Valid() {
		t.Fatal("semantic-stream-v1 block table is empty")
	}
	rowsJSON, err := decodeColumnRetainedSemanticStreamV1BlockRowsJSON(iter.UnsafeValue())
	if err != nil {
		t.Fatalf("decode semantic-stream-v1 block rows: %v", err)
	}
	if len(rowsJSON) != len(docs) {
		t.Fatalf("decoded retained rows=%d want %d", len(rowsJSON), len(docs))
	}
	var retained map[string]any
	if err := json.Unmarshal(rowsJSON[5], &retained); err != nil {
		t.Fatalf("decode retained row JSON: %v", err)
	}
	if _, ok := retained["row_id"]; ok {
		t.Fatalf("retained payload still contains declared row_id: %s", rowsJSON[5])
	}
	if _, ok := retained["kind"]; ok {
		t.Fatalf("retained payload still contains declared kind: %s", rowsJSON[5])
	}
	if _, ok := retained["commit"]; !ok {
		t.Fatalf("retained payload lost commit field: %s", rowsJSON[5])
	}
	if got, want := retained["note"], "quote \" slash \\ smile \u263a"; got != want {
		t.Fatalf("retained payload note=%q want %q from escaped JSON string: %s", got, want, rowsJSON[5])
	}
	var duplicateRetained map[string]any
	if err := json.Unmarshal(rowsJSON[6], &duplicateRetained); err != nil {
		t.Fatalf("decode duplicate retained row JSON: %v", err)
	}
	payload, ok := duplicateRetained["payload"].(map[string]any)
	if !ok {
		t.Fatalf("duplicate retained payload=%T want object: %s", duplicateRetained["payload"], rowsJSON[6])
	}
	if _, ok := payload["old"]; ok {
		t.Fatalf("duplicate retained payload kept overwritten root object: %s", rowsJSON[6])
	}
	if got, ok := payload["kept"].(bool); !ok || !got {
		t.Fatalf("duplicate retained payload missing last root object: %s", rowsJSON[6])
	}
	inner, ok := payload["inner"].(map[string]any)
	if !ok {
		t.Fatalf("duplicate retained nested payload=%T want object: %s", payload["inner"], rowsJSON[6])
	}
	if got, want := inner["old"], float64(2); got != want {
		t.Fatalf("duplicate retained nested old=%v want %v: %s", got, want, rowsJSON[6])
	}
	if got, want := duplicateRetained["commit"], "kept-commit"; got != want {
		t.Fatalf("duplicate retained commit=%q want %q: %s", got, want, rowsJSON[6])
	}
	note, ok := duplicateRetained["note"].([]any)
	if !ok || len(note) != 1 || note[0] != "kept-note" {
		t.Fatalf("duplicate retained note=%#v want last duplicate array: %s", duplicateRetained["note"], rowsJSON[6])
	}
}

func TestColumnRetainedPayloadSemanticStreamV1NestedDeclaredPathsMatchRetainedJSONPipeline(t *testing.T) {
	cfg := ColumnStoreConfig{
		Enabled: true,
		Columns: []ColumnStoreColumn{
			{Name: "a_b", Path: "a.b", ValueType: ColumnStoreValueString, Nullable: true},
			{Name: "operation", Path: "commit.operation", ValueType: ColumnStoreValueString, Nullable: true},
		},
		RetainedPayload:         ColumnRetainedPayloadNonColumn,
		RetainedPayloadEncoding: ColumnRetainedPayloadEncodingSemanticStreamV1,
		Reconstruction:          ColumnReconstructionRetainedPayloadAndColumns,
	}
	docs := [][]byte{
		[]byte(`{"a":{"b":"drop","c":1},"commit":{"operation":"create","collection":"keep"},"literal.path":"keep","payload":{"body":"kept"}}`),
		[]byte(`{"a":{"b":"old","old":true},"a":{"b":"drop","c":{"d":1,"d":2},"empty":{"b":"not declared"}},"commit":{"operation":"old","record":{"text":"old"}},"commit":{"operation":"drop","record":{"text":"keep-last"},"blank":{}}}`),
		[]byte(`{"a":"scalar","commit":"scalar","other":{}}`),
		[]byte(`{"a":{"b":"drop"},"commit":{"operation":"drop"}}`),
		[]byte(`{"a.b":"literal","commit.operation":"literal","a":{"b":"drop","x":"keep"}}`),
		[]byte(`{"a":{"b":null,"x":"keep"},"commit":{"operation":null,"collection":"keep"},"payload":"nulls"}`),
		[]byte(`{"a":{"b":"old","b":"drop","x":"keep"},"commit":{"operation":"old","operation":"drop","record":{"text":"keep"}}}`),
	}
	ids := [][]byte{
		[]byte("doc-nested-0"),
		[]byte("doc-nested-1"),
		[]byte("doc-nested-2"),
		[]byte("doc-nested-3"),
		[]byte("doc-nested-4"),
		[]byte("doc-nested-5"),
		[]byte("doc-nested-6"),
	}
	prepared, err := prepareColumnRetainedPayloadInsertBatchStorageDocumentsWithIDs(cfg, ids, docs, nil)
	if err != nil {
		t.Fatalf("prepare semantic-stream-v1 nested declared documents: %v", err)
	}
	if !prepared.declaredRowsReady {
		t.Fatal("semantic-stream-v1 nested declared paths did not prepare declared rows")
	}
	if len(prepared.declaredRows) != len(docs) {
		t.Fatalf("declared rows=%d want %d", len(prepared.declaredRows), len(docs))
	}
	assertNestedRetainedSemanticStreamDeclaredRow := func(rowIdx int, wantID, wantAB, wantOperation string, wantABPresent, wantABNull, wantOperationPresent, wantOperationNull bool) {
		t.Helper()
		row := prepared.declaredRows[rowIdx]
		if got := string(row.ID); got != wantID {
			t.Fatalf("declared row %d ID=%q want %q", rowIdx, got, wantID)
		}
		if len(row.Values) != 2 {
			t.Fatalf("declared row %d values=%d want 2", rowIdx, len(row.Values))
		}
		gotAB := row.Values[0]
		if gotAB.Type != ColumnStoreValueString {
			t.Fatalf("declared row %d a.b type=%q want string", rowIdx, gotAB.Type)
		}
		if gotAB.Present != wantABPresent {
			t.Fatalf("declared row %d a.b present=%t want %t", rowIdx, gotAB.Present, wantABPresent)
		}
		if gotAB.Null != wantABNull {
			t.Fatalf("declared row %d a.b null=%t want %t", rowIdx, gotAB.Null, wantABNull)
		}
		if !wantABNull && gotAB.String != wantAB {
			t.Fatalf("declared row %d a.b=%q want %q", rowIdx, gotAB.String, wantAB)
		}
		gotOperation := row.Values[1]
		if gotOperation.Type != ColumnStoreValueString {
			t.Fatalf("declared row %d commit.operation type=%q want string", rowIdx, gotOperation.Type)
		}
		if gotOperation.Present != wantOperationPresent {
			t.Fatalf("declared row %d commit.operation present=%t want %t", rowIdx, gotOperation.Present, wantOperationPresent)
		}
		if gotOperation.Null != wantOperationNull {
			t.Fatalf("declared row %d commit.operation null=%t want %t", rowIdx, gotOperation.Null, wantOperationNull)
		}
		if !wantOperationNull && gotOperation.String != wantOperation {
			t.Fatalf("declared row %d commit.operation=%q want %q", rowIdx, gotOperation.String, wantOperation)
		}
	}
	assertNestedRetainedSemanticStreamDeclaredRow(0, "doc-nested-0", "drop", "create", true, false, true, false)
	assertNestedRetainedSemanticStreamDeclaredRow(1, "doc-nested-1", "drop", "drop", true, false, true, false)
	assertNestedRetainedSemanticStreamDeclaredRow(2, "doc-nested-2", "", "", false, true, false, true)
	assertNestedRetainedSemanticStreamDeclaredRow(3, "doc-nested-3", "drop", "drop", true, false, true, false)
	assertNestedRetainedSemanticStreamDeclaredRow(4, "doc-nested-4", "drop", "", true, false, false, true)
	assertNestedRetainedSemanticStreamDeclaredRow(5, "doc-nested-5", "", "", true, true, true, true)
	assertNestedRetainedSemanticStreamDeclaredRow(6, "doc-nested-6", "drop", "drop", true, false, true, false)
	if columnRetainedSemanticStreamV1TestBytesAliasBytes(prepared.declaredRows[0].ID, ids[0]) {
		t.Fatal("nested prepared declared row ID aliases mutable input ID")
	}
	if columnRetainedSemanticStreamV1TestStringAliasesBytes(prepared.declaredRows[0].Values[0].String, docs[0]) {
		t.Fatal("nested prepared declared string aliases mutable input document")
	}
	if prepared.semanticStreamBlocks == nil {
		t.Fatal("prepare semantic-stream-v1 documents did not return block table")
	}
	defer resetCollectionRunTable(prepared.semanticStreamBlocks)

	iter := prepared.semanticStreamBlocks.NewIterator(nil, nil)
	defer func() { _ = iter.Close() }()
	if !iter.Valid() {
		t.Fatal("semantic-stream-v1 block table is empty")
	}
	gotBlock := append([]byte(nil), iter.UnsafeValue()...)
	iter.Next()
	if iter.Valid() {
		t.Fatal("semantic-stream-v1 block table has more than one block")
	}
	if err := iter.Error(); err != nil {
		t.Fatalf("iterate semantic-stream-v1 block table: %v", err)
	}

	retainedJSON := make([][]byte, len(docs))
	for i, doc := range docs {
		retained, err := columnRetainedPayloadJSONFromJSONDocument(cfg, doc)
		if err != nil {
			t.Fatalf("legacy retained JSON row %d: %v", i, err)
		}
		retainedJSON[i] = retained
	}
	wantBlock, err := encodeColumnRetainedSemanticStreamV1Block(retainedJSON)
	if err != nil {
		t.Fatalf("legacy semantic-stream-v1 block: %v", err)
	}

	rowsJSON, err := decodeColumnRetainedSemanticStreamV1BlockRowsJSON(gotBlock)
	if err != nil {
		t.Fatalf("decode semantic-stream-v1 rows: %v", err)
	}
	wantRowsJSON, err := decodeColumnRetainedSemanticStreamV1BlockRowsJSON(wantBlock)
	if err != nil {
		t.Fatalf("decode legacy semantic-stream-v1 rows: %v", err)
	}
	if len(rowsJSON) != len(wantRowsJSON) {
		t.Fatalf("semantic-stream-v1 nested declared row count=%d want %d", len(rowsJSON), len(wantRowsJSON))
	}
	for i := range rowsJSON {
		assertJSONEqualM13C(t, rowsJSON[i], wantRowsJSON[i])
	}
	row1 := decodeRetainedSemanticStreamTestObject(t, rowsJSON[1])
	a1 := row1["a"].(map[string]any)
	if _, ok := a1["b"]; ok {
		t.Fatalf("declared nested a.b leaked in row 1: %s", rowsJSON[1])
	}
	c1 := a1["c"].(map[string]any)
	if got, want := c1["d"], float64(2); got != want {
		t.Fatalf("duplicate nested retained a.c.d=%v want %v: %s", got, want, rowsJSON[1])
	}
	if got := row1["commit"].(map[string]any)["record"].(map[string]any)["text"]; got != "keep-last" {
		t.Fatalf("duplicate retained commit record text=%v want keep-last: %s", got, rowsJSON[1])
	}
	row2 := decodeRetainedSemanticStreamTestObject(t, rowsJSON[2])
	if got := row2["a"]; got != "scalar" {
		t.Fatalf("non-object declared ancestor a=%v want scalar: %s", got, rowsJSON[2])
	}
	if got := row2["commit"]; got != "scalar" {
		t.Fatalf("non-object declared ancestor commit=%v want scalar: %s", got, rowsJSON[2])
	}
	row3 := decodeRetainedSemanticStreamTestObject(t, rowsJSON[3])
	if got := len(row3["a"].(map[string]any)); got != 0 {
		t.Fatalf("empty retained a len=%d want 0: %s", got, rowsJSON[3])
	}
	if got := len(row3["commit"].(map[string]any)); got != 0 {
		t.Fatalf("empty retained commit len=%d want 0: %s", got, rowsJSON[3])
	}
	row4 := decodeRetainedSemanticStreamTestObject(t, rowsJSON[4])
	if got := row4["a.b"]; got != "literal" {
		t.Fatalf("literal dotted root key a.b=%v want literal: %s", got, rowsJSON[4])
	}
	if got := row4["commit.operation"]; got != "literal" {
		t.Fatalf("literal dotted root key commit.operation=%v want literal: %s", got, rowsJSON[4])
	}
	row6 := decodeRetainedSemanticStreamTestObject(t, rowsJSON[6])
	a6 := row6["a"].(map[string]any)
	if _, ok := a6["b"]; ok {
		t.Fatalf("duplicate terminal declared a.b leaked in row 6: %s", rowsJSON[6])
	}
	if got := a6["x"]; got != "keep" {
		t.Fatalf("duplicate terminal retained a.x=%v want keep: %s", got, rowsJSON[6])
	}
	commit6 := row6["commit"].(map[string]any)
	if _, ok := commit6["operation"]; ok {
		t.Fatalf("duplicate terminal declared commit.operation leaked in row 6: %s", rowsJSON[6])
	}
	if got := commit6["record"].(map[string]any)["text"]; got != "keep" {
		t.Fatalf("duplicate terminal retained commit.record.text=%v want keep: %s", got, rowsJSON[6])
	}

	ids[0][0] = 'X'
	dropStart := bytes.Index(docs[0], []byte("drop"))
	if dropStart < 0 {
		t.Fatal("drop token not found in source document")
	}
	copy(docs[0][dropStart:], []byte("mut!"))
	if got := string(prepared.declaredRows[0].ID); got != "doc-nested-0" {
		t.Fatalf("nested prepared declared row ID changed after source ID mutation: %q", got)
	}
	if got := prepared.declaredRows[0].Values[0].String; got != "drop" {
		t.Fatalf("nested prepared declared string changed after source document mutation: %q", got)
	}
}

func decodeRetainedSemanticStreamTestObject(t testing.TB, raw []byte) map[string]any {
	t.Helper()
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		t.Fatalf("decode retained semantic-stream row %s: %v", raw, err)
	}
	return obj
}

func TestPrepareColumnWritePublishInputUsesPreparedDocuments(t *testing.T) {
	cfg := &ColumnStoreConfig{
		Enabled: true,
		Columns: []ColumnStoreColumn{
			{Name: "row_id", Path: "row_id", ValueType: ColumnStoreValueInt64},
		},
	}
	documents := []columnWriteDocument{{
		ID:       []byte("doc-1"),
		Document: []byte(`not-json`),
		declaredValues: []columnDeclaredValue{{
			Type:    ColumnStoreValueInt64,
			Present: true,
			Int64:   42,
		}},
		declaredValuesReady: true,
	}}
	input, err := prepareColumnWritePublishInputBeforeCommandWAL(columnWritePublishInput{
		meta: CollectionMeta{Options: CollectionOptions{
			DocumentFormat: DocumentFormatJSON,
			ColumnStore:    cfg,
		}},
		operation: ColumnPublishOperationInsert,
		documents: documents,
		rows:      1,
	})
	if err != nil {
		t.Fatalf("prepare with declared rows ready: %v", err)
	}
	if len(input.declaredRows) != 1 || input.declaredRows[0].Values[0].Int64 != 42 {
		t.Fatalf("prepared declared rows not preserved: %+v", input.declaredRows)
	}
	if input.documentExtraction != 0 {
		t.Fatalf("prepared declared rows recorded document extraction: %s", input.documentExtraction)
	}
}

func TestColumnRetainedPayloadSemanticStreamV1PreservesRetainedJSONNumbers(t *testing.T) {
	dir := t.TempDir()
	enableColumnRetainedPlacementCommandWAL(t, dir)

	d := openColumnRetainedPlacementDB(t, dir, backenddb.Options{})
	defer func() { _ = d.Close() }()
	col := createColumnRetainedSemanticStreamCollection(t, d, "events")
	ids := [][]byte{[]byte("doc-big"), []byte("doc-control")}
	docs := [][]byte{
		[]byte(`{"row_id":0,"kind":"kind-0","payload":{"big":9007199254740993,"decimal":0.100000000000000005}}`),
		[]byte(`{"row_id":1,"kind":"kind-1","payload":{"big":9007199254740995,"decimal":0.200000000000000005}}`),
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	got, err := col.Get(ids[0])
	if err != nil {
		t.Fatalf("Get semantic stream number doc: %v", err)
	}
	assertJSONNumberString(t, got, []string{"payload", "big"}, "9007199254740993")
	assertJSONNumberString(t, got, []string{"payload", "decimal"}, "0.100000000000000005")

	view, err := col.OpenCollectionReadView()
	if err != nil {
		t.Fatalf("OpenCollectionReadView: %v", err)
	}
	response, err := view.FetchDocumentsByID([][]byte{ids[1]}, DocumentFetchOptions{})
	closeErr := view.Close()
	if err != nil {
		t.Fatalf("FetchDocumentsByID: %v", err)
	}
	if closeErr != nil {
		t.Fatalf("Close read view: %v", closeErr)
	}
	if len(response.Results) != 1 || !response.Results[0].Found {
		t.Fatalf("FetchDocumentsByID results=%+v", response.Results)
	}
	assertJSONNumberString(t, response.Results[0].Document, []string{"payload", "big"}, "9007199254740995")
	assertJSONNumberString(t, response.Results[0].Document, []string{"payload", "decimal"}, "0.200000000000000005")
}

func TestColumnRetainedPayloadSemanticStreamV1SideRootRewrite(t *testing.T) {
	dir := t.TempDir()
	enableColumnRetainedPlacementCommandWAL(t, dir)

	d := openColumnRetainedPlacementDB(t, dir, backenddb.Options{})
	col := createColumnRetainedSemanticStreamCollection(t, d, "events")
	ids, docs := retainedSemanticStreamDocuments(64)
	if _, err := col.InsertBatch(ids, docs); err != nil {
		t.Fatalf("InsertBatch first block: %v", err)
	}
	_, _, ptr := requireColumnRetainedSemanticStreamLocatorAndBlockPointer(t, d, "events", ids[8])
	if err := d.Close(); err != nil {
		t.Fatalf("Close after first block: %v", err)
	}

	d = openColumnRetainedPlacementDB(t, dir, backenddb.Options{})
	col = openColumnRetainedPlacementCollection(t, d, "events")
	moreIDs, moreDocs := retainedSemanticStreamDocumentsFrom(64, 8)
	if _, err := col.InsertBatch(moreIDs, moreDocs); err != nil {
		_ = d.Close()
		t.Fatalf("InsertBatch second block: %v", err)
	}
	rewriteStats, err := d.ValueLogRewriteOnline(context.Background(), backenddb.ValueLogRewriteOnlineOptions{
		SourceFileIDs: []uint32{ptr.FileID},
		BatchSize:     1,
	})
	if err != nil {
		_ = d.Close()
		t.Fatalf("ValueLogRewriteOnline: %v", err)
	}
	if rewriteStats.RecordsCopied == 0 {
		_ = d.Close()
		t.Fatalf("ValueLogRewriteOnline copied no semantic stream records: %+v", rewriteStats)
	}
	_, _, rewrittenPtr := requireColumnRetainedSemanticStreamLocatorAndBlockPointer(t, d, "events", ids[8])
	if rewrittenPtr.FileID == ptr.FileID {
		_ = d.Close()
		t.Fatalf("semantic stream block pointer still references rewrite source segment %d", ptr.FileID)
	}
	if got, err := col.Get(ids[8]); err != nil {
		_ = d.Close()
		t.Fatalf("Get semantic stream doc after rewrite: %v", err)
	} else {
		assertRetainedSemanticStreamDocument(t, got, 8)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close after rewrite: %v", err)
	}

	reopen := openColumnRetainedPlacementDB(t, dir, backenddb.Options{})
	defer func() { _ = reopen.Close() }()
	reopenedCol := openColumnRetainedPlacementCollection(t, reopen, "events")
	if got, err := reopenedCol.Get(ids[8]); err != nil {
		t.Fatalf("Get semantic stream doc after rewrite reopen: %v", err)
	} else {
		assertRetainedSemanticStreamDocument(t, got, 8)
	}
	_, _, reopenedPtr := requireColumnRetainedSemanticStreamLocatorAndBlockPointer(t, reopen, "events", ids[8])
	if reopenedPtr != rewrittenPtr {
		t.Fatalf("rewritten semantic stream pointer changed after reopen: got=%+v want=%+v", reopenedPtr, rewrittenPtr)
	}
}

func TestColumnRetainedPayloadSemanticStreamV1ReclaimsSideBlockAfterDelete(t *testing.T) {
	dir := t.TempDir()
	enableColumnRetainedPlacementCommandWAL(t, dir)

	d := openColumnRetainedPlacementDB(t, dir, backenddb.Options{})
	defer func() { _ = d.Close() }()
	col := createColumnRetainedSemanticStreamCollection(t, d, "events")
	ids, docs := retainedSemanticStreamDocuments(64)
	if _, err := col.InsertBatch(ids, docs); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	blockKey, _, _ := requireColumnRetainedSemanticStreamLocatorAndBlockPointer(t, d, "events", ids[0])

	deleted, err := col.DeleteBatch(ids[:63])
	if err != nil {
		t.Fatalf("DeleteBatch partial block: %v", err)
	}
	if deleted != 63 {
		t.Fatalf("DeleteBatch deleted=%d want 63", deleted)
	}
	liveBlockKey, _, _ := requireColumnRetainedSemanticStreamLocatorAndBlockPointer(t, d, "events", ids[63])
	if !bytes.Equal(liveBlockKey, blockKey) {
		t.Fatalf("live row moved to block %x want original %x", liveBlockKey, blockKey)
	}
	if got, err := col.Get(ids[63]); err != nil {
		t.Fatalf("Get remaining semantic row: %v", err)
	} else {
		assertRetainedSemanticStreamDocument(t, got, 63)
	}

	deletedOne, err := col.DeleteDocument(ids[63])
	if err != nil {
		t.Fatalf("DeleteDocument final row: %v", err)
	}
	if !deletedOne {
		t.Fatal("DeleteDocument final row reported deleted=false")
	}
	requireColumnRetainedSemanticStreamBlockDeleted(t, d, "events", blockKey)
}

func TestColumnRetainedPayloadSemanticStreamV1ReclaimsSideBlockAfterUpdateBatch(t *testing.T) {
	dir := t.TempDir()
	enableColumnRetainedPlacementCommandWAL(t, dir)

	d := openColumnRetainedPlacementDB(t, dir, backenddb.Options{})
	defer func() { _ = d.Close() }()
	col := createColumnRetainedSemanticStreamCollection(t, d, "events")
	ids, docs := retainedSemanticStreamDocuments(64)
	if _, err := col.InsertBatch(ids, docs); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	blockKey, _, _ := requireColumnRetainedSemanticStreamLocatorAndBlockPointer(t, d, "events", ids[8])

	items := make([]UpdateBatchItem, len(ids))
	for i := range ids {
		row := i
		replacement := retainedSemanticStreamUpdatedDocument(row)
		items[i] = UpdateBatchItem{
			DocumentID: ids[i],
			Update: func(current []byte) ([]byte, bool, error) {
				return replacement, true, nil
			},
		}
	}
	results, err := col.UpdateBatch(items)
	if err != nil {
		t.Fatalf("UpdateBatch: %v", err)
	}
	if len(results) != len(ids) {
		t.Fatalf("UpdateBatch results=%d want %d", len(results), len(ids))
	}
	for i, result := range results {
		if !result.Matched || !result.Modified {
			t.Fatalf("UpdateBatch result[%d]=%+v want matched+modified", i, result)
		}
	}
	requireColumnRetainedSemanticStreamBlockDeleted(t, d, "events", blockKey)
	replacementBlockKey, replacementRow, replacementPtr := requireColumnRetainedSemanticStreamLocatorAndBlockPointer(t, d, "events", ids[8])
	if bytes.Equal(replacementBlockKey, blockKey) {
		t.Fatalf("updated row still references reclaimed block %x", blockKey)
	}
	if replacementRow != 8 {
		t.Fatalf("updated semantic locator row=%d want 8", replacementRow)
	}
	if !page.IsValueLogFileID(replacementPtr.FileID) {
		t.Fatalf("updated semantic stream block pointer file id=%d is not value-log backed", replacementPtr.FileID)
	}
	if got, err := col.Get(ids[8]); err != nil {
		t.Fatalf("Get updated semantic row: %v", err)
	} else {
		assertJSONMapEqual1875(t, got, map[string]any{
			"row_id":  float64(8),
			"kind":    "updated-kind-8",
			"payload": "updated-payload-008",
			"commit": map[string]any{
				"cid": "updated-cid-008",
			},
		})
	}
}

func TestColumnRetainedPayloadSemanticStreamV1UpdateWritesReplacementBlock(t *testing.T) {
	dir := t.TempDir()
	enableColumnRetainedPlacementCommandWAL(t, dir)

	d := openColumnRetainedPlacementDB(t, dir, backenddb.Options{})
	defer func() { _ = d.Close() }()
	col := createColumnRetainedSemanticStreamCollection(t, d, "events")
	ids, docs := retainedSemanticStreamDocuments(1)
	if _, err := col.InsertBatch(ids, docs); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	blockKey, _, _ := requireColumnRetainedSemanticStreamLocatorAndBlockPointer(t, d, "events", ids[0])

	matched, modified, err := col.Update(ids[0], func(current []byte) ([]byte, bool, error) {
		assertRetainedSemanticStreamDocument(t, current, 0)
		return retainedSemanticStreamUpdatedDocument(0), true, nil
	})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if !matched || !modified {
		t.Fatalf("Update matched=%v modified=%v want true,true", matched, modified)
	}
	requireColumnRetainedSemanticStreamBlockDeleted(t, d, "events", blockKey)
	replacementBlockKey, replacementRow, _ := requireColumnRetainedSemanticStreamLocatorAndBlockPointer(t, d, "events", ids[0])
	if bytes.Equal(replacementBlockKey, blockKey) {
		t.Fatalf("updated row still references reclaimed block %x", blockKey)
	}
	if replacementRow != 0 {
		t.Fatalf("updated semantic locator row=%d want 0", replacementRow)
	}
	if got, err := col.Get(ids[0]); err != nil {
		t.Fatalf("Get updated semantic row: %v", err)
	} else {
		assertJSONMapEqual1875(t, got, map[string]any{
			"row_id":  float64(0),
			"kind":    "updated-kind-0",
			"payload": "updated-payload-000",
			"commit": map[string]any{
				"cid": "updated-cid-000",
			},
		})
	}
}

func TestColumnRetainedPayloadSemanticStreamV1DefaultUpdateBatchWritesReplacementBlock(t *testing.T) {
	dir := t.TempDir()
	enableColumnRetainedPlacementCommandWAL(t, dir)

	d := openColumnRetainedPlacementDB(t, dir, backenddb.Options{})
	defer func() { _ = d.Close() }()
	col := createColumnRetainedSemanticStreamDefaultCollection(t, d, "events")
	ids, docs := retainedSemanticStreamDocuments(4)
	if _, err := col.InsertBatch(ids, docs); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	blockKey, _, _ := requireColumnRetainedSemanticStreamLocatorAndBlockPointer(t, d, "events", ids[1])

	items := make([]UpdateBatchItem, len(ids))
	for i := range ids {
		row := i
		replacement := retainedSemanticStreamUpdatedDocument(row)
		items[i] = UpdateBatchItem{
			DocumentID: ids[i],
			Update: func(current []byte) ([]byte, bool, error) {
				return replacement, true, nil
			},
		}
	}
	results, err := col.UpdateBatch(items)
	if err != nil {
		t.Fatalf("UpdateBatch: %v", err)
	}
	if len(results) != len(ids) {
		t.Fatalf("UpdateBatch results=%d want %d", len(results), len(ids))
	}
	requireColumnRetainedSemanticStreamBlockDeleted(t, d, "events", blockKey)
	replacementBlockKey, replacementRow, _ := requireColumnRetainedSemanticStreamLocatorAndBlockPointer(t, d, "events", ids[1])
	if bytes.Equal(replacementBlockKey, blockKey) {
		t.Fatalf("updated default row still references reclaimed block %x", blockKey)
	}
	if replacementRow != 1 {
		t.Fatalf("updated default semantic locator row=%d want 1", replacementRow)
	}
	if got, err := col.Get(ids[1]); err != nil {
		t.Fatalf("Get updated default semantic row: %v", err)
	} else {
		assertJSONMapEqual1875(t, got, map[string]any{
			"row_id":  float64(1),
			"kind":    "updated-kind-1",
			"payload": "updated-payload-001",
			"commit": map[string]any{
				"cid": "updated-cid-001",
			},
		})
	}
}

func enableColumnRetainedPlacementCommandWAL(t testing.TB, dir string) {
	t.Helper()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1}}); err != nil {
		t.Fatalf("SaveFormatConfig: %v", err)
	}
}

func openColumnRetainedPlacementDB(t testing.TB, dir string, opts backenddb.Options) *backenddb.DB {
	t.Helper()
	opts.Dir = dir
	opts.DisableBackgroundPrune = true
	d, err := backenddb.Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return d
}

func createColumnRetainedPlacementCollection(t testing.TB, d *backenddb.DB, name string) *Collection {
	t.Helper()
	cfg := &ColumnStoreConfig{
		Enabled: true,
		Columns: []ColumnStoreColumn{
			{Name: "row_id", Path: "row_id", ValueType: ColumnStoreValueInt64, Owner: TypedStorageOwnerRowAsset},
			{Name: "kind", Path: "kind", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerColumnPart, Dictionary: true},
		},
		RetainedPayload:         ColumnRetainedPayloadNonColumn,
		RetainedPayloadEncoding: ColumnRetainedPayloadEncodingTemplateV1,
		Reconstruction:          ColumnReconstructionRetainedPayloadAndColumns,
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: name, Options: CollectionOptions{DocumentFormat: DocumentFormatJSON, ColumnStore: cfg}}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	return openColumnRetainedPlacementCollection(t, d, name)
}

func createColumnRetainedSemanticStreamCollection(t testing.TB, d *backenddb.DB, name string) *Collection {
	t.Helper()
	cfg := &ColumnStoreConfig{
		Enabled: true,
		Columns: []ColumnStoreColumn{
			{Name: "row_id", Path: "row_id", ValueType: ColumnStoreValueInt64, Owner: TypedStorageOwnerRowAsset},
			{Name: "kind", Path: "kind", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerColumnPart, Dictionary: true},
		},
		RetainedPayload:         ColumnRetainedPayloadNonColumn,
		RetainedPayloadEncoding: ColumnRetainedPayloadEncodingSemanticStreamV1,
		Reconstruction:          ColumnReconstructionRetainedPayloadAndColumns,
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: name, Options: CollectionOptions{DocumentFormat: DocumentFormatJSON, ColumnStore: cfg}}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	return openColumnRetainedPlacementCollection(t, d, name)
}

func createColumnRetainedSemanticStreamDefaultCollection(t testing.TB, d *backenddb.DB, name string) *Collection {
	t.Helper()
	cfg := &ColumnStoreConfig{
		Enabled: true,
		Columns: []ColumnStoreColumn{
			{Name: "row_id", Path: "row_id", ValueType: ColumnStoreValueInt64, Owner: TypedStorageOwnerRowAsset},
			{Name: "kind", Path: "kind", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerColumnPart, Dictionary: true},
		},
		RetainedPayload: ColumnRetainedPayloadNonColumn,
		Reconstruction:  ColumnReconstructionRetainedPayloadAndColumns,
	}
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: name, Options: CollectionOptions{DocumentFormat: DocumentFormatJSON, ColumnStore: cfg}}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	return openColumnRetainedPlacementCollection(t, d, name)
}

func openColumnRetainedPlacementCollection(t testing.TB, d *backenddb.DB, name string) *Collection {
	t.Helper()
	col, err := NewCollectionManager(d).OpenCollection(name)
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}
	return col
}

func retainedPlacementDocument(payload string, rowID int) []byte {
	return []byte(fmt.Sprintf(`{"row_id":%d,"kind":"kind-%d","payload":"%s"}`, rowID, rowID, strings.Repeat(payload, 10)))
}

func reportColumnRetainedSemanticStreamBenchmarkStoredBlockBytes(b *testing.B, table memtable.Table, inputBytes int64) {
	b.Helper()
	iter := table.NewIterator(nil, nil)
	defer func() { _ = iter.Close() }()
	var storedBytes int64
	var blocks int
	for ; iter.Valid(); iter.Next() {
		blocks++
		storedBytes += int64(len(iter.UnsafeValue()))
	}
	if err := iter.Error(); err != nil {
		b.Fatalf("iterate semantic-stream-v1 benchmark block table: %v", err)
	}
	if blocks == 0 {
		b.Fatal("semantic-stream-v1 benchmark block table is empty")
	}
	b.ReportMetric(float64(storedBytes), "stored_block_B/op")
	b.ReportMetric(float64(storedBytes)/float64(inputBytes), "stored/input")
}

func BenchmarkColumnRetainedPayloadSemanticStreamV1PrepareRootFastPath(b *testing.B) {
	cfg := ColumnStoreConfig{
		Enabled: true,
		Columns: []ColumnStoreColumn{
			{Name: "row_id", Path: "row_id", ValueType: ColumnStoreValueInt64, Owner: TypedStorageOwnerRowAsset},
			{Name: "kind", Path: "kind", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerColumnPart, Dictionary: true},
		},
		RetainedPayload:         ColumnRetainedPayloadNonColumn,
		RetainedPayloadEncoding: ColumnRetainedPayloadEncodingSemanticStreamV1,
		Reconstruction:          ColumnReconstructionRetainedPayloadAndColumns,
	}
	ids, docs := retainedSemanticStreamDocuments(columnRetainedSemanticStreamV1BlockRows)
	var bytesPerIteration int64
	for _, doc := range docs {
		bytesPerIteration += int64(len(doc))
	}
	b.ReportAllocs()
	b.SetBytes(bytesPerIteration)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		prepared, err := prepareColumnRetainedPayloadInsertBatchStorageDocumentsWithIDs(cfg, ids, docs, nil)
		if err != nil {
			b.Fatalf("prepare semantic-stream-v1 documents with ids: %v", err)
		}
		if !prepared.declaredRowsReady || len(prepared.declaredRows) != len(docs) {
			b.Fatalf("prepared declared rows ready=%t len=%d want %d", prepared.declaredRowsReady, len(prepared.declaredRows), len(docs))
		}
		if prepared.semanticStreamBlocks == nil {
			b.Fatal("prepare semantic-stream-v1 documents did not return block table")
		}
		if i == 0 {
			b.StopTimer()
			reportColumnRetainedSemanticStreamBenchmarkStoredBlockBytes(b, prepared.semanticStreamBlocks, bytesPerIteration)
			b.StartTimer()
		}
		resetCollectionRunTable(prepared.semanticStreamBlocks)
	}
}

func BenchmarkColumnRetainedPayloadSemanticStreamV1PrepareRootFastPathMultiBlock(b *testing.B) {
	cfg := ColumnStoreConfig{
		Enabled: true,
		Columns: []ColumnStoreColumn{
			{Name: "row_id", Path: "row_id", ValueType: ColumnStoreValueInt64, Owner: TypedStorageOwnerRowAsset},
			{Name: "kind", Path: "kind", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerColumnPart, Dictionary: true},
		},
		RetainedPayload:         ColumnRetainedPayloadNonColumn,
		RetainedPayloadEncoding: ColumnRetainedPayloadEncodingSemanticStreamV1,
		Reconstruction:          ColumnReconstructionRetainedPayloadAndColumns,
	}
	ids, docs := retainedSemanticStreamDocuments(columnRetainedSemanticStreamV1BlockRows * 4)
	var bytesPerIteration int64
	for _, doc := range docs {
		bytesPerIteration += int64(len(doc))
	}
	b.ReportAllocs()
	b.SetBytes(bytesPerIteration)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		prepared, err := prepareColumnRetainedPayloadInsertBatchStorageDocumentsWithIDs(cfg, ids, docs, nil)
		if err != nil {
			b.Fatalf("prepare semantic-stream-v1 documents with ids: %v", err)
		}
		if !prepared.declaredRowsReady || len(prepared.declaredRows) != len(docs) {
			b.Fatalf("prepared declared rows ready=%t len=%d want %d", prepared.declaredRowsReady, len(prepared.declaredRows), len(docs))
		}
		if prepared.semanticStreamBlocks == nil {
			b.Fatal("prepare semantic-stream-v1 documents did not return block table")
		}
		if i == 0 {
			b.StopTimer()
			reportColumnRetainedSemanticStreamBenchmarkStoredBlockBytes(b, prepared.semanticStreamBlocks, bytesPerIteration)
			b.StartTimer()
		}
		resetCollectionRunTable(prepared.semanticStreamBlocks)
	}
}

func BenchmarkColumnRetainedPayloadSemanticStreamV1PrepareNestedDeclaredPaths(b *testing.B) {
	cfg := ColumnStoreConfig{
		Enabled: true,
		Columns: []ColumnStoreColumn{
			{Name: "row_id", Path: "row_id", ValueType: ColumnStoreValueInt64, Owner: TypedStorageOwnerRowAsset},
			{Name: "kind", Path: "kind", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerColumnPart, Dictionary: true},
			{Name: "cid", Path: "commit.cid", ValueType: ColumnStoreValueString, Owner: TypedStorageOwnerColumnPart, Dictionary: true},
		},
		RetainedPayload:         ColumnRetainedPayloadNonColumn,
		RetainedPayloadEncoding: ColumnRetainedPayloadEncodingSemanticStreamV1,
		Reconstruction:          ColumnReconstructionRetainedPayloadAndColumns,
	}
	ids, docs := retainedSemanticStreamDocuments(columnRetainedSemanticStreamV1BlockRows)
	var bytesPerIteration int64
	for _, doc := range docs {
		bytesPerIteration += int64(len(doc))
	}
	b.ReportAllocs()
	b.SetBytes(bytesPerIteration)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		prepared, err := prepareColumnRetainedPayloadInsertBatchStorageDocumentsWithIDs(cfg, ids, docs, nil)
		if err != nil {
			b.Fatalf("prepare semantic-stream-v1 documents with ids: %v", err)
		}
		if prepared.semanticStreamBlocks == nil {
			b.Fatal("prepare semantic-stream-v1 documents did not return block table")
		}
		if i == 0 {
			b.StopTimer()
			reportColumnRetainedSemanticStreamBenchmarkStoredBlockBytes(b, prepared.semanticStreamBlocks, bytesPerIteration)
			b.StartTimer()
		}
		resetCollectionRunTable(prepared.semanticStreamBlocks)
	}
}

func retainedSemanticStreamDocuments(count int) ([][]byte, [][]byte) {
	return retainedSemanticStreamDocumentsFrom(0, count)
}

func retainedSemanticStreamTestStreamsFromDocuments(t testing.TB, docs [][]byte) *columnRetainedSemanticStreamStreams {
	t.Helper()
	streams := newColumnRetainedSemanticStreamStreams()
	for row, document := range docs {
		trimmed := bytes.TrimSpace(document)
		if len(trimmed) == 0 {
			trimmed = []byte("{}")
		}
		var root json.RawMessage
		if err := json.Unmarshal(trimmed, &root); err != nil {
			t.Fatalf("unmarshal retained semantic stream test row %d: %v", row, err)
		}
		if err := collectColumnRetainedSemanticStreamPaths(root, nil, uint64(row), streams); err != nil {
			t.Fatalf("collect retained semantic stream test row %d: %v", row, err)
		}
	}
	return streams
}

func retainedSemanticStreamDocumentsFrom(start, count int) ([][]byte, [][]byte) {
	ids := make([][]byte, count)
	docs := make([][]byte, count)
	for i := 0; i < count; i++ {
		row := start + i
		ids[i] = []byte(fmt.Sprintf("doc-%06d", row))
		docs[i] = []byte(fmt.Sprintf(
			`{"row_id":%d,"kind":"kind-%d","payload":"payload-%03d","commit":{"cid":"bafy-test-%06d","rev":"3l-%06d","record":{"$type":"app.bsky.feed.post","createdAt":"2026-06-13T12:%02d:00Z","subject":{"uri":"at://did:plc:%06d/app.bsky.feed.post/%06d","cid":"bafy-subject-%06d"},"text":"semantic stream retained payload %03d"}}}`,
			row,
			row%7,
			row%11,
			row,
			row,
			row%60,
			row%19,
			row,
			row,
			row,
		))
	}
	return ids, docs
}

func retainedSemanticStreamUpdatedDocument(row int) []byte {
	return []byte(fmt.Sprintf(`{"row_id":%d,"kind":"updated-kind-%d","payload":"updated-payload-%03d","commit":{"cid":"updated-cid-%03d"}}`, row, row, row, row))
}

func assertRetainedSemanticStreamDocument(t testing.TB, document []byte, row int) {
	t.Helper()
	assertJSONMapEqual1875(t, document, map[string]any{
		"row_id":  float64(row),
		"kind":    fmt.Sprintf("kind-%d", row%7),
		"payload": fmt.Sprintf("payload-%03d", row%11),
		"commit": map[string]any{
			"cid": fmt.Sprintf("bafy-test-%06d", row),
			"rev": fmt.Sprintf("3l-%06d", row),
			"record": map[string]any{
				"$type":     "app.bsky.feed.post",
				"createdAt": fmt.Sprintf("2026-06-13T12:%02d:00Z", row%60),
				"subject": map[string]any{
					"uri": fmt.Sprintf("at://did:plc:%06d/app.bsky.feed.post/%06d", row%19, row),
					"cid": fmt.Sprintf("bafy-subject-%06d", row),
				},
				"text": fmt.Sprintf("semantic stream retained payload %03d", row),
			},
		},
	})
}

func assertJSONNumberString(t testing.TB, document []byte, path []string, want string) {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(document))
	decoder.UseNumber()
	var root map[string]any
	if err := decoder.Decode(&root); err != nil {
		t.Fatalf("document=%q is not valid JSON: %v", document, err)
	}
	var current any = root
	for _, segment := range path {
		obj, ok := current.(map[string]any)
		if !ok {
			t.Fatalf("path %v reached %T at segment %q in document=%s", path, current, segment, document)
		}
		next, ok := obj[segment]
		if !ok {
			t.Fatalf("path %v missing segment %q in document=%s", path, segment, document)
		}
		current = next
	}
	number, ok := current.(json.Number)
	if !ok {
		t.Fatalf("path %v value=%v (%T) want JSON number %q in document=%s", path, current, current, want, document)
	}
	if number.String() != want {
		t.Fatalf("path %v number=%q want %q in document=%s", path, number.String(), want, document)
	}
}

func requireColumnRetainedPlacementPointer(t testing.TB, d *backenddb.DB, collection string, documentID []byte) page.ValuePtr {
	t.Helper()
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	defer func() { _ = snap.Close() }()
	catalog, err := loadCollectionCatalog(snap, collection)
	if err != nil {
		t.Fatalf("loadCollectionCatalog: %v", err)
	}
	if catalog == nil {
		t.Fatalf("collection catalog %q missing", collection)
	}
	entry, _, err := collectionGetEntryAtCatalogRoot(snap, catalog, collectionPrimaryRootName(collection), documentID)
	if err != nil {
		t.Fatalf("collection primary entry %q: %v", string(documentID), err)
	}
	if entry.Flags&node.FlagPointer == 0 {
		t.Fatalf("collection primary entry %q flags=%#x value_len=%d want retained payload ValuePtr", string(documentID), entry.Flags, len(entry.Value))
	}
	if len(entry.Value) != 0 {
		t.Fatalf("collection primary pointer entry %q carried inline value bytes len=%d", string(documentID), len(entry.Value))
	}
	if !page.IsValueLogFileID(entry.ValuePtr.FileID) {
		t.Fatalf("collection primary entry %q pointer file id=%d is not a value-log file", string(documentID), entry.ValuePtr.FileID)
	}
	return entry.ValuePtr
}

func requireColumnRetainedSemanticStreamLocatorAndBlockPointer(t testing.TB, d *backenddb.DB, collection string, documentID []byte) ([]byte, uint64, page.ValuePtr) {
	t.Helper()
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	defer func() { _ = snap.Close() }()
	catalog, err := loadCollectionCatalog(snap, collection)
	if err != nil {
		t.Fatalf("loadCollectionCatalog: %v", err)
	}
	if catalog == nil {
		t.Fatalf("collection catalog %q missing", collection)
	}
	entry, _, err := collectionGetEntryAtCatalogRoot(snap, catalog, collectionPrimaryRootName(collection), documentID)
	if err != nil {
		t.Fatalf("collection primary entry %q: %v", string(documentID), err)
	}
	if entry.Flags&node.FlagPointer != 0 {
		t.Fatalf("semantic stream primary entry %q flags=%#x want inline locator", string(documentID), entry.Flags)
	}
	blockKey, row, ok, err := parseColumnRetainedSemanticStreamV1Locator(entry.Value)
	if err != nil {
		t.Fatalf("parse semantic stream locator: %v", err)
	}
	if !ok {
		t.Fatalf("primary entry %q value %x is not a semantic stream locator", string(documentID), entry.Value)
	}
	blockEntry, _, err := collectionGetEntryAtCatalogRoot(snap, catalog, collectionRetainedSemanticStreamRootName(collection), blockKey)
	if err != nil {
		t.Fatalf("semantic stream block entry: %v", err)
	}
	if blockEntry.Flags&node.FlagPointer == 0 {
		t.Fatalf("semantic stream block flags=%#x value_len=%d want value-log pointer", blockEntry.Flags, len(blockEntry.Value))
	}
	if !page.IsValueLogFileID(blockEntry.ValuePtr.FileID) {
		t.Fatalf("semantic stream block pointer file id=%d is not a value-log file", blockEntry.ValuePtr.FileID)
	}
	return blockKey, row, blockEntry.ValuePtr
}

func requireColumnRetainedSemanticStreamStoredBlock(t testing.TB, d *backenddb.DB, collection string, blockKey []byte) []byte {
	t.Helper()
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	defer func() { _ = snap.Close() }()
	catalog, err := loadCollectionCatalog(snap, collection)
	if err != nil {
		t.Fatalf("loadCollectionCatalog: %v", err)
	}
	if catalog == nil {
		t.Fatalf("collection catalog %q missing", collection)
	}
	block, found, err := collectionGetAppendAtCatalogRoot(snap, catalog, collectionRetainedSemanticStreamRootName(collection), blockKey, nil)
	if err != nil {
		t.Fatalf("semantic stream block read: %v", err)
	}
	if !found {
		t.Fatalf("semantic stream block %x missing", blockKey)
	}
	return block
}

func requireColumnRetainedSemanticStreamBlockDeleted(t testing.TB, d *backenddb.DB, collection string, blockKey []byte) {
	t.Helper()
	snap := d.AcquireSnapshot()
	if snap == nil {
		t.Fatal("AcquireSnapshot returned nil")
	}
	defer func() { _ = snap.Close() }()
	catalog, err := loadCollectionCatalog(snap, collection)
	if err != nil {
		t.Fatalf("loadCollectionCatalog: %v", err)
	}
	if catalog == nil {
		t.Fatalf("collection catalog %q missing", collection)
	}
	entry, _, err := collectionGetEntryAtCatalogRoot(snap, catalog, collectionRetainedSemanticStreamRootName(collection), blockKey)
	if errors.Is(err, tree.ErrKeyNotFound) {
		return
	}
	if err != nil {
		t.Fatalf("semantic stream block entry after reclaim: %v", err)
	}
	if entry.Flags&node.FlagTombstone == 0 {
		t.Fatalf("semantic stream block %x flags=%#x value_len=%d want tombstone or missing", blockKey, entry.Flags, len(entry.Value))
	}
}

func valueLogPathForFileID(t testing.TB, dir string, fileID uint32) string {
	t.Helper()
	lane, seq := valuelog.DecodeFileID(fileID)
	return filepath.Join(backenddb.ValueLogDirPath(dir), fmt.Sprintf("value-l%d-%06d.log", lane, seq))
}

func writeRetainedPlacementSynthetic(t testing.TB, dir, collection string, cfg *ColumnStoreConfig, count int, marker string) int64 {
	t.Helper()
	enableColumnRetainedPlacementCommandWAL(t, dir)
	d := openColumnRetainedPlacementDB(t, dir, backenddb.Options{ValueLog: backenddb.ValueLogOptions{Compression: backenddb.ValueLogCompressionOff}})
	mgr := NewCollectionManager(d)
	options := CollectionOptions{DocumentFormat: DocumentFormatJSON}
	if cfg != nil {
		options.ColumnStore = cfg
	}
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: collection, Options: options}); err != nil {
		_ = d.Close()
		t.Fatalf("CreateCollection %s: %v", collection, err)
	}
	col, err := mgr.OpenCollection(collection)
	if err != nil {
		_ = d.Close()
		t.Fatalf("OpenCollection %s: %v", collection, err)
	}
	ids := make([][]byte, count)
	docs := make([][]byte, count)
	for i := 0; i < count; i++ {
		ids[i] = []byte(fmt.Sprintf("doc-%06d", i))
		docs[i] = []byte(fmt.Sprintf(`{"row_id":%d,"kind":"kind-%03d","payload":"%s-%06d-%s"}`, i, i%17, marker, i, strings.Repeat("x", 260)))
	}
	if _, err := col.InsertBatch(ids, docs); err != nil {
		_ = d.Close()
		t.Fatalf("InsertBatch %s: %v", collection, err)
	}
	if cfg != nil {
		_ = requireColumnRetainedPlacementPointer(t, d, collection, ids[0])
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close %s: %v", collection, err)
	}
	return retainedPlacementDirSize(t, backenddb.LeafLogDirPath(dir))
}

func retainedPlacementDirSize(t testing.TB, dir string) int64 {
	t.Helper()
	var total int64
	err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		total += info.Size()
		return nil
	})
	if err != nil && !os.IsNotExist(err) {
		t.Fatalf("walk %s: %v", dir, err)
	}
	return total
}

func filesUnderDirContain(t testing.TB, dir string, needle []byte) bool {
	t.Helper()
	found := false
	err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if found || entry.IsDir() {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if bytes.Contains(data, needle) {
			found = true
		}
		return nil
	})
	if err != nil && !os.IsNotExist(err) {
		t.Fatalf("walk %s: %v", dir, err)
	}
	return found
}

type retainedPlacementTemplatePackAppender struct {
	values [][]byte
}

func (a *retainedPlacementTemplatePackAppender) AppendValues(values [][]byte) ([]page.ValuePtr, error) {
	ptrs := make([]page.ValuePtr, len(values))
	for i, value := range values {
		a.values = append(a.values, bytes.Clone(value))
		ptrs[i] = page.ValuePtr{
			Offset: uint64(len(a.values)),
			Length: uint32(len(value)),
			FileID: page.ValueLogFileID(1),
		}
	}
	return ptrs, nil
}

func (*retainedPlacementTemplatePackAppender) Flush() error { return nil }

func (*retainedPlacementTemplatePackAppender) Sync() error { return nil }

func (*retainedPlacementTemplatePackAppender) CurrentValueLogSegment() (string, uint32, bool) {
	return "", 0, false
}

type retainedPlacementTemplatePackMismatchedAppender struct {
	retainedPlacementTemplatePackAppender
}

func (a *retainedPlacementTemplatePackMismatchedAppender) AppendValues(values [][]byte) ([]page.ValuePtr, error) {
	ptrs, err := a.retainedPlacementTemplatePackAppender.AppendValues(values)
	if err != nil || len(ptrs) == 0 {
		return ptrs, err
	}
	return ptrs[:len(ptrs)-1], nil
}

func retainedPlacementTemplateV1StoredDocuments(t testing.TB, count int) [][]byte {
	t.Helper()
	encoded := make([][]byte, count)
	for i := 0; i < count; i++ {
		var raw []byte
		if i%2 == 0 {
			raw = []byte(fmt.Sprintf(`{"payload":"even-%06d","shape_even":%d}`, i, i))
		} else {
			raw = []byte(fmt.Sprintf(`{"payload":"odd-%06d","shape_odd":%d,"extra":"%s"}`, i, i, strings.Repeat("x", i%5+1)))
		}
		doc, err := EncodeTemplateV1DocumentJSON(raw)
		if err != nil {
			t.Fatalf("EncodeTemplateV1DocumentJSON %d: %v", i, err)
		}
		encoded[i] = doc
	}
	prepared, _, _, _, err := prepareTemplateV1InsertDocuments(encoded, nil, false, false)
	if err != nil {
		t.Fatalf("prepareTemplateV1InsertDocuments: %v", err)
	}
	if len(prepared) != count {
		t.Fatalf("prepared documents=%d want %d", len(prepared), count)
	}
	return prepared
}

func retainedPlacementTemplateV1CompressibleStoredDocuments(t testing.TB, count int) [][]byte {
	t.Helper()
	encoded := make([][]byte, count)
	for i := 0; i < count; i++ {
		var raw []byte
		switch i % 4 {
		case 0:
			raw = []byte(fmt.Sprintf(
				`{"tenant":"store-a","event":"checkout","cart":"%s","amount":%d,"sku":"sku-a-%03d","coupon":"%s"}`,
				strings.Repeat("cart-line-price-tax-", 28),
				1000+i,
				i%11,
				strings.Repeat("save-a-", 10),
			))
		case 1:
			raw = []byte(fmt.Sprintf(
				`{"tenant":"store-b","event":"impression","campaign":"%s","slot":%d,"creative":"creative-b-%03d","visible":%t}`,
				strings.Repeat("homepage-banner-source-", 26),
				i%13,
				i%7,
				i%3 == 0,
			))
		case 2:
			raw = []byte(fmt.Sprintf(
				`{"tenant":"store-c","event":"fulfillment","route":"%s","warehouse":"wh-c-%02d","items":%d,"late":%t}`,
				strings.Repeat("zone-carrier-sort-", 30),
				i%5,
				i%19,
				i%4 == 0,
			))
		default:
			raw = []byte(fmt.Sprintf(
				`{"tenant":"store-d","event":"support","case":"case-d-%06d","topic":"%s","priority":%d,"agent":"agent-d-%02d"}`,
				i,
				strings.Repeat("refund-chat-transcript-", 24),
				i%4,
				i%9,
			))
		}
		doc, err := EncodeTemplateV1DocumentJSON(raw)
		if err != nil {
			t.Fatalf("EncodeTemplateV1DocumentJSON compressible %d: %v", i, err)
		}
		encoded[i] = doc
	}
	prepared, _, _, _, err := prepareTemplateV1InsertDocuments(encoded, nil, false, false)
	if err != nil {
		t.Fatalf("prepareTemplateV1InsertDocuments compressible: %v", err)
	}
	if len(prepared) != count {
		t.Fatalf("prepared compressible documents=%d want %d", len(prepared), count)
	}
	return prepared
}

func retainedPlacementTemplateV1BlockFrameStats(t testing.TB, values [][]byte) valuelog.FrameStats {
	t.Helper()
	records := make([]valuelog.Record, len(values))
	for i, value := range values {
		records[i] = valuelog.Record{RID: uint64(i + 1), Value: value}
	}
	writer := valuelog.NewWriterWithSink(io.Discard, page.ValueLogFileID(1))
	writer.SetBlockCompression(valuelog.BlockCodecZSTD, true)
	_, stats, err := writer.AppendFrameWithStats(0, nil, records)
	if err != nil {
		t.Fatalf("AppendFrameWithStats: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close frame writer: %v", err)
	}
	return stats
}
