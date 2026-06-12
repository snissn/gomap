package collections

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
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
	if rewriteStats.SourceSegmentsUnreferenced == 0 {
		postRewriteGC, err := d.ValueLogGC(context.Background(), backenddb.ValueLogGCOptions{})
		if err != nil {
			_ = d.Close()
			t.Fatalf("ValueLogGC after rewrite: %v", err)
		}
		if postRewriteGC.SegmentsDeleted == 0 {
			_ = d.Close()
			t.Fatalf("rewrite source segment was not reclaimed: rewrite=%+v gc=%+v", rewriteStats, postRewriteGC)
		}
	}
	if _, err := os.Stat(pathB); err == nil || !os.IsNotExist(err) {
		_ = d.Close()
		t.Fatalf("rewritten source segment %s stat err=%v, want removed", pathB, err)
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
		RetainedPayload: ColumnRetainedPayloadNonColumn,
		Reconstruction:  ColumnReconstructionRetainedPayloadAndColumns,
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
