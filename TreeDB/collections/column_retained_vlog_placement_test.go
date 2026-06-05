package collections

import (
	"bytes"
	"context"
	"fmt"
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
