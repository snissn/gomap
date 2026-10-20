package collections

import (
	"bytes"
	"context"
	"errors"
	"os"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

// Mirrors the maintained mixed-segment rewrite lifecycle with V2 enabled before
// the first Open. Complete namespace-scoped filtering must retain original owners.
func TestColumnAssetDependencyDirectoryV2RewriteMixedSegment(t *testing.T) {
	requireColumnAssetExactDestructiveGCTest(t)
	dir := t.TempDir()
	if err := backenddb.SaveFormatConfig(dir, backenddb.FormatConfig{RequiredFeatures: []string{backenddb.RequiredFeatureCommandWALV1, backenddb.RequiredFeatureDependencyDirectoryV2}}); err != nil {
		t.Fatalf("SaveFormatConfig V2 before Open: %v", err)
	}
	d := openCollectionCommandWALDB(t, dir)
	dClosed := false
	defer func() {
		if !dClosed {
			if err := d.Close(); err != nil {
				t.Errorf("cleanup Close: %v", err)
			}
		}
	}()
	mgr := NewCollectionManager(d)
	if _, err := mgr.CreateCollection(&CollectionMeta{Name: "events", Options: CollectionOptions{ColumnStore: testColumnStoreConfig(nil)}}); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint collection: %v", err)
	}
	col := openColumnStoreCollectionM10B(t, d, mgr)

	if _, err := col.InsertBatch([][]byte{[]byte("e1"), []byte("e2")}, [][]byte{
		[]byte(`{"time_us":1,"kind":"like","did":"d1"}`),
		[]byte(`{"time_us":2,"kind":"post","did":"d2"}`),
	}); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	// Bind the nonempty real asset closure to V2 before the complete namespace
	// requirements rewrite. The fresh producer append is not the target path.
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint nonempty V2 closure: %v", err)
	}
	beforeRefs := columnManifestAssetRefsForCollectionM12A(t, d, col)
	if len(beforeRefs) == 0 {
		t.Fatal("manifest refs empty, test requires live physical assets")
	}
	candidate := writeColumnAssetReachabilityCandidateM15A(t, d, col, 3, 99)
	if candidate.FileID != beforeRefs[0].FileID {
		t.Fatalf("candidate file_id=%d live file_id=%d, test requires mixed segment", candidate.FileID, beforeRefs[0].FileID)
	}
	oldSegmentPath, err := columnAssetSegmentPath(d.ColumnAssetRootDir(), candidate)
	if err != nil {
		t.Fatalf("columnAssetSegmentPath: %v", err)
	}

	dry, err := col.ColumnAssetRewrite(context.Background(), ColumnAssetRewriteOptions{
		DryRun:        true,
		Detailed:      true,
		CandidateRefs: []ColumnAssetRef{candidate},
	})
	if err != nil {
		t.Fatalf("ColumnAssetRewrite dry-run: %v", err)
	}
	if !dry.DryRun || dry.SegmentsEligible != 1 || dry.SegmentsRewritten != 0 || dry.RefsEligible != len(beforeRefs) {
		t.Fatalf("dry-run stats=%+v want one eligible mixed segment and %d eligible refs", dry, len(beforeRefs))
	}
	if dry.Plan.Segments.Mixed != 1 || dry.Plan.RewriteDebtBytes != candidate.Length {
		t.Fatalf("dry-run plan segments=%+v debt=%d want one mixed segment with candidate debt %d", dry.Plan.Segments, dry.Plan.RewriteDebtBytes, candidate.Length)
	}
	if len(dry.Plan.Entries) == 0 {
		t.Fatal("detailed dry-run rewrite plan omitted ref entries")
	}
	for i, entry := range dry.Plan.Entries {
		if len(entry.Sources) == 0 {
			t.Fatalf("detailed dry-run entry[%d] ref=%+v omitted sources", i, entry.Ref)
		}
		if i > 0 && compareColumnAssetRefs(dry.Plan.Entries[i-1].Ref, entry.Ref) > 0 {
			t.Fatalf("detailed dry-run entries are not sorted at %d: prev=%+v current=%+v", i, dry.Plan.Entries[i-1].Ref, entry.Ref)
		}
	}

	// Non-destructive rewrite permits an old reader and retains the old segment.
	old := d.AcquireSnapshot()
	if old == nil {
		t.Fatal("nil pre-rewrite Snapshot")
	}
	defer func() {
		if old != nil {
			if err := old.Close(); err != nil {
				t.Errorf("cleanup old Snapshot: %v", err)
			}
		}
	}()
	oldCatalog, err := col.catalogForSnapshot(old)
	if err != nil {
		t.Fatal(err)
	}
	oldRecords, err := loadColumnManifestRecordsFromRoot(old, oldCatalog.rootID(collectionColumnManifestRootName("events")))
	if err != nil {
		t.Fatal(err)
	}
	oldValue, found, err := collectionGetAppendAtCatalogRoot(old, oldCatalog, collectionPrimaryRootName("events"), []byte("e1"), nil)
	if err != nil || !found {
		t.Fatalf("old e1 before rewrite: found=%t err=%v", found, err)
	}
	oldValue = bytes.Clone(oldValue)

	stats, err := col.ColumnAssetRewrite(context.Background(), ColumnAssetRewriteOptions{
		Detailed:      true,
		CandidateRefs: []ColumnAssetRef{candidate},
	})
	if err != nil {
		t.Fatalf("ColumnAssetRewrite: %v", err)
	}
	if stats.DryRun || stats.SegmentsEligible != 1 || stats.SegmentsRewritten != 1 || stats.RefsRemapped != len(beforeRefs) {
		t.Fatalf("stats=%+v want one rewritten segment and %d remapped refs", stats, len(beforeRefs))
	}
	if stats.BytesCopied <= 0 || stats.BytesReclaimable < candidate.Length {
		t.Fatalf("stats=%+v want copied live bytes and reclaimable candidate bytes >= %d", stats, candidate.Length)
	}
	if len(stats.SupersededRefs) != len(beforeRefs) || len(stats.RemappedRefs) != len(beforeRefs) {
		t.Fatalf("stats superseded=%d remapped=%d want %d", len(stats.SupersededRefs), len(stats.RemappedRefs), len(beforeRefs))
	}

	afterRefs := columnManifestAssetRefsForCollectionM12A(t, d, col)
	assertColumnAssetRefsRemappedM15C(t, beforeRefs, afterRefs)
	for _, ref := range afterRefs {
		if _, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), ref); err != nil {
			t.Fatalf("remapped ref %+v unreadable: %v", ref, err)
		}
	}
	if _, err := os.Stat(oldSegmentPath); err != nil {
		t.Fatalf("rewrite removed old mixed segment before GC: %v", err)
	}

	gotRecords, err := loadColumnManifestRecordsFromRoot(old, oldCatalog.rootID(collectionColumnManifestRootName("events")))
	if err != nil || !equalColumnManifestRecords(gotRecords, oldRecords) {
		t.Fatalf("old manifest changed across rewrite: %v", err)
	}
	gotValue, found, err := collectionGetAppendAtCatalogRoot(old, oldCatalog, collectionPrimaryRootName("events"), []byte("e1"), nil)
	if err != nil || !found || !bytes.Equal(gotValue, oldValue) {
		t.Fatalf("old e1 changed across rewrite: found=%t err=%v", found, err)
	}
	for _, ref := range beforeRefs {
		if _, err := readColumnPhysicalAssetFromManager(d.ColumnAssetRootDir(), ref); err != nil {
			t.Fatalf("old asset unreadable while Snapshot held: ref=%+v err=%v", ref, err)
		}
	}
	assertCollectionDocument(t, col, "e1", `{"time_us":1,"kind":"like","did":"d1"}`)
	assertCollectionDocument(t, col, "e2", `{"time_us":2,"kind":"post","did":"d2"}`)
	oldCatalog = nil
	if err := old.Close(); err != nil {
		t.Fatalf("Close old Snapshot: %v", err)
	}
	old = nil

	if err := d.Close(); err != nil {
		t.Fatalf("Close after rewrite: %v", err)
	}
	dClosed = true
	reopen := openCollectionCommandWALDB(t, dir)
	defer func() {
		if err := reopen.Close(); err != nil {
			t.Errorf("cleanup reopened Close: %v", err)
		}
	}()
	reopened := openColumnStoreCollectionM10B(t, reopen)
	reopenRefs := columnManifestAssetRefsForCollectionM12A(t, reopen, reopened)
	assertColumnAssetRefsEqualM15C(t, afterRefs, reopenRefs)
	diag, err := reopened.scanColumnPhysicalRows(columnPhysicalScanRequest{})
	if err != nil {
		t.Fatalf("scanColumnPhysicalRows after reopen: %v", err)
	}
	afterPhysicalRefs := columnManifestPhysicalAssetRefsForTestM1634(afterRefs)
	if diag.RowsScanned != 2 || diag.AssetRefs != len(afterPhysicalRefs) {
		t.Fatalf("diag=%+v want 2 rows and %d remapped physical refs", diag, len(afterPhysicalRefs))
	}
	gcStats, err := reopened.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{
		CandidateRefs: append(append([]ColumnAssetRef(nil), stats.SupersededRefs...), candidate),
	})
	if err != nil {
		t.Fatalf("ColumnAssetGC after rewrite: %v", err)
	}
	if gcStats.SegmentsEligible != 1 || gcStats.SegmentsDeleted != 0 {
		t.Fatalf("first gc stats=%+v want old mixed segment retained by fallback durable generation", gcStats)
	}
	if _, err := os.Stat(oldSegmentPath); err != nil {
		t.Fatalf("first GC removed fallback generation segment: %v", err)
	}
	advanceColumnAssetDurableFallbackM15C(t, reopen)
	gcStats, err = reopened.ColumnAssetGC(context.Background(), ColumnAssetGCOptions{
		CandidateRefs: append(append([]ColumnAssetRef(nil), stats.SupersededRefs...), candidate),
	})
	if err != nil {
		t.Fatalf("ColumnAssetGC after fallback advance: %v", err)
	}
	if gcStats.SegmentsDeleted != 1 || gcStats.BytesDeleted == 0 {
		t.Fatalf("gc stats=%+v want old mixed segment deleted after fallback advance", gcStats)
	}
	if _, err := os.Stat(oldSegmentPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("old segment still exists or unexpected stat error: %v", err)
	}
	for _, ref := range reopenRefs {
		if _, err := readColumnPhysicalAssetFromManager(reopen.ColumnAssetRootDir(), ref); err != nil {
			t.Fatalf("live remapped ref %+v unreadable after GC: %v", ref, err)
		}
	}
}
