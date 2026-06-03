package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestValueLogGC_EmptySet_NoValueLogSegments(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	stats, err := db.ValueLogGC(context.Background(), ValueLogGCOptions{})
	if err != nil {
		t.Fatalf("ValueLogGC: %v", err)
	}
	if stats != (ValueLogGCStats{}) {
		t.Fatalf("expected zero stats for empty value-log set, got %+v", stats)
	}
}

func TestValueLogGC_IgnoresInlineLeafLogWhenDBDefaultUsesPagerLeaves(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{
		Dir:                    dir,
		DisableBackgroundPrune: true,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if db.indexOuterLeavesInValueLog {
		t.Fatalf("indexOuterLeavesInValueLog=true, want false for this regression")
	}
	inlineAppender, err := newReplayInlineAppender(db, nil, nil)
	if err != nil {
		t.Fatalf("new replay inline appender: %v", err)
	}
	defer func() { _ = inlineAppender.close() }()
	db.SetValueLogAppender(inlineAppender)
	db.SetLeafPageLog(replayInlineLeafPageLog{appender: inlineAppender})
	defer db.SetValueLogAppender(nil)
	defer db.SetLeafPageLog(nil)
	if db.leafPageLog == nil {
		t.Fatalf("inline appender did not install a leaf page log")
	}

	if _, err := db.AppendValueLogValues([][]byte{bytes.Repeat([]byte("ordinary-value|"), 32)}); err != nil {
		t.Fatalf("append ordinary value-log value: %v", err)
	}
	appender := db.currentValueLogAppender()
	if appender == nil {
		t.Fatalf("missing value-log appender")
	}
	primaryPath, primaryFileID, ok := appender.CurrentValueLogSegment()
	if !ok || primaryPath == "" || primaryFileID == 0 {
		t.Fatalf("ordinary value-log segment not opened: path=%q id=%d ok=%t", primaryPath, primaryFileID, ok)
	}

	table := mustFrozenSystemMemtable(t, "doc/u1", "document")
	_, rootIDs, err := db.PublishOrderedRootGroup(nil, []OrderedRootPublishInput{{
		BaseRoot:      0,
		Iter:          table.NewIterator(nil, nil),
		StoragePolicy: OrderedRootStorageValueLogLeaves,
	}})
	if err != nil {
		t.Fatalf("publish value-log leaf root: %v", err)
	}
	if len(rootIDs) != 1 {
		t.Fatalf("rootIDs=%d want 1", len(rootIDs))
	}
	leafPtrs := requireLeafLogRootChildren(t, db, rootIDs[0])
	leafFileID := page.ValueLogFileID(leafPtrs[0].FileID)

	preSet := db.valueLogManager.CurrentSetNoRefresh()
	if preSet == nil {
		t.Fatalf("missing value-log set before GC")
	}
	if _, ok := preSet.Files[primaryFileID]; !ok {
		_ = db.valueLogManager.Release(preSet)
		t.Fatalf("ordinary value-log segment %d missing before GC", primaryFileID)
	}
	if _, ok := preSet.Files[leafFileID]; !ok {
		_ = db.valueLogManager.Release(preSet)
		t.Fatalf("leaf-log segment %d missing before GC", leafFileID)
	}
	_ = db.valueLogManager.Release(preSet)

	stats, err := db.ValueLogGC(context.Background(), ValueLogGCOptions{ProtectedPaths: []string{primaryPath}})
	if err != nil {
		t.Fatalf("ValueLogGC: %v", err)
	}
	if stats.SegmentsEligible != 0 || stats.SegmentsDeleted != 0 {
		t.Fatalf("leaf-log segment was considered ordinary GC work: %+v", stats)
	}

	postSet := db.valueLogManager.CurrentSetNoRefresh()
	if postSet == nil {
		t.Fatalf("missing value-log set after GC")
	}
	if _, ok := postSet.Files[leafFileID]; !ok {
		_ = db.valueLogManager.Release(postSet)
		t.Fatalf("leaf-log segment %d missing from current set after GC", leafFileID)
	}
	_ = db.valueLogManager.Release(postSet)

	snap := db.AcquireSnapshot()
	if snap == nil {
		t.Fatal("expected snapshot")
	}
	defer snap.Close()
	entry, err := snap.GetEntryAtRoot(rootIDs[0], []byte("doc/u1"))
	if err != nil {
		t.Fatalf("read value-log leaf root after GC: %v", err)
	}
	if got := string(entry.Value); got != "document" {
		t.Fatalf("value after GC=%q want document", got)
	}
}

func TestValueLogGC_RemovesUnreferencedSegment(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	walDir := filepath.Join(dir, "value_vlog")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("mkdir wal: %v", err)
	}

	path1 := filepath.Join(walDir, "value-l0-000001.log")
	id1, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("fileid1: %v", err)
	}
	w1, err := valuelog.NewWriter(path1, id1)
	if err != nil {
		t.Fatalf("writer1: %v", err)
	}
	w1.SetBlockCompression(valuelog.BlockCodecSnappy, true)
	ptr1, err := w1.Append(0, nil, 1, bytes.Repeat([]byte("value-1|"), 128))
	if err != nil {
		t.Fatalf("append1: %v", err)
	}
	if err := w1.Close(); err != nil {
		t.Fatalf("close1: %v", err)
	}

	path2 := filepath.Join(walDir, "value-l0-000002.log")
	id2, err := valuelog.EncodeFileID(0, 2)
	if err != nil {
		t.Fatalf("fileid2: %v", err)
	}
	w2, err := valuelog.NewWriter(path2, id2)
	if err != nil {
		t.Fatalf("writer2: %v", err)
	}
	ptr2, err := w2.Append(0, nil, 2, bytes.Repeat([]byte("value-2|"), 64))
	if err != nil {
		t.Fatalf("append2: %v", err)
	}
	if err := w2.Close(); err != nil {
		t.Fatalf("close2: %v", err)
	}

	b := db.NewBatch()
	ptrBatch, ok := b.(interface {
		SetPointer(key []byte, ptr page.ValuePtr) error
	})
	if !ok {
		t.Fatalf("missing SetPointer on batch")
	}
	if err := ptrBatch.SetPointer([]byte("k1"), ptr1); err != nil {
		t.Fatalf("set k1: %v", err)
	}
	if err := ptrBatch.SetPointer([]byte("k2"), ptr2); err != nil {
		t.Fatalf("set k2: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = b.Close()

	if err := db.Delete([]byte("k1")); err != nil {
		t.Fatalf("delete k1: %v", err)
	}

	stats, err := db.ValueLogGC(context.Background(), ValueLogGCOptions{})
	if err != nil {
		t.Fatalf("ValueLogGC: %v", err)
	}
	if stats.SegmentsDeleted == 0 {
		t.Fatalf("expected at least one segment deleted, got %+v", stats)
	}

	if _, err := os.Stat(path1); err == nil || !os.IsNotExist(err) {
		t.Fatalf("expected segment1 to be removed, err=%v", err)
	}
	if _, err := os.Stat(path2); err != nil {
		t.Fatalf("expected segment2 to remain, err=%v", err)
	}
}

func TestValueLogGC_ProtectedPathsDoNotKeepHistoricalRewriteLanes(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	appendPointersInNewSegment(t, dir, 0, 1, 1_000, 1, func(int) []byte {
		return bytes.Repeat([]byte("lane0-seq1|"), 32)
	})
	appendPointersInNewSegment(t, dir, 0, 2, 2_000, 1, func(int) []byte {
		return bytes.Repeat([]byte("lane0-seq2|"), 32)
	})
	appendPointersInNewSegment(t, dir, 250, 1, 3_000, 1, func(int) []byte {
		return bytes.Repeat([]byte("lane250-seq1|"), 32)
	})
	appendPointersInNewSegment(t, dir, 250, 2, 4_000, 1, func(int) []byte {
		return bytes.Repeat([]byte("lane250-seq2|"), 32)
	})

	if err := db.RefreshValueLogSet(); err != nil {
		t.Fatalf("RefreshValueLogSet: %v", err)
	}

	protected := []string{
		filepath.Join(dir, "value_vlog", "value-l0-000002.log"),
	}

	stats, err := db.ValueLogGC(context.Background(), ValueLogGCOptions{ProtectedPaths: protected})
	if err != nil {
		t.Fatalf("ValueLogGC: %v", err)
	}
	if stats.SegmentsDeleted < 2 {
		t.Fatalf("expected GC to delete historical unprotected rewrite-lane segments, got %+v", stats)
	}

	for _, path := range []string{
		filepath.Join(dir, "value_vlog", "value-l250-000001.log"),
		filepath.Join(dir, "value_vlog", "value-l250-000002.log"),
	} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("expected %s to be deleted, err=%v", filepath.Base(path), err)
		}
	}

	for _, path := range []string{
		filepath.Join(dir, "value_vlog", "value-l0-000001.log"),
		filepath.Join(dir, "value_vlog", "value-l0-000002.log"),
	} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("expected protected-lane window to retain %s, err=%v", filepath.Base(path), err)
		}
	}
}

func TestValueLogGC_ObservedSourceReclaimActiveRequiresExplicitOption(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	appendPointersInNewSegment(t, dir, 0, 1, 1_000, 1, func(int) []byte {
		return bytes.Repeat([]byte("lane0-seq1|"), 32)
	})
	appendPointersInNewSegment(t, dir, 0, 2, 2_000, 1, func(int) []byte {
		return bytes.Repeat([]byte("lane0-seq2|"), 32)
	})

	if err := db.RefreshValueLogSet(); err != nil {
		t.Fatalf("RefreshValueLogSet: %v", err)
	}

	activeID, err := valuelog.EncodeFileID(0, 2)
	if err != nil {
		t.Fatalf("active fileid: %v", err)
	}
	activePath := filepath.Join(dir, "value_vlog", "value-l0-000002.log")

	stats, err := db.ValueLogGC(context.Background(), ValueLogGCOptions{
		ObservedSourceFileIDs:            []uint32{activeID},
		ObservedSourceAssumeUnreferenced: true,
	})
	if err != nil {
		t.Fatalf("ValueLogGC without active reclaim: %v", err)
	}
	if stats.ObservedSourceSegmentsActive != 1 || stats.ObservedSourceSegmentsDeleted != 0 {
		t.Fatalf("observed active source was not protected by default: %+v", stats)
	}
	if _, err := os.Stat(activePath); err != nil {
		t.Fatalf("expected active source to remain without explicit reclaim: %v", err)
	}

	stats, err = db.ValueLogGC(context.Background(), ValueLogGCOptions{
		ObservedSourceFileIDs:            []uint32{activeID},
		ObservedSourceAssumeUnreferenced: true,
		ObservedSourceReclaimActive:      true,
	})
	if err != nil {
		t.Fatalf("ValueLogGC with active reclaim: %v", err)
	}
	if stats.ObservedSourceSegmentsEligible != 1 || stats.ObservedSourceSegmentsDeleted != 1 {
		t.Fatalf("observed active source was not reclaimed with explicit option: %+v", stats)
	}
	if _, err := os.Stat(activePath); !os.IsNotExist(err) {
		t.Fatalf("expected active source to be removed with explicit reclaim, err=%v", err)
	}
}

func TestValueLogGC_ProtectedPathBreakdownStats(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	for seq := 1; seq <= 5; seq++ {
		seq := seq
		appendPointersInNewSegment(t, dir, 0, uint32(seq), uint64(seq)*1_000, 1, func(int) []byte {
			return bytes.Repeat([]byte(fmt.Sprintf("lane0-seq%d|", seq)), 32)
		})
	}

	if err := db.RefreshValueLogSet(); err != nil {
		t.Fatalf("RefreshValueLogSet: %v", err)
	}

	inUseOnlyPath := filepath.Join(dir, "value_vlog", "value-l0-000001.log")
	retainedOnlyPath := filepath.Join(dir, "value_vlog", "value-l0-000002.log")
	overlapPath := filepath.Join(dir, "value_vlog", "value-l0-000003.log")
	observedInUseID, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("observed in-use fileid: %v", err)
	}
	observedRetainedID, err := valuelog.EncodeFileID(0, 2)
	if err != nil {
		t.Fatalf("observed retained fileid: %v", err)
	}
	observedOverlapID, err := valuelog.EncodeFileID(0, 3)
	if err != nil {
		t.Fatalf("observed overlap fileid: %v", err)
	}

	stats, err := db.ValueLogGC(context.Background(), ValueLogGCOptions{
		DryRun:                 true,
		ProtectedInUsePaths:    []string{inUseOnlyPath, overlapPath},
		ProtectedRetainedPaths: []string{retainedOnlyPath, overlapPath},
		ObservedSourceFileIDs:  []uint32{observedInUseID, observedRetainedID, observedOverlapID},
	})
	if err != nil {
		t.Fatalf("ValueLogGC: %v", err)
	}

	if stats.SegmentsTotal != 5 {
		t.Fatalf("segments total=%d want 5", stats.SegmentsTotal)
	}
	if stats.SegmentsActive != 2 {
		t.Fatalf("segments active=%d want 2", stats.SegmentsActive)
	}
	if stats.SegmentsProtected != 3 {
		t.Fatalf("segments protected=%d want 3", stats.SegmentsProtected)
	}
	if stats.SegmentsProtectedInUse != 1 {
		t.Fatalf("segments protected in-use=%d want 1", stats.SegmentsProtectedInUse)
	}
	if stats.SegmentsProtectedRetained != 1 {
		t.Fatalf("segments protected retained=%d want 1", stats.SegmentsProtectedRetained)
	}
	if stats.SegmentsProtectedOverlap != 1 {
		t.Fatalf("segments protected overlap=%d want 1", stats.SegmentsProtectedOverlap)
	}
	if stats.SegmentsProtectedOther != 0 {
		t.Fatalf("segments protected other=%d want 0", stats.SegmentsProtectedOther)
	}
	if stats.SegmentsEligible != 0 {
		t.Fatalf("segments eligible=%d want 0", stats.SegmentsEligible)
	}
	if stats.SegmentsDeleted != 0 {
		t.Fatalf("segments deleted=%d want 0", stats.SegmentsDeleted)
	}
	if stats.BytesProtected <= 0 {
		t.Fatalf("bytes protected=%d want >0", stats.BytesProtected)
	}
	if stats.BytesProtectedInUse <= 0 || stats.BytesProtectedRetained <= 0 || stats.BytesProtectedOverlap <= 0 {
		t.Fatalf("expected non-zero protected byte buckets, got %+v", stats)
	}
	if stats.BytesProtectedOther != 0 {
		t.Fatalf("bytes protected other=%d want 0", stats.BytesProtectedOther)
	}
	if stats.ObservedSourceSegments != 3 {
		t.Fatalf("observed source segments=%d want 3", stats.ObservedSourceSegments)
	}
	if stats.ObservedSourceSegmentsReferenced != 0 {
		t.Fatalf("observed source segments referenced=%d want 0", stats.ObservedSourceSegmentsReferenced)
	}
	if stats.ObservedSourceSegmentsActive != 0 {
		t.Fatalf("observed source segments active=%d want 0", stats.ObservedSourceSegmentsActive)
	}
	if stats.ObservedSourceSegmentsProtected != 3 {
		t.Fatalf("observed source segments protected=%d want 3", stats.ObservedSourceSegmentsProtected)
	}
	if stats.ObservedSourceSegmentsProtectedInUse != 1 {
		t.Fatalf("observed source segments protected in-use=%d want 1", stats.ObservedSourceSegmentsProtectedInUse)
	}
	if stats.ObservedSourceSegmentsProtectedRetained != 1 {
		t.Fatalf("observed source segments protected retained=%d want 1", stats.ObservedSourceSegmentsProtectedRetained)
	}
	if stats.ObservedSourceSegmentsProtectedOverlap != 1 {
		t.Fatalf("observed source segments protected overlap=%d want 1", stats.ObservedSourceSegmentsProtectedOverlap)
	}
	if stats.ObservedSourceSegmentsProtectedOther != 0 {
		t.Fatalf("observed source segments protected other=%d want 0", stats.ObservedSourceSegmentsProtectedOther)
	}
	if stats.ObservedSourceSegmentsEligible != 0 {
		t.Fatalf("observed source segments eligible=%d want 0", stats.ObservedSourceSegmentsEligible)
	}
	if stats.ObservedSourceSegmentsDeleted != 0 {
		t.Fatalf("observed source segments deleted=%d want 0", stats.ObservedSourceSegmentsDeleted)
	}
	if stats.ObservedSourceSegmentsPending != 0 {
		t.Fatalf("observed source segments pending=%d want 0", stats.ObservedSourceSegmentsPending)
	}
	if stats.ObservedSourceBytes <= 0 {
		t.Fatalf("observed source bytes=%d want >0", stats.ObservedSourceBytes)
	}
	if stats.ObservedSourceBytesProtected <= 0 {
		t.Fatalf("observed source bytes protected=%d want >0", stats.ObservedSourceBytesProtected)
	}
	if stats.ObservedSourceBytesProtectedInUse <= 0 ||
		stats.ObservedSourceBytesProtectedRetained <= 0 ||
		stats.ObservedSourceBytesProtectedOverlap <= 0 {
		t.Fatalf("expected non-zero observed source protected byte buckets, got %+v", stats)
	}
	if stats.ObservedSourceBytesProtectedOther != 0 {
		t.Fatalf("observed source bytes protected other=%d want 0", stats.ObservedSourceBytesProtectedOther)
	}
	if stats.ObservedSourceBytesEligible != 0 {
		t.Fatalf("observed source bytes eligible=%d want 0", stats.ObservedSourceBytesEligible)
	}
	if stats.ObservedSourceBytesDeleted != 0 {
		t.Fatalf("observed source bytes deleted=%d want 0", stats.ObservedSourceBytesDeleted)
	}
	if stats.ObservedSourceBytesPending != 0 {
		t.Fatalf("observed source bytes pending=%d want 0", stats.ObservedSourceBytesPending)
	}
}

func TestValueLogGC_KeepsReferencedPointerSegments_WithOuterLeavesInValueLog(t *testing.T) {
	dir := t.TempDir()

	walDir := filepath.Join(dir, "value_vlog")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("mkdir wal: %v", err)
	}

	// Use an otherwise-unused lane so TreeDB can create its own lane0 segments
	// for outer leaves without colliding with this test's explicit segments.
	const lane uint32 = 100

	path1 := filepath.Join(walDir, "value-l100-000001.log")
	id1, err := valuelog.EncodeFileID(lane, 1)
	if err != nil {
		t.Fatalf("fileid1: %v", err)
	}
	w1, err := valuelog.NewWriter(path1, id1)
	if err != nil {
		t.Fatalf("writer1: %v", err)
	}
	ptr1, err := w1.Append(0, nil, 1, bytes.Repeat([]byte("value-1|"), 64))
	if err != nil {
		t.Fatalf("append1: %v", err)
	}
	if err := w1.Close(); err != nil {
		t.Fatalf("close1: %v", err)
	}

	path2 := filepath.Join(walDir, "value-l100-000002.log")
	id2, err := valuelog.EncodeFileID(lane, 2)
	if err != nil {
		t.Fatalf("fileid2: %v", err)
	}
	w2, err := valuelog.NewWriter(path2, id2)
	if err != nil {
		t.Fatalf("writer2: %v", err)
	}
	ptr2, err := w2.Append(0, nil, 2, bytes.Repeat([]byte("value-2|"), 64))
	if err != nil {
		t.Fatalf("append2: %v", err)
	}
	if err := w2.Close(); err != nil {
		t.Fatalf("close2: %v", err)
	}

	db, err := Open(Options{
		Dir:                        dir,
		DisableBackgroundPrune:     true,
		IndexOuterLeavesInValueLog: true,
		IndexColumnarLeaves:        true,
		IndexPackedValuePtr:        true,
		LeafPrefixCompression:      true,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	leafLog := newRewriteWriter(filepath.Join(dir, "value_vlog"), 0, 0, 0)
	leafLog.blockCompression = false
	leafLog.blockCodec = valuelog.BlockCodecSnappy
	db.SetLeafPageLog(leafLog)
	defer func() {
		_ = leafLog.Close()
		_ = db.Close()
	}()

	b := db.NewBatch()
	ptrBatch, ok := b.(interface {
		SetPointer(key []byte, ptr page.ValuePtr) error
	})
	if !ok {
		t.Fatalf("missing SetPointer on batch")
	}
	if err := ptrBatch.SetPointer([]byte("k1"), ptr1); err != nil {
		t.Fatalf("set k1: %v", err)
	}
	if err := ptrBatch.SetPointer([]byte("k2"), ptr2); err != nil {
		t.Fatalf("set k2: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = b.Close()

	// Lane100 seq2 is active; the GC must still treat the older referenced
	// segment (seq1) as referenced and keep it.
	if _, err := db.ValueLogGC(context.Background(), ValueLogGCOptions{}); err != nil {
		t.Fatalf("ValueLogGC: %v", err)
	}

	if _, err := os.Stat(path1); err != nil {
		t.Fatalf("expected referenced non-active segment to remain, err=%v", err)
	}

	got, err := db.Get([]byte("k1"))
	if err != nil {
		t.Fatalf("get k1: %v", err)
	}
	if !bytes.Equal(got, bytes.Repeat([]byte("value-1|"), 64)) {
		t.Fatalf("k1 mismatch after GC")
	}
}

func TestValueLogGC_IncrementalParityWithFullScan(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	key := func(i int) []byte { return []byte(fmt.Sprintf("k%06d", i)) }
	valueA := func(i int) []byte { return bytes.Repeat([]byte{byte(i % 251)}, 256) }
	valueB := func(i int) []byte { return bytes.Repeat([]byte{byte((i + 17) % 251)}, 512) }

	ptrsA := appendPointersInNewSegment(t, dir, 0, 1, 10_000, 320, valueA)
	ptrsB := appendPointersInNewSegment(t, dir, 0, 2, 20_000, 120, valueB)

	b := db.NewBatch().(*Batch)
	for i := 0; i < 320; i++ {
		if err := b.SetPointer(key(i), ptrsA[i]); err != nil {
			t.Fatalf("set initial pointer %d: %v", i, err)
		}
	}
	if err := b.Write(); err != nil {
		t.Fatalf("write initial: %v", err)
	}
	_ = b.Close()

	b = db.NewBatch().(*Batch)
	for i := 0; i < 90; i++ {
		if err := b.Delete(key(i)); err != nil {
			t.Fatalf("delete %d: %v", i, err)
		}
	}
	for i := 160; i < 280; i++ {
		if err := b.SetPointer(key(i), ptrsB[i-160]); err != nil {
			t.Fatalf("set overwrite pointer %d: %v", i, err)
		}
	}
	if err := b.Write(); err != nil {
		t.Fatalf("write churn: %v", err)
	}
	_ = b.Close()

	seq := db.currentCommitSeq()
	incRefs, ok := db.valueLogRefTracker.referencedSet(seq)
	if !ok {
		t.Fatalf("expected incremental ref set for seq=%d", seq)
	}

	fullCounts, fullSeq, err := db.scanValueLogRefCounts(context.Background())
	if err != nil {
		t.Fatalf("scanValueLogRefCounts: %v", err)
	}
	if fullSeq != seq {
		t.Fatalf("scan seq mismatch: got=%d want=%d", fullSeq, seq)
	}

	fullRefs := valueLogRefSetFromCounts(fullCounts)
	if !reflect.DeepEqual(incRefs, fullRefs) {
		t.Fatalf("incremental/full-scan mismatch: incremental=%d full=%d", len(incRefs), len(fullRefs))
	}
}

func TestValueLogGC_IncrementalCounterRollbackOnFailedCommit(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	oldValue := bytes.Repeat([]byte("old"), 128)
	newValue := bytes.Repeat([]byte("new"), 128)
	oldPtr := appendPointersInNewSegment(t, dir, 0, 1, 30_000, 1, func(int) []byte { return oldValue })[0]
	newPtr := appendPointersInNewSegment(t, dir, 0, 2, 40_000, 1, func(int) []byte { return newValue })[0]

	b := db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("k"), oldPtr); err != nil {
		t.Fatalf("seed set pointer: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	_ = b.Close()

	beforeSeq := db.currentCommitSeq()
	beforeRefs, ok := db.valueLogRefTracker.referencedSet(beforeSeq)
	if !ok {
		t.Fatalf("expected incremental ref set before failpoint seq=%d", beforeSeq)
	}

	db.testFailFinalizeCommit.Store(true)
	b = db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("k"), newPtr); err != nil {
		t.Fatalf("failpoint set pointer: %v", err)
	}
	err = b.Write()
	_ = b.Close()
	db.testFailFinalizeCommit.Store(false)
	if !errors.Is(err, errTestFinalizeCommitFailpoint) {
		t.Fatalf("expected failpoint error, got %v", err)
	}

	afterSeq := db.currentCommitSeq()
	if afterSeq != beforeSeq {
		t.Fatalf("commit seq changed on failed commit: before=%d after=%d", beforeSeq, afterSeq)
	}
	afterRefs, ok := db.valueLogRefTracker.referencedSet(afterSeq)
	if !ok {
		t.Fatalf("expected incremental ref set after failpoint seq=%d", afterSeq)
	}
	if !reflect.DeepEqual(beforeRefs, afterRefs) {
		t.Fatalf("ref set changed on failed commit")
	}

	got, err := db.Get([]byte("k"))
	if err != nil {
		t.Fatalf("get after failed commit: %v", err)
	}
	if !bytes.Equal(got, oldValue) {
		t.Fatalf("value changed on failed commit")
	}
}

func TestValueLogGC_IncrementalMode_RebuildOnCorruption(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	ptrs := appendPointersInNewSegment(t, dir, 0, 1, 50_000, 200, func(i int) []byte {
		return bytes.Repeat([]byte{byte(i % 251)}, 256)
	})

	b := db.NewBatch().(*Batch)
	for i := 0; i < 200; i++ {
		key := []byte(fmt.Sprintf("k%05d", i))
		if err := b.SetPointer(key, ptrs[i]); err != nil {
			t.Fatalf("seed set pointer %d: %v", i, err)
		}
	}
	if err := b.Write(); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	_ = b.Close()

	if _, err := db.ValueLogGC(context.Background(), ValueLogGCOptions{DryRun: true}); err != nil {
		t.Fatalf("ValueLogGC dry-run: %v", err)
	}

	metaPath := db.valueLogRefCountsPath()
	orig, err := os.ReadFile(metaPath)
	if err != nil {
		t.Fatalf("read metadata: %v", err)
	}
	if len(orig) == 0 {
		t.Fatalf("expected non-empty metadata")
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	corrupt := []byte("not-a-valid-metadata-file")
	if err := os.WriteFile(metaPath, corrupt, 0o644); err != nil {
		t.Fatalf("write corrupt metadata: %v", err)
	}

	db, err = Open(Options{
		Dir: dir,
	})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = db.Close() }()

	seq := db.currentCommitSeq()
	if _, ok := db.valueLogRefTracker.referencedSet(seq); !ok {
		t.Fatalf("expected ref tracker rebuilt for seq=%d", seq)
	}

	if _, err := db.ValueLogGC(context.Background(), ValueLogGCOptions{DryRun: true}); err != nil {
		t.Fatalf("ValueLogGC after rebuild: %v", err)
	}

	after, err := os.ReadFile(metaPath)
	if err != nil {
		t.Fatalf("read rebuilt metadata: %v", err)
	}
	if bytes.Equal(after, corrupt) {
		t.Fatalf("expected metadata rewrite after corruption")
	}
	if _, err := decodeValueLogRefCounts(after); err != nil {
		t.Fatalf("rebuilt metadata decode: %v", err)
	}
}

func TestValueLogGC_HealthMetadata_UpdatesAfterDeleteAndRewrite(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() {
		if db != nil {
			_ = db.Close()
		}
	}()

	ptrsA := appendPointersInNewSegment(t, dir, 0, 1, 70_000, 1, func(i int) []byte {
		return bytes.Repeat([]byte("a"), 512)
	})
	ptrsB := appendPointersInNewSegment(t, dir, 0, 2, 80_000, 1, func(i int) []byte {
		return bytes.Repeat([]byte("b"), 512)
	})

	b := db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("k1"), ptrsA[0]); err != nil {
		t.Fatalf("set k1: %v", err)
	}
	if err := b.SetPointer([]byte("k2"), ptrsB[0]); err != nil {
		t.Fatalf("set k2: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	_ = b.Close()

	if err := db.Delete([]byte("k1")); err != nil {
		t.Fatalf("delete k1: %v", err)
	}
	if _, err := db.ValueLogGC(context.Background(), ValueLogGCOptions{}); err != nil {
		t.Fatalf("ValueLogGC: %v", err)
	}

	healthPath := valueLogHealthPath(dir)
	health, err := loadValueLogHealth(healthPath)
	if err != nil {
		t.Fatalf("load health after GC: %v", err)
	}
	if _, ok := health[ptrsA[0].FileID]; ok {
		t.Fatalf("expected deleted segment %d removed from health metadata", ptrsA[0].FileID)
	}
	h2, ok := health[ptrsB[0].FileID]
	if !ok {
		t.Fatalf("expected referenced segment %d in health metadata", ptrsB[0].FileID)
	}
	if h2.SegmentBytes <= 0 {
		t.Fatalf("expected positive segment bytes for segment %d, got %+v", ptrsB[0].FileID, h2)
	}
	if h2.LiveBytes < 0 || h2.LiveBytes > h2.SegmentBytes {
		t.Fatalf("expected bounded live bytes for segment %d, got %+v", ptrsB[0].FileID, h2)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	db = nil
	stats, err := ValueLogRewriteOffline(Options{Dir: dir})
	if err != nil {
		t.Fatalf("ValueLogRewriteOffline: %v", err)
	}
	if stats.SegmentsAfter == 0 {
		t.Fatalf("expected rewritten segments, got %+v", stats)
	}

	healthAfterRewrite, err := loadValueLogHealth(healthPath)
	if err != nil {
		t.Fatalf("load health after rewrite: %v", err)
	}
	if len(healthAfterRewrite) == 0 {
		t.Fatalf("expected health metadata entries after rewrite")
	}
	foundRewrite := false
	for _, h := range healthAfterRewrite {
		if h.RewriteCount > 0 {
			foundRewrite = true
			break
		}
	}
	if !foundRewrite {
		t.Fatalf("expected rewrite_count > 0 after rewrite, got %+v", healthAfterRewrite)
	}
}

func TestValueLogGC_HealthMetadata_PreservesPinnedZombieSegment(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ptrsA := appendPointersInNewSegment(t, dir, 0, 1, 81_000, 1, func(i int) []byte {
		return bytes.Repeat([]byte("a"), 256)
	})
	ptrsB := appendPointersInNewSegment(t, dir, 0, 2, 82_000, 1, func(i int) []byte {
		return bytes.Repeat([]byte("b"), 256)
	})

	b := db.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("k1"), ptrsA[0]); err != nil {
		t.Fatalf("set k1: %v", err)
	}
	if err := b.SetPointer([]byte("k2"), ptrsB[0]); err != nil {
		t.Fatalf("set k2: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	_ = b.Close()

	pinned := db.valueLogManager.CurrentSetNoRefresh()
	if pinned == nil {
		t.Fatalf("expected pinned value-log set")
	}
	defer func() { _ = db.valueLogManager.Release(pinned) }()

	zombieFile, ok := pinned.Files[ptrsA[0].FileID]
	if !ok || zombieFile == nil || zombieFile.Path == "" {
		t.Fatalf("missing pinned segment for %d", ptrsA[0].FileID)
	}

	if err := db.Delete([]byte("k1")); err != nil {
		t.Fatalf("delete k1: %v", err)
	}
	if _, err := db.ValueLogGC(context.Background(), ValueLogGCOptions{}); err != nil {
		t.Fatalf("ValueLogGC: %v", err)
	}

	if _, err := os.Stat(zombieFile.Path); err != nil {
		t.Fatalf("expected pinned zombie segment to remain on disk: %v", err)
	}

	health, err := loadValueLogHealth(valueLogHealthPath(dir))
	if err != nil {
		t.Fatalf("load health after gc: %v", err)
	}
	h, ok := health[ptrsA[0].FileID]
	if !ok {
		t.Fatalf("expected health entry for pinned zombie segment %d", ptrsA[0].FileID)
	}
	if h.SegmentBytes <= 0 {
		t.Fatalf("expected positive segment bytes for pinned zombie segment: %+v", h)
	}
	if h.LiveBytes != 0 {
		t.Fatalf("expected zero live bytes for unreferenced pinned zombie segment: %+v", h)
	}
}

func valueLogRefSetFromCounts(counts map[uint32]uint64) map[uint32]struct{} {
	out := make(map[uint32]struct{}, len(counts))
	for fileID, n := range counts {
		if n == 0 {
			continue
		}
		out[fileID] = struct{}{}
	}
	return out
}

func appendPointersInNewSegment(t *testing.T, dir string, lane, seq uint32, ridBase uint64, n int, valueAt func(i int) []byte) []page.ValuePtr {
	t.Helper()
	walDir := filepath.Join(dir, "value_vlog")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		t.Fatalf("mkdir wal: %v", err)
	}
	fileID, err := valuelog.EncodeFileID(lane, seq)
	if err != nil {
		t.Fatalf("encode file id lane=%d seq=%d: %v", lane, seq, err)
	}
	path := filepath.Join(walDir, fmt.Sprintf("value-l%d-%06d.log", lane, seq))
	w, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	ptrs := make([]page.ValuePtr, 0, n)
	for i := 0; i < n; i++ {
		ptr, err := w.Append(0, nil, ridBase+uint64(i), valueAt(i))
		if err != nil {
			t.Fatalf("append rid=%d: %v", ridBase+uint64(i), err)
		}
		ptrs = append(ptrs, ptr)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	return ptrs
}
