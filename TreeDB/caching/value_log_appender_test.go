package caching

import (
	"bytes"
	"errors"
	"os"
	"testing"

	batchpkg "github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

func TestCachingValueLogExternalRefFlusherSyncsRotatedSegments(t *testing.T) {
	dir := t.TempDir()
	oldFileID, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("EncodeFileID old: %v", err)
	}
	currentFileID, err := valuelog.EncodeFileID(0, 2)
	if err != nil {
		t.Fatalf("EncodeFileID current: %v", err)
	}

	db := &DB{
		valueLogDir: dir,
		lanes:       make([]lane, 1),
	}
	db.lanes[0].id = 0
	db.lanes[0].vlogSeq = 2
	db.lanes[0].vlogPath = valuelog.SegmentPath(dir, currentFileID)
	appender := &cachingValueLogAppender{db: db, lane: &db.lanes[0]}

	if err := appender.FlushValueLogExternalRefs([]uint32{currentFileID}, true); err != nil {
		t.Fatalf("FlushValueLogExternalRefs current segment: %v", err)
	}
	if err := appender.FlushValueLogExternalRefs([]uint32{oldFileID}, true); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("FlushValueLogExternalRefs missing rotated segment error=%v, want os.ErrNotExist", err)
	}
	if got := db.valueLogSyncPathCalls[valueLogSyncPathExternalRef].Load(); got != 1 {
		t.Fatalf("logical external-ref calls after open failure=%d, want 1", got)
	}
	if got := db.valueLogSyncPathErrors[valueLogSyncPathExternalRef].Load(); got != 1 {
		t.Fatalf("logical external-ref errors after open failure=%d, want 1", got)
	}
	if got := db.valueLogRotatedFileSyncCalls.Load(); got != 0 {
		t.Fatalf("rotated file-sync calls after open failure=%d, want 0", got)
	}
	if err := os.WriteFile(valuelog.SegmentPath(dir, oldFileID), []byte("old segment"), 0o644); err != nil {
		t.Fatalf("write old segment: %v", err)
	}
	injected := errors.New("injected rotated sync failure")
	db.testSyncRotatedValueLogFile = func(*os.File) error { return injected }
	if err := appender.FlushValueLogExternalRefs([]uint32{oldFileID}, true); !errors.Is(err, injected) {
		t.Fatalf("FlushValueLogExternalRefs injected rotated sync error=%v, want %v", err, injected)
	}
	if got := db.valueLogSyncPathCalls[valueLogSyncPathExternalRef].Load(); got != 2 {
		t.Fatalf("logical external-ref calls after sync failure=%d, want 2", got)
	}
	if got := db.valueLogSyncPathErrors[valueLogSyncPathExternalRef].Load(); got != 2 {
		t.Fatalf("logical external-ref errors after sync failure=%d, want 2", got)
	}
	if got := db.valueLogRotatedFileSyncCalls.Load(); got != 1 {
		t.Fatalf("rotated file-sync calls after sync failure=%d, want 1", got)
	}
	if got := db.valueLogRotatedFileSyncErrors.Load(); got != 1 {
		t.Fatalf("rotated file-sync errors after sync failure=%d, want 1", got)
	}
	db.testSyncRotatedValueLogFile = nil
	if err := appender.FlushValueLogExternalRefs([]uint32{oldFileID, currentFileID, oldFileID}, true); err != nil {
		t.Fatalf("FlushValueLogExternalRefs rotated segment: %v", err)
	}
	if got := db.valueLogSyncPathCalls[valueLogSyncPathExternalRef].Load(); got != 3 {
		t.Fatalf("logical external-ref calls after successful deduplicated sync=%d, want 3", got)
	}
	if got := db.valueLogSyncPathErrors[valueLogSyncPathExternalRef].Load(); got != 2 {
		t.Fatalf("logical external-ref errors after successful deduplicated sync=%d, want 2", got)
	}
	if got := db.valueLogRotatedFileSyncCalls.Load(); got != 2 {
		t.Fatalf("rotated file-sync calls after successful deduplicated sync=%d, want 2", got)
	}
	if got := db.valueLogRotatedFileSyncErrors.Load(); got != 1 {
		t.Fatalf("rotated file-sync errors after successful deduplicated sync=%d, want 1", got)
	}
	if got := db.valueLogSyncPathWaitNs[valueLogSyncPathExternalRef].Load(); got != 0 {
		t.Fatalf("logical rotated external-ref wait ns=%d, want 0 without an active writer", got)
	}
	if got := db.valueLogSyncPathNs[valueLogSyncPathExternalRef].Load(); got == 0 {
		t.Fatal("logical rotated external-ref ns=0, want observed time")
	}
	if got := db.valueLogRotatedFileSyncNs.Load(); got == 0 {
		t.Fatal("rotated file-sync ns=0, want observed time")
	}
}

func TestCachingValueLogExternalRefFlusherAccountsForRotatedCommandFrameSegment(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{
		Dir:                    dir,
		Durability:             backenddb.DurabilityDurable,
		CommandWAL:             true,
		DisableBackgroundPrune: true,
		ValueLog: backenddb.ValueLogOptions{
			PointerThreshold: 1,
			ForcePointers:    true,
		},
	})
	if err != nil {
		t.Fatalf("backend Open: %v", err)
	}
	db, err := Open(dir, backend, Options{
		ExternalCommandWAL:       true,
		FlushThreshold:           1 << 30,
		JournalLanes:             1,
		ValueLogPointerThreshold: 1,
		ForceValueLogPointers:    true,
	})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("cache Open: %v", err)
	}
	defer func() {
		_ = db.Close()
		_ = backend.Close()
	}()

	ptrs, err := backend.AppendValueLogValues([][]byte{bytes.Repeat([]byte("v"), 2048)})
	if err != nil {
		t.Fatalf("AppendValueLogValues: %v", err)
	}
	if len(ptrs) != 1 || ptrs[0].FileID == 0 {
		t.Fatalf("AppendValueLogValues pointers=%v, want one value-log pointer", ptrs)
	}
	referencedFileID := ptrs[0].FileID
	if err := db.rotateValueLogLocked(&db.lanes[0]); err != nil {
		t.Fatalf("rotateValueLogLocked: %v", err)
	}
	_, activeFileID, ok := cachingValueLogSegmentForLane(&db.lanes[0])
	if !ok || activeFileID == 0 || activeFileID == referencedFileID {
		t.Fatalf("active file ID after rotation=(%d,%t), want nonzero and different from referenced %d", activeFileID, ok, referencedFileID)
	}

	before := db.Stats()
	lsn, err := backend.AppendRawKVCommandWALOrderedEntries([]batchpkg.Entry{{
		Type:     batchpkg.OpPut,
		Key:      []byte("rotated-command-ref"),
		ValuePtr: ptrs[0],
		IsPtr:    true,
	}}, true)
	if err != nil {
		t.Fatalf("AppendRawKVCommandWALOrderedEntries: %v", err)
	}
	if lsn == 0 {
		t.Fatal("AppendRawKVCommandWALOrderedEntries LSN=0, want an appended command frame")
	}
	after := db.Stats()

	for key, want := range map[string]uint64{
		"treedb.command_wal.append.count_total":                         1,
		"treedb.command_wal.file_sync.calls_total":                      1,
		"treedb.cache.value_log.sync.external_ref.calls_total":          2,
		"treedb.cache.value_log.sync.external_ref.errors_total":         0,
		"treedb.cache.value_log.file_sync.rotated_segment.calls_total":  1,
		"treedb.cache.value_log.file_sync.rotated_segment.errors_total": 0,
		"treedb.cache.value_log.file_sync.calls_total":                  2,
		"treedb.cache.value_log.file_sync.errors_total":                 0,
	} {
		got := requireStatUint64(t, after, key) - requireStatUint64(t, before, key)
		if got != want {
			t.Fatalf("%s delta=%d, want %d (referenced=%d active=%d)", key, got, want, referencedFileID, activeFileID)
		}
	}
	for _, key := range []string{
		"treedb.cache.value_log.sync.external_ref.ns_total",
		"treedb.cache.value_log.file_sync.rotated_segment.ns_total",
		"treedb.cache.value_log.file_sync.ns_total",
	} {
		got := requireStatUint64(t, after, key) - requireStatUint64(t, before, key)
		if got == 0 {
			t.Fatalf("%s delta=0, want observed time", key)
		}
	}
}

func TestCachingValueLogExternalRefFlusherEmptyIDsSyncsAllPendingLanes(t *testing.T) {
	db := &DB{lanes: make([]lane, 2)}
	writers := make([]*vlogDirtyOrderWriter, len(db.lanes))
	for i := range db.lanes {
		writers[i] = &vlogDirtyOrderWriter{}
		db.lanes[i].id = i
		db.lanes[i].vlog = writers[i]
		db.lanes[i].vlogSyncPending.Store(true)
	}
	appender := &cachingValueLogAppender{db: db, lane: &db.lanes[0]}

	if err := appender.FlushValueLogExternalRefs(nil, true); err != nil {
		t.Fatalf("FlushValueLogExternalRefs all pending: %v", err)
	}
	for i := range db.lanes {
		if got := writers[i].syncs.Load(); got != 1 {
			t.Fatalf("lane %d syncs=%d, want 1", i, got)
		}
		if db.lanes[i].vlogSyncPending.Load() {
			t.Fatalf("lane %d still has a pending sync", i)
		}
	}
}
