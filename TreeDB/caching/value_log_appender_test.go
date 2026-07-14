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

func TestCachingValueLogExternalRefSyncCoalescingGuards(t *testing.T) {
	type setupResult struct {
		db       *DB
		lane     *lane
		writer   *vlogDirtyOrderWriter
		appender *cachingValueLogAppender
		fileID   uint32
	}
	setup := func(t *testing.T) setupResult {
		t.Helper()
		db := &DB{
			closeCh:       make(chan struct{}),
			splitValueLog: true,
			valueLogDir:   t.TempDir(),
			lanes:         make([]lane, 1),
		}
		l := &db.lanes[0]
		l.id = 0
		l.vlogSeq = 1
		fileID, err := valuelog.EncodeFileID(0, 1)
		if err != nil {
			t.Fatalf("EncodeFileID: %v", err)
		}
		l.vlogPath = valuelog.SegmentPath(db.valueLogDir, fileID)
		w := &vlogDirtyOrderWriter{}
		l.vlog = w
		l.syncing.Store(true)
		ptrs, err := db.appendValueLog(l, 0, nil, []valuelog.Record{{RID: 1, Value: []byte("covered")}}, journalDurabilitySync)
		if err != nil {
			t.Fatalf("appendValueLog materialization sync: %v", err)
		}
		if len(ptrs) != 1 {
			putValueLogPtrs(ptrs)
			t.Fatalf("appendValueLog pointers=%d, want 1", len(ptrs))
		}
		putValueLogPtrs(ptrs)
		return setupResult{
			db:       db,
			lane:     l,
			writer:   w,
			appender: &cachingValueLogAppender{db: db, lane: l},
			fileID:   fileID,
		}
	}

	t.Run("same_reserved_segment_and_unchanged_writer", func(t *testing.T) {
		got := setup(t)
		if err := got.appender.FlushValueLogExternalRefs([]uint32{got.fileID}, true); err != nil {
			t.Fatalf("FlushValueLogExternalRefs: %v", err)
		}
		if syncs := got.writer.syncs.Load(); syncs != 1 {
			t.Fatalf("writer syncs=%d, want only the materialization sync", syncs)
		}
		if calls := got.db.valueLogSyncPathCalls[valueLogSyncPathExternalRef].Load(); calls != 0 {
			t.Fatalf("external-ref sync calls=%d, want 0", calls)
		}
	})

	for _, tc := range []struct {
		name   string
		mutate func(*setupResult)
	}{
		{
			name: "intervening_writer_growth",
			mutate: func(got *setupResult) {
				got.lane.vlogMu.Lock()
				got.writer.size++
				got.lane.vlogMu.Unlock()
			},
		},
		{
			name: "sync_reservation_released",
			mutate: func(got *setupResult) {
				got.lane.syncing.Store(false)
			},
		},
		{
			name: "segment_rotated",
			mutate: func(got *setupResult) {
				got.lane.vlogMu.Lock()
				got.lane.vlogSeq = 2
				fileID, err := valuelog.EncodeFileID(0, 2)
				if err != nil {
					got.lane.vlogMu.Unlock()
					t.Fatalf("EncodeFileID rotated: %v", err)
				}
				got.lane.vlogPath = valuelog.SegmentPath(got.db.valueLogDir, fileID)
				got.fileID = fileID
				got.lane.vlogMu.Unlock()
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := setup(t)
			tc.mutate(&got)
			if err := got.appender.FlushValueLogExternalRefs([]uint32{got.fileID}, true); err != nil {
				t.Fatalf("FlushValueLogExternalRefs: %v", err)
			}
			if syncs := got.writer.syncs.Load(); syncs != 2 {
				t.Fatalf("writer syncs=%d, want materialization plus conservative external-ref sync", syncs)
			}
			if calls := got.db.valueLogSyncPathCalls[valueLogSyncPathExternalRef].Load(); calls != 1 {
				t.Fatalf("external-ref sync calls=%d, want 1", calls)
			}
		})
	}

	t.Run("fallback_sync_failure_propagates", func(t *testing.T) {
		got := setup(t)
		injected := errors.New("injected conservative external-ref sync failure")
		got.lane.syncing.Store(false)
		got.writer.syncErr = injected
		if err := got.appender.FlushValueLogExternalRefs([]uint32{got.fileID}, true); !errors.Is(err, injected) {
			t.Fatalf("FlushValueLogExternalRefs error=%v, want %v", err, injected)
		}
		if calls := got.db.valueLogSyncPathErrors[valueLogSyncPathExternalRef].Load(); calls != 1 {
			t.Fatalf("external-ref sync errors=%d, want 1", calls)
		}
	})
}

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

	if err := os.WriteFile(valuelog.SegmentPath(dir, currentFileID), []byte("current segment"), 0o644); err != nil {
		t.Fatalf("write current segment: %v", err)
	}
	if err := appender.FlushValueLogExternalRefs([]uint32{currentFileID}, true); err != nil {
		t.Fatalf("FlushValueLogExternalRefs current segment: %v", err)
	}
	if got := db.valueLogRotatedFileSyncCalls.Load(); got != 1 {
		t.Fatalf("direct file-sync calls for nil active writer=%d, want 1", got)
	}
	if err := appender.FlushValueLogExternalRefs([]uint32{oldFileID}, true); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("FlushValueLogExternalRefs missing rotated segment error=%v, want os.ErrNotExist", err)
	}
	if got := db.valueLogSyncPathCalls[valueLogSyncPathExternalRef].Load(); got != 2 {
		t.Fatalf("logical external-ref calls after open failure=%d, want 2", got)
	}
	if got := db.valueLogSyncPathErrors[valueLogSyncPathExternalRef].Load(); got != 1 {
		t.Fatalf("logical external-ref errors after open failure=%d, want 1", got)
	}
	if got := db.valueLogRotatedFileSyncCalls.Load(); got != 1 {
		t.Fatalf("rotated file-sync calls after open failure=%d, want 1", got)
	}
	if err := os.WriteFile(valuelog.SegmentPath(dir, oldFileID), []byte("old segment"), 0o644); err != nil {
		t.Fatalf("write old segment: %v", err)
	}
	injected := errors.New("injected rotated sync failure")
	db.testSyncRotatedValueLogFile = func(*os.File) error { return injected }
	if err := appender.FlushValueLogExternalRefs([]uint32{oldFileID}, true); !errors.Is(err, injected) {
		t.Fatalf("FlushValueLogExternalRefs injected rotated sync error=%v, want %v", err, injected)
	}
	if got := db.valueLogSyncPathCalls[valueLogSyncPathExternalRef].Load(); got != 3 {
		t.Fatalf("logical external-ref calls after sync failure=%d, want 3", got)
	}
	if got := db.valueLogSyncPathErrors[valueLogSyncPathExternalRef].Load(); got != 2 {
		t.Fatalf("logical external-ref errors after sync failure=%d, want 2", got)
	}
	if got := db.valueLogRotatedFileSyncCalls.Load(); got != 2 {
		t.Fatalf("rotated file-sync calls after sync failure=%d, want 2", got)
	}
	if got := db.valueLogRotatedFileSyncErrors.Load(); got != 1 {
		t.Fatalf("rotated file-sync errors after sync failure=%d, want 1", got)
	}
	db.testSyncRotatedValueLogFile = nil
	if err := appender.FlushValueLogExternalRefs([]uint32{oldFileID, currentFileID, oldFileID}, true); err != nil {
		t.Fatalf("FlushValueLogExternalRefs rotated segment: %v", err)
	}
	if got := db.valueLogSyncPathCalls[valueLogSyncPathExternalRef].Load(); got != 5 {
		t.Fatalf("logical external-ref calls after successful deduplicated sync=%d, want 5", got)
	}
	if got := db.valueLogSyncPathErrors[valueLogSyncPathExternalRef].Load(); got != 2 {
		t.Fatalf("logical external-ref errors after successful deduplicated sync=%d, want 2", got)
	}
	if got := db.valueLogRotatedFileSyncCalls.Load(); got != 4 {
		t.Fatalf("rotated file-sync calls after successful deduplicated sync=%d, want 4", got)
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

func TestCachingValueLogExternalRefFlusherDefersRotatedCommandFrameSyncToPinnedDebt(t *testing.T) {
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
		"treedb.command_wal.append.count_total":    1,
		"treedb.command_wal.file_sync.calls_total": 1,
		// Command-WAL V2 captures and syncs the referenced rotated segment
		// through exact retained-handle debt. The cache flusher only drains
		// userspace visibility and must not reopen or sync either segment.
		"treedb.cache.value_log.sync.external_ref.calls_total":          0,
		"treedb.cache.value_log.sync.external_ref.errors_total":         0,
		"treedb.cache.value_log.file_sync.rotated_segment.calls_total":  0,
		"treedb.cache.value_log.file_sync.rotated_segment.errors_total": 0,
		"treedb.cache.value_log.file_sync.calls_total":                  0,
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
		beforeNs := requireStatUint64(t, before, key)
		afterNs := requireStatUint64(t, after, key)
		// A completed file sync can fit inside one timer tick on Windows. Calls
		// and errors above are the exact accounting boundary; elapsed counters
		// are cumulative and may legitimately have a zero delta.
		if afterNs < beforeNs {
			t.Fatalf("%s decreased from %d to %d", key, beforeNs, afterNs)
		}
	}
	if aggregate, rotated :=
		requireStatUint64(t, after, "treedb.cache.value_log.file_sync.ns_total"),
		requireStatUint64(t, after, "treedb.cache.value_log.file_sync.rotated_segment.ns_total"); aggregate < rotated {
		t.Fatalf("aggregate file-sync ns=%d, want >= rotated-segment ns=%d", aggregate, rotated)
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
