package caching

import (
	"bytes"
	"errors"
	"os"
	"runtime"
	"testing"
	"time"

	batchpkg "github.com/snissn/gomap/TreeDB/batch"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
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
	if runtime.GOOS == "windows" {
		t.Skip("durable command-WAL activation fails closed without stable namespace persistence")
	}
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

func TestCachingValueLogAppenderPreparedOrdinaryBlockFramesPreserveOrderAfterReopen(t *testing.T) {
	previousProcs := runtime.GOMAXPROCS(4)
	t.Cleanup(func() { runtime.GOMAXPROCS(previousProcs) })

	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir, Durability: backenddb.DurabilityDurable})
	if err != nil {
		t.Fatalf("backend Open: %v", err)
	}
	db, err := Open(dir, backend, Options{
		JournalLanes:                       1,
		ValueLogCompression:                uint8(vlogCompressionBlock),
		ValueLogBlockTargetCompressedBytes: 256,
	})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("cache Open: %v", err)
	}
	// Keep the test on the ordinary block grouping path rather than the retained
	// storage-first grouping policy, which intentionally coalesces these values.
	db.valueLogThreshold = 1 << 30

	values := make([][]byte, 16)
	for i := range values {
		values[i] = make([]byte, 8<<10)
		for j := range values[i] {
			values[i][j] = byte((i*131 + j*17 + j>>4) % 251)
		}
	}
	ptrs, err := backend.AppendValueLogValues(values)
	if err != nil {
		_ = db.Close()
		_ = backend.Close()
		t.Fatalf("AppendValueLogValues: %v", err)
	}
	if len(ptrs) != len(values) {
		_ = db.Close()
		_ = backend.Close()
		t.Fatalf("AppendValueLogValues pointers=%d, want %d", len(ptrs), len(values))
	}
	db.lanes[0].vlogPrepMu.Lock()
	workers := db.lanes[0].vlogPrepWorkers
	db.lanes[0].vlogPrepMu.Unlock()
	if workers == 0 {
		_ = db.Close()
		_ = backend.Close()
		t.Fatalf("ordinary block append did not start prepared-frame workers (mode=%d target=%d max=%d channel=%t block-k-count=%d max-k=%d)", db.valueLogCompressionMode, db.valueLogBlockTargetBytes, db.lanes[0].vlogPrepMaxWorkers, db.lanes[0].vlogPrepCh != nil, db.lanes[0].vlogBlockKCount[0].Load(), db.lanes[0].vlogBlockKMax[0].Load())
	}
	for i, ptr := range ptrs {
		if i > 0 && ptr.FileID == ptrs[i-1].FileID && ptr.Offset <= ptrs[i-1].Offset {
			_ = db.Close()
			_ = backend.Close()
			t.Fatalf("pointer %d offset=%d, want > previous offset=%d in file %d", i, ptr.Offset, ptrs[i-1].Offset, ptr.FileID)
		}
		got, err := db.ReadValueLogRecord(ptr)
		if err != nil {
			_ = db.Close()
			_ = backend.Close()
			t.Fatalf("read value %d: %v", i, err)
		}
		if !bytes.Equal(got, values[i]) {
			_ = db.Close()
			_ = backend.Close()
			t.Fatalf("read value %d mismatch", i)
		}
	}
	if err := db.Close(); err != nil {
		_ = backend.Close()
		t.Fatalf("close cache: %v", err)
	}
	if err := backend.Close(); err != nil {
		t.Fatalf("close backend: %v", err)
	}

	reopenedBackend, err := backenddb.Open(backenddb.Options{Dir: dir, Durability: backenddb.DurabilityDurable})
	if err != nil {
		t.Fatalf("reopen backend: %v", err)
	}
	reopened, err := Open(dir, reopenedBackend, Options{JournalLanes: 1})
	if err != nil {
		_ = reopenedBackend.Close()
		t.Fatalf("reopen cache: %v", err)
	}
	defer func() {
		_ = reopened.Close()
		_ = reopenedBackend.Close()
	}()
	for i, ptr := range ptrs {
		got, err := reopened.ReadValueLogRecord(ptr)
		if err != nil {
			t.Fatalf("reopen read value %d: %v", i, err)
		}
		if !bytes.Equal(got, values[i]) {
			t.Fatalf("reopen read value %d mismatch", i)
		}
	}
}

func TestCachingValueLogAppenderStripesLanesAndPreservesOrderAfterReopen(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir, Durability: backenddb.DurabilityDurable})
	if err != nil {
		t.Fatalf("backend Open: %v", err)
	}
	db, err := Open(dir, backend, Options{
		JournalLanes:             4,
		ValueLogGenerationPolicy: uint8(backenddb.ValueLogGenerationOff),
	})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("cache Open: %v", err)
	}

	values := make([][]byte, 32)
	for i := range values {
		values[i] = bytes.Repeat([]byte{byte(i + 1)}, 1024+i)
	}
	ptrs, err := backend.AppendValueLogValues(values)
	if err != nil {
		_ = db.Close()
		_ = backend.Close()
		t.Fatalf("AppendValueLogValues: %v", err)
	}
	lanes := make(map[uint32]struct{})
	for i, ptr := range ptrs {
		laneID, _ := valuelog.DecodeFileID(ptr.FileID)
		lanes[laneID] = struct{}{}
		got, err := db.ReadValueLogRecord(ptr)
		if err != nil {
			t.Fatalf("read value %d: %v", i, err)
		}
		if !bytes.Equal(got, values[i]) {
			t.Fatalf("read value %d mismatch", i)
		}
	}
	if len(lanes) != 4 {
		t.Fatalf("native-root append used %d lanes, want 4: %v", len(lanes), lanes)
	}
	segments, err := (&cachingValueLogAppender{db: db, lane: &db.lanes[0]}).CurrentValueLogSegmentsSnapshot()
	if err != nil {
		t.Fatalf("CurrentValueLogSegmentsSnapshot: %v", err)
	}
	if len(segments) != 4 {
		t.Fatalf("current value-log segments=%v, want one per lane", segments)
	}
	if err := backend.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := db.Close(); err != nil {
		_ = backend.Close()
		t.Fatalf("close cache: %v", err)
	}
	if err := backend.Close(); err != nil {
		t.Fatalf("close backend: %v", err)
	}

	reopenedBackend, err := backenddb.Open(backenddb.Options{Dir: dir, Durability: backenddb.DurabilityDurable})
	if err != nil {
		t.Fatalf("reopen backend: %v", err)
	}
	reopened, err := Open(dir, reopenedBackend, Options{
		JournalLanes:             4,
		ValueLogGenerationPolicy: uint8(backenddb.ValueLogGenerationOff),
	})
	if err != nil {
		_ = reopenedBackend.Close()
		t.Fatalf("reopen cache: %v", err)
	}
	defer func() {
		_ = reopened.Close()
		_ = reopenedBackend.Close()
	}()
	for i, ptr := range ptrs {
		got, err := reopened.ReadValueLogRecord(ptr)
		if err != nil {
			t.Fatalf("reopen read value %d: %v", i, err)
		}
		if !bytes.Equal(got, values[i]) {
			t.Fatalf("reopen read value %d mismatch", i)
		}
	}
}

func TestCachingValueLogAppenderStripesOnlyAcrossHotGenerationLanes(t *testing.T) {
	db := &DB{
		lanes:                    make([]lane, 6),
		valueLogGenerationPolicy: uint8(backenddb.ValueLogGenerationHotWarmCold),
		valueLogHotLanes:         []int{0, 3, 4, 5},
	}
	appender := &cachingValueLogAppender{db: db, lane: &db.lanes[0]}
	want := []int{0, 3, 4, 5, 0, 3, 4, 5}
	for i, wantLane := range want {
		if got := appender.laneForRID(uint64(i + 1)); got != wantLane {
			t.Fatalf("RID %d lane=%d, want hot lane %d", i+1, got, wantLane)
		}
	}
}

func TestCachingValueLogAppenderSingleHotLaneKeepsDirectPath(t *testing.T) {
	db := &DB{
		lanes:                    make([]lane, 3),
		valueLogGenerationPolicy: uint8(backenddb.ValueLogGenerationHotWarmCold),
		valueLogHotLanes:         []int{0},
	}
	db.lanes[0].id = 0
	db.lanes[0].vlogSeq = 1
	fileID, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	db.lanes[0].vlogPath = valuelog.SegmentPath(t.TempDir(), fileID)
	db.lanes[0].vlog = &vlogDirtyOrderWriter{}
	appender := &cachingValueLogAppender{db: db, lane: &db.lanes[0]}

	ptrs, err := appender.appendRecords([]valuelog.Record{{RID: 1, Value: []byte("payload")}})
	if err != nil {
		t.Fatalf("appendRecords: %v", err)
	}
	if len(ptrs) != 1 {
		t.Fatalf("pointers=%v, want one", ptrs)
	}
	if db.lanes[1].vlog != nil || db.lanes[2].vlog != nil {
		t.Fatal("single hot-lane append touched reserved generation lanes")
	}
}

func TestCachingValueLogAppenderConcurrentAppendsPreserveValues(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir, Durability: backenddb.DurabilityDurable})
	if err != nil {
		t.Fatalf("backend Open: %v", err)
	}
	db, err := Open(dir, backend, Options{
		JournalLanes:             4,
		ValueLogGenerationPolicy: uint8(backenddb.ValueLogGenerationOff),
	})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("cache Open: %v", err)
	}
	defer func() {
		_ = db.Close()
		_ = backend.Close()
	}()

	type result struct {
		values [][]byte
		ptrs   []page.ValuePtr
		err    error
	}
	results := make(chan result, 8)
	for worker := 0; worker < cap(results); worker++ {
		go func() {
			values := make([][]byte, 8)
			for i := range values {
				values[i] = bytes.Repeat([]byte{byte(worker*len(values) + i + 1)}, 1024+i)
			}
			ptrs, err := backend.AppendValueLogValues(values)
			results <- result{values: values, ptrs: ptrs, err: err}
		}()
	}
	for range cap(results) {
		result := <-results
		if result.err != nil {
			t.Fatalf("AppendValueLogValues: %v", result.err)
		}
		for i, ptr := range result.ptrs {
			got, err := db.ReadValueLogRecord(ptr)
			if err != nil {
				t.Fatalf("read value %d: %v", i, err)
			}
			if !bytes.Equal(got, result.values[i]) {
				t.Fatalf("read value %d mismatch", i)
			}
		}
	}
}

func TestCachingValueLogAppenderLaneFailureReturnsNoPointers(t *testing.T) {
	injected := errors.New("injected lane append failure")
	db := &DB{closeCh: make(chan struct{}), splitValueLog: true, lanes: make([]lane, 2)}
	for i := range db.lanes {
		fileID, err := valuelog.EncodeFileID(uint32(i), 1)
		if err != nil {
			t.Fatalf("EncodeFileID lane %d: %v", i, err)
		}
		db.lanes[i].id = i
		db.lanes[i].vlogSeq = 1
		db.lanes[i].vlogPath = valuelog.SegmentPath(t.TempDir(), fileID)
		db.lanes[i].vlog = &vlogDirtyOrderWriter{}
	}
	db.lanes[1].vlog.(*vlogDirtyOrderWriter).appendErr = injected
	appender := &cachingValueLogAppender{db: db, lane: &db.lanes[0]}

	ptrs, err := appender.AppendValues([][]byte{[]byte("lane-0"), []byte("lane-1")})
	if !errors.Is(err, injected) {
		t.Fatalf("AppendValues error=%v, want %v", err, injected)
	}
	if ptrs != nil {
		t.Fatalf("AppendValues pointers=%v, want nil on partial lane failure", ptrs)
	}
}

func TestPrepareAppendFramesBlockBackoffPersistsAcrossWorkerTasks(t *testing.T) {
	previousProcs := runtime.GOMAXPROCS(4)
	t.Cleanup(func() { runtime.GOMAXPROCS(previousProcs) })

	db := &DB{closeCh: make(chan struct{}), lanes: make([]lane, 1)}
	db.startVlogDictPreparer(&db.lanes[0])
	t.Cleanup(func() {
		close(db.closeCh)
		db.wg.Wait()
	})

	const (
		frames     = 8
		valueBytes = 32 << 10
	)
	records := make([]valuelog.Record, frames)
	rawBytes := 0
	for i := range records {
		value := make([]byte, valueBytes)
		state := uint32(i + 1)
		for j := range value {
			state ^= state << 13
			state ^= state >> 17
			state ^= state << 5
			value[j] = byte(state)
		}
		records[i] = valuelog.Record{RID: uint64(i + 1), Value: value}
		rawBytes += len(value)
	}

	attempted := 0
	for batch := 0; batch < 2; batch++ {
		prepared, _, err := db.prepareAppendFrames(
			&db.lanes[0], 0, nil, records, 1, rawBytes, vlogWriteBlock, false,
			valuelog.BlockCodecZSTD, 0, 0, 0, time.Time{},
		)
		if err != nil {
			t.Fatalf("prepare batch %d: %v", batch, err)
		}
		for i := range prepared {
			if prepared[i].stats.Attempted {
				attempted++
			}
			if prepared[i].stats.Kept {
				t.Fatalf("batch %d frame %d unexpectedly kept incompressible data", batch, i)
			}
		}
		releasePreparedDictFrames(prepared)
		putVlogPreparedFrames(prepared)
	}
	if max := db.lanes[0].vlogPrepMaxWorkers * 2; attempted > max {
		t.Fatalf("compression attempts=%d, want <= %d with persistent worker backoff", attempted, max)
	}

	compressible := make([]valuelog.Record, frames)
	for i := range compressible {
		compressible[i] = valuelog.Record{RID: uint64(i + 1), Value: bytes.Repeat([]byte{byte(i + 1)}, valueBytes)}
	}
	prepared, _, err := db.prepareAppendFrames(
		&db.lanes[0], 0, nil, compressible, 1, rawBytes, vlogWriteBlock, true,
		valuelog.BlockCodecZSTD, 0, 0, 0, time.Time{},
	)
	if err != nil {
		t.Fatalf("prepare forced probe: %v", err)
	}
	defer func() {
		releasePreparedDictFrames(prepared)
		putVlogPreparedFrames(prepared)
	}()
	for i := range prepared {
		if !prepared[i].stats.Attempted || !prepared[i].stats.Kept {
			t.Fatalf("forced probe frame %d stats=%+v, want attempted and kept", i, prepared[i].stats)
		}
	}
}

func TestPrepareAppendFramesCloseWaitsForSubmittedTasks(t *testing.T) {
	db := &DB{closeCh: make(chan struct{}), lanes: make([]lane, 1)}
	l := &db.lanes[0]
	l.vlogPrepMaxWorkers = 2
	l.vlogPrepWorkers = 2
	l.vlogPrepCh = make(chan vlogDictPrepareTask, vlogDictPrepBuffer)

	started := make(chan struct{})
	release := make(chan struct{})
	released := false
	stop := make(chan struct{})
	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		first := true
		for {
			select {
			case task := <-l.vlogPrepCh:
				if first {
					first = false
					close(started)
					<-release
				}
				task.out <- vlogDictPrepareResult{fi: task.fi, err: errWALClosed}
			case <-stop:
				return
			}
		}
	}()
	t.Cleanup(func() {
		if !released {
			close(release)
		}
		close(stop)
		<-consumerDone
	})

	records := []valuelog.Record{
		{RID: 1, Value: bytes.Repeat([]byte("a"), 128<<10)},
		{RID: 2, Value: bytes.Repeat([]byte("b"), 128<<10)},
	}
	done := make(chan error, 1)
	go func() {
		prepared, _, err := db.prepareAppendFrames(
			l, 0, nil, records, 1, 256<<10, vlogWriteBlock, false,
			valuelog.BlockCodecZSTD, 0, 0, 0, time.Time{},
		)
		if prepared != nil {
			releasePreparedDictFrames(prepared)
			putVlogPreparedFrames(prepared)
		}
		done <- err
	}()

	<-started
	close(db.closeCh)
	select {
	case err := <-done:
		t.Fatalf("prepare returned before submitted task completed: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	released = true
	select {
	case err := <-done:
		if !errors.Is(err, errWALClosed) {
			t.Fatalf("prepare error=%v, want %v", err, errWALClosed)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("prepare did not return after submitted task completed")
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

func TestCachingValueLogAppenderSyncsAllPendingLanes(t *testing.T) {
	db := &DB{lanes: make([]lane, 2)}
	writers := make([]*vlogDirtyOrderWriter, len(db.lanes))
	for i := range db.lanes {
		writers[i] = &vlogDirtyOrderWriter{}
		db.lanes[i].id = i
		db.lanes[i].vlog = writers[i]
		db.lanes[i].vlogSyncPending.Store(true)
	}
	appender := &cachingValueLogAppender{db: db, lane: &db.lanes[0]}

	if err := appender.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	for i := range db.lanes {
		if got := writers[i].syncs.Load(); got != 1 {
			t.Fatalf("lane %d syncs=%d, want 1", i, got)
		}
	}
}
