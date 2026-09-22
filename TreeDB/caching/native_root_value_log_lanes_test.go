package caching

import (
	"bytes"
	"strconv"
	"strings"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

func TestNativeRootValueLogAppendWidth(t *testing.T) {
	for _, tc := range []struct {
		physical int
		logical  int
		want     int
	}{{1, 1, 1}, {2, 2, 2}, {2, 4, 4}, {4, 4, 4}, {6, 12, 8}, {16, 32, 8}, {64, 64, 8}} {
		if got := nativeRootValueLogAppendWidth(tc.physical, tc.logical); got != tc.want {
			t.Fatalf("nativeRootValueLogAppendWidth(%d, %d)=%d want %d", tc.physical, tc.logical, got, tc.want)
		}
	}
}

func TestNextNativeRootValueLogAppendSeqRejectsExhaustion(t *testing.T) {
	db := &DB{nativeRootValueLogAppendShared: true}
	db.nativeRootValueLogAppendLanes = []*lane{{id: 0}}
	db.nativeRootValueLogAppendSeq.Store(leafLogMaxSegmentSeqForTest)
	if _, err := db.nextNativeRootValueLogAppendSeq(); err == nil || !strings.Contains(err.Error(), "sequence space exhausted") {
		t.Fatalf("nextNativeRootValueLogAppendSeq exhaustion error=%v, want sequence space exhausted", err)
	}
	if got := db.nativeRootValueLogAppendSeq.Load(); got != leafLogMaxSegmentSeqForTest {
		t.Fatalf("sequence advanced after exhaustion: got %d want %d", got, uint32(leafLogMaxSegmentSeqForTest))
	}
}

func TestCachingValueLogAppenderIndependentHotWritersPreserveOrderAfterReopen(t *testing.T) {
	dir := t.TempDir()
	backend, err := backenddb.Open(backenddb.Options{Dir: dir, Durability: backenddb.DurabilityDurable})
	if err != nil {
		t.Fatalf("backend Open: %v", err)
	}
	db, err := Open(dir, backend, Options{
		JournalLanes:             3,
		ValueLogGenerationPolicy: uint8(backenddb.ValueLogGenerationHotWarmCold),
	})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("cache Open: %v", err)
	}

	lanes := db.nativeRootValueLogAppendLanesSnapshot()
	if len(lanes) < 2 {
		db.initNativeRootValueLogAppendLanesWithWidth(4)
		db.valueLogReader.SetMultiCurrentWritableLane(0, true)
		backend.SetMultiCurrentWritableValueLogLane(0, true)
		for _, l := range db.nativeRootValueLogAppendAuxLanesSnapshot() {
			db.startVlogWriter(l)
		}
		lanes = db.nativeRootValueLogAppendLanesSnapshot()
	}
	for i, l := range lanes {
		if l.id != 0 || l.vlogGenerationClass != vlogGenerationClassHot {
			t.Fatalf("writer %d topology=(lane=%d class=%d), want logical hot lane 0", i, l.id, l.vlogGenerationClass)
		}
	}
	if got := []uint8{db.lanes[0].vlogGenerationClass, db.lanes[1].vlogGenerationClass, db.lanes[2].vlogGenerationClass}; got[0] != vlogGenerationClassHot || got[1] != vlogGenerationClassWarm || got[2] != vlogGenerationClassCold {
		t.Fatalf("journal generation topology=%v want hot/warm/cold", got)
	}

	values := make([][]byte, len(lanes)*8)
	for i := range values {
		values[i] = bytes.Repeat([]byte{byte(i + 1)}, 1024+i)
	}
	ptrs, err := backend.AppendValueLogValues(values)
	if err != nil {
		_ = db.Close()
		_ = backend.Close()
		t.Fatalf("AppendValueLogValues: %v", err)
	}
	files := make(map[uint32]struct{}, len(lanes))
	for i, ptr := range ptrs {
		laneID, _ := valuelog.DecodeFileID(ptr.FileID)
		if laneID != 0 {
			t.Fatalf("value %d used logical lane %d want hot lane 0", i, laneID)
		}
		files[ptr.FileID] = struct{}{}
		got, err := db.ReadValueLogRecord(ptr)
		if err != nil {
			t.Fatalf("read value %d: %v", i, err)
		}
		if !bytes.Equal(got, values[i]) {
			t.Fatalf("read value %d mismatch", i)
		}
	}
	if len(files) != len(lanes) {
		t.Fatalf("physical current files=%d want %d", len(files), len(lanes))
	}
	stats := db.Stats()
	for key, want := range map[string]string{
		"treedb.cache.journal_lanes.effective":                     "3",
		"treedb.cache.journal_lanes.hot":                           "1",
		"treedb.cache.journal_lanes.warm":                          "1",
		"treedb.cache.journal_lanes.cold":                          "1",
		"treedb.cache.native_root_vlog_append.writers":             strconv.Itoa(len(lanes)),
		"treedb.cache.native_root_vlog_append.shared_logical_lane": "true",
	} {
		if got := stats[key]; got != want {
			t.Fatalf("stat %s=%q want %q", key, got, want)
		}
	}
	for i := range lanes {
		if got := stats["treedb.cache.native_root_vlog_append.writer."+strconv.Itoa(i)+".records_total"]; got == "" || got == "0" {
			t.Fatalf("writer %d records_total=%q want non-zero", i, got)
		}
	}
	segments, err := (&cachingValueLogAppender{db: db, lane: &db.lanes[0]}).CurrentValueLogSegmentsSnapshot()
	if err != nil {
		t.Fatalf("CurrentValueLogSegmentsSnapshot: %v", err)
	}
	wantSegments := len(db.lanes) + len(lanes) - 1
	if len(segments) != wantSegments {
		t.Fatalf("current segments=%d want canonical plus auxiliary writers=%d", len(segments), wantSegments)
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
		JournalLanes:             3,
		ValueLogGenerationPolicy: uint8(backenddb.ValueLogGenerationHotWarmCold),
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

func TestCachingValueLogAppenderSyncsAuxiliaryHotWriters(t *testing.T) {
	db := &DB{lanes: make([]lane, 1), nativeRootValueLogAppendShared: true}
	aux := &lane{id: 0}
	db.nativeRootValueLogAppendLanes = []*lane{&db.lanes[0], aux}
	writers := []*vlogDirtyOrderWriter{{}, {}}
	for i, l := range db.nativeRootValueLogAppendLanes {
		l.vlog = writers[i]
		l.vlogSyncPending.Store(true)
	}

	if err := (&cachingValueLogAppender{db: db, lane: &db.lanes[0]}).Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	for i, writer := range writers {
		if got := writer.syncs.Load(); got != 1 {
			t.Fatalf("writer %d syncs=%d want 1", i, got)
		}
	}
}
