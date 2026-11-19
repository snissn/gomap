package treedb

import (
	"bytes"
	"os"
	"runtime"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestCOWSharedValueLogWritersSurviveRepeatedMaintenance(t *testing.T) {
	// Exercise the production shared-writer dispatch even on a one-CPU runner.
	previous := runtime.GOMAXPROCS(4)
	t.Cleanup(func() { runtime.GOMAXPROCS(previous) })
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_LOOP", "1")
	t.Setenv("TREEDB_DISABLE_VLOG_GENERATION_CHECKPOINT_KICK", "1")

	opts := OptionsFor(ProfileCommandWALRelaxed, t.TempDir())
	opts.MemtableMode = "cow_btree"
	opts.JournalLanes = 3
	opts.FlushThreshold = 1 << 30
	opts.COWMemtableLimits = DefaultCOWMemtableLimits()
	opts.COWMemtableLimits.MaxResources = 32
	database, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if database != nil {
			if err := database.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	path := backenddb.ValueLogDirPath(database.backend.Dir())
	values := make([][]byte, 64)
	for i := range values {
		values[i] = bytes.Repeat([]byte{byte(i + 1)}, 1024)
	}
	appendValues := func() []page.ValuePtr {
		t.Helper()
		ptrs, err := database.backend.AppendValueLogValues(values)
		if err != nil {
			t.Fatal(err)
		}
		return ptrs
	}
	before := appendValues()
	oldPaths := make(map[uint32]os.FileInfo)
	var maxSeq uint32
	for _, ptr := range before {
		lane, seq := valuelog.DecodeFileID(ptr.FileID)
		if lane != 0 {
			t.Fatalf("native append from logical lane %d, want 0", lane)
		}
		maxSeq = max(maxSeq, seq)
		if _, ok := oldPaths[ptr.FileID]; !ok {
			fi, err := os.Stat(valuelog.SegmentPath(path, ptr.FileID))
			if err != nil {
				t.Fatal(err)
			}
			oldPaths[ptr.FileID] = fi
		}
	}
	if len(oldPaths) < 4 {
		t.Fatalf("shared writers not exercised: %d distinct physical writers", len(oldPaths))
	}
	key, value := []byte("durable-key"), bytes.Repeat([]byte("retained"), 1024)
	if err := database.SetSync(key, value); err != nil {
		t.Fatal(err)
	}
	if err := database.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	held := database.AcquireSnapshot()
	if held == nil {
		t.Fatal("no held reader")
	}
	defer func() {
		if held != nil {
			if err := held.Close(); err != nil {
				t.Error(err)
			}
		}
	}()

	fileCount := func() int {
		t.Helper()
		entries, err := os.ReadDir(path)
		if err != nil {
			t.Fatal(err)
		}
		return len(entries)
	}
	checkPaths := func() {
		t.Helper()
		for id, old := range oldPaths {
			info, err := os.Stat(valuelog.SegmentPath(path, id))
			if err != nil || !os.SameFile(old, info) || info.Size() < old.Size() {
				t.Fatalf("original value-log file %d changed: %v %v", id, info, err)
			}
		}
	}
	checkPointers := func(ptrs []page.ValuePtr) {
		t.Helper()
		for i, ptr := range ptrs {
			got, err := database.backend.ReadValueLogRecordAppend(ptr, nil)
			if err != nil || !bytes.Equal(got, values[i]) {
				t.Fatalf("native pointer %d changed: %v", i, err)
			}
		}
	}
	runMaintenance := func(want int) {
		t.Helper()
		for i := 0; i < opts.COWMemtableLimits.MaxResources; i++ {
			if _, err := database.ValueLogGC(t.Context(), ValueLogGCOptions{Mode: ValueLogGCModeOnline, DryRun: true}); err != nil {
				t.Fatalf("dry-run value-log GC %d (files=%d): %v", i, fileCount(), err)
			}
		}
		if got := fileCount(); got != want {
			t.Fatalf("no-op maintenance created value-log files: before=%d after=%d", want, got)
		}
	}
	runMaintenance(fileCount())
	checkPaths()
	checkPointers(before)

	// A physical writer outside the shared reservation owner still fences
	// older writers to prevent sequence collision.
	externalSeq := maxSeq + 100
	externalID, err := valuelog.EncodeFileID(0, externalSeq)
	if err != nil {
		t.Fatal(err)
	}
	externalPath := valuelog.SegmentPath(path, externalID)
	rid, err := database.cached.ReserveValueLogRIDs(1)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := valuelog.NewWriter(externalPath, externalID)
	if err != nil {
		t.Fatal(err)
	}
	externalValue := []byte("out-of-band value")
	ptr, appendErr := writer.Append(0, nil, rid, externalValue)
	closeErr := writer.Close()
	if appendErr != nil || closeErr != nil {
		t.Fatalf("write external value-log file: %v, %v", appendErr, closeErr)
	}
	external, err := os.Stat(externalPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.ValueLogGC(t.Context(), ValueLogGCOptions{Mode: ValueLogGCModeOnline, DryRun: true}); err != nil {
		t.Fatal(err)
	}
	after := appendValues()
	for i, ptr := range after {
		if lane, seq := valuelog.DecodeFileID(ptr.FileID); lane != 0 || seq <= externalSeq {
			t.Fatalf("writer %d did not move past unreserved sequence %d: lane=%d seq=%d", i, externalSeq, lane, seq)
		}
	}
	runMaintenance(fileCount())
	checkPaths()
	checkPointers(before)
	checkPointers(after)
	if fi, err := os.Stat(externalPath); err != nil || !os.SameFile(external, fi) || external.Size() != fi.Size() {
		t.Fatalf("external file replaced: %v %v", fi, err)
	}
	if got, err := database.backend.ReadValueLogRecordAppend(ptr, nil); err != nil || !bytes.Equal(got, externalValue) {
		t.Fatalf("external value changed: got=%q err=%v", got, err)
	}
	if got, err := held.Get(key); err != nil || !bytes.Equal(got, value) {
		t.Fatalf("held reader lost durable key: %v", err)
	}
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	held = nil
	closing := database
	database = nil
	if err := closing.Close(); err != nil {
		t.Fatal(err)
	}
	database, err = Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := database.Get(key); err != nil || !bytes.Equal(got, value) {
		t.Fatalf("reopen lost durable key: %v", err)
	}
	checkPaths()
	checkPointers(before)
	checkPointers(after)
	if got, err := database.backend.ReadValueLogRecordAppend(ptr, nil); err != nil || !bytes.Equal(got, externalValue) {
		t.Fatalf("reopen changed external value: got=%q err=%v", got, err)
	}
}
