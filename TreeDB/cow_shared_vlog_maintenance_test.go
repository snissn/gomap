package treedb

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestCOWSharedValueLogWritersSurviveRepeatedMaintenance(t *testing.T) {
	for _, tc := range []struct {
		name                      string
		removeExternalBeforeRetry bool
		fillReservationGap        bool
	}{
		{name: "external_segment_retained"},
		{name: "external_segment_removed_before_retry", removeExternalBeforeRetry: true},
		{name: "empty_external_reservation_gap", fillReservationGap: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testCOWSharedValueLogMaintenance(t, tc.removeExternalBeforeRetry, tc.fillReservationGap)
		})
	}
}

func testCOWSharedValueLogMaintenance(t *testing.T, removeExternalBeforeRetry, fillReservationGap bool) {
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
	if opts.COWMemtableLimits.MaxResources != 256 {
		t.Fatalf("default COW resource cap = %d, want 256", opts.COWMemtableLimits.MaxResources)
	}
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
	readFiles := func() map[string]os.FileInfo {
		t.Helper()
		entries, err := os.ReadDir(path)
		if err != nil {
			t.Fatal(err)
		}
		files := make(map[string]os.FileInfo, len(entries))
		for _, entry := range entries {
			info, err := entry.Info()
			if err != nil {
				t.Fatal(err)
			}
			files[entry.Name()] = info
		}
		return files
	}
	dryRun := func() error {
		_, err := database.ValueLogGC(t.Context(), ValueLogGCOptions{Mode: ValueLogGCModeOnline, DryRun: true})
		return err
	}
	runMaintenance := func() {
		t.Helper()
		before := readFiles()
		if err := dryRun(); err != nil {
			t.Fatal(err)
		}
		first := readFiles()
		if len(first) <= len(before) {
			t.Fatalf("populated writers did not reconcile: before=%d after=%d", len(before), len(first))
		}
		for name, info := range first {
			if _, existed := before[name]; !existed && info.Size() != 0 {
				t.Fatalf("first reconciliation installed a nonempty successor: %s size=%d", name, info.Size())
			}
		}
		if err := dryRun(); err != nil {
			t.Fatal(err)
		}
		plateau := readFiles()
		for i := 2; i < opts.COWMemtableLimits.MaxResources; i++ {
			if err := dryRun(); err != nil {
				t.Fatalf("dry-run value-log GC pass %d (files=%d): %v", i+1, fileCount(), err)
			}
		}
		final := readFiles()
		t.Logf("public dry-run GC passes=%d limit=%d initial=%d first=%d plateau=%d final=%d", opts.COWMemtableLimits.MaxResources, opts.COWMemtableLimits.MaxResources, len(before), len(first), len(plateau), len(final))
		if len(final) != len(plateau) {
			t.Fatalf("idle maintenance changed physical inventory: plateau=%d final=%d", len(plateau), len(final))
		}
		for name, info := range plateau {
			after, exists := final[name]
			if !exists || !os.SameFile(info, after) || info.Size() != after.Size() {
				t.Fatalf("idle maintenance changed segment %s: before=%v after=%v", name, info, after)
			}
		}
	}
	runMaintenance()
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
	// Neither an invalid encoded sequence nor a sequence that would wrap on
	// conversion to uint32 may conceal the valid external advance.
	invalidPaths := []string{
		filepath.Join(path, "value-l0-8388608.log"),
		filepath.Join(path, "value-l0-4294967297.log"),
	}
	for _, invalid := range invalidPaths {
		if err := os.WriteFile(invalid, nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	// Block the second successor so the first writer completes its rotation
	// before an actual storage error leaves the remaining writers behind.
	firstID, err := valuelog.EncodeFileID(0, externalSeq+1)
	if err != nil {
		t.Fatal(err)
	}
	blockedID, err := valuelog.EncodeFileID(0, externalSeq+2)
	if err != nil {
		t.Fatal(err)
	}
	blockedPath := valuelog.SegmentPath(path, blockedID)
	if err := os.Mkdir(blockedPath, 0o700); err != nil {
		t.Fatal(err)
	}
	_, err = database.ValueLogGC(t.Context(), ValueLogGCOptions{Mode: ValueLogGCModeOnline, DryRun: true})
	var pathErr *os.PathError
	if !errors.As(err, &pathErr) || pathErr.Path != blockedPath {
		t.Fatalf("GC did not reach the blocked successor: %v", err)
	}
	firstPath := valuelog.SegmentPath(path, firstID)
	first, err := os.Stat(firstPath)
	if err != nil || !first.Mode().IsRegular() {
		t.Fatalf("first writer did not install its successor: %v %v", first, err)
	}
	oldPaths[firstID] = first
	if err := os.Remove(blockedPath); err != nil {
		t.Fatal(err)
	}
	if removeExternalBeforeRetry {
		if err := os.Remove(externalPath); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := database.ValueLogGC(t.Context(), ValueLogGCOptions{Mode: ValueLogGCModeOnline, DryRun: true}); err != nil {
		t.Fatal(err)
	}
	requiredFence := externalSeq
	if fillReservationGap {
		// The failed successor reservation left a gap underneath newer
		// owned empty peers. A valid empty file in that gap still fences
		// the lower empty writer, even though the allocator is ahead.
		if err := os.WriteFile(blockedPath, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		gap, err := os.Stat(blockedPath)
		if err != nil {
			t.Fatal(err)
		}
		oldPaths[blockedID] = gap
		if err := dryRun(); err != nil {
			t.Fatal(err)
		}
		requiredFence = externalSeq + 2
	}
	after := appendValues()
	var usedFirst bool
	for i, ptr := range after {
		usedFirst = usedFirst || ptr.FileID == firstID
		if lane, seq := valuelog.DecodeFileID(ptr.FileID); lane != 0 || seq <= requiredFence {
			t.Fatalf("writer %d did not fence past observed sequence %d: lane=%d seq=%d", i, requiredFence, lane, seq)
		}
	}
	if fillReservationGap && usedFirst {
		t.Fatal("the empty reservation gap did not fence the older writer")
	}
	if !fillReservationGap && !usedFirst {
		t.Fatal("fence retry rotated the already advanced writer")
	}
	for _, invalid := range invalidPaths {
		if err := os.Remove(invalid); err != nil {
			t.Fatal(err)
		}
	}
	runMaintenance()
	checkPaths()
	checkPointers(before)
	checkPointers(after)
	if !removeExternalBeforeRetry {
		if fi, err := os.Stat(externalPath); err != nil || !os.SameFile(external, fi) || external.Size() != fi.Size() {
			t.Fatalf("external file replaced: %v %v", fi, err)
		}
		if got, err := database.backend.ReadValueLogRecordAppend(ptr, nil); err != nil || !bytes.Equal(got, externalValue) {
			t.Fatalf("external value changed: got=%q err=%v", got, err)
		}
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
	if !removeExternalBeforeRetry {
		if got, err := database.backend.ReadValueLogRecordAppend(ptr, nil); err != nil || !bytes.Equal(got, externalValue) {
			t.Fatalf("reopen changed external value: got=%q err=%v", got, err)
		}
	}
}
