package caching

import (
	"bytes"
	"context"
	"os"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestVlogGenerationGC_ProtectsRetainedSegmentsBeforeFlush(t *testing.T) {
	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("backend open: %v", err)
	}

	db, err := Open(dir, backend, Options{
		FlushThreshold:           256 << 20,
		DisableWAL:               true,
		RelaxedSync:              true,
		AllowUnsafe:              true,
		ValueLogPointerThreshold: 1,
		ValueLogMaxSegmentBytes:  4 << 10,
		ValueLogGenerationPolicy: uint8(backenddb.ValueLogGenerationHotWarmCold),
		// Keep rewrite disabled; this regression is about GC protection.
		ValueLogRewriteTriggerTotalBytes: 1 << 60,
	})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("Open: %v", err)
	}

	// Write a value-log-backed key, then force-rotate so it points at a non-active
	// segment that is still only referenced by cached state.
	val1 := bytes.Repeat([]byte("a"), 8<<10)
	if err := db.Set([]byte("k1"), val1); err != nil {
		_ = db.Close()
		t.Fatalf("Set k1: %v", err)
	}
	// Force-rotate the value log so k1's pointer refers to a non-active segment
	// that is still only referenced by in-memory state.
	beforeSeq := 0
	beforePath := ""
	func() {
		l := &db.lanes[0]
		l.vlogMu.Lock()
		beforeSeq = l.vlogSeq
		beforePath = l.vlogPath
		l.vlogMu.Unlock()
	}()
	if err := db.rotateValueLogLocked(&db.lanes[0]); err != nil {
		_ = db.Close()
		t.Fatalf("rotateValueLogLocked: %v", err)
	}
	func() {
		l := &db.lanes[0]
		l.vlogMu.Lock()
		afterSeq := l.vlogSeq
		afterPath := l.vlogPath
		closed := len(l.vlogClosedSizes)
		l.vlogMu.Unlock()
		if afterSeq <= beforeSeq || afterPath == "" || afterPath == beforePath || closed == 0 {
			_ = db.Close()
			t.Fatalf("expected vlog rotation: seq %d->%d path %q->%q closed=%d", beforeSeq, afterSeq, beforePath, afterPath, closed)
		}
	}()

	// Run the same GC path used by the generational scheduler. Without retained
	// path protection this can delete the just-rotated segment and corrupt the
	// in-memory pointers before they are flushed to the backend.
	db.maybeRunVlogGenerationMaintenance(true)

	if err := db.Checkpoint(); err != nil {
		_ = db.Close()
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	backend2, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("backend reopen: %v", err)
	}
	defer backend2.Close()

	reopen, err := Open(dir, backend2, Options{
		FlushThreshold:           256 << 20,
		DisableWAL:               true,
		RelaxedSync:              true,
		AllowUnsafe:              true,
		ValueLogPointerThreshold: 1,
		ValueLogMaxSegmentBytes:  4 << 10,
		ValueLogGenerationPolicy: uint8(backenddb.ValueLogGenerationOff),
	})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopen.Close()

	got, err := reopen.Get([]byte("k1"))
	if err != nil {
		t.Fatalf("reopen Get k1: %v", err)
	}
	if !bytes.Equal(got, val1) {
		t.Fatalf("value mismatch after reopen: got=%dB want=%dB", len(got), len(val1))
	}
}

func TestVlogGenerationGC_CheckpointsBeforeGCWhenRetainedPathsIncomplete(t *testing.T) {
	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("backend open: %v", err)
	}

	db, err := Open(dir, backend, Options{
		FlushThreshold:           256 << 20,
		DisableWAL:               true,
		RelaxedSync:              true,
		AllowUnsafe:              true,
		MemtableShards:           1,
		JournalLanes:             1,
		ValueLogCompression:      1, // off
		ValueLogPointerThreshold: 1,
		ValueLogMaxSegmentBytes:  4 << 10,
		ValueLogGenerationPolicy: uint8(backenddb.ValueLogGenerationHotWarmCold),
		// Keep rewrite disabled; this regression is about GC safety.
		ValueLogRewriteTriggerTotalBytes: 1 << 60,
	})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("Open: %v", err)
	}

	val1 := bytes.Repeat([]byte("a"), 8<<10)
	if err := db.Set([]byte("k1"), val1); err != nil {
		_ = db.Close()
		t.Fatalf("Set k1: %v", err)
	}

	// The flush threshold keeps data in-memory; backend must not see it yet.
	if got, err := backend.Get([]byte("k1")); err != nil {
		_ = db.Close()
		t.Fatalf("backend Get k1: %v", err)
	} else if got != nil {
		_ = db.Close()
		t.Fatalf("expected backend to miss k1 before checkpoint; got=%dB", len(got))
	}

	// Force-rotate the value log so k1's pointer refers to a non-active segment
	// that is still only referenced by in-memory state.
	k1Path := ""
	func() {
		l := &db.lanes[0]
		l.vlogMu.Lock()
		k1Path = l.vlogPath
		l.vlogMu.Unlock()
	}()
	if k1Path == "" {
		_ = db.Close()
		t.Fatalf("expected non-empty value-log path for k1")
	}
	if err := db.rotateValueLogLocked(&db.lanes[0]); err != nil {
		_ = db.Close()
		t.Fatalf("rotateValueLogLocked: %v", err)
	}
	keepPath := ""
	func() {
		l := &db.lanes[0]
		l.vlogMu.Lock()
		keepPath = l.vlogPath
		l.vlogMu.Unlock()
	}()
	if keepPath == "" || keepPath == k1Path {
		_ = db.Close()
		t.Fatalf("expected rotated value-log path; k1=%q keep=%q", k1Path, keepPath)
	}
	if info, err := os.Stat(k1Path); err != nil {
		_ = db.Close()
		t.Fatalf("stat k1 segment: %v", err)
	} else if info.Size() == 0 {
		_ = db.Close()
		t.Fatalf("expected k1 segment to have data; size=0")
	}

	// Simulate a bug/edge case where retained-path protection is non-empty but
	// missing a rotated segment that still backs in-memory pointers.
	db.valueLogMu.Lock()
	db.valueLogRetain = map[string]struct{}{keepPath: {}}
	db.valueLogMu.Unlock()

	db.maybeRunVlogGenerationMaintenance(true)

	// The maintenance path establishes a stable backend boundary before GC, so
	// k1 must now be readable from the backend.
	if got, err := backend.Get([]byte("k1")); err != nil {
		_ = db.Close()
		t.Fatalf("backend Get k1 after maintenance: %v", err)
	} else if !bytes.Equal(got, val1) {
		_ = db.Close()
		t.Fatalf("backend value mismatch after maintenance: got=%dB want=%dB", len(got), len(val1))
	}

	if _, err := os.Stat(k1Path); err != nil {
		_ = db.Close()
		t.Fatalf("expected k1 segment to remain after maintenance; stat: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	backend2, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("backend reopen: %v", err)
	}
	defer backend2.Close()

	reopen, err := Open(dir, backend2, Options{
		FlushThreshold:           256 << 20,
		DisableWAL:               true,
		RelaxedSync:              true,
		AllowUnsafe:              true,
		MemtableShards:           1,
		JournalLanes:             1,
		ValueLogCompression:      1, // off
		ValueLogPointerThreshold: 1,
		ValueLogMaxSegmentBytes:  4 << 10,
		ValueLogGenerationPolicy: uint8(backenddb.ValueLogGenerationOff),
	})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopen.Close()

	got, err := reopen.Get([]byte("k1"))
	if err != nil {
		t.Fatalf("reopen Get k1: %v", err)
	}
	if !bytes.Equal(got, val1) {
		t.Fatalf("value mismatch after reopen: got=%dB want=%dB", len(got), len(val1))
	}
}

func TestVlogGenerationGC_ProtectedPathsSnapshotDoesNotDeleteNewerSegments(t *testing.T) {
	dir := t.TempDir()

	backend, err := backenddb.Open(backenddb.Options{Dir: dir})
	if err != nil {
		t.Fatalf("backend open: %v", err)
	}

	db, err := Open(dir, backend, Options{
		FlushThreshold:           256 << 20,
		DisableWAL:               true,
		RelaxedSync:              true,
		AllowUnsafe:              true,
		MemtableShards:           1,
		JournalLanes:             1,
		ValueLogPointerThreshold: 1,
		ValueLogMaxSegmentBytes:  4 << 10,
		ValueLogGenerationPolicy: uint8(backenddb.ValueLogGenerationOff),
	})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	// Establish a stable backend boundary and create the initial (protected)
	// value-log segment.
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	protected := append(db.valueLogRetainedPaths(), db.currentValueLogPaths()...)
	if len(protected) == 0 {
		t.Fatalf("expected non-empty protected paths snapshot")
	}

	// Rotate after taking the snapshot so subsequent writes land in a segment
	// that is not present in the protected-path list.
	if err := db.rotateValueLogLocked(&db.lanes[0]); err != nil {
		t.Fatalf("rotateValueLogLocked(before write): %v", err)
	}

	val1 := bytes.Repeat([]byte("a"), 8<<10)
	if err := db.Set([]byte("k1"), val1); err != nil {
		t.Fatalf("Set k1: %v", err)
	}

	k1Path := ""
	func() {
		l := &db.lanes[0]
		l.vlogMu.Lock()
		k1Path = l.vlogPath
		l.vlogMu.Unlock()
	}()
	if k1Path == "" {
		t.Fatalf("expected non-empty value-log path for k1")
	}

	// Force rotate again so k1 points at a non-active segment. The GC must not
	// delete it, even though the protected-path snapshot pre-dates the segment.
	if err := db.rotateValueLogLocked(&db.lanes[0]); err != nil {
		t.Fatalf("rotateValueLogLocked(after write): %v", err)
	}

	gcer, ok := db.backend.(backendValueLogGCer)
	if !ok {
		t.Fatalf("backend does not implement ValueLogGC")
	}
	if _, err := gcer.ValueLogGC(context.Background(), backenddb.ValueLogGCOptions{ProtectedPaths: protected}); err != nil {
		t.Fatalf("ValueLogGC: %v", err)
	}

	if _, err := os.Stat(k1Path); err != nil {
		t.Fatalf("expected k1 segment to remain after GC; stat: %v", err)
	}

	got, err := db.Get([]byte("k1"))
	if err != nil {
		t.Fatalf("Get k1 after GC: %v", err)
	}
	if !bytes.Equal(got, val1) {
		t.Fatalf("value mismatch after GC: got=%dB want=%dB", len(got), len(val1))
	}
}
