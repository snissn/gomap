package caching

import (
	"bytes"
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

	// Force the first value-log segment to exceed max bytes so the second write
	// triggers a rotation. The first segment then becomes eligible for GC even
	// though it is still referenced by in-memory pointers.
	val1 := bytes.Repeat([]byte("a"), 8<<10)
	if err := db.Set([]byte("k1"), val1); err != nil {
		_ = db.Close()
		t.Fatalf("Set k1: %v", err)
	}
	if err := db.Set([]byte("k2"), []byte("b")); err != nil {
		_ = db.Close()
		t.Fatalf("Set k2: %v", err)
	}

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
		ValueLogCompression:      1, // off: ensure value-log segment rotation triggers deterministically
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
	if err := db.Set([]byte("k2"), []byte("b")); err != nil {
		_ = db.Close()
		t.Fatalf("Set k2: %v", err)
	}

	// The flush threshold keeps data in-memory; backend must not see it yet.
	if got, err := backend.Get([]byte("k1")); err != nil {
		_ = db.Close()
		t.Fatalf("backend Get k1: %v", err)
	} else if got != nil {
		_ = db.Close()
		t.Fatalf("expected backend to miss k1 before checkpoint; got=%dB", len(got))
	}

	// Simulate a bug/edge case where retained-path protection is non-empty but
	// missing a rotated segment that still backs in-memory pointers.
	rotatedPath := ""
	keepPath := ""
	for i := range db.lanes {
		l := &db.lanes[i]
		l.vlogMu.Lock()
		if len(l.vlogClosedSizes) > 0 && l.vlogPath != "" {
			for path := range l.vlogClosedSizes {
				rotatedPath = path
				break
			}
			keepPath = l.vlogPath
			l.vlogMu.Unlock()
			break
		}
		l.vlogMu.Unlock()
	}
	if rotatedPath == "" || keepPath == "" || rotatedPath == keepPath {
		_ = db.Close()
		t.Fatalf("expected a rotated + current vlog path; rotated=%q keep=%q", rotatedPath, keepPath)
	}
	db.valueLogMu.Lock()
	db.valueLogRetain = map[string]struct{}{keepPath: {}}
	db.valueLogMu.Unlock()

	db.maybeRunVlogGenerationMaintenance(true)

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
