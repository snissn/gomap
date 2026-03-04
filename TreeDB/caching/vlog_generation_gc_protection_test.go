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
