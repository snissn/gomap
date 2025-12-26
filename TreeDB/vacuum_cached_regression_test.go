package treedb

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"
)

// Regression test for cached-mode vacuum: ensure that running online vacuum
// while using cached mode does not lose already-checkpointed keys.
func TestVacuumIndexOnline_CachedMode_DoesNotLoseCheckpointedKeys(t *testing.T) {
	dir := t.TempDir()

	opts := Options{
		Dir:  dir,
		Mode: ModeCached,

		DisableWAL:          true,
		RelaxedSync:         true,
		DisableReadChecksum: true,
		AllowUnsafe:         true,

		FlushThreshold: 1 << 20, // small-ish to force flush rotations during the test
		KeepRecent:     1,
		ChunkSize:      64 * 1024,
	}

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Seed a stable keyset and checkpoint it so those keys exist only in the backend.
	const keys = 4000
	val1 := bytes.Repeat([]byte("a"), 128)
	for i := 0; i < keys; i++ {
		k := []byte(fmt.Sprintf("k%06d", i))
		if err := db.Set(k, val1); err != nil {
			t.Fatalf("set: %v", err)
		}
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	// Concurrently churn unrelated keys while vacuum runs, to exercise the online
	// delta path.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		val2 := bytes.Repeat([]byte("b"), 128)
		for n := 0; ; n++ {
			select {
			case <-stop:
				return
			default:
			}
			k := []byte(fmt.Sprintf("x%06d", n%2000))
			_ = db.Set(k, val2)
			if n%100 == 0 {
				_ = db.Checkpoint()
			}
		}
	}()

	if err := db.VacuumIndexOnline(ctx); err != nil {
		close(stop)
		<-done
		t.Fatalf("vacuum: %v", err)
	}
	close(stop)
	<-done

	// Final checkpoint so the backend is fully materialized before verification.
	if err := db.Checkpoint(); err != nil {
		t.Fatalf("checkpoint2: %v", err)
	}

	// Verify the original stable keyset still exists and is intact.
	for i := 0; i < keys; i++ {
		k := []byte(fmt.Sprintf("k%06d", i))
		got, err := db.Get(k)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if !bytes.Equal(got, val1) {
			t.Fatalf("value mismatch for %q", k)
		}
	}
}
