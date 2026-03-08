package caching

import (
	"os"
	"path/filepath"
	"testing"

	backenddb "github.com/snissn/gomap/TreeDB/db"
)

func TestCheckpoint_IgnoresMissingRetainedValueLogPath(t *testing.T) {
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
		ValueLogCompression:      1,
		ValueLogPointerThreshold: 1,
		ValueLogGenerationPolicy: uint8(backenddb.ValueLogGenerationOff),
	})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})

	stalePath := filepath.Join(dir, "value-l0-000001.log")
	if err := os.WriteFile(stalePath, []byte("stale"), 0o600); err != nil {
		t.Fatalf("seed stale path: %v", err)
	}
	if err := os.Remove(stalePath); err != nil {
		t.Fatalf("remove stale path: %v", err)
	}

	db.markValueLogRetain(stalePath)
	l := &db.lanes[0]
	l.vlogMu.Lock()
	l.vlogClosedSizes = map[string]int64{stalePath: 2 << 30}
	l.vlogClosedBytes.Store(2 << 30)
	l.vlogMu.Unlock()
	db.valueLogRetainedClosedBytes.Store(2 << 30)

	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	db.waitForRetainedValueLogPrune()
	if err := db.backgroundError(); err != nil {
		t.Fatalf("backgroundError: %v", err)
	}
	if db.valueLogRetained(stalePath) {
		t.Fatalf("expected stale retained path to be forgotten")
	}

	l.vlogMu.Lock()
	_, tracked := l.vlogClosedSizes[stalePath]
	closedBytes := l.vlogClosedBytes.Load()
	l.vlogMu.Unlock()
	if tracked {
		t.Fatalf("expected stale path to be untracked from closed sizes")
	}
	if got := db.valueLogRetainedClosedBytes.Load(); got != 0 {
		t.Fatalf("expected retained closed bytes to be cleared, got %d", got)
	}
	if closedBytes != 0 {
		t.Fatalf("expected lane closed bytes to be cleared, got %d", closedBytes)
	}
}
