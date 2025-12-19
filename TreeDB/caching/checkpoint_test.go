package caching

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestCachingDB_Checkpoint_TrimsWAL(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{FlushThreshold: 1})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Write enough data that WAL rotation will produce multiple segments (WAL reuse
	// is disabled once a segment exceeds ~10MiB).
	val := bytes.Repeat([]byte("v"), 1<<20) // 1MiB
	for i := 0; i < 20; i++ {
		k := []byte(fmt.Sprintf("k%03d", i))
		if err := db.Set(k, val); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}

	walDir := filepath.Join(dir, "wal")
	before, err := os.ReadDir(walDir)
	if err != nil {
		t.Fatalf("ReadDir(wal): %v", err)
	}

	walFilesBefore := 0
	for _, ent := range before {
		if ent.IsDir() {
			continue
		}
		walFilesBefore++
	}
	if walFilesBefore < 2 {
		t.Fatalf("expected multiple WAL segments before checkpoint, got %d", walFilesBefore)
	}

	if err := db.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}

	after, err := os.ReadDir(walDir)
	if err != nil {
		t.Fatalf("ReadDir(wal) after: %v", err)
	}

	walFilesAfter := 0
	for _, ent := range after {
		if ent.IsDir() {
			continue
		}
		walFilesAfter++
	}
	if walFilesAfter != 1 {
		t.Fatalf("expected exactly 1 WAL segment after checkpoint, got %d", walFilesAfter)
	}
}
