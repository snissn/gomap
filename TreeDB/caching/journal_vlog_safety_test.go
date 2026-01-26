package caching

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/page"
)

func TestCachingDB_JournalModeDoesNotDependOnValueLogFilesAfterCheckpoint(t *testing.T) {
	dir := t.TempDir()

	backend, err := db.Open(db.Options{Dir: dir, ChunkSize: 64 * 1024})
	if err != nil {
		t.Fatalf("backend open: %v", err)
	}

	cache, err := Open(dir, backend, Options{
		FlushThreshold:           1,
		ValueLogPointerThreshold: 1,
	})
	if err != nil {
		_ = backend.Close()
		t.Fatalf("cache open: %v", err)
	}

	key := []byte("k1")
	val := bytes.Repeat([]byte("v"), page.DefaultInlineThreshold+64)
	if err := cache.SetSync(key, val); err != nil {
		_ = cache.Close()
		t.Fatalf("SetSync: %v", err)
	}
	if err := cache.Checkpoint(); err != nil {
		_ = cache.Close()
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := cache.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	walDir := filepath.Join(dir, "wal")
	entries, err := os.ReadDir(walDir)
	if err != nil && !os.IsNotExist(err) {
		t.Fatalf("ReadDir(wal): %v", err)
	}
	for _, ent := range entries {
		if ent.IsDir() {
			continue
		}
		if !strings.HasSuffix(ent.Name(), ".log") {
			continue
		}
		if err := os.Remove(filepath.Join(walDir, ent.Name())); err != nil && !os.IsNotExist(err) {
			t.Fatalf("remove wal file %s: %v", ent.Name(), err)
		}
	}

	backend2, err := db.Open(db.Options{Dir: dir, ChunkSize: 64 * 1024})
	if err != nil {
		t.Fatalf("backend reopen: %v", err)
	}
	defer backend2.Close()

	got, err := backend2.Get(key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, val) {
		t.Fatalf("Get mismatch after deleting wal logs")
	}
}

