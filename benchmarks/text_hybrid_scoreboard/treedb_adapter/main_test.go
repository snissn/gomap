package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestStorageBytesClassifiesWALDirectory(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "wal"), 0o755); err != nil {
		t.Fatal(err)
	}
	files := map[string]string{
		"data.slab":           "data",
		"wal/commit-l0-1.log": "wal",
		"writer.lock":         "lock",
	}
	for name, content := range files {
		path := filepath.Join(root, filepath.FromSlash(name))
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	durable, wal, transient := storageBytes(root)
	if durable != int64(len("data")) || wal != int64(len("wal")) || transient != int64(len("lock")) {
		t.Fatalf("storageBytes() = durable %d, WAL %d, transient %d", durable, wal, transient)
	}
}
