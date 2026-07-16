package db

import (
	"os"
	"path/filepath"
	"testing"
)

func TestOpen_DefaultChunkSize(t *testing.T) {
	dir := t.TempDir()

	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })

	if err := d.SetSync([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("SetSync: %v", err)
	}

	info, err := os.Stat(filepath.Join(dir, "index.db"))
	if err != nil {
		t.Fatalf("stat index.db: %v", err)
	}
	if got := info.Size(); got < defaultChunkSize || got%defaultChunkSize != 0 {
		t.Fatalf("index.db size=%d want a positive multiple of default chunk size %d", got, defaultChunkSize)
	}
}
