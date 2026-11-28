package treedb

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/dictdb"
)

// Regression test: dictionaries live inside maindb's SystemRootPageID with
// pager-resident system leaves, even when IndexOuterLeavesInValueLog=true, and
// no separate dictdb/ side-store directory is created.
func TestDictDB_InMainDBSystemRootWithOuterLeavesInValueLog(t *testing.T) {
	dir := t.TempDir()

	opts := OptionsFor(ProfileFast, dir)
	EnableValueLogDictCompression(&opts)
	opts.ValueLog.Compression = ValueLogCompressionDict

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if _, err := os.Stat(filepath.Join(dir, "dictdb")); !os.IsNotExist(err) {
		t.Fatalf("expected no dictdb directory, stat err=%v", err)
	}

	store := dictdb.New(db.backend)
	want := bytes.Repeat([]byte("system-root-dict-payload|"), 200)
	id, err := store.PutDictBytes(context.Background(), want)
	if err != nil {
		t.Fatalf("PutDictBytes: %v", err)
	}
	got, err := store.GetDictBytes(context.Background(), id)
	if err != nil {
		t.Fatalf("GetDictBytes: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("GetDictBytes mismatch")
	}
}
