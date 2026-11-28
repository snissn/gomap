package main

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"

	treedbdb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/dictdb"
)

func TestOpenDictLookupResolvesMainDBSystemRootByDefault(t *testing.T) {
	root := t.TempDir()
	mainDir := filepath.Join(root, "maindb")

	backend, err := treedbdb.Open(treedbdb.Options{
		Dir:       mainDir,
		ChunkSize: 64 << 10,
	})
	if err != nil {
		t.Fatalf("open maindb: %v", err)
	}
	want := bytes.Repeat([]byte("wal-classify-maindb-system-dict|"), 64)
	store := dictdb.New(backend)
	dictID, err := store.PutDictBytes(context.Background(), want)
	if err != nil {
		_ = backend.Close()
		t.Fatalf("PutDictBytes: %v", err)
	}
	if err := backend.Close(); err != nil {
		t.Fatalf("close maindb: %v", err)
	}

	lookup, closeLookup, err := openDictLookup(filepath.Join(mainDir, "value_vlog"), "")
	if err != nil {
		t.Fatalf("openDictLookup: %v", err)
	}
	if closeLookup != nil {
		defer func() { _ = closeLookup() }()
	}
	if lookup == nil {
		t.Fatal("expected non-nil dict lookup from maindb")
	}
	got, err := lookup(dictID)
	if err != nil {
		t.Fatalf("lookup(%d): %v", dictID, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("dict bytes mismatch: got=%d want=%d", len(got), len(want))
	}
}
