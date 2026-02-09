package db

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
)

func TestInlineCommitSkipsValueLogRefresh(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	fileID, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	path := filepath.Join(dir, "wal", "value-l0-000001.log")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll(wal): %v", err)
	}
	w, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if _, err := w.Append(0, nil, 1, bytes.Repeat([]byte("x"), 128)); err != nil {
		_ = w.Close()
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	// Inline write should publish a new state without refreshing value-log files.
	if err := d.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Set inline: %v", err)
	}
	st := d.State()
	if st == nil || st.ValueLogSet == nil {
		t.Fatalf("state missing value-log set")
	}
	if _, ok := st.ValueLogSet.Files[fileID]; ok {
		t.Fatalf("inline commit unexpectedly refreshed value-log set with segment %d", fileID)
	}

	// Explicit refresh should discover the new segment.
	if err := d.RefreshValueLogSet(); err != nil {
		t.Fatalf("RefreshValueLogSet: %v", err)
	}
	st2 := d.State()
	if st2 == nil || st2.ValueLogSet == nil {
		t.Fatalf("state missing value-log set after refresh")
	}
	if _, ok := st2.ValueLogSet.Files[fileID]; !ok {
		t.Fatalf("RefreshValueLogSet did not discover segment %d", fileID)
	}
}

func TestPointerCommitRefreshesValueLogSet(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	fileID, err := valuelog.EncodeFileID(0, 1)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	path := filepath.Join(dir, "wal", "value-l0-000001.log")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll(wal): %v", err)
	}
	w, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	value := bytes.Repeat([]byte("p"), 128)
	ptr, err := w.Append(0, nil, 1, value)
	if err != nil {
		_ = w.Close()
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	b := d.NewBatch().(*Batch)
	defer func() { _ = b.Close() }()
	if err := b.SetPointer([]byte("kp"), ptr); err != nil {
		t.Fatalf("SetPointer: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("Write: %v", err)
	}

	st := d.State()
	if st == nil || st.ValueLogSet == nil {
		t.Fatalf("state missing value-log set")
	}
	if _, ok := st.ValueLogSet.Files[fileID]; !ok {
		t.Fatalf("pointer commit did not refresh value-log set with segment %d", fileID)
	}

	got, err := d.Get([]byte("kp"))
	if err != nil {
		t.Fatalf("Get pointer value: %v", err)
	}
	if !bytes.Equal(got, value) {
		t.Fatalf("Get pointer value mismatch: got %d bytes, want %d", len(got), len(value))
	}
}
