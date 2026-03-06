package db

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/valuelog"
	"github.com/snissn/gomap/TreeDB/page"
)

func writeValueLogRecord(t *testing.T, dir string, lane, seq uint32, value []byte, rid uint64) (uint32, page.ValuePtr) {
	t.Helper()
	fileID, err := valuelog.EncodeFileID(lane, seq)
	if err != nil {
		t.Fatalf("EncodeFileID: %v", err)
	}
	path := filepath.Join(dir, "wal", fmt.Sprintf("value-l%d-%06d.log", lane, seq))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll(wal): %v", err)
	}
	w, err := valuelog.NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	ptr, err := w.Append(0, nil, rid, value)
	if err != nil {
		_ = w.Close()
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
	return fileID, ptr
}

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

func TestLeafPageCommitPublishesLeafRefSegment(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{
		Dir:                        dir,
		IndexOuterLeavesInValueLog: true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	if err := d.Set([]byte("k"), bytes.Repeat([]byte("v"), 256)); err != nil {
		t.Fatalf("Set: %v", err)
	}

	st := d.State()
	if st == nil || st.ValueLogSet == nil {
		t.Fatalf("state missing value-log set")
	}
	ptr, ok := page.DecodeLeafRef(st.RootPageID)
	if !ok {
		t.Fatalf("expected leaf-ref root page, got %d", st.RootPageID)
	}
	if _, ok := st.ValueLogSet.Files[ptr.FileID]; !ok {
		t.Fatalf("leaf-page commit missing segment %d from current value-log set", ptr.FileID)
	}
}

func TestCurrentSetRefresh_InlineThenPointerThenInline(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	value := bytes.Repeat([]byte("p"), 256)
	fileID, ptr := writeValueLogRecord(t, dir, 0, 1, value, 1)

	b := d.NewBatch().(*Batch)
	defer func() { _ = b.Close() }()
	if err := b.Set([]byte("k"), []byte("inline-1")); err != nil {
		t.Fatalf("Set inline1: %v", err)
	}
	if err := b.SetPointer([]byte("k"), ptr); err != nil {
		t.Fatalf("SetPointer: %v", err)
	}
	if err := b.Set([]byte("k"), []byte("inline-2")); err != nil {
		t.Fatalf("Set inline2: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("Write: %v", err)
	}

	st := d.State()
	if st == nil || st.ValueLogSet == nil {
		t.Fatalf("state missing value-log set")
	}
	if _, ok := st.ValueLogSet.Files[fileID]; !ok {
		t.Fatalf("touched segment %d was not published in CurrentSet", fileID)
	}

	got, err := d.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get inline final value: %v", err)
	}
	if !bytes.Equal(got, []byte("inline-2")) {
		t.Fatalf("Get mismatch: got %q want %q", got, "inline-2")
	}
}

func TestCurrentSetRefresh_DeleteOnlyBatch_NoFalseRefresh(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	value := bytes.Repeat([]byte("a"), 256)
	fileID1, ptr1 := writeValueLogRecord(t, dir, 0, 1, value, 1)

	b := d.NewBatch().(*Batch)
	if err := b.SetPointer([]byte("kp"), ptr1); err != nil {
		t.Fatalf("SetPointer: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("Write pointer batch: %v", err)
	}
	_ = b.Close()

	st := d.State()
	if st == nil || st.ValueLogSet == nil {
		t.Fatalf("state missing value-log set")
	}
	if _, ok := st.ValueLogSet.Files[fileID1]; !ok {
		t.Fatalf("expected segment %d to be published after pointer batch", fileID1)
	}

	fileID2, _ := writeValueLogRecord(t, dir, 0, 2, value, 2)

	del := d.NewBatch().(*Batch)
	defer func() { _ = del.Close() }()
	if err := del.Delete([]byte("kp")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := del.Write(); err != nil {
		t.Fatalf("Write delete-only batch: %v", err)
	}

	st2 := d.State()
	if st2 == nil || st2.ValueLogSet == nil {
		t.Fatalf("state missing value-log set after delete-only write")
	}
	if _, ok := st2.ValueLogSet.Files[fileID2]; ok {
		t.Fatalf("delete-only batch unexpectedly refreshed CurrentSet with untouched segment %d", fileID2)
	}
}
