package vlog

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/snissn/gomap/TreeDB/page"
)

func TestWriterReaderRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "vlog-000001.log")
	fileID := uint32(0x80000001)

	w, err := NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	ptr, err := w.Append(OpSet, []byte("k1"), []byte("value1"))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	_, err = w.Append(OpDelete, []byte("k2"), nil)
	if err != nil {
		t.Fatalf("Append delete: %v", err)
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r, err := NewReader(path, fileID)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	op, key, val, gotPtr, err := r.ReadNext()
	if err != nil {
		t.Fatalf("ReadNext: %v", err)
	}
	if op != OpSet {
		t.Fatalf("op: got %d want %d", op, OpSet)
	}
	if string(key) != "k1" || string(val) != "value1" {
		t.Fatalf("record: got %q=%q", key, val)
	}
	if gotPtr != ptr {
		t.Fatalf("ptr mismatch: got %#v want %#v", gotPtr, ptr)
	}

	op, key, val, _, err = r.ReadNext()
	if err != nil {
		t.Fatalf("ReadNext delete: %v", err)
	}
	if op != OpDelete || string(key) != "k2" || len(val) != 0 {
		t.Fatalf("delete record mismatch: op=%d key=%q val=%q", op, key, val)
	}

	_, _, _, _, err = r.ReadNext()
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF, got %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("Close reader: %v", err)
	}
}

func TestReadAt(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "vlog-000001.log")
	fileID := uint32(0x80000001)

	w, err := NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	ptr, err := w.Append(OpSet, []byte("k1"), []byte("value1"))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f.Close()

	got, err := ReadAt(f, ptr, true)
	if err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if string(got) != "value1" {
		t.Fatalf("ReadAt value: got %q want %q", got, "value1")
	}
}

func TestFileReadUnsafeViaMmap(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("mmap not supported on windows")
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "vlog-000001.log")
	fileID := uint32(0x80000001)

	w, err := NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	ptr, err := w.Append(OpSet, []byte("k1"), []byte("value1"))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	f, err := openFile(path, fileID)
	if err != nil {
		t.Fatalf("openFile: %v", err)
	}
	defer f.Close()

	f.remapToFileSize()

	data, _ := f.mmapData.Load().([]byte)
	if data == nil {
		t.Fatalf("expected mmap data to be present")
	}

	got, err := f.ReadUnsafe(ptr, true)
	if err != nil {
		t.Fatalf("ReadUnsafe: %v", err)
	}
	if string(got) != "value1" {
		t.Fatalf("ReadUnsafe value: got %q want %q", got, "value1")
	}
}

func TestAppendBatchPointers(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "vlog-000001.log")
	fileID := uint32(0x80000001)

	w, err := NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	ptrs, err := w.AppendBatch([]Record{
		{Op: OpSet, Key: []byte("k1"), Value: []byte("value1")},
		{Op: OpDelete, Key: []byte("k2")},
		{Op: OpSet, Key: []byte("k3"), Value: []byte("value3")},
	})
	if err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if len(ptrs) != 3 {
		t.Fatalf("ptrs: got %d want %d", len(ptrs), 3)
	}
	if ptrs[0].FileID != fileID || ptrs[0].Offset == 0 {
		t.Fatalf("ptrs[0] unexpected: %#v", ptrs[0])
	}
	if ptrs[1] != (page.ValuePtr{}) {
		t.Fatalf("ptrs[1] expected zero, got %#v", ptrs[1])
	}
	if ptrs[2].FileID != fileID || ptrs[2].Offset == 0 {
		t.Fatalf("ptrs[2] unexpected: %#v", ptrs[2])
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f.Close()

	got, err := ReadAt(f, ptrs[0], true)
	if err != nil {
		t.Fatalf("ReadAt k1: %v", err)
	}
	if string(got) != "value1" {
		t.Fatalf("ReadAt k1: got %q want %q", got, "value1")
	}
	got, err = ReadAt(f, ptrs[2], true)
	if err != nil {
		t.Fatalf("ReadAt k3: %v", err)
	}
	if string(got) != "value3" {
		t.Fatalf("ReadAt k3: got %q want %q", got, "value3")
	}
}

func TestCorruptCRC(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "vlog-000001.log")
	fileID := uint32(0x80000001)

	w, err := NewWriter(path, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	_, err = w.Append(OpSet, []byte("k1"), []byte("value1"))
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if len(data) < HeaderSize+1 {
		t.Fatalf("short record")
	}
	data[HeaderSize] ^= 0xff
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	r, err := NewReader(path, fileID)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	_, _, _, _, err = r.ReadNext()
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("expected ErrCorrupt, got %v", err)
	}
	_ = r.Close()
}

func TestRotateToOpenFailureKeepsWriter(t *testing.T) {
	dir := t.TempDir()
	path1 := filepath.Join(dir, "vlog-000001.log")
	path2 := filepath.Join(dir, "missing", "vlog-000002.log")
	fileID := uint32(0x80000001)

	w, err := NewWriter(path1, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if _, err := w.Append(OpSet, []byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.RotateTo(path2, fileID+1); err == nil {
		t.Fatalf("expected RotateTo to fail for missing dir")
	}
	if _, err := w.Append(OpSet, []byte("k2"), []byte("v2")); err != nil {
		t.Fatalf("Append after failed RotateTo: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r, err := NewReader(path1, fileID)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer r.Close()

	op, key, val, _, err := r.ReadNext()
	if err != nil {
		t.Fatalf("ReadNext1: %v", err)
	}
	if op != OpSet || string(key) != "k1" || string(val) != "v1" {
		t.Fatalf("record1 mismatch: op=%d key=%q val=%q", op, key, val)
	}
	op, key, val, _, err = r.ReadNext()
	if err != nil {
		t.Fatalf("ReadNext2: %v", err)
	}
	if op != OpSet || string(key) != "k2" || string(val) != "v2" {
		t.Fatalf("record2 mismatch: op=%d key=%q val=%q", op, key, val)
	}
	if _, _, _, _, err = r.ReadNext(); !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF, got %v", err)
	}
}

func TestVlogNewWriterSyncDirFailure(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "vlog-000001.log")
	fileID := uint32(0x80000001)

	orig := syncDirFn
	t.Cleanup(func() { syncDirFn = orig })
	syncDirFn = func(string) error { return errors.New("syncdir fail") }

	if _, err := NewWriter(path, fileID); err == nil {
		t.Fatalf("expected NewWriter to fail when syncDir fails")
	}
}

func TestVlogRotateToSyncDirFailureKeepsWriter(t *testing.T) {
	dir := t.TempDir()
	path1 := filepath.Join(dir, "vlog-000001.log")
	path2 := filepath.Join(dir, "vlog-000002.log")
	fileID := uint32(0x80000001)

	orig := syncDirFn
	t.Cleanup(func() { syncDirFn = orig })
	syncDirFn = func(path string) error {
		if path == path2 {
			return errors.New("syncdir fail")
		}
		return nil
	}

	w, err := NewWriter(path1, fileID)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if _, err := w.Append(OpSet, []byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.RotateTo(path2, fileID+1); err == nil {
		t.Fatalf("expected RotateTo to fail when syncDir fails")
	}
	if _, err := w.Append(OpSet, []byte("k2"), []byte("v2")); err != nil {
		t.Fatalf("Append after failed RotateTo: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r, err := NewReader(path1, fileID)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer r.Close()

	op, key, val, _, err := r.ReadNext()
	if err != nil {
		t.Fatalf("ReadNext1: %v", err)
	}
	if op != OpSet || string(key) != "k1" || string(val) != "v1" {
		t.Fatalf("record1 mismatch: op=%d key=%q val=%q", op, key, val)
	}
	op, key, val, _, err = r.ReadNext()
	if err != nil {
		t.Fatalf("ReadNext2: %v", err)
	}
	if op != OpSet || string(key) != "k2" || string(val) != "v2" {
		t.Fatalf("record2 mismatch: op=%d key=%q val=%q", op, key, val)
	}
	if _, _, _, _, err = r.ReadNext(); err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
}
