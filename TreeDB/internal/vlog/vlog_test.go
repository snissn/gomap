package vlog

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
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
