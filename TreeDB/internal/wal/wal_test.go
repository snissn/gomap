package wal

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestWALRoundTrip(t *testing.T) {
	path := "test.wal"
	defer os.Remove(path)

	w, err := NewWriter(path)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	ops := []struct {
		seq uint64
		op  byte
		key []byte
		val []byte
	}{
		{1, OpSet, []byte("key1"), []byte("val1")},
		{2, OpDelete, []byte("key2"), nil},
		{3, OpSet, []byte("key3"), []byte("val3")},
	}

	for _, op := range ops {
		if err := w.Append(op.seq, op.op, op.key, op.val); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}

	r, err := NewReader(path)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer r.Close()

	for i, want := range ops {
		seq, op, k, v, err := r.ReadNext()
		if err != nil {
			t.Fatalf("ReadNext %d: %v", i, err)
		}
		if seq != want.seq {
			t.Errorf("seq %d: got %d, want %d", i, seq, want.seq)
		}
		if op != want.op {
			t.Errorf("op %d: got %d, want %d", i, op, want.op)
		}
		if !bytes.Equal(k, want.key) {
			t.Errorf("key %d: got %q, want %q", i, k, want.key)
		}
		if !bytes.Equal(v, want.val) {
			t.Errorf("val %d: got %q, want %q", i, v, want.val)
		}
	}

	_, _, _, _, err = r.ReadNext()
	if err != io.EOF {
		t.Errorf("expected EOF, got %v", err)
	}
}

func TestWALWriterRotateTo(t *testing.T) {
	dir := t.TempDir()
	path1 := dir + "/wal-000001.log"
	path2 := dir + "/wal-000002.log"

	w, err := NewWriter(path1)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	syncCalled := false
	w.syncFn = func(_ *os.File) error {
		syncCalled = true
		return nil
	}

	pendingCap := cap(w.pending)
	scratchCap := cap(w.scratch)

	if err := w.Append(1, OpSet, []byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.RotateTo(path2); err != nil {
		t.Fatalf("RotateTo: %v", err)
	}
	if !syncCalled {
		t.Fatalf("expected RotateTo to sync before closing")
	}
	if cap(w.pending) != pendingCap || cap(w.scratch) != scratchCap {
		t.Fatalf("expected buffers to be reused across RotateTo")
	}
	if err := w.Append(2, OpSet, []byte("k2"), []byte("v2")); err != nil {
		t.Fatalf("Append after RotateTo: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r1, err := NewReader(path1)
	if err != nil {
		t.Fatalf("NewReader1: %v", err)
	}
	defer r1.Close()
	_, op, k, v, err := r1.ReadNext()
	if err != nil {
		t.Fatalf("ReadNext1: %v", err)
	}
	if op != OpSet || !bytes.Equal(k, []byte("k1")) || !bytes.Equal(v, []byte("v1")) {
		t.Fatalf("unexpected record in wal1: op=%d key=%q val=%q", op, k, v)
	}
	_, _, _, _, err = r1.ReadNext()
	if err != io.EOF {
		t.Fatalf("expected EOF for wal1, got %v", err)
	}

	r2, err := NewReader(path2)
	if err != nil {
		t.Fatalf("NewReader2: %v", err)
	}
	defer r2.Close()
	_, op, k, v, err = r2.ReadNext()
	if err != nil {
		t.Fatalf("ReadNext2: %v", err)
	}
	if op != OpSet || !bytes.Equal(k, []byte("k2")) || !bytes.Equal(v, []byte("v2")) {
		t.Fatalf("unexpected record in wal2: op=%d key=%q val=%q", op, k, v)
	}
	_, _, _, _, err = r2.ReadNext()
	if err != io.EOF {
		t.Fatalf("expected EOF for wal2, got %v", err)
	}
}

func TestWALRotateToOpenFailureKeepsWriter(t *testing.T) {
	dir := t.TempDir()
	path1 := filepath.Join(dir, "wal-000001.log")
	path2 := filepath.Join(dir, "missing", "wal-000002.log")

	w, err := NewWriter(path1)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	if err := w.Append(1, OpSet, []byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.RotateTo(path2); err == nil {
		t.Fatalf("expected RotateTo to fail for missing dir")
	}
	if err := w.Append(2, OpSet, []byte("k2"), []byte("v2")); err != nil {
		t.Fatalf("Append after failed RotateTo: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r, err := NewReader(path1)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer r.Close()

	_, op, k, v, err := r.ReadNext()
	if err != nil {
		t.Fatalf("ReadNext1: %v", err)
	}
	if op != OpSet || !bytes.Equal(k, []byte("k1")) || !bytes.Equal(v, []byte("v1")) {
		t.Fatalf("record1 mismatch: op=%d key=%q val=%q", op, k, v)
	}
	_, op, k, v, err = r.ReadNext()
	if err != nil {
		t.Fatalf("ReadNext2: %v", err)
	}
	if op != OpSet || !bytes.Equal(k, []byte("k2")) || !bytes.Equal(v, []byte("v2")) {
		t.Fatalf("record2 mismatch: op=%d key=%q val=%q", op, k, v)
	}
	if _, _, _, _, err = r.ReadNext(); err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
}

func TestWALNewWriterSyncDirFailure(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal-000001.log")

	orig := syncDirFn
	t.Cleanup(func() { syncDirFn = orig })
	syncDirFn = func(string) error { return errors.New("syncdir fail") }

	if _, err := NewWriter(path); err == nil {
		t.Fatalf("expected NewWriter to fail when syncDir fails")
	}
}

func TestWALRotateToSyncDirFailureKeepsWriter(t *testing.T) {
	dir := t.TempDir()
	path1 := filepath.Join(dir, "wal-000001.log")
	path2 := filepath.Join(dir, "wal-000002.log")

	orig := syncDirFn
	t.Cleanup(func() { syncDirFn = orig })
	syncDirFn = func(path string) error {
		if path == path2 {
			return errors.New("syncdir fail")
		}
		return nil
	}

	w, err := NewWriter(path1)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	if err := w.Append(1, OpSet, []byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.RotateTo(path2); err == nil {
		t.Fatalf("expected RotateTo to fail when syncDir fails")
	}
	if err := w.Append(2, OpSet, []byte("k2"), []byte("v2")); err != nil {
		t.Fatalf("Append after failed RotateTo: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r, err := NewReader(path1)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer r.Close()

	_, op, k, v, err := r.ReadNext()
	if err != nil {
		t.Fatalf("ReadNext1: %v", err)
	}
	if op != OpSet || !bytes.Equal(k, []byte("k1")) || !bytes.Equal(v, []byte("v1")) {
		t.Fatalf("record1 mismatch: op=%d key=%q val=%q", op, k, v)
	}
	_, op, k, v, err = r.ReadNext()
	if err != nil {
		t.Fatalf("ReadNext2: %v", err)
	}
	if op != OpSet || !bytes.Equal(k, []byte("k2")) || !bytes.Equal(v, []byte("v2")) {
		t.Fatalf("record2 mismatch: op=%d key=%q val=%q", op, k, v)
	}
	if _, _, _, _, err = r.ReadNext(); err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
}

func TestWALMaxSegmentSize(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal-000001.log")

	w, err := NewWriterWithOptions(path, Options{MaxSegmentSize: 64})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer func() { _ = w.Close() }()

	err = w.Append(1, OpSet, bytes.Repeat([]byte("k"), 60), nil)
	if !errors.Is(err, ErrRecordTooLarge) {
		t.Fatalf("expected ErrRecordTooLarge, got %v", err)
	}
}
