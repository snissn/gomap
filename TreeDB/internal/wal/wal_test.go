package wal

import (
	"bytes"
	"io"
	"os"
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
		op  byte
		key []byte
		val []byte
	}{
		{OpSet, []byte("key1"), []byte("val1")},
		{OpDelete, []byte("key2"), nil},
		{OpSet, []byte("key3"), []byte("val3")},
	}

	for _, op := range ops {
		if err := w.Append(op.op, op.key, op.val); err != nil {
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
		op, k, v, err := r.ReadNext()
		if err != nil {
			t.Fatalf("ReadNext %d: %v", i, err)
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

	_, _, _, err = r.ReadNext()
	if err != io.EOF {
		t.Errorf("expected EOF, got %v", err)
	}
}
