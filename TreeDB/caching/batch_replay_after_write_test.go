package caching

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
)

func TestBatch_ReplayAfterWrite(t *testing.T) {
	dir := t.TempDir()
	backend := NewMockBackend()

	db, err := Open(dir, backend, Options{DisableWAL: true, AllowUnsafe: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	b := db.NewBatch()
	key := []byte("k")
	val1 := []byte("v1")
	val2 := []byte("v2")

	if err := b.Set(key, val1); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("Write: %v", err)
	}

	var got []batch.Entry
	if err := b.Replay(func(e batch.Entry) error {
		got = append(got, e)
		return nil
	}); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("Replay count: got %d, want 1", len(got))
	}
	if got[0].Type != batch.OpPut {
		t.Fatalf("Replay[0].Type: got %v, want %v", got[0].Type, batch.OpPut)
	}
	if !bytes.Equal(got[0].Key, key) {
		t.Fatalf("Replay[0].Key: got %q, want %q", got[0].Key, key)
	}
	if !bytes.Equal(got[0].Value, val1) {
		t.Fatalf("Replay[0].Value: got %q, want %q", got[0].Value, val1)
	}

	if n, err := b.GetByteSize(); err != nil || n != 0 {
		t.Fatalf("GetByteSize after Write: got (%d, %v), want (0, nil)", n, err)
	}

	// Allow reuse without explicit Reset: the first mutation after a successful
	// Write should implicitly clear prior ops.
	if err := b.Set(key, val2); err != nil {
		t.Fatalf("Set after Write: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("Write 2: %v", err)
	}

	got = got[:0]
	if err := b.Replay(func(e batch.Entry) error {
		got = append(got, e)
		return nil
	}); err != nil {
		t.Fatalf("Replay 2: %v", err)
	}
	if len(got) != 1 || got[0].Type != batch.OpPut || !bytes.Equal(got[0].Value, val2) {
		t.Fatalf("Replay 2: got %#v, want single put of %q", got, val2)
	}

	b.Reset()
	got = got[:0]
	if err := b.Replay(func(e batch.Entry) error {
		got = append(got, e)
		return nil
	}); err != nil {
		t.Fatalf("Replay after Reset: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("Replay after Reset count: got %d, want 0", len(got))
	}
}
