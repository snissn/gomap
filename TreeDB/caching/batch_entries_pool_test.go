package caching

import (
	"bytes"
	"testing"

	"github.com/snissn/gomap/TreeDB/batch"
)

func TestBatchWrite_ReplayAfterWrite_SetView(t *testing.T) {
	backend := NewMockBackend()
	db, err := Open(t.TempDir(), backend, Options{
		DisableWAL:      true,
		AllowUnsafe:     true,
		FlushThreshold:  1 << 20,
		DisableValueLog: true,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	b := db.NewBatch()
	key := []byte("k1")
	val := []byte("v1")
	if err := b.SetView(key, val); err != nil {
		t.Fatalf("SetView: %v", err)
	}
	if err := b.Write(); err != nil {
		t.Fatalf("Write: %v", err)
	}

	var replayed []batch.Entry
	if err := b.Replay(func(e batch.Entry) error {
		replayed = append(replayed, e)
		return nil
	}); err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(replayed) != 1 {
		t.Fatalf("Replay count: got %d, want 1", len(replayed))
	}
	if replayed[0].Type != batch.OpPut {
		t.Fatalf("Replay[0].Type: got %v, want %v", replayed[0].Type, batch.OpPut)
	}
	if !bytes.Equal(replayed[0].Key, key) {
		t.Fatalf("Replay[0].Key: got %q, want %q", replayed[0].Key, key)
	}
	if !bytes.Equal(replayed[0].Value, val) {
		t.Fatalf("Replay[0].Value: got %q, want %q", replayed[0].Value, val)
	}

	valGot, err := db.Get([]byte("k1"))
	if err != nil {
		t.Fatalf("Get k1: %v", err)
	}
	if string(valGot) != "v1" {
		t.Fatalf("unexpected k1 value: %q", valGot)
	}

	b.Reset()
	replayed = replayed[:0]
	if err := b.Replay(func(e batch.Entry) error {
		replayed = append(replayed, e)
		return nil
	}); err != nil {
		t.Fatalf("Replay after Reset: %v", err)
	}
	if len(replayed) != 0 {
		t.Fatalf("Replay after Reset count: got %d, want 0", len(replayed))
	}

	_ = b.Close()
}
