package hashdb

import (
	"encoding/binary"
	"os"
	"testing"
)

func TestBatchWriterFlush(t *testing.T) {
	dir, err := os.MkdirTemp("", "batcher-test-*")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	store := &HashDB{}
	if err := store.NewWithShards(dir, 2); err != nil {
		t.Fatalf("init store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	bw := NewBatchWriter(store, 4)

	for i := 0; i < 10; i++ {
		k := []byte{byte(i)}
		v := []byte{byte(i + 1)}
		if err := bw.Add(k, v); err != nil {
			t.Fatalf("add %d: %v", i, err)
		}
	}
	if err := bw.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	for i := 0; i < 10; i++ {
		k := []byte{byte(i)}
		v, err := store.Get(k)
		if err != nil {
			t.Fatalf("get %d: %v", i, err)
		}
		if len(v) != 1 || v[0] != byte(i+1) {
			t.Fatalf("value mismatch for %d: %v", i, v)
		}
	}
}

func TestBatchWriterReusedKeyBuffer(t *testing.T) {
	dir, err := os.MkdirTemp("", "batcher-test-*")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	store := &HashDB{}
	if err := store.NewWithShards(dir, 2); err != nil {
		t.Fatalf("init store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	bw := NewBatchWriter(store, 4)

	var k [8]byte
	for i := 0; i < 100; i++ {
		binary.BigEndian.PutUint64(k[:], uint64(i))
		if err := bw.Add(k[:], []byte{byte(i)}); err != nil {
			t.Fatalf("add %d: %v", i, err)
		}
	}
	if err := bw.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	for i := 0; i < 100; i++ {
		binary.BigEndian.PutUint64(k[:], uint64(i))
		v, err := store.Get(k[:])
		if err != nil {
			t.Fatalf("get %d: %v", i, err)
		}
		if len(v) != 1 || v[0] != byte(i) {
			t.Fatalf("value mismatch for %d: %v", i, v)
		}
	}
}
