package gomap

import (
	"os"
	"testing"
	"time"
)

func TestCachedHashmapDistributed(t *testing.T) {
	dir, err := os.MkdirTemp("", "cached-hashmap-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	h, err := NewCachedHashmapDistributed(dir, 2, 8, 1<<20, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("init cached hashmap: %v", err)
	}
	defer h.Close()

	if err := h.Add([]byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("add: %v", err)
	}
	// Should serve from cache before flush.
	if v, _ := h.Get([]byte("k1")); string(v) != "v1" {
		t.Fatalf("expected cached v1, got %s", v)
	}
	if err := h.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	// After flush, backing store should have the value.
	if v, _ := h.h.Get([]byte("k1")); string(v) != "v1" {
		t.Fatalf("expected flushed v1, got %s", v)
	}
	if err := h.Delete([]byte("k1")); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := h.Flush(); err != nil {
		t.Fatalf("flush after delete: %v", err)
	}
	if v, _ := h.h.Get([]byte("k1")); v != nil {
		t.Fatalf("expected delete persisted, got %s", v)
	}
}
