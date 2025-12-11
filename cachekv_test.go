package gomap

import (
	"os"
	"testing"
	"time"
)

// DummyKV implements KVStore for testing CacheKV.
type DummyKV struct {
	data map[string][]byte
}

func (d *DummyKV) Get(key []byte) ([]byte, error) {
	if v, ok := d.data[string(key)]; ok {
		return v, nil
	}
	return nil, nil
}
func (d *DummyKV) Put(key, value []byte) error {
	d.data[string(key)] = append([]byte(nil), value...)
	return nil
}
func (d *DummyKV) Delete(key []byte) error {
	delete(d.data, string(key))
	return nil
}

func TestCacheKVFlush(t *testing.T) {
	base := &DummyKV{data: make(map[string][]byte)}
	c := NewCacheKV(base, 2, 1024, 0)

	if err := c.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	if v, _ := c.Get([]byte("a")); string(v) != "1" {
		t.Fatalf("expected cached value")
	}
	// Trigger flush by exceeding entries
	if err := c.Put([]byte("b"), []byte("2")); err != nil {
		t.Fatal(err)
	}
	if err := c.Put([]byte("c"), []byte("3")); err != nil {
		t.Fatal(err)
	}
	if v, _ := base.Get([]byte("a")); string(v) != "1" {
		t.Fatalf("expected flushed value 1, got %s", v)
	}
	if v, _ := base.Get([]byte("c")); string(v) != "3" {
		t.Fatalf("expected flushed value 3, got %s", v)
	}
	if err := c.Delete([]byte("a")); err != nil {
		t.Fatal(err)
	}
	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}
	if v, _ := base.Get([]byte("a")); v != nil {
		t.Fatalf("expected delete flushed, got %s", v)
	}
}

func TestCacheKVTicker(t *testing.T) {
	base := &DummyKV{data: make(map[string][]byte)}
	c := NewCacheKV(base, 100, 1<<20, 50*time.Millisecond)
	defer c.Close()

	if err := c.Put([]byte("x"), []byte("y")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	v, _ := base.Get([]byte("x"))
	if string(v) != "y" {
		t.Fatalf("expected ticker flush, got %s", v)
	}
}

// Ensure CacheKV works with HashmapDistributed.
func TestCacheKVWithGomap(t *testing.T) {
	dir, err := os.MkdirTemp("", "cachekv-gomap-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	h := &HashmapDistributed{}
	if err := h.NewWithShards(dir, 2); err != nil {
		t.Fatal(err)
	}
	// Simple adapter to KVStore.
	kv := &hashmapKV{h}
	c := NewCacheKV(kv, 4, 1024, 0)
	if err := c.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}
	v, _ := h.Get([]byte("k"))
	if string(v) != "v" {
		t.Fatalf("expected v, got %s", v)
	}
}

type hashmapKV struct {
	h *HashmapDistributed
}

func (m *hashmapKV) Get(key []byte) ([]byte, error)   { return m.h.Get(key) }
func (m *hashmapKV) Put(key, value []byte) error      { return m.h.Add(key, value) }
func (m *hashmapKV) Delete(key []byte) error          { return m.h.Delete(key) }
