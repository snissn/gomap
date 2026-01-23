package hashdb

import (
	"sync"
	"testing"
	"time"
)

// DummyKV implements KVStore for testing CacheKV.
type DummyKV struct {
	mu   sync.RWMutex
	data map[string][]byte
}

func (d *DummyKV) Get(key []byte) ([]byte, error) {
	k := string(key)
	d.mu.RLock()
	v, ok := d.data[k]
	d.mu.RUnlock()
	if ok {
		return append([]byte(nil), v...), nil
	}
	return nil, nil
}
func (d *DummyKV) Put(key, value []byte) error {
	k := string(key)
	valueCopy := append([]byte(nil), value...)
	d.mu.Lock()
	d.data[k] = valueCopy
	d.mu.Unlock()
	return nil
}
func (d *DummyKV) Delete(key []byte) error {
	k := string(key)
	d.mu.Lock()
	delete(d.data, k)
	d.mu.Unlock()
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
	// Flush to ensure all pending items are persisted.
	if err := c.Flush(); err != nil {
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

func TestCacheKVPutNoCopy(t *testing.T) {
	base := &DummyKV{data: make(map[string][]byte)}
	c := NewCacheKV(base, 100, 1<<20, 0)

	val := []byte("v1")
	if err := c.PutNoCopy([]byte("a"), val); err != nil {
		t.Fatal(err)
	}

	// Mutating val affects cached value because PutNoCopy avoids copying.
	val[0] = 'V'

	got, _ := c.Get([]byte("a"))
	if string(got) != "V1" {
		t.Fatalf("expected no-copy value, got %s", got)
	}

	if err := c.Flush(); err != nil {
		t.Fatal(err)
	}
	got, _ = base.Get([]byte("a"))
	if string(got) != "V1" {
		t.Fatalf("expected flushed no-copy value, got %s", got)
	}
}
