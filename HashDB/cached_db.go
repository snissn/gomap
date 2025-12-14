package hashdb

import (
	"fmt"
	"time"
)

// CachedHashmap wraps a single Hashmap with a write-back cache.
// WARNING: no WAL; cached writes are volatile until flushed.
type CachedHashmap struct {
	h     *Hashmap
	cache *CacheKV
}

// NewCachedHashmap initializes a new cached Hashmap at the given folder.
// maxEntries/maxBytes control flush thresholds; flushInterval <=0 disables ticker flush.
func NewCachedHashmap(folder string, maxEntries, maxBytes int, flushInterval time.Duration) (*CachedHashmap, error) {
	h := &Hashmap{}
	if err := h.New(folder); err != nil {
		return nil, err
	}
	kv := &hashmapKVAdapter{h: h}
	cache := NewCacheKV(kv, maxEntries, maxBytes, flushInterval)
	return &CachedHashmap{h: h, cache: cache}, nil
}

func (c *CachedHashmap) Get(key []byte) ([]byte, error)     { return c.cache.Get(key) }
func (c *CachedHashmap) Add(key []byte, value []byte) error { return c.cache.Put(key, value) }
func (c *CachedHashmap) Delete(key []byte) error            { return c.cache.Delete(key) }
func (c *CachedHashmap) Flush() error                       { return c.cache.Flush() }
func (c *CachedHashmap) Close() error                       { return c.cache.Close() }

func (c *CachedHashmap) SetCompression(enabled bool) {
	c.h.SetCompression(enabled)
}

func (c *CachedHashmap) Update(key []byte, callback func([]byte) ([]byte, error)) error {
	// For simplicity, bypass cache for read-modify-write to avoid stale reads.
	return c.h.Update(key, callback)
}

func (c *CachedHashmap) Clear() error {
	if err := c.cache.Flush(); err != nil {
		return err
	}
	return c.h.Clear()
}

func (c *CachedHashmap) Compact() error {
	if err := c.cache.Flush(); err != nil {
		return err
	}
	return c.h.Compact()
}

func (c *CachedHashmap) AddMany(items []Item) error {
	// Route batched writes through the write-back cache so they are
	// coalesced with other pending writes. CacheKV will flush to the
	// underlying Hashmap using AddMany when thresholds or timers fire.
	for _, it := range items {
		if err := c.cache.Put(it.Key, it.Value); err != nil {
			return err
		}
	}
	return nil
}

func (c *CachedHashmap) Stats() Stats {
	return c.h.Stats()
}

// hashmapKVAdapter adapts Hashmap to KVStore.
type hashmapKVAdapter struct {
	h *Hashmap
}

func (m *hashmapKVAdapter) Get(key []byte) ([]byte, error) { return m.h.Get(key) }
func (m *hashmapKVAdapter) Put(key, value []byte) error    { return m.h.Add(key, value) }
func (m *hashmapKVAdapter) Delete(key []byte) error        { return m.h.Delete(key) }

// PutMany allows CacheKV to batch writes down to the underlying Hashmap using AddMany.
func (m *hashmapKVAdapter) PutMany(keys [][]byte, vals [][]byte) error {
	if len(keys) != len(vals) {
		return fmt.Errorf("PutMany: keys/vals length mismatch")
	}
	items := make([]Item, len(keys))
	for i := range keys {
		items[i] = Item{Key: keys[i], Value: vals[i]}
	}
	return m.h.AddMany(items)
}
