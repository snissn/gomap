package gomap

import (
	"time"
)

// hashmapKV adapts HashmapDistributed to the KVStore interface expected by CacheKV.
type hashmapKV struct {
	h *HashmapDistributed
}

func (m *hashmapKV) Get(key []byte) ([]byte, error)   { return m.h.Get(key) }
func (m *hashmapKV) Put(key, value []byte) error      { return m.h.Add(key, value) }
func (m *hashmapKV) Delete(key []byte) error          { return m.h.Delete(key) }

// CachedHashmapDistributed wraps HashmapDistributed with a write-back cache.
// WARNING: no WAL; cached writes are volatile until flushed.
type CachedHashmapDistributed struct {
	cache *CacheKV
	h     *HashmapDistributed
}

// NewCachedHashmapDistributed creates a cached hashmap with the given parameters.
// flushInterval <=0 disables timer-based flush.
func NewCachedHashmapDistributed(folder string, shards int, maxEntries, maxBytes int, flushInterval time.Duration) (*CachedHashmapDistributed, error) {
	h := &HashmapDistributed{}
	if shards > 0 {
		if err := h.NewWithShards(folder, shards); err != nil {
			return nil, err
		}
	} else {
		if err := h.New(folder); err != nil {
			return nil, err
		}
	}
	kv := &hashmapKV{h: h}
	cache := NewCacheKV(kv, maxEntries, maxBytes, flushInterval)
	return &CachedHashmapDistributed{
		cache: cache,
		h:     h,
	}, nil
}

func (c *CachedHashmapDistributed) Get(key []byte) ([]byte, error) {
	return c.cache.Get(key)
}

func (c *CachedHashmapDistributed) Add(key []byte, value []byte) error {
	return c.cache.Put(key, value)
}

func (c *CachedHashmapDistributed) Delete(key []byte) error {
	return c.cache.Delete(key)
}

// Flush forces pending writes to disk.
func (c *CachedHashmapDistributed) Flush() error {
	return c.cache.Flush()
}

// Close flushes pending writes and stops the flush loop.
func (c *CachedHashmapDistributed) Close() error {
	return c.cache.Close()
}
