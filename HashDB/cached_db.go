package hashdb

import (
	"fmt"
	"time"
)

// CachedDB wraps a single DB with a write-back cache.
// WARNING: no WAL; cached writes are volatile until flushed.
type CachedDB struct {
	db    *DB
	cache *CacheKV
}

// CachedHashmap is kept as a compatibility alias for older code.
// New code should use CachedDB.
type CachedHashmap = CachedDB

// NewCachedDB initializes a new cached DB at the given folder.
// maxEntries/maxBytes control flush thresholds; flushInterval <=0 disables ticker flush.
func NewCachedDB(folder string, maxEntries, maxBytes int, flushInterval time.Duration) (*CachedDB, error) {
	db := &DB{}
	if err := db.New(folder); err != nil {
		return nil, err
	}
	kv := &dbKVAdapter{db: db}
	cache := NewCacheKV(kv, maxEntries, maxBytes, flushInterval)
	return &CachedDB{db: db, cache: cache}, nil
}

// NewCachedHashmap initializes a new cached DB at the given folder (compatibility wrapper).
func NewCachedHashmap(folder string, maxEntries, maxBytes int, flushInterval time.Duration) (*CachedHashmap, error) {
	return NewCachedDB(folder, maxEntries, maxBytes, flushInterval)
}

func (c *CachedDB) Get(key []byte) ([]byte, error)     { return c.cache.Get(key) }
func (c *CachedDB) Put(key []byte, value []byte) error { return c.cache.Put(key, value) }
func (c *CachedDB) Add(key []byte, value []byte) error { return c.Put(key, value) }
func (c *CachedDB) Delete(key []byte) error            { return c.cache.Delete(key) }
func (c *CachedDB) Flush() error                       { return c.cache.Flush() }
func (c *CachedDB) Close() error {
	var firstErr error
	if c.cache != nil {
		firstErr = c.cache.Close()
	}
	if c.db != nil {
		if err := c.db.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (c *CachedDB) SetCompression(enabled bool) {
	c.db.SetCompression(enabled)
}

func (c *CachedDB) Update(key []byte, callback func([]byte) ([]byte, error)) error {
	// For simplicity, bypass cache for read-modify-write to avoid stale reads.
	return c.db.Update(key, callback)
}

func (c *CachedDB) Clear() error {
	if err := c.cache.Flush(); err != nil {
		return err
	}
	return c.db.Clear()
}

func (c *CachedDB) Compact() error {
	if err := c.cache.Flush(); err != nil {
		return err
	}
	return c.db.Compact()
}

func (c *CachedDB) PutMany(items []Item) error {
	// Route batched writes through the write-back cache so they are
	// coalesced with other pending writes. CacheKV will flush to the
	// underlying DB using PutMany when thresholds or timers fire.
	for _, it := range items {
		if err := c.cache.Put(it.Key, it.Value); err != nil {
			return err
		}
	}
	return nil
}

func (c *CachedDB) AddMany(items []Item) error {
	return c.PutMany(items)
}

func (c *CachedDB) Stats() Stats {
	return c.db.Stats()
}

// dbKVAdapter adapts DB to KVStore.
type dbKVAdapter struct {
	db *DB
}

func (m *dbKVAdapter) Get(key []byte) ([]byte, error) { return m.db.Get(key) }
func (m *dbKVAdapter) Put(key, value []byte) error    { return m.db.Put(key, value) }
func (m *dbKVAdapter) Delete(key []byte) error        { return m.db.Delete(key) }

// PutMany allows CacheKV to batch writes down to the underlying DB using PutMany.
func (m *dbKVAdapter) PutMany(keys [][]byte, vals [][]byte) error {
	if len(keys) != len(vals) {
		return fmt.Errorf("PutMany: keys/vals length mismatch")
	}
	items := make([]Item, len(keys))
	for i := range keys {
		items[i] = Item{Key: keys[i], Value: vals[i]}
	}
	return m.db.PutMany(items)
}
