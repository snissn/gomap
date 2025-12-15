package hashdb

import (
	"fmt"
	"sync"
	"time"
)

// CachedDB wraps a single DB with a write-back cache.
// WARNING: no WAL; cached writes are volatile until flushed.
type CachedDB struct {
	db    *DB
	cache *CacheKV

	// backendMu serializes access to db from the cache flush loop and any direct db calls.
	backendMu sync.RWMutex
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
	c := &CachedDB{db: db}
	kv := &dbKVAdapter{db: db, mu: &c.backendMu}
	c.cache = NewCacheKV(kv, maxEntries, maxBytes, flushInterval)
	return c, nil
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

// ApplyBatch applies a set of operations at the cache layer. Pending cache writes are
// volatile until flushed; use ApplyBatchSync for a durable commit.
func (c *CachedDB) ApplyBatch(ops []BatchOp) error {
	for _, op := range ops {
		switch op.Type {
		case BatchOpPut:
			if err := c.cache.Put(op.Key, op.Value); err != nil {
				return err
			}
		case BatchOpDelete:
			if err := c.cache.Delete(op.Key); err != nil {
				return err
			}
		default:
			return fmt.Errorf("apply batch: unknown op type %d", op.Type)
		}
	}
	return nil
}

// ApplyBatchSync flushes the write-back cache and then performs a durable batch commit
// on the backend DB.
func (c *CachedDB) ApplyBatchSync(ops []BatchOp) error {
	if err := c.cache.Flush(); err != nil {
		return err
	}
	c.backendMu.Lock()
	defer c.backendMu.Unlock()
	return c.db.ApplyBatchSync(ops)
}

// PutSync flushes the write-back cache and then performs a durable write to the backend.
// Without a WAL, the cache itself is volatile; PutSync is the supported durability path.
func (c *CachedDB) PutSync(key []byte, value []byte) error {
	if err := c.cache.Flush(); err != nil {
		return err
	}
	c.backendMu.Lock()
	defer c.backendMu.Unlock()
	return c.db.PutSync(key, value)
}

// DeleteSync flushes the write-back cache and then performs a durable delete on the backend.
func (c *CachedDB) DeleteSync(key []byte) error {
	if err := c.cache.Flush(); err != nil {
		return err
	}
	c.backendMu.Lock()
	defer c.backendMu.Unlock()
	return c.db.DeleteSync(key)
}

func (c *CachedDB) getWithHash(key []byte, keyHash Hash) ([]byte, error) {
	return c.cache.getWithHash(key, keyHash)
}
func (c *CachedDB) Close() error {
	var firstErr error
	if c.cache != nil {
		firstErr = c.cache.Close()
	}
	if c.db != nil {
		c.backendMu.Lock()
		err := c.db.Close()
		c.backendMu.Unlock()
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (c *CachedDB) SetCompression(enabled bool) {
	c.backendMu.Lock()
	defer c.backendMu.Unlock()
	c.db.SetCompression(enabled)
}

func (c *CachedDB) Update(key []byte, callback func([]byte) ([]byte, error)) error {
	// For simplicity, bypass cache for read-modify-write to avoid stale reads.
	if err := c.cache.Flush(); err != nil {
		return err
	}
	c.backendMu.Lock()
	defer c.backendMu.Unlock()
	return c.db.Update(key, callback)
}

func (c *CachedDB) Clear() error {
	if err := c.cache.Flush(); err != nil {
		return err
	}
	c.backendMu.Lock()
	defer c.backendMu.Unlock()
	return c.db.Clear()
}

func (c *CachedDB) Compact() error {
	if err := c.cache.Flush(); err != nil {
		return err
	}
	c.backendMu.Lock()
	defer c.backendMu.Unlock()
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
	c.backendMu.RLock()
	defer c.backendMu.RUnlock()
	return c.db.Stats()
}

// dbKVAdapter adapts DB to KVStore.
type dbKVAdapter struct {
	db *DB
	mu *sync.RWMutex
}

func (m *dbKVAdapter) Get(key []byte) ([]byte, error) {
	if m.mu != nil {
		m.mu.RLock()
		defer m.mu.RUnlock()
	}
	return m.db.Get(key)
}
func (m *dbKVAdapter) Put(key, value []byte) error {
	if m.mu != nil {
		m.mu.Lock()
		defer m.mu.Unlock()
	}
	return m.db.Put(key, value)
}
func (m *dbKVAdapter) Delete(key []byte) error {
	if m.mu != nil {
		m.mu.Lock()
		defer m.mu.Unlock()
	}
	return m.db.Delete(key)
}

func (m *dbKVAdapter) getWithHash(key []byte, keyHash Hash) ([]byte, error) {
	if m.mu != nil {
		m.mu.RLock()
		defer m.mu.RUnlock()
	}
	return m.db.getWithHash(key, keyHash)
}

// PutMany allows CacheKV to batch writes down to the underlying DB using PutMany.
func (m *dbKVAdapter) PutMany(keys [][]byte, vals [][]byte) error {
	if len(keys) != len(vals) {
		return fmt.Errorf("PutMany: keys/vals length mismatch")
	}
	items := make([]Item, len(keys))
	for i := range keys {
		items[i] = Item{Key: keys[i], Value: vals[i]}
	}
	if m.mu != nil {
		m.mu.Lock()
		defer m.mu.Unlock()
	}
	return m.db.PutMany(items)
}
