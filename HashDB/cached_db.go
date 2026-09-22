package hashdb

import (
	"fmt"
	"path/filepath"
	"sync"
	"time"
)

// CachedDB wraps a single DB with a write-back cache.
// By default there is no WAL; cached writes are volatile until flushed.
// A WAL can be enabled via NewCachedDBWithOptions.
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
	return NewCachedDBWithOptions(folder, maxEntries, maxBytes, flushInterval, CachedDBOptions{})
}

// CachedDBOptions configures CachedDB behavior.
type CachedDBOptions struct {
	CacheWAL CacheWALOptions

	// IndexMemoryPolicy applies to the underlying on-disk DB's mmap index.
	// This must be set before opening the DB.
	IndexMemoryPolicy    IndexMemoryPolicy
	IndexMemoryPolicySet bool
}

// NewCachedDBWithOptions opens a cached DB with explicit caching and WAL options.
func NewCachedDBWithOptions(folder string, maxEntries, maxBytes int, flushInterval time.Duration, opts CachedDBOptions) (*CachedDB, error) {
	db := &DB{}
	if opts.IndexMemoryPolicySet {
		db.SetIndexMemoryPolicy(opts.IndexMemoryPolicy)
	}
	if err := db.New(folder); err != nil {
		return nil, err
	}
	c := &CachedDB{db: db}
	kv := &dbKVAdapter{db: db, mu: &c.backendMu}

	if opts.CacheWAL.FsyncPolicy == CacheWALDisabled {
		c.cache = NewCacheKV(kv, maxEntries, maxBytes, flushInterval)
		return c, nil
	}

	cache, err := NewCacheKVWithWAL(kv, maxEntries, maxBytes, flushInterval, filepath.Join(folder, "cache.wal"), opts.CacheWAL)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	c.cache = cache
	return c, nil
}

// NewCachedHashmap initializes a new cached DB at the given folder (compatibility wrapper).
func NewCachedHashmap(folder string, maxEntries, maxBytes int, flushInterval time.Duration) (*CachedHashmap, error) {
	return NewCachedDB(folder, maxEntries, maxBytes, flushInterval)
}

// Get returns the value for a key, consulting the write-back cache first.
func (c *CachedDB) Get(key []byte) ([]byte, error) { return c.cache.Get(key) }

// Put inserts or updates a key in the write-back cache.
func (c *CachedDB) Put(key []byte, value []byte) error { return c.cache.Put(key, value) }

// PutNoCopyValue inserts or updates a key in the write-back cache without copying the value.
// Caller must not mutate value after calling (it may be retained until flushed).
func (c *CachedDB) PutNoCopyValue(key []byte, value []byte) error {
	return c.cache.PutNoCopyValue(key, value)
}

// PutNoCopyKeyValueUnsafe inserts or updates a key in the write-back cache without copying the key or value.
// Caller must not mutate key or value after calling (they may be retained until flushed).
func (c *CachedDB) PutNoCopyKeyValueUnsafe(key []byte, value []byte) error {
	return c.cache.PutNoCopyKeyValueUnsafe(key, value)
}

// PutNoCopy inserts or updates a key in the write-back cache without copying the value.
//
// Deprecated: use PutNoCopyValue.
func (c *CachedDB) PutNoCopy(key []byte, value []byte) error { return c.PutNoCopyValue(key, value) }

// PutNoCopyUnsafe inserts or updates a key in the write-back cache without copying the key or value.
//
// Deprecated: use PutNoCopyKeyValueUnsafe.
func (c *CachedDB) PutNoCopyUnsafe(key []byte, value []byte) error {
	return c.PutNoCopyKeyValueUnsafe(key, value)
}

// Add is a compatibility alias for Put.
func (c *CachedDB) Add(key []byte, value []byte) error { return c.Put(key, value) }

// Delete removes a key from the write-back cache.
func (c *CachedDB) Delete(key []byte) error { return c.cache.Delete(key) }

// Flush flushes cached writes to the backend without forcing an fsync.
func (c *CachedDB) Flush() error { return c.cache.Flush() }

// Sync flushes cached writes and fsyncs the backend for durability.
func (c *CachedDB) Sync() error {
	if c == nil || c.cache == nil || c.db == nil {
		return nil
	}
	if err := c.cache.SyncWAL(); err != nil {
		return err
	}
	if err := c.cache.Flush(); err != nil {
		return err
	}
	c.backendMu.Lock()
	defer c.backendMu.Unlock()
	return c.db.Sync()
}

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
	if err := c.cache.SyncWAL(); err != nil {
		return err
	}
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
	if err := c.cache.SyncWAL(); err != nil {
		return err
	}
	if err := c.cache.Flush(); err != nil {
		return err
	}
	c.backendMu.Lock()
	defer c.backendMu.Unlock()
	return c.db.PutSync(key, value)
}

// DeleteSync flushes the write-back cache and then performs a durable delete on the backend.
func (c *CachedDB) DeleteSync(key []byte) error {
	if err := c.cache.SyncWAL(); err != nil {
		return err
	}
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

func (c *CachedDB) getManyWithHashes(keys [][]byte, hashes []Hash) ([][]byte, []error) {
	return c.cache.getManyWithHashes(keys, hashes)
}

// Close flushes cached writes and closes the underlying backend.
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

// SetCompression enables or disables backend compression for values.
func (c *CachedDB) SetCompression(enabled bool) {
	c.backendMu.Lock()
	defer c.backendMu.Unlock()
	c.db.SetCompression(enabled)
}

// SetMaxProbeGroupsBeforeResize sets a probe-length guard on the backend DB.
func (c *CachedDB) SetMaxProbeGroupsBeforeResize(groups uint64) {
	c.backendMu.Lock()
	defer c.backendMu.Unlock()
	c.db.SetMaxProbeGroupsBeforeResize(groups)
}

// Update performs a read-modify-write against the backend with cache flush.
func (c *CachedDB) Update(key []byte, callback func([]byte) ([]byte, error)) error {
	// For simplicity, bypass cache for read-modify-write to avoid stale reads.
	if err := c.cache.Flush(); err != nil {
		return err
	}
	c.backendMu.Lock()
	defer c.backendMu.Unlock()
	return c.db.Update(key, callback)
}

// Clear removes all keys from the backend after flushing the cache.
func (c *CachedDB) Clear() error {
	if err := c.cache.Flush(); err != nil {
		return err
	}
	c.backendMu.Lock()
	defer c.backendMu.Unlock()
	return c.db.Clear()
}

// Compact triggers a backend compaction after flushing the cache.
func (c *CachedDB) Compact() error {
	if err := c.cache.Flush(); err != nil {
		return err
	}
	c.backendMu.Lock()
	defer c.backendMu.Unlock()
	return c.db.Compact()
}

// PutMany inserts a batch of items into the write-back cache.
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

// AddMany is a compatibility alias for PutMany.
func (c *CachedDB) AddMany(items []Item) error {
	return c.PutMany(items)
}

// Stats returns backend stats with cache synchronization.
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

// Get returns the value for a key with serialized access to the underlying DB.
func (m *dbKVAdapter) Get(key []byte) ([]byte, error) {
	if m.mu != nil {
		m.mu.RLock()
		defer m.mu.RUnlock()
	}
	return m.db.Get(key)
}

// Put inserts or updates a key in the underlying DB with serialized access.
func (m *dbKVAdapter) Put(key, value []byte) error {
	if m.mu != nil {
		m.mu.Lock()
		defer m.mu.Unlock()
	}
	return m.db.Put(key, value)
}

// Delete removes a key from the underlying DB with serialized access.
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

func (m *dbKVAdapter) getManyWithHashes(keys [][]byte, hashes []Hash) ([][]byte, []error) {
	if m.mu != nil {
		m.mu.RLock()
		defer m.mu.RUnlock()
	}
	return m.db.getManyWithHashes(keys, hashes)
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
