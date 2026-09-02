package hashdb

import (
	"errors"
	"sync"
	"time"
)

// KVStore is the minimal interface implemented by backends used here.
type KVStore interface {
	Get(key []byte) ([]byte, error)
	Put(key, value []byte) error
	Delete(key []byte) error
}

type hashGetter interface {
	getWithHash(key []byte, keyHash Hash) ([]byte, error)
}

type hashManyGetter interface {
	getManyWithHashes(keys [][]byte, hashes []Hash) ([][]byte, []error)
}

type cachePutMode uint8

const (
	cachePutCopyKey cachePutMode = 1 << iota
	cachePutCopyValue
)

// cacheEntry stores a pending write or delete.
type cacheEntry struct {
	key   []byte
	value []byte
	del   bool
}

// CacheKV buffers writes and flushes them to the underlying store.
// WARNING: by default there is no WAL; pending writes are lost on crash before flush.
type CacheKV struct {
	backend KVStore

	mu         sync.RWMutex
	pending    map[string]cacheEntry
	flushing   map[string]cacheEntry // Items currently being flushed (visible to Get)
	pendingLen int

	flushMu sync.Mutex // Serializes Flush operations to backend

	walMu sync.Mutex
	wal   *cacheWAL

	maxEntries int
	maxBytes   int

	batchKeys [][]byte
	batchVals [][]byte

	flushInterval time.Duration
	stopCh        chan struct{}
	wg            sync.WaitGroup
}

// SyncWAL fsyncs the cache WAL (if enabled).
// This is intended to be used by higher-level *Sync operations so the WAL is durable
// even if a crash occurs during a backend flush.
func (c *CacheKV) SyncWAL() error {
	c.walMu.Lock()
	defer c.walMu.Unlock()
	if c.wal == nil {
		return nil
	}
	if c.wal.fsync == CacheWALFsyncOnSync {
		return c.wal.Sync()
	}
	return nil
}

// NewCacheKV wraps a KVStore with a write-back cache.
// flushInterval <=0 disables timer flushes.
func NewCacheKV(backend KVStore, maxEntries, maxBytes int, flushInterval time.Duration) *CacheKV {
	c, err := NewCacheKVWithWAL(backend, maxEntries, maxBytes, flushInterval, "", CacheWALOptions{FsyncPolicy: CacheWALDisabled})
	if err != nil {
		// WAL is disabled, so this should never happen. Keep the old signature.
		panic(err)
	}
	return c
}

// NewCacheKVWithWAL wraps a KVStore with a write-back cache and an optional WAL.
// When WAL is enabled, pending writes can be recovered after a crash (depending on fsync policy).
func NewCacheKVWithWAL(backend KVStore, maxEntries, maxBytes int, flushInterval time.Duration, walPath string, walOpts CacheWALOptions) (*CacheKV, error) {
	if maxEntries <= 0 {
		maxEntries = 1024
	}
	if maxBytes <= 0 {
		maxBytes = 1 << 20 // 1MB
	}
	c := &CacheKV{
		backend:       backend,
		maxEntries:    maxEntries,
		maxBytes:      maxBytes,
		flushInterval: flushInterval,
		stopCh:        make(chan struct{}),
	}

	wal, recovered, err := openCacheWAL(walPath, walOpts.FsyncPolicy)
	if err != nil {
		return nil, err
	}
	c.wal = wal
	if recovered != nil {
		c.pending = recovered
		for k, e := range recovered {
			keyLen := len(e.key)
			if keyLen == 0 {
				keyLen = len(k)
			}
			if e.del {
				c.pendingLen += keyLen
			} else {
				c.pendingLen += keyLen + len(e.value)
			}
		}
	} else {
		c.pending = make(map[string]cacheEntry)
	}

	if flushInterval > 0 {
		c.wg.Add(1)
		go c.flushLoop()
	}
	return c, nil
}

// Get returns the cached value for a key (or fetches it from the backend).
func (c *CacheKV) Get(key []byte) ([]byte, error) {
	k := bytesToString(key)
	c.mu.RLock()
	if e, ok := c.pending[k]; ok {
		c.mu.RUnlock()
		if e.del {
			return nil, nil
		}
		return e.value, nil
	}
	if e, ok := c.flushing[k]; ok {
		c.mu.RUnlock()
		if e.del {
			return nil, nil
		}
		return e.value, nil
	}
	c.mu.RUnlock()
	return c.backend.Get(key)
}

func (c *CacheKV) getWithHash(key []byte, keyHash Hash) ([]byte, error) {
	k := bytesToString(key)
	c.mu.RLock()
	if e, ok := c.pending[k]; ok {
		c.mu.RUnlock()
		if e.del {
			return nil, nil
		}
		return e.value, nil
	}
	if e, ok := c.flushing[k]; ok {
		c.mu.RUnlock()
		if e.del {
			return nil, nil
		}
		return e.value, nil
	}
	c.mu.RUnlock()

	if hg, ok := c.backend.(hashGetter); ok {
		return hg.getWithHash(key, keyHash)
	}
	return c.backend.Get(key)
}

func (c *CacheKV) getManyWithHashes(keys [][]byte, hashes []Hash) ([][]byte, []error) {
	values := make([][]byte, len(keys))
	errs := make([]error, len(keys))

	if len(keys) == 0 {
		return values, errs
	}

	useHashes := len(hashes) == len(keys)
	misses := make([]int, 0, len(keys))

	c.mu.RLock()
	pending := c.pending
	flushing := c.flushing
	for i, key := range keys {
		k := bytesToString(key)
		if e, ok := pending[k]; ok {
			if e.del {
				values[i] = nil
			} else {
				values[i] = e.value
			}
			continue
		}
		if e, ok := flushing[k]; ok {
			if e.del {
				values[i] = nil
			} else {
				values[i] = e.value
			}
			continue
		}
		misses = append(misses, i)
	}
	c.mu.RUnlock()

	if len(misses) == 0 {
		return values, errs
	}

	if hmg, ok := c.backend.(hashManyGetter); ok && useHashes {
		missKeys := make([][]byte, len(misses))
		missHashes := make([]Hash, len(misses))
		for j, i := range misses {
			missKeys[j] = keys[i]
			missHashes[j] = hashes[i]
		}

		missVals, missErrs := hmg.getManyWithHashes(missKeys, missHashes)
		for j, i := range misses {
			values[i] = missVals[j]
			errs[i] = missErrs[j]
		}
		return values, errs
	}

	if hg, ok := c.backend.(hashGetter); ok && useHashes {
		for _, i := range misses {
			val, err := hg.getWithHash(keys[i], hashes[i])
			if err != nil {
				errs[i] = err
				continue
			}
			values[i] = val
		}
		return values, errs
	}

	for _, i := range misses {
		val, err := c.backend.Get(keys[i])
		if err != nil {
			errs[i] = err
			continue
		}
		values[i] = val
	}

	return values, errs
}

func (c *CacheKV) put(key, value []byte, mode cachePutMode) error {
	if mode&cachePutCopyKey != 0 {
		key = append([]byte(nil), key...)
	} else {
		// Prevent accidental in-place growth when callers append() to borrowed slices.
		// This does not protect against mutating existing bytes.
		key = key[:len(key):len(key)]
	}

	if mode&cachePutCopyValue != 0 {
		value = append([]byte(nil), value...)
	} else {
		// Prevent accidental in-place growth when callers append() to values returned from Get().
		// This matches the cap=len behavior of Put() without forcing an extra allocation here.
		value = value[:len(value):len(value)]
	}

	k := bytesToString(key)

	c.walMu.Lock()
	if c.wal != nil {
		if err := c.wal.appendPut(key, value); err != nil {
			c.walMu.Unlock()
			return err
		}
	}
	c.mu.Lock()
	c.pending[k] = cacheEntry{key: key, value: value}
	c.pendingLen += len(key) + len(value)
	shouldFlush := len(c.pending) >= c.maxEntries || c.pendingLen >= c.maxBytes
	c.mu.Unlock()
	c.walMu.Unlock()
	if shouldFlush {
		return c.Flush()
	}
	return nil
}

// Put inserts or updates a key in the write-back cache.
//
// Put variants:
//   - Put: copies key and value (safe default).
//   - PutNoCopyValue: copies key, borrows value (value must remain immutable until flushed).
//   - PutNoCopyKeyValueUnsafe: borrows key and value (key/value must remain immutable and not be reused until flushed).
func (c *CacheKV) Put(key, value []byte) error {
	return c.put(key, value, cachePutCopyKey|cachePutCopyValue)
}

// PutNoCopyValue inserts or updates a key without copying the value.
// Caller must not mutate value after calling (it may be retained until flushed).
func (c *CacheKV) PutNoCopyValue(key, value []byte) error {
	return c.put(key, value, cachePutCopyKey)
}

// PutNoCopyKeyValueUnsafe inserts or updates a key without copying the key or value.
// Caller must not mutate key or value after calling (they may be retained until flushed).
//
// This is unsafe because the cache uses an unsafe bytes->string conversion for map keys.
// If the key bytes are modified or reused (e.g. from a pooled network buffer), it can
// corrupt the cache map.
func (c *CacheKV) PutNoCopyKeyValueUnsafe(key, value []byte) error {
	return c.put(key, value, 0)
}

// PutNoCopy inserts or updates a key without copying the value.
//
// Deprecated: use PutNoCopyValue.
func (c *CacheKV) PutNoCopy(key, value []byte) error {
	return c.PutNoCopyValue(key, value)
}

// PutNoCopyUnsafe inserts or updates a key without copying the key or value.
//
// Deprecated: use PutNoCopyKeyValueUnsafe.
func (c *CacheKV) PutNoCopyUnsafe(key, value []byte) error {
	return c.PutNoCopyKeyValueUnsafe(key, value)
}

// Delete removes a key via the write-back cache.
func (c *CacheKV) Delete(key []byte) error {
	keyCopy := append([]byte(nil), key...)
	k := bytesToString(keyCopy)

	c.walMu.Lock()
	if c.wal != nil {
		if err := c.wal.appendDelete(keyCopy); err != nil {
			c.walMu.Unlock()
			return err
		}
	}
	c.mu.Lock()
	c.pending[k] = cacheEntry{key: keyCopy, del: true}
	c.pendingLen += len(keyCopy)
	shouldFlush := len(c.pending) >= c.maxEntries || c.pendingLen >= c.maxBytes
	c.mu.Unlock()
	c.walMu.Unlock()
	if shouldFlush {
		return c.Flush()
	}
	return nil
}

// Flush writes pending changes to the backend.
// Flush flushes queued cache entries to the backend.
func (c *CacheKV) Flush() error {
	c.flushMu.Lock()
	defer c.flushMu.Unlock()

	c.mu.Lock()
	if len(c.pending) == 0 {
		c.mu.Unlock()
		return nil
	}
	entries := c.pending
	c.pending = make(map[string]cacheEntry)
	c.flushing = entries
	c.pendingLen = 0
	c.mu.Unlock()

	var backendErr error
	defer func() {
		c.mu.Lock()
		if backendErr != nil {
			// Restore flushing entries back into pending on error.
			if c.pending == nil {
				c.pending = make(map[string]cacheEntry)
			}
			for k, e := range c.flushing {
				c.pending[k] = e
				keyLen := len(e.key)
				if keyLen == 0 {
					keyLen = len(k)
				}
				if e.del {
					c.pendingLen += keyLen
				} else {
					c.pendingLen += keyLen + len(e.value)
				}
			}
		}
		c.flushing = nil
		c.mu.Unlock()
	}()

	if cap(c.batchKeys) < len(entries) {
		c.batchKeys = make([][]byte, 0, len(entries))
	} else {
		c.batchKeys = c.batchKeys[:0]
	}
	if cap(c.batchVals) < len(entries) {
		c.batchVals = make([][]byte, 0, len(entries))
	} else {
		c.batchVals = c.batchVals[:0]
	}
	batchKeys := c.batchKeys
	batchVals := c.batchVals
	for k, e := range entries {
		keyBytes := e.key
		if len(keyBytes) == 0 && len(k) > 0 {
			keyBytes = []byte(k)
		}
		if e.del {
			if err := c.backend.Delete(keyBytes); err != nil {
				backendErr = err
				return backendErr
			}
			continue
		}
		batchKeys = append(batchKeys, keyBytes)
		batchVals = append(batchVals, e.value)
	}

	if len(batchKeys) > 0 {
		if b, ok := c.backend.(interface {
			PutMany([][]byte, [][]byte) error
		}); ok {
			if err := b.PutMany(batchKeys, batchVals); err != nil {
				backendErr = err
				return backendErr
			}
		} else {
			for i := range batchKeys {
				if err := c.backend.Put(batchKeys[i], batchVals[i]); err != nil {
					backendErr = err
					return backendErr
				}
			}
		}
	}
	c.batchKeys = batchKeys
	c.batchVals = batchVals

	// WAL compaction: rewrite to contain only still-pending writes (those that arrived during this flush).
	if c.wal != nil {
		c.walMu.Lock()
		pendingSnapshot := make(map[string]cacheEntry)
		c.mu.RLock()
		for k, e := range c.pending {
			pendingSnapshot[k] = e
		}
		c.mu.RUnlock()
		walErr := c.wal.rewrite(pendingSnapshot)
		c.walMu.Unlock()
		if walErr != nil {
			return walErr
		}
	}

	return nil
}

// Close stops the flush loop and flushes remaining writes.
// Close stops background workers and closes the cache WAL if present.
func (c *CacheKV) Close() error {
	if c.flushInterval > 0 {
		close(c.stopCh)
		c.wg.Wait()
	}
	err := c.Flush()
	c.walMu.Lock()
	err = errors.Join(err, c.wal.Close())
	c.walMu.Unlock()
	return err
}

func (c *CacheKV) flushLoop() {
	defer c.wg.Done()
	t := time.NewTicker(c.flushInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			_ = c.Flush()
		case <-c.stopCh:
			return
		}
	}
}
